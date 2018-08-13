package drainer

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/node"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pump"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/tikv"
	"golang.org/x/net/context"
)

type notifyResult struct {
	err error
	wg  sync.WaitGroup
}

// Collector keeps all online pump infomation and publish window's lower boundary
type Collector struct {
	clusterID uint64
	batch     int32
	interval  time.Duration
	reg       *node.EtcdRegistry
	timeout   time.Duration
	tiStore   kv.Storage
	pumps     map[string]*Pump
	syncer    *Syncer
	latestTS  int64
	cp        checkpoint.CheckPoint

	syncedCheckTime int
	safeForwardTime int

	// notifyChan notifies the new pump is comming
	notifyChan chan *notifyResult
	// expose savepoints to HTTP.
	mu struct {
		sync.Mutex
		status *HTTPStatus
	}

	merger *Merger

	errCh chan error
}

// NewCollector returns an instance of Collector
func NewCollector(cfg *Config, clusterID uint64, w *DepositWindow, s *Syncer, cpt checkpoint.CheckPoint) (*Collector, error) {
	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	session.RegisterStore("tikv", tikv.Driver{})
	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	tiStore, err := session.NewStore(tiPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cli, err := etcd.NewClientFromCfg(urlv.StringSlice(), cfg.EtcdTimeout, node.DefaultRootPath, cfg.tls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c := &Collector{
		clusterID:       clusterID,
		interval:        time.Duration(cfg.DetectInterval) * time.Second,
		reg:             node.NewEtcdRegistry(cli, cfg.EtcdTimeout),
		timeout:         cfg.PumpTimeout,
		pumps:           make(map[string]*Pump),
		syncer:          s,
		cp:              cpt,
		tiStore:         tiStore,
		notifyChan:      make(chan *notifyResult),
		syncedCheckTime: cfg.SyncedCheckTime,
		safeForwardTime: cfg.SafeForwardTime,
		merger:          NewMerger(),
		errCh:           make(chan error),
	}

	return c, nil
}

func (c *Collector) publishBinlogs(ctx context.Context) {
	defer log.Info("publishBinlogs quit")

	for {
		select {
		case <-ctx.Done():
			return
		case mergeItem := <-c.merger.Output():
			item := mergeItem.(*binlogItem)
			binlog := item.binlog

			if binlog.CommitTs == binlog.StartTs {
				log.Debug("fake binlog ts: ", binlog.CommitTs)
				continue
			}

			if binlog.DdlJobId > 0 {
				for {
					var job *model.Job
					var err error
					err1 := util.RetryOnError(10, time.Second, fmt.Sprintf("get ddl job by id %d error", binlog.DdlJobId), func() error {
						job, err = getDDLJob(c.tiStore, binlog.DdlJobId)
						if err != nil {
							return err
						}
						return nil
					})

					if err1 != nil {
						log.Error("get DDL job by id %d error %v", binlog.DdlJobId, errors.Trace(err1))
						c.errCh <- err1
						return
					}

					if job == nil {
						time.Sleep(time.Second)
						continue
					}

					if job.State == model.JobStateCancelled {
						break
					} else {
						item.SetJob(job)
						c.syncer.Add(item)
						ddlJobsCounter.Add(float64(1))
						break
					}
				}
			} else {
				c.syncer.Add(item)
			}
		}
	}
}

// Start run a loop of collecting binlog from pumps online
func (c *Collector) Start(ctx context.Context) {
	defer func() {
		for _, p := range c.pumps {
			p.Close()
		}
		if err := c.reg.Close(); err != nil {
			log.Error(err.Error())
		}
	}()

	go c.publishBinlogs(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case nr := <-c.notifyChan:
			nr.err = c.updateStatus(ctx)
			nr.wg.Done()
		case <-time.After(c.interval):
			c.updateStatus(ctx)
		case err := <-c.errCh:
			log.Errorf("collector meets error %v", err)
			return
		}
	}
}

// updateCollectStatus updates the http status of the Collector.
func (c *Collector) updateCollectStatus(synced bool) {
	status := HTTPStatus{
		Synced:  synced,
		PumpPos: make(map[string]int64),
		LastTS:  c.merger.GetLastTS(),
	}

	for nodeID, pump := range c.pumps {
		pumpPositionGauge.WithLabelValues(nodeID).Set(float64(pump.latestTS))
	}

	c.mu.Lock()
	c.mu.status = &status
	c.mu.Unlock()
}

// updateStatus queries pumps' status, pause pull binlog for paused pump,
// continue pull binlog for online pump, and deletes offline pump.
func (c *Collector) updateStatus(ctx context.Context) error {
	begin := time.Now()
	defer func() {
		publishBinlogHistogram.WithLabelValues("drainer").Observe(time.Since(begin).Seconds())
	}()

	if err := c.updatePumpStatus(ctx); err != nil {
		log.Errorf("DetectPumps error: %v", errors.ErrorStack(err))
		return errors.Trace(err)
	}

	return nil
}

func (c *Collector) updatePumpStatus(ctx context.Context) error {
	nodes, err := c.reg.Nodes(ctx, "pumps")
	if err != nil {
		return errors.Trace(err)
	}

	// query lastest ts from pd
	c.latestTS = c.queryLatestTsFromPD()

	for _, n := range nodes {
		// format and check the nodeID
		n.NodeID, err = pump.FormatNodeID(n.NodeID)
		if err != nil {
			return errors.Trace(err)
		}

		p, ok := c.pumps[n.NodeID]
		if !ok {
			// if pump is offline, ignore it
			if n.State == node.Offline {
				continue
			}

			mergeSource := MergeSource{ID: n.NodeID}

			// initial pump
			commitTS := c.cp.Pos()
			p, err := NewPump(n.NodeID, n.Addr, c.clusterID, c.timeout, commitTS)
			if err != nil {
				// we should still add source for this pump
				c.merger.AddSource(mergeSource)
				return errors.Trace(err)
			}
			c.pumps[n.NodeID] = p
			mergeSource.Source = p.PullBinlog(ctx, p.latestTS)
			c.merger.AddSource(mergeSource)
		} else {
			switch n.State {
			case node.Pausing:
				// do nothing
			case node.Paused:
				p.Pause()
			case node.Online:
				p.Continue()
			case node.Closing:
				// pump is closing, and need wait all the binlog is send to drainer, so do nothing here.
			case node.Offline:
				// before pump change status to offline, it needs to check all the binlog save in this pump had already been consumed in drainer.
				// so when the pump is offline, we can remove this pump directly.
				c.merger.RemoveSource(n.NodeID)
				// release invalid connection
				p.Close()
				delete(c.pumps, n.NodeID)
				log.Infof("node(%s) of cluster(%d) has been removed and release the connection to it",
					p.nodeID, p.clusterID)
			}
		}
	}
	return nil
}

func (c *Collector) queryLatestTsFromPD() int64 {
	version, err := c.tiStore.CurrentVersion()
	if err != nil {
		log.Errorf("get current version error: %v", err)
		return 0
	}

	return int64(version.Ver)
}

// Notify notifies to detcet pumps
func (c *Collector) Notify() error {
	nr := &notifyResult{}
	nr.wg.Add(1)
	c.notifyChan <- nr
	nr.wg.Wait()
	return nr.err
}

// Status exposes collector's status to HTTP handler.
func (c *Collector) Status(w http.ResponseWriter, r *http.Request) {
	c.HTTPStatus().Status(w, r)
}

// HTTPStatus returns a snapshot of current http status.
func (c *Collector) HTTPStatus() *HTTPStatus {
	var status *HTTPStatus
	c.mu.Lock()
	status = c.mu.status

	// if the merger don't have any binlog to be merged and syncer don't have binlog input in a minitue,
	// we can think the all binlog is synced
	if c.merger.IsEmpty() && time.Since(c.syncer.GetLastSyncTime()) > time.Duration(c.syncedCheckTime)*time.Minute {
		status.Synced = true
	}

	c.mu.Unlock()
	return status
}
