package drainer

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/node"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pump"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"golang.org/x/net/context"
)

const (
	getDDLJobRetryTime = 10
)

type notifyResult struct {
	err error
	wg  sync.WaitGroup
}

// Collector collects binlog from all pump, and send binlog to syncer.
type Collector struct {
	clusterID uint64
	interval  time.Duration
	reg       *node.EtcdRegistry
	tiStore   kv.Storage
	pumps     map[string]*Pump
	syncer    *Syncer
	latestTS  int64
	cp        checkpoint.CheckPoint

	syncedCheckTime int

	// notifyChan notifies the new pump is comming
	notifyChan chan *notifyResult
	// expose savepoints to HTTP.
	mu struct {
		sync.Mutex
		status *HTTPStatus
	}

	merger *Merger

	errCh chan error
	wg    sync.WaitGroup
}

// NewCollector returns an instance of Collector
func NewCollector(cfg *Config, clusterID uint64, s *Syncer, cpt checkpoint.CheckPoint) (*Collector, error) {
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
		pumps:           make(map[string]*Pump),
		syncer:          s,
		cp:              cpt,
		tiStore:         tiStore,
		notifyChan:      make(chan *notifyResult),
		syncedCheckTime: cfg.SyncedCheckTime,
		merger:          NewMerger(cpt.TS(), heapStrategy),
		errCh:           make(chan error, 10),
	}

	return c, nil
}

func (c *Collector) publishBinlogs(ctx context.Context) {
	defer func() {
		c.wg.Done()
		log.Info("publishBinlogs quit")
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case mergeItem := <-c.merger.Output():
			item := mergeItem.(*binlogItem)
			binlog := item.binlog

			if binlog.DdlJobId > 0 {
				for {
					var job *model.Job
					err := util.RetryOnError(getDDLJobRetryTime, time.Second, fmt.Sprintf("get ddl job by id %d error", binlog.DdlJobId), func() error {
						var err1 error
						job, err1 = getDDLJob(c.tiStore, binlog.DdlJobId)
						return err1
					})

					if err != nil {
						log.Errorf("get DDL job by id %d error %v", binlog.DdlJobId, errors.Trace(err))
						c.reportErr(ctx, err)
						return
					}

					if job == nil {
						time.Sleep(time.Second)
						continue
					}

					// according to DDL
					// only when reach this two state will write binlog:
					if job.State != model.JobStateSynced &&
						job.State != model.JobStateRollbackDone {
						log.Warnf("unexpected job, job id %d state: %v", job.ID, job.State)
					}

					if !skipJob(job) {
						item.SetJob(job)
						c.syncer.Add(item)
						ddlJobsCounter.Add(float64(1))
					}
					break
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

		c.wg.Wait()
	}()

	c.wg.Add(1)
	go c.publishBinlogs(ctx)

	// add all the pump to merger
	c.merger.Stop()
	c.updateStatus(ctx)
	c.merger.Continue()

	// update status when had pump notify or reach wait time
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
		LastTS:  c.merger.GetLatestTS(),
	}

	for nodeID, pump := range c.pumps {
		status.PumpPos[nodeID] = pump.latestTS
		pumpPositionGauge.WithLabelValues(nodeID).Set(float64(oracle.ExtractPhysical(uint64(pump.latestTS))))
	}

	c.mu.Lock()
	c.mu.status = &status
	c.mu.Unlock()
}

// updateStatus queries pumps' status, pause pull binlog for paused pump,
// continue pull binlog for online pump, and deletes offline pump.
func (c *Collector) updateStatus(ctx context.Context) error {
	if err := c.updatePumpStatus(ctx); err != nil {
		log.Errorf("DetectPumps error: %v", errors.ErrorStack(err))
		return errors.Trace(err)
	}

	c.updateCollectStatus(false)

	return nil
}

func (c *Collector) updatePumpStatus(ctx context.Context) error {
	nodes, err := c.reg.Nodes(ctx, "pumps")
	if err != nil {
		return errors.Trace(err)
	}

	// query lastest ts from pd
	c.latestTS, err = util.QueryLatestTsFromPD(c.tiStore)
	if err != nil {
		return errors.Trace(err)
	}

	for _, n := range nodes {
		// format and check the nodeID
		n.NodeID = pump.FormatNodeID(n.NodeID)

		p, ok := c.pumps[n.NodeID]
		if !ok {
			// if pump is offline, ignore it
			if n.State == node.Offline {
				continue
			}

			commitTS := c.merger.GetLatestTS()
			p := NewPump(n.NodeID, n.Addr, c.clusterID, commitTS, c.errCh)
			c.pumps[n.NodeID] = p
			c.merger.AddSource(MergeSource{
				ID:     n.NodeID,
				Source: p.PullBinlog(ctx, commitTS),
			})

		} else {
			switch n.State {
			case node.Pausing:
				// do nothing
			case node.Paused:
				p.Pause()
			case node.Online:
				p.Continue(ctx)
			case node.Closing:
				// pump is closing, and need wait all the binlog is send to drainer, so do nothing here.
			case node.Offline:
				// before pump change status to offline, it needs to check all the binlog save in this pump had already been consumed in drainer.
				// so when the pump is offline, we can remove this pump directly.
				c.pumps[n.NodeID].Close()
				c.merger.RemoveSource(n.NodeID)
				delete(c.pumps, n.NodeID)
				log.Infof("node(%s) of cluster(%d) has been removed and release the connection to it",
					p.nodeID, p.clusterID)
			}
		}
	}
	return nil
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
	defer c.mu.Unlock()
	status = c.mu.status

	if status == nil {
		return &HTTPStatus{
			Synced: false,
		}

	}

	// if syncer don't have binlog input in a minitue,
	// we can think all the binlog is synced
	if time.Since(c.syncer.GetLastSyncTime()) > time.Duration(c.syncedCheckTime)*time.Minute {
		status.Synced = true
	}
	status.LastTS = c.syncer.GetLatestCommitTS()

	return status
}

func (c *Collector) reportErr(ctx context.Context, err error) {
	log.Errorf("reportErr receive error %s", err)
	select {
	case <-ctx.Done():
		return
	case c.errCh <- err:
		return
	}
}
