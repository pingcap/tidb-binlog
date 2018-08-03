package drainer

import (
	"fmt"
	"math"
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
	"github.com/pingcap/tidb/meta"
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
	clusterID    uint64
	batch        int32
	interval     time.Duration
	reg          *node.EtcdRegistry
	timeout      time.Duration
	window       *DepositWindow
	tiClient     *tikv.LockResolver
	tiStore      kv.Storage
	pumps        map[string]*Pump
	offlines     map[string]struct{}
	syncer       *Syncer
	latestTS     int64
	cp           checkpoint.CheckPoint

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
}

// NewCollector returns an instance of Collector
func NewCollector(cfg *Config, clusterID uint64, w *DepositWindow, s *Syncer, cpt checkpoint.CheckPoint) (*Collector, error) {
	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tiClient, err := tikv.NewLockResolver(urlv.StringSlice(), cfg.Security.ToTiDBSecurityConfig())
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
		offlines:        make(map[string]struct{}),
		window:          w,
		syncer:          s,
		cp:              cpt,
		tiClient:        tiClient,
		tiStore:         tiStore,
		notifyChan:      make(chan *notifyResult),
		syncedCheckTime: cfg.SyncedCheckTime,
		safeForwardTime: cfg.SafeForwardTime,
		merger:          NewMerger(),
	}

	go c.startAddToSyncer()

	return c, nil
}

func (c *Collector) startAddToSyncer() {
	for mergeItem := range c.merger.Output() {
		item := mergeItem.(*binlogItem)
		binlog := item.binlog

		if binlog.CommitTs == binlog.StartTs {
			log.Debug("fake binlog ts: ", binlog.CommitTs)
			continue
		}

		// TODO when will fail, handle it in better way
		if binlog.DdlJobId > 0 {
			for {
				job, err := getDDLJob(c.tiStore, binlog.DdlJobId)
				if err != nil {
					log.Error(err)
					time.Sleep(time.Second)
					continue
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

	log.Debug("startAddToSyncer quit")
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
		if err := c.tiStore.Close(); err != nil {
			log.Error(err.Error())
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case nr := <-c.notifyChan:
			nr.err = c.updateStatus(ctx)
			nr.wg.Done()
		case <-time.After(c.interval):
			c.updateStatus(ctx)
		}
	}
}

// updateCollectStatus updates the http status of the Collector.
func (c *Collector) updateCollectStatus(synced bool) {
	status := HTTPStatus{
		Synced:  synced,
		PumpPos: make(map[string]int64),
	}

	for nodeID, pump := range c.pumps {
		status.PumpPos[nodeID] = pump.currentPos
		offsetGauge.WithLabelValues(nodeID).Set(float64(pump.currentPos))
	}
	status.DepositWindow.Lower = c.window.LoadLower()
	status.DepositWindow.Upper = c.window.LoadUpper()

	c.mu.Lock()
	c.mu.status = &status
	c.mu.Unlock()
}

// updateStatus queries pumps' status , deletes the offline pump
// and updates pumps' latest ts
func (c *Collector) updateStatus(ctx context.Context) error {
	begin := time.Now()
	defer func() {
		publishBinlogHistogram.WithLabelValues("drainer").Observe(time.Since(begin).Seconds())
	}()

	if err := c.updatePumpStatus(ctx); err != nil {
		log.Errorf("DetectPumps error: %v", errors.ErrorStack(err))
		c.updateCollectStatus(false)
		return errors.Trace(err)
	}

	windowUpper := c.latestTS
	windowLower := c.getLatestValidCommitTS()
	//c.publish(ctx, windowUpper, windowLower)
	c.updateCollectStatus(windowLower == windowUpper)
	return nil
}

func (c *Collector) updatePumpStatus(ctx context.Context) error {
	nodes, err := c.reg.Nodes(ctx, "pumps")
	if err != nil {
		return errors.Trace(err)
	}

	// get current binlog's commit ts which in process
	//currentCommitTS := c.cp.Pos()
	//safeTS := getSafeTS(currentCommitTS, int64(c.safeForwardTime))
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
			if n.State == node.Offline || n.State == node.Paused {
			//if n.State == node.Offline {
				/*
				if n.UpdateTS <= safeTS {
					continue
				}

				if _, exist := c.offlines[n.NodeID]; exist {
					continue
				}*/
				continue
			}

			// initial pump
			safeTS := c.getSavePoints(ctx, n.NodeID)

			log.Infof("node %s get save point %v", n.NodeID, safeTS)
			p, err := NewPump(n.NodeID, c.clusterID, c.timeout, c.window, c.tiStore, safeTS)
			//p.addr = n.Host
 			//log.Debug("get pump addr: ", n.Host)
			if err != nil {
				return errors.Trace(err)
			}
			c.pumps[n.NodeID] = p
			delete(c.offlines, n.NodeID)
			c.merger.AddSource(MergeSource{
				ID:     p.nodeID,
				Source: p.PullBinlog(ctx, p.latestPos),
			})
		} else {
			// update pumps' latestTS
			p.UpdateLatestTS(c.latestTS)
			if n.State == node.Offline {
				//if !p.hadFinished(c.window.LoadLower()) {
				//	log.Errorf("pump %s has messages that is not consumed", p.nodeID)
				//	continue
				//}

				// release invalid connection
				p.Close()
				delete(c.pumps, n.NodeID)
				c.offlines[n.NodeID] = struct{}{}
				log.Infof("node(%s) of cluster(%d)  has been removed and release the connection to it",
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

// select min of all pumps' latestValidCommitTS
func (c *Collector) getLatestValidCommitTS() int64 {
	var latest int64 = math.MaxInt64
	for _, p := range c.pumps {
		latestCommitTS := p.GetLatestValidCommitTS()
		if latestCommitTS < latest {
			latest = latestCommitTS
		}
	}
	if latest == math.MaxInt64 {
		latest = 0
	}

	return latest
}

// LoadHistoryDDLJobs loads all history DDL jobs from TiDB
func (c *Collector) LoadHistoryDDLJobs() ([]*model.Job, error) {
	version, err := c.tiStore.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshot, err := c.tiStore.GetSnapshot(version)
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapMeta := meta.NewSnapshotMeta(snapshot)
	jobs, err := snapMeta.GetAllHistoryDDLJobs()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return jobs, nil
}

func (c *Collector) getSavePoints(ctx context.Context, nodeID string) int64 {
	commitTS := c.cp.Pos()

	//topic := pump.TopicName(strconv.FormatUint(c.clusterID, 10), nodeID)
	safeCommitTS := getSafeTS(commitTS, int64(c.safeForwardTime))
	log.Infof("commit ts %d's safe commit ts is %d", commitTS, safeCommitTS)

	return safeCommitTS
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

	interval := time.Duration(util.TsToTimestamp(status.DepositWindow.Upper) - util.TsToTimestamp(status.DepositWindow.Lower))
	// if the gap between lower and upper is small and don't have binlog input in a minitue,
	// we can think the all binlog is synced
	if interval < time.Duration(2)*c.interval && time.Since(c.syncer.GetLastSyncTime()) > time.Duration(c.syncedCheckTime)*time.Minute {
		status.Synced = true
	}

	c.mu.Unlock()
	return status
}
