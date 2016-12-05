package cistern

import (
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb-binlog/pump"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
)

// Collector keeps all connections to pumps, and pulls binlog from each pump server periodically.
// If find any pump server gone away collector should halt processing until recovery.
// Each Prewrite binlog in a batch must be paired up with a Commit or Abort binlog that have same startTS.
// If there are some ones who don't have a girlfriend:), it should request for the next batch after a while,
// and finally abort the txn in TiKV for that single ones.
// After a batch processing is complete, collector will update the savepoint of each pump binlog stored in Etcd.
type Collector struct {
	clusterID uint64
	batch     int32
	interval  time.Duration
	reg       *pump.EtcdRegistry
	timeout   time.Duration
	window    *DepositWindow
	boltdb    store.Store
	tiClient  *tikv.LockResolver
	tiStore   kv.Storage

	muPump struct {
		sync.Mutex
		pumps map[string]*Pump
	}

	// expose savepoints to HTTP.
	mu struct {
		sync.Mutex
		status *HTTPStatus
	}
}

// NewCollector returns an instance of Collector
func NewCollector(cfg *Config, clusterID uint64, s store.Store, w *DepositWindow) (*Collector, error) {
	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tiClient, err := tikv.NewLockResolver(urlv.StringSlice())
	if err != nil {
		return nil, errors.Trace(err)
	}
	tidb.RegisterStore("tikv", tikv.Driver{})
	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	tiStore, err := tidb.NewStore(tiPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cli, err := etcd.NewClientFromCfg(urlv.StringSlice(), cfg.EtcdTimeout, etcd.DefaultRootPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Collector{
		clusterID: clusterID,
		interval:  time.Duration(cfg.CollectInterval) * time.Second,
		reg:       pump.NewEtcdRegistry(cli, cfg.EtcdTimeout),
		timeout:   cfg.PumpTimeout,
		window:    w,
		boltdb:    s,
		tiClient:  tiClient,
		tiStore:   tiStore,
	}, nil
}

// Start run a loop of collecting binlog from pumps online
func (c *Collector) Start(ctx context.Context) {
	c.muPump.pumps = make(map[string]*Pump)
	defer func() {
		c.muPump.Lock()
		for _, p := range c.muPump.pumps {
			p.Close()
		}
		c.muPump.Unlock()
		if err := c.reg.Close(); err != nil {
			log.Error(err.Error())
		}
		if err := c.tiStore.Close(); err != nil {
			log.Error(err.Error())
		}
		log.Info("Collector goroutine exited")
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.interval):
			synced, err := c.detectPumps(ctx)
			if err != nil {
				log.Errorf("DetectPumps error: %v", errors.ErrorStack(err))
				synced = false
			}
			c.updateStatus(synced)
		}
	}
}

// updateStatus updates the http status of the Collector.
func (c *Collector) updateStatus(synced bool) {
	status := HTTPStatus{
		Synced:  synced,
		PumpPos: make(map[string]binlog.Pos),
	}

	c.muPump.Lock()
	for nodeID, pump := range c.muPump.pumps {
		status.PumpPos[nodeID] = pump.current
	}
	c.muPump.Lock()
	status.DepositWindow.Lower = c.window.LoadLower()
	status.DepositWindow.Upper = c.window.LoadUpper()

	c.mu.Lock()
	c.mu.status = &status
	c.mu.Unlock()
}

// collect pulls binlog from pumps, return whether cistern is synced with
// pump after this round of collect.
func (c *Collector) detectPumps(ctx context.Context) (synced bool, err error) {
	if err1 := c.prepare(ctx); err1 != nil {
		err = errors.Trace(err1)
		return
	}

	windowUpper := c.getLatestCommitTS()
	windowLower := c.getLaetsValidCommitTS()
	if windowLower == windowUpper {
		synced = true
	}

	c.window.SaveUpper(windowUpper)
	windowGauge.WithLabelValues("upper").Set(float64(windowUpper))
	c.publish(windowLower)
	return
}

func (c *Collector) prepare(ctx context.Context) error {
	nodes, err := c.reg.Nodes(ctx, "pumps")
	if err != nil {
		return errors.Trace(err)
	}
	exists := make(map[string]bool)
	c.muPump.Lock()
	defer c.muPump.Unlock()
	for _, n := range nodes {
		_, ok := c.muPump.pumps[n.NodeID]
		if !ok {
			// this isn't the best way to init pump, we will fix it in the new way
			pos, err := c.getSavePoints(n.NodeID)
			if err != nil {
				return errors.Trace(err)
			}

			log.Infof("node %s get save point %v", n.NodeID, pos)
			p, err := NewPump(n.NodeID, c.clusterID, n.Host, c.timeout, c.window, pos, c.boltdb, c.tiStore)
			if err != nil {
				return errors.Trace(err)
			}
			c.muPump.pumps[n.NodeID] = p
			p.StartCollect(ctx, c.tiClient)
		}
		exists[n.NodeID] = true
	}
	for id, p := range c.muPump.pumps {
		if !exists[id] {
			// release invalid connection
			p.Close()
			delete(c.muPump.pumps, id)
			log.Infof("node(%s) of cluster(%d)  has been removed and release the connection to it",
				id, p.clusterID)
		}
	}

	return nil
}

func (c *Collector) publish(end int64) error {
	start := c.window.LoadLower()
	if end > start {
		if err := c.window.PersistLower(end); err != nil {
			return errors.Trace(err)
		}

		windowGauge.WithLabelValues("lower").Set(float64(end))
	}
	return nil
}

func (c *Collector) getLatestCommitTS() int64 {
	var latest int64
	for _, p := range c.muPump.pumps {
		latestCommitTS := p.GetLatestCommitTS()
		if latestCommitTS > latest {
			latest = latestCommitTS
		}
	}

	return latest
}

func (c *Collector) getLaetsValidCommitTS() int64 {
	var latest int64 = math.MaxInt64
	c.muPump.Lock()
	for _, p := range c.muPump.pumps {
		latestCommitTS := p.GetLatestValidCommitTS()
		if latestCommitTS < latest {
			latest = latestCommitTS
		}
	}
	c.muPump.Unlock()

	return latest
}

func (c *Collector) getSavePoints(nodeID string) (binlog.Pos, error) {
	var savePoint = binlog.Pos{}
	payload, err := c.boltdb.Get(savepointNamespace, []byte(nodeID))
	if err != nil {
		if errors.IsNotFound(err) {
			return savePoint, nil
		}

		return savePoint, errors.Trace(err)
	}
	if err := savePoint.Unmarshal(payload); err != nil {
		return savePoint, errors.Trace(err)
	}
	return savePoint, nil
}

// LoadHistoryDDLJobs loads all history DDL jobs from TiDB
func (c *Collector) LoadHistoryDDLJobs() error {
	version, err := c.tiStore.CurrentVersion()
	if err != nil {
		return errors.Trace(err)
	}
	snapshot, err := c.tiStore.GetSnapshot(version)
	if err != nil {
		return errors.Trace(err)
	}
	snapMeta := meta.NewSnapshotMeta(snapshot)
	jobs, err := snapMeta.GetAllHistoryDDLJobs()
	if err != nil {
		return errors.Trace(err)
	}
	for _, job := range jobs {
		if job.State == model.JobCancelled {
			continue
		}
		key := codec.EncodeInt([]byte{}, job.ID)
		_, err = c.boltdb.Get(ddlJobNamespace, key)
		if err != nil {
			if !errors.IsNotFound(err) {
				return errors.Trace(err)
			}
			if err := decodeJob(job); err != nil {
				return errors.Trace(err)
			}
			payload, err := job.Encode()
			if err != nil {
				return errors.Trace(err)
			}
			if err := c.boltdb.Put(ddlJobNamespace, key, payload); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
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
	c.mu.Unlock()
	return status
}
