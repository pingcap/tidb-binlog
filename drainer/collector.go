package drainer

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/tidb-binlog/pkg/offsets"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pump"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
)

type notifyResult struct {
	err error
	wg  sync.WaitGroup
}

// Collector keeps all online pump infomation and publish window's lower boundary
type Collector struct {
	clusterID  uint64
	batch      int32
	kafkaAddrs []string
	interval   time.Duration
	reg        *pump.EtcdRegistry
	timeout    time.Duration
	window     *DepositWindow
	tiClient   *tikv.LockResolver
	tiStore    kv.Storage
	pumps      map[string]*Pump
	bh         *binlogHeap
	syncer     *Syncer
	latestTS   int64
	cp         checkpoint.CheckPoint

	offsetSeeker    offsets.Seeker
	initialCommitTs int64

	// notifyChan notifies the new pump is comming
	notifyChan chan *notifyResult
	// expose savepoints to HTTP.
	mu struct {
		sync.Mutex
		status *HTTPStatus
	}
}

// NewCollector returns an instance of Collector
func NewCollector(cfg *Config, clusterID uint64, w *DepositWindow, s *Syncer, cpt checkpoint.CheckPoint) (*Collector, error) {
	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	kafkaAddrs, err := flags.ParseHostPortAddr(cfg.KafkaAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	offsetSeeker, err := createOffsetSeeker(kafkaAddrs)
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
		clusterID:       clusterID,
		interval:        time.Duration(cfg.DetectInterval) * time.Second,
		kafkaAddrs:      kafkaAddrs,
		reg:             pump.NewEtcdRegistry(cli, cfg.EtcdTimeout),
		timeout:         cfg.PumpTimeout,
		pumps:           make(map[string]*Pump),
		bh:              newBinlogHeap(maxHeapSize),
		window:          w,
		syncer:          s,
		cp:              cpt,
		tiClient:        tiClient,
		tiStore:         tiStore,
		notifyChan:      make(chan *notifyResult),
		offsetSeeker:    offsetSeeker,
		initialCommitTs: cfg.InitialCommitTS,
	}, nil
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
		PumpPos: make(map[string]binlog.Pos),
	}

	for nodeID, pump := range c.pumps {
		status.PumpPos[nodeID] = pump.current
		savepointGauge.WithLabelValues(nodeID).Set(posToFloat(&pump.current))
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
	if err := c.updatePumpStatus(ctx); err != nil {
		log.Errorf("DetectPumps error: %v", errors.ErrorStack(err))
		c.updateCollectStatus(false)
		return errors.Trace(err)
	}

	windowUpper := c.latestTS
	windowLower := c.getLatestValidCommitTS()
	c.publish(ctx, windowUpper, windowLower)
	c.updateCollectStatus(windowLower == windowUpper)
	return nil
}

func (c *Collector) updatePumpStatus(ctx context.Context) error {
	nodes, err := c.reg.Nodes(ctx, "pumps")
	if err != nil {
		return errors.Trace(err)
	}
	exists := make(map[string]bool)
	for _, n := range nodes {
		_, ok := c.pumps[n.NodeID]
		if !ok {
			pos, err := c.getSavePoints(n.NodeID)
			if err != nil {
				return errors.Trace(err)
			}

			log.Infof("node %s get save point %v", n.NodeID, pos)
			p, err := NewPump(n.NodeID, c.clusterID, c.kafkaAddrs, c.timeout, c.window, c.tiStore, pos)
			if err != nil {
				return errors.Trace(err)
			}
			c.pumps[n.NodeID] = p
			p.StartCollect(ctx, c.tiClient)
		}
		exists[n.NodeID] = true
	}

	// query lastest ts from pd
	c.latestTS = c.queryLatestTsFromPD()
	for id, p := range c.pumps {
		if !exists[id] {
			latestPos, err := c.reg.GetOfflineSign(ctx, "pumps", id)
			if err != nil {
				log.Errorf("query offline pump error %v", err)
				continue
			}
			if !p.hadFinished(latestPos) {
				log.Errorf("pump %s has messages that is not consumed", id)
				continue
			}

			// release invalid connection
			p.Close()
			delete(c.pumps, id)
			log.Infof("node(%s) of cluster(%d)  has been removed and release the connection to it",
				id, p.clusterID)
			continue
		}
		// update pumps' latestTS
		p.UpdateLatestTS(c.latestTS)
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

func (c *Collector) publish(ctx context.Context, upper, lower int64) {
	oldLower := c.window.LoadLower()
	oldUpper := c.window.LoadUpper()

	if lower > oldLower {
		c.window.SaveLower(lower)
		c.publishBinlogs(ctx, oldLower, lower)
		windowGauge.WithLabelValues("lower").Set(float64(lower))
	}
	if upper > oldUpper {
		c.window.SaveUpper(upper)
		windowGauge.WithLabelValues("upper").Set(float64(upper))
	}
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

// publishBinlogs collects binlogs whose commitTS are in (minTS, maxTS], then publish them in ascending commitTS order
func (c *Collector) publishBinlogs(ctx context.Context, minTS, maxTS int64) {
	// multiple ways sort:
	// 1. get multiple way sorted binlogs
	// 2. use heap to merge sort
	// todo: use multiple goroutines to collect sorted binlogs
	bss := make(map[string]binlogItems)
	binlogOffsets := make(map[string]int)
	for id, p := range c.pumps {
		bs := p.collectBinlogs(minTS, maxTS)
		if bs.Len() > 0 {
			bss[id] = bs
			binlogOffsets[id] = 1
			// first push the first item into heap every pump
			c.bh.push(ctx, bs[0])
		}
	}

	item := c.bh.pop()
	for item != nil {
		c.syncer.Add(item)
		// if binlogOffsets[item.nodeID] == len(bss[item.nodeID]), all binlogs must be pushed into heap, delete it from bss
		if binlogOffsets[item.nodeID] == len(bss[item.nodeID]) {
			delete(bss, item.nodeID)
		} else {
			// push next item into heap and increase the offset
			c.bh.push(ctx, bss[item.nodeID][binlogOffsets[item.nodeID]])
			binlogOffsets[item.nodeID] = binlogOffsets[item.nodeID] + 1
		}
		item = c.bh.pop()
	}
}

func (c *Collector) getSavePoints(nodeID string) (binlog.Pos, error) {
	_, poss := c.cp.Pos()
	pos, ok := poss[nodeID]
	if ok {
		return pos, nil
	}

	if c.initialCommitTs > 0 {
		topic := pump.TopicName(strconv.FormatUint(c.clusterID, 10), nodeID)
		offsets, err := c.offsetSeeker.Do(topic, c.initialCommitTs, 0, 0, []int32{pump.DefaultTopicPartition()})
		if err == nil {
			return binlog.Pos{Offset: offsets[0]}, nil
		}
	}

	return binlog.Pos{}, nil
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
	c.mu.Unlock()
	return status
}
