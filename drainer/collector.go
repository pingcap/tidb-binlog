// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package drainer

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
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
	"go.uber.org/zap"
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

	// notifyChan notifies the new pump is coming
	notifyChan chan *notifyResult
	// expose savepoints to HTTP.
	mu struct {
		sync.Mutex
		status *HTTPStatus
	}

	merger *Merger

	errCh chan error
}

var (
	getDDLJobRetryWait = time.Second

	// Make it possible to mock the following functions in tests
	newStore      = session.NewStore
	newClient     = etcd.NewClientFromCfg
	fDDLJobGetter = getDDLJob
)

// NewCollector returns an instance of Collector
func NewCollector(cfg *Config, clusterID uint64, s *Syncer, cpt checkpoint.CheckPoint) (*Collector, error) {
	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := session.RegisterStore("tikv", tikv.Driver{}); err != nil {
		if !strings.Contains(err.Error(), "already registered") {
			return nil, errors.Trace(err)
		}
	}
	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	tiStore, err := newStore(tiPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cli, err := newClient(urlv.StringSlice(), cfg.EtcdTimeout, node.DefaultRootPath, cfg.tls)
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
	defer log.Info("publishBinlogs quit")

	for {
		select {
		case <-ctx.Done():
			return
		case mergeItem := <-c.merger.Output():
			item := mergeItem.(*binlogItem)
			if err := c.syncBinlog(item); err != nil {
				c.reportErr(ctx, err)
				return
			}
		}
	}
}

// Start run a loop of collecting binlog from pumps online
func (c *Collector) Start(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		c.publishBinlogs(ctx)
		wg.Done()
	}()

	c.keepUpdatingStatus(ctx, c.updateStatus)

	for _, p := range c.pumps {
		p.Close()
	}
	if err := c.reg.Close(); err != nil {
		log.Error(err.Error())
	}

	wg.Wait()
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
		log.Error("updatePumpStatus failed", zap.Error(err))
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
		c.handlePumpStatusUpdate(ctx, n)
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

	// If the syncer has no binlog input for more than `c.syncedCheckTime` minitue,
	// we may consider all binlogs synced
	if time.Since(c.syncer.GetLastSyncTime()) > time.Duration(c.syncedCheckTime)*time.Minute {
		status.Synced = true
	}
	status.LastTS = c.syncer.GetLatestCommitTS()

	return status
}

func (c *Collector) reportErr(ctx context.Context, err error) {
	log.Error("reportErr receive error", zap.Error(err))
	select {
	case <-ctx.Done():
	case c.errCh <- err:
	}
}

func (c *Collector) syncBinlog(item *binlogItem) error {
	binlog := item.binlog
	if binlog.DdlJobId > 0 {
		msgPrefix := fmt.Sprintf("get ddl job by id %d error", binlog.DdlJobId)
		var job *model.Job
		for {
			err := util.RetryOnError(getDDLJobRetryTime, getDDLJobRetryWait, msgPrefix, func() error {
				var err1 error
				job, err1 = fDDLJobGetter(c.tiStore, binlog.DdlJobId)
				return err1
			})

			if err != nil {
				log.Error("get DDL job failed", zap.Int64("id", binlog.DdlJobId), zap.Error(err))
				return errors.Trace(err)
			}

			if job != nil {
				break
			}

			time.Sleep(time.Second)
		}

		log.Info("get ddl job", zap.Stringer("job", job))

		if skipJob(job) {
			return nil
		}
		item.SetJob(job)
		ddlJobsCounter.Add(float64(1))
	}
	c.syncer.Add(item)
	return nil
}

func (c *Collector) handlePumpStatusUpdate(ctx context.Context, n *node.Status) {
	n.NodeID = pump.FormatNodeID(n.NodeID)

	p, ok := c.pumps[n.NodeID]
	if !ok {
		// if pump is offline, ignore it
		if n.State == node.Offline {
			return
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
			c.merger.RemoveSource(n.NodeID)
			c.pumps[n.NodeID].Close()
			delete(c.pumps, n.NodeID)
			log.Info("node of cluster has been removed and release the connection to it",
				zap.String("nodeID", p.nodeID), zap.Uint64("clusterID", p.clusterID))
		}
	}
}

func (c *Collector) keepUpdatingStatus(ctx context.Context, fUpdate func(context.Context) error) {
	// add all the pump to merger
	c.merger.Stop()
	fUpdate(ctx)
	c.merger.Continue()

	// update status when had pump notify or reach wait time
	for {
		select {
		case <-ctx.Done():
			return
		case nr := <-c.notifyChan:
			nr.err = fUpdate(ctx)
			nr.wg.Done()
		case <-time.After(c.interval):
			if err := fUpdate(ctx); err != nil {
				log.Error("Failed to update collector status", zap.Error(err))
			}
		case err := <-c.errCh:
			log.Error("collector meets error", zap.Error(err))
			return
		}
	}
}
