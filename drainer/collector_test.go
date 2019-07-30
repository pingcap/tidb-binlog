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
	"context"
	"crypto/tls"
	"errors"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/node"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pb "github.com/pingcap/tipb/go-binlog"
)

type dummyCheckpoint struct {
	checkpoint.CheckPoint
	commitTS int64
}

func (cp dummyCheckpoint) TS() int64 {
	return cp.commitTS
}

func (cp dummyCheckpoint) Close() error {
	return nil
}

type dummyStore struct {
	kv.Storage
}

type collectorSuite struct{}

type dummyStorage struct {
	kv.Storage
}

func (ds dummyStorage) Begin() (kv.Transaction, error) {
	return nil, nil
}

func (ds dummyStorage) BeginWithStartTS(startTS uint64) (kv.Transaction, error) {
	return nil, nil
}

func (ds dummyStorage) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
	return nil, nil
}

func (ds dummyStorage) GetClient() kv.Client {
	return nil
}

func (ds dummyStorage) Close() error {
	return nil
}

func (ds dummyStorage) UUID() string {
	return ""
}

func (ds dummyStorage) CurrentVersion() (kv.Version, error) {
	return kv.NewVersion(2), nil
}

func (ds dummyStorage) GetOracle() oracle.Oracle {
	return nil
}

func (ds dummyStorage) SupportDeleteRange() (supported bool) {
	return false
}

func (ds dummyStorage) Name() string {
	return ""
}

func (ds dummyStorage) Describe() string {
	return ""
}

func (ds dummyStorage) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	return nil, nil
}

var DefaultRootPath = "/tidb-binlog/v1"
var _ = Suite(&collectorSuite{})

func (s *collectorSuite) TestUpdateCollectStatus(c *C) {
	merger := Merger{latestTS: 2019}
	pumps := map[string]*Pump{
		"node1": {nodeID: "node1", latestTS: 1001},
		"node2": {nodeID: "node2", latestTS: 1003},
		"node3": {nodeID: "node3", latestTS: 1001},
	}

	col := Collector{merger: &merger, pumps: pumps}
	col.updateCollectStatus(true)
	status := col.mu.status
	c.Assert(status.Synced, IsTrue)
	c.Assert(status.LastTS, Equals, merger.latestTS)
	c.Assert(status.PumpPos, DeepEquals, map[string]int64{
		"node1": 1001,
		"node2": 1003,
		"node3": 1001,
	})
}

func (s *collectorSuite) TestNotify(c *C) {
	col := Collector{notifyChan: make(chan *notifyResult)}
	go func() {
		nr := <-col.notifyChan
		nr.err = errors.New("Hello")
		nr.wg.Done()
	}()
	c.Assert(col.Notify(), ErrorMatches, "Hello")
}

type newCollectorSuite struct{}

var _ = Suite(&newCollectorSuite{})

func (s *newCollectorSuite) TestShouldRejectInvalidURLs(c *C) {
	cfg := Config{EtcdURLs: "123asdfasdf:12-12"}
	_, err := NewCollector(&cfg, 0, nil, dummyCheckpoint{})
	c.Assert(err, ErrorMatches, ".*URL.*")
}

func (s *newCollectorSuite) TestFailedToOpenStore(c *C) {
	var tiPath string

	origNew := newStore
	defer func() { newStore = origNew }()
	newStore = func(path string) (kv.Storage, error) {
		tiPath = path
		return nil, errors.New("store")
	}

	cfg := Config{EtcdURLs: "http://localhost:9090"}
	_, err := NewCollector(&cfg, 0, nil, dummyCheckpoint{})
	c.Assert(err, ErrorMatches, "store")
	c.Assert(tiPath, Equals, "tikv://localhost:9090?disableGC=true")
}

func (s *newCollectorSuite) TestFailedToCreatePDCli(c *C) {
	origNew := newStore
	defer func() { newStore = origNew }()
	newStore = func(path string) (kv.Storage, error) {
		return dummyStore{}, nil
	}

	origNewCli := newClient
	defer func() { newClient = origNewCli }()
	newClient = func(endpoints []string, dialTimeout time.Duration, root string, security *tls.Config) (*etcd.Client, error) {
		return nil, errors.New("etcd")
	}

	cfg := Config{EtcdURLs: "http://localhost:9090"}
	_, err := NewCollector(&cfg, 0, nil, dummyCheckpoint{})
	c.Assert(err, ErrorMatches, "etcd")
}

func (s *newCollectorSuite) TestReturnCollector(c *C) {
	origNew := newStore
	defer func() { newStore = origNew }()
	newStore = func(path string) (kv.Storage, error) {
		return dummyStore{}, nil
	}

	origNewCli := newClient
	defer func() { newClient = origNewCli }()
	newClient = func(endpoints []string, dialTimeout time.Duration, root string, security *tls.Config) (*etcd.Client, error) {
		return &etcd.Client{}, nil
	}

	cfg := Config{EtcdURLs: "http://localhost:9090"}
	collector, err := NewCollector(&cfg, 1984, nil, dummyCheckpoint{})
	c.Assert(err, IsNil)
	c.Assert(collector.clusterID, Equals, uint64(1984))
}

type syncBinlogSuite struct{}

var _ = Suite(&syncBinlogSuite{})

func (s *syncBinlogSuite) TestShouldAddToSyncer(c *C) {
	syncer := Syncer{
		input: make(chan *binlogItem, 1),
	}
	col := Collector{syncer: &syncer}

	item := binlogItem{
		binlog: &pb.Binlog{DdlJobId: 0, CommitTs: 2311945},
	}

	err := col.syncBinlog(&item)

	c.Assert(err, IsNil)
	c.Assert(len(syncer.input), Equals, 1)
	get := <-syncer.input
	c.Assert(get.binlog.CommitTs, Equals, item.binlog.CommitTs)
}

func (s *syncBinlogSuite) TestGetDDLFailed(c *C) {
	origDDLGetter := fDDLJobGetter
	fDDLJobGetter = func(tiStore kv.Storage, id int64) (*model.Job, error) {
		return nil, errors.New("getDDLJob")
	}
	defer func() { fDDLJobGetter = origDDLGetter }()

	origRetryWait := getDDLJobRetryWait
	getDDLJobRetryWait = time.Millisecond
	defer func() { getDDLJobRetryWait = origRetryWait }()

	col := Collector{}

	item := binlogItem{
		binlog: &pb.Binlog{DdlJobId: 1024, CommitTs: 1305432},
	}

	err := col.syncBinlog(&item)
	c.Assert(err, ErrorMatches, "getDDLJob")
}

func (s *syncBinlogSuite) TestShouldSetJob(c *C) {
	job := model.Job{
		ID:    12345,
		State: model.JobStateSynced,
	}

	origDDLGetter := fDDLJobGetter
	fDDLJobGetter = func(tiStore kv.Storage, id int64) (*model.Job, error) {
		return &job, nil
	}
	defer func() { fDDLJobGetter = origDDLGetter }()

	syncer := Syncer{
		input: make(chan *binlogItem, 1),
	}
	col := Collector{syncer: &syncer}

	item := binlogItem{
		binlog: &pb.Binlog{DdlJobId: 1024, CommitTs: 1305432},
	}

	err := col.syncBinlog(&item)
	c.Assert(err, IsNil)

	c.Assert(item.job.ID, Equals, job.ID)
	c.Assert(item.job.State, Equals, job.State)
}

type publishSuite struct{}

var _ = Suite(&publishSuite{})

func (s *publishSuite) assertStopInTime(ctx context.Context, col *Collector, c *C) {
	signal := make(chan struct{})
	go func() {
		col.publishBinlogs(ctx)
		close(signal)
	}()

	select {
	case <-signal:
	case <-time.After(time.Second):
		c.Fatal("publish doesn't stop in time")
	}
}

func (s *publishSuite) TestCanBeStopped(c *C) {
	col := Collector{merger: &Merger{output: make(chan MergeItem)}}

	ctx, cancel := context.WithCancel(context.Background())
	go cancel()
	s.assertStopInTime(ctx, &col, c)
}

func (s *publishSuite) TestShouldStopIfSyncFailed(c *C) {
	// Use a mocking ddl job getter that always fails
	origDDLGetter := fDDLJobGetter
	fDDLJobGetter = func(tiStore kv.Storage, id int64) (*model.Job, error) {
		return nil, errors.New("getDDLJob")
	}
	defer func() { fDDLJobGetter = origDDLGetter }()

	// Set retry wait time to be a short time to avoid waiting a long time
	origRetryWait := getDDLJobRetryWait
	getDDLJobRetryWait = time.Millisecond
	defer func() { getDDLJobRetryWait = origRetryWait }()

	col := Collector{
		merger: &Merger{output: make(chan MergeItem, 1)},
		errCh:  make(chan error, 1),
	}

	// Send a DDL to trigger the error
	DDLItem := binlogItem{
		binlog: &pb.Binlog{DdlJobId: 1232, CommitTs: 2311945},
	}
	col.merger.output <- &DDLItem
	s.assertStopInTime(context.Background(), &col, c)
	c.Assert(col.errCh, HasLen, 1)
	c.Assert(<-col.errCh, ErrorMatches, "getDDLJob")
}

type updatePumpSuite struct{}

var _ = Suite(&updatePumpSuite{})

func (s *updatePumpSuite) TestShouldIgnoreOfflineNewNode(c *C) {
	n := node.Status{State: node.Offline, NodeID: "new"}
	col := Collector{pumps: make(map[string]*Pump)}
	col.handlePumpStatusUpdate(context.Background(), &n)
	c.Assert(col.pumps, HasLen, 0)
}

func (s *updatePumpSuite) TestShouldAddNewNode(c *C) {
	n := node.Status{State: node.Online, NodeID: "new"}
	merger := Merger{latestTS: 2019, sources: make(map[string]MergeSource)}
	col := Collector{
		pumps:  make(map[string]*Pump),
		merger: &merger,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Stop the underlying PullBinlog goroutine ASAP
	col.handlePumpStatusUpdate(ctx, &n)
	c.Assert(col.pumps, HasKey, "new")
	c.Assert(col.pumps["new"].latestTS, Equals, merger.latestTS)
}

func (s *updatePumpSuite) TestHandleExistingNode(c *C) {
	n := node.Status{State: node.Paused, NodeID: "node"}
	merger := Merger{
		sources: map[string]MergeSource{
			"node": {ID: "node"},
		},
	}
	col := Collector{
		pumps: map[string]*Pump{
			"node": {nodeID: "node", logger: log.L()},
		},
		merger: &merger,
	}
	c.Assert(col.pumps["node"].isPaused, Equals, int32(0))
	col.handlePumpStatusUpdate(context.Background(), &n)
	c.Assert(col.pumps["node"].isPaused, Equals, int32(1))

	n.State = node.Online
	col.handlePumpStatusUpdate(context.Background(), &n)
	c.Assert(col.pumps["node"].isPaused, Equals, int32(0))

	n.State = node.Offline
	col.handlePumpStatusUpdate(context.Background(), &n)
	c.Assert(col.pumps, Not(HasKey), "node")
	c.Assert(merger.sources, Not(HasKey), "node")
}

type HTTPStatusSuite struct{}

var _ = Suite(&HTTPStatusSuite{})

func (s *HTTPStatusSuite) TestReturnDefault(c *C) {
	col := Collector{}
	st := col.HTTPStatus()
	c.Assert(st.Synced, IsFalse)
	c.Assert(st.PumpPos, HasLen, 0)
}

func (s *HTTPStatusSuite) TestSyncedShouldBeSet(c *C) {
	syncer := Syncer{
		lastSyncTime: time.Now().Add(-time.Minute),
		cp:           dummyCheckpoint{commitTS: 999},
	}
	col := Collector{
		syncer:          &syncer,
		syncedCheckTime: 1,
	}
	col.mu.status = &HTTPStatus{Synced: false}

	st := col.HTTPStatus()
	c.Assert(st.Synced, IsTrue)
	c.Assert(st.LastTS, Equals, syncer.cp.TS())
}

type reportErrSuite struct{}

var _ = Suite(&reportErrSuite{})

func (s *reportErrSuite) TestCanBeStopped(c *C) {
	col := Collector{errCh: make(chan error)}
	ctx, cancel := context.WithCancel(context.Background())
	signal := make(chan struct{})
	go func() {
		col.reportErr(ctx, errors.New("report"))
		close(signal)
	}()

	cancel()

	select {
	case <-signal:
	case <-time.After(time.Second):
		c.Fatal("Can not be stopped in time")
	}
}

func (s *reportErrSuite) TestErrReported(c *C) {
	col := Collector{errCh: make(chan error, 3)}
	col.reportErr(context.Background(), errors.New("report"))
	c.Assert(<-col.errCh, ErrorMatches, "report")
}

type keepUpdatingSuite struct{}

var _ = Suite(&keepUpdatingSuite{})

func (s *keepUpdatingSuite) waitForSignal(c *C, signal <-chan struct{}, msg string) {
	select {
	case <-signal:
	case <-time.After(time.Second):
		c.Fatal(msg)
	}
}

func (s *keepUpdatingSuite) TestShouldStopWhenDone(c *C) {
	var callCount int
	ctx, cancel := context.WithCancel(context.Background())
	col := Collector{
		merger:   &Merger{},
		interval: 10 * time.Second,
	}
	signal := make(chan struct{})
	go func() {
		col.keepUpdatingStatus(ctx, func(ctx context.Context) error {
			callCount++
			return nil
		})
		close(signal)
	}()

	cancel()

	s.waitForSignal(c, signal, "Doesn't stop in time after canceled")
	c.Assert(callCount, Equals, 1)
}

func (s *keepUpdatingSuite) TestShouldStopOnErr(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	col := Collector{
		merger:   &Merger{},
		interval: time.Second,
		errCh:    make(chan error, 1),
	}
	signal := make(chan struct{})
	go func() {
		col.keepUpdatingStatus(ctx, func(ctx context.Context) error {
			return nil
		})
		close(signal)
	}()
	col.errCh <- errors.New("Terrible error")
	s.waitForSignal(c, signal, "Doesn't stop in time after error occurs")
}

func (s *keepUpdatingSuite) TestShouldUpdateWhenNotified(c *C) {
	var callCount int
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	col := Collector{
		merger:     &Merger{},
		notifyChan: make(chan *notifyResult),
		interval:   10 * time.Second,
	}
	go col.keepUpdatingStatus(ctx, func(ctx context.Context) error {
		defer func() {
			callCount++
		}()
		if callCount%2 == 0 {
			return errors.New("testing")
		}
		return nil
	})
	signal := make(chan struct{})
	go func() {
		// Start from 1 since the first call is outside the loop
		for i := 1; i < 10; i++ {
			err := col.Notify()
			if i%2 == 0 {
				c.Assert(err, ErrorMatches, "testing")
			} else {
				c.Assert(err, IsNil)
			}
		}
		close(signal)
	}()
	// Wait until all notifies finish or timeout
	s.waitForSignal(c, signal, "Notifies don't finish in time")
	c.Assert(callCount, Equals, 10)
}

func (s *keepUpdatingSuite) TestUpdateStatus(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	etcdClient := etcd.NewClient(testEtcdCluster.RandClient(), DefaultRootPath)
	r := node.NewEtcdRegistry(etcdClient, time.Duration(5)*time.Second)
	ns := &node.Status{
		NodeID:  "test",
		Addr:    "test",
		State:   node.Online,
		IsAlive: true,
	}
	err := r.UpdateNode(context.Background(), "pumps", ns)
	c.Assert(err, IsNil)

	col := Collector{
		merger:     &Merger{latestTS: 5, sources: map[string]MergeSource{}},
		notifyChan: make(chan *notifyResult),
		interval:   10 * time.Second,
		reg:        r,
		pumps:      map[string]*Pump{},
		tiStore:    dummyStorage{},
	}
	var TS int64 = 5
	err = col.updateStatus(ctx)
	c.Assert(err, IsNil)
	c.Assert(col.mu, NotNil)
	c.Assert(col.mu.status.LastTS, Equals, TS)
	c.Assert(col.mu.status.Synced, IsFalse)
	c.Assert(col.mu.status.PumpPos["test"], Equals, TS)
}
