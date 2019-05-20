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

package pump

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	. "github.com/pingcap/check"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/node"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pump/storage"
	"github.com/pingcap/tipb/go-binlog"
	pb "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var testEtcdCluster *integration.ClusterV3

func TestPump(t *testing.T) {
	testEtcdCluster = integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	TestingT(t)
}

type writeBinlogSuite struct{}

var _ = Suite(&writeBinlogSuite{})

func (s *writeBinlogSuite) TestIgnoreEmptyRequest(c *C) {
	server := &Server{}
	resp, err := server.WriteBinlog(context.Background(), &binlog.WriteBinlogReq{})
	c.Assert(resp, NotNil)
	c.Assert(err, IsNil)
	c.Assert(server.writeBinlogCount, Equals, int64(0))
}

func (s *writeBinlogSuite) TestReturnErrIfClusterIDMismatched(c *C) {
	server := &Server{clusterID: 42}
	req := &binlog.WriteBinlogReq{}
	req.ClusterID = 53
	resp, err := server.writeBinlog(context.Background(), req, false)
	c.Assert(resp, IsNil)
	c.Assert(err, ErrorMatches, ".*mismatch.*")
}

func (s *writeBinlogSuite) TestIgnoreReqWithInvalidPayload(c *C) {
	server := &Server{clusterID: 42}
	req := &binlog.WriteBinlogReq{ClusterID: 42, Payload: []byte("invalid")}
	resp, err := server.writeBinlog(context.Background(), req, false)
	c.Assert(resp.Errmsg, Equals, "unexpected EOF")
	c.Assert(err, NotNil)
}

type fakeNode struct{}

func (n *fakeNode) ID() string                                                   { return "fakenode-long" }
func (n *fakeNode) ShortID() string                                              { return "fakenode" }
func (n *fakeNode) RefreshStatus(ctx context.Context, status *node.Status) error { return nil }
func (n *fakeNode) Heartbeat(ctx context.Context) <-chan error                   { return make(chan error) }
func (n *fakeNode) Notify(ctx context.Context) error                             { return nil }
func (n *fakeNode) NodeStatus() *node.Status                                     { return &node.Status{State: node.Paused} }
func (n *fakeNode) NodesStatus(ctx context.Context) ([]*node.Status, error) {
	return []*node.Status{}, nil
}
func (n *fakeNode) Quit() error { return nil }

func (s *writeBinlogSuite) TestDetectNoOnline(c *C) {
	server := &Server{clusterID: 42, node: &fakeNode{}}

	log := new(binlog.Binlog)
	data, err := log.Marshal()
	if err != nil {
		c.Fatal("Fail to marshal binlog")
	}
	req := &binlog.WriteBinlogReq{ClusterID: 42, Payload: data}
	_, err = server.writeBinlog(context.Background(), req, false)
	c.Assert(err, ErrorMatches, ".*no online.*")
}

type pullBinlogsSuite struct{}

var _ = Suite(&pullBinlogsSuite{})

type fakePullBinlogsServer struct {
	grpc.ServerStream
	ctx  context.Context
	sent []*binlog.PullBinlogResp
}

func newFakePullBinlogsServer() *fakePullBinlogsServer {
	return &fakePullBinlogsServer{
		ctx:  context.Background(),
		sent: []*binlog.PullBinlogResp{},
	}
}

func (x *fakePullBinlogsServer) Context() context.Context { return x.ctx }
func (x *fakePullBinlogsServer) Send(m *binlog.PullBinlogResp) error {
	x.sent = append(x.sent, m)
	return nil
}

func (s *pullBinlogsSuite) TestReturnErrIfClusterIDMismatched(c *C) {
	server := &Server{clusterID: 42}
	req := &binlog.PullBinlogReq{ClusterID: 43}
	err := server.PullBinlogs(req, newFakePullBinlogsServer())
	c.Assert(err, ErrorMatches, ".*mismatch.*")
}

type noOpStorage struct{}

func (s *noOpStorage) WriteBinlog(binlog *pb.Binlog) error        { return nil }
func (s *noOpStorage) GCTS(ts int64)                              {}
func (s *noOpStorage) MaxCommitTS() int64                         { return 0 }
func (s *noOpStorage) GetBinlog(ts int64) (*binlog.Binlog, error) { return nil, nil }
func (s *noOpStorage) PullCommitBinlog(ctx context.Context, last int64) <-chan []byte {
	return make(chan []byte)
}
func (s *noOpStorage) Close() error { return nil }

type fakePullable struct{ noOpStorage }

func (s *fakePullable) PullCommitBinlog(ctx context.Context, last int64) <-chan []byte {
	chl := make(chan []byte)
	go func() {
		for i := 0; i < 3; i++ {
			chl <- []byte(fmt.Sprintf("payload_%d", i))
		}
		close(chl)
	}()
	return chl
}

func (s *pullBinlogsSuite) TestPullBinlogFromStorage(c *C) {
	ctx := context.Background()
	server := &Server{clusterID: 42, storage: &fakePullable{}, ctx: ctx}
	req := &binlog.PullBinlogReq{
		ClusterID: 42,
		StartFrom: binlog.Pos{
			Suffix: 1,
			Offset: 97,
		},
	}
	stream := newFakePullBinlogsServer()
	err := server.PullBinlogs(req, stream)
	c.Assert(err, IsNil)
	c.Assert(stream.sent, HasLen, 3)
	for i, resp := range stream.sent {
		c.Assert(string(resp.Entity.Payload), Equals, fmt.Sprintf("payload_%d", i))
	}
}

type genForwardBinlogSuite struct{}

var _ = Suite(&genForwardBinlogSuite{})

func (s *genForwardBinlogSuite) TestShouldExitWhenCanceled(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	server := &Server{
		clusterID: 42,
		storage:   &fakePullable{},
		ctx:       ctx,
		cfg: &Config{
			GenFakeBinlogInterval: 1,
		},
	}
	signal := make(chan struct{})
	server.wg.Add(1)
	go func() {
		server.genForwardBinlog()
		close(signal)
	}()
	cancel()
	select {
	case <-signal:
	case <-time.After(100 * time.Millisecond):
		c.Fatal("Can't be canceled")
	}
}

type fakeWritable struct {
	noOpStorage
	binlogs []pb.Binlog
}

func (s *fakeWritable) WriteBinlog(binlog *pb.Binlog) error {
	s.binlogs = append(s.binlogs, *binlog)
	return nil
}

func (s *genForwardBinlogSuite) TestSendFakeBinlog(c *C) {
	origGetTSO := utilGetTSO
	utilGetTSO = func(cli pd.Client) (int64, error) {
		return 42, nil
	}
	defer func() {
		utilGetTSO = origGetTSO
	}()
	ctx, cancel := context.WithCancel(context.Background())
	storage := fakeWritable{binlogs: make([]pb.Binlog, 1)}
	server := &Server{
		clusterID: 42,
		storage:   &storage,
		ctx:       ctx,
		cfg: &Config{
			GenFakeBinlogInterval: 1,
		},
	}
	server.wg.Add(1)
	go func() {
		server.genForwardBinlog()
	}()
	time.Sleep(time.Duration(server.cfg.GenFakeBinlogInterval*2) * time.Second)
	cancel()
	server.wg.Wait()
	c.Assert(len(storage.binlogs) > 0, IsTrue)
	var foundFake bool
	for _, bl := range storage.binlogs {
		if bl.Tp == pb.BinlogType_Rollback && bl.CommitTs == 42 && bl.StartTs == 42 {
			foundFake = true
		}
	}
	c.Assert(foundFake, IsTrue)
}

type startHeartbeatSuite struct{}

var _ = Suite(&startHeartbeatSuite{})

type heartbeartNode struct {
	fakeNode
	errChl chan error
}

func (n *heartbeartNode) Heartbeat(ctx context.Context) <-chan error {
	return n.errChl
}

func (s *startHeartbeatSuite) TestOnlyLogNonCancelErr(c *C) {
	var hook util.LogHook
	hook.SetUp()
	defer hook.TearDown()

	errChl := make(chan error)
	node := heartbeartNode{errChl: errChl}
	server := &Server{node: &node, ctx: context.Background()}
	server.startHeartbeat()
	errChl <- errors.New("test")
	errChl <- context.Canceled
	close(errChl)

	c.Assert(hook.Entrys, HasLen, 1)
	c.Assert(hook.Entrys[0].Message, Matches, ".*send heartbeat failed.*")
}

type printServerInfoSuite struct{}

var _ = Suite(&printServerInfoSuite{})

type dummyStorage struct {
	storage.Storage
	gcTS        int64
	maxCommitTS int64
}

func (ds *dummyStorage) MaxCommitTS() int64 {
	return ds.maxCommitTS
}

func (ds *dummyStorage) GCTS(ts int64) {
	ds.gcTS = ts
}

func (s *printServerInfoSuite) TestReturnWhenServerIsDone(c *C) {
	originalInterval := serverInfoOutputInterval
	serverInfoOutputInterval = 50 * time.Millisecond

	defer func() {
		serverInfoOutputInterval = originalInterval
	}()

	var hook util.LogHook
	hook.SetUp()
	defer hook.TearDown()

	ctx, cancel := context.WithCancel(context.Background())
	server := &Server{storage: &dummyStorage{}, ctx: ctx}
	signal := make(chan struct{})

	server.wg.Add(1)
	go func() {
		server.printServerInfo()
		close(signal)
	}()

	time.Sleep(3 * serverInfoOutputInterval)
	cancel()

	select {
	case <-signal:
	case <-time.After(time.Second):
		c.Fatal("Fail to return when server's done")
	}

	for i, entry := range hook.Entrys {
		if i == len(hook.Entrys)-1 {
			c.Assert(entry.Message, Matches, ".*printServerInfo exit.*")
		} else {
			c.Assert(entry.Message, Matches, ".*server info tick.*")
		}
	}
}

type pumpStatusSuite struct{}

var _ = Suite(&pumpStatusSuite{})

type failStatusNode struct {
	node.Node
}

func (n failStatusNode) NodesStatus(ctx context.Context) ([]*node.Status, error) {
	return nil, errors.New("Query Status Failed")
}

func (s *pumpStatusSuite) TestShouldRetErrIfFailToGetStatus(c *C) {
	server := &Server{node: failStatusNode{}}
	status := server.PumpStatus()
	c.Assert(status.ErrMsg, Equals, "Query Status Failed")
}

type nodeWithStatus struct {
	node.Node
}

func (n nodeWithStatus) NodesStatus(ctx context.Context) (sts []*node.Status, err error) {
	sts = []*node.Status{
		{NodeID: "pump1"},
		{NodeID: "pump4"},
		{NodeID: "pump2"},
	}
	return
}

func (s *pumpStatusSuite) TestShouldRetErrIfFailToGetCommitTS(c *C) {
	origGetTSO := utilGetTSO
	utilGetTSO = func(cli pd.Client) (int64, error) { return -1, errors.New("PD Failure") }
	defer func() { utilGetTSO = origGetTSO }()

	server := &Server{node: nodeWithStatus{}}
	status := server.PumpStatus()
	c.Assert(status.ErrMsg, Equals, "PD Failure")
}

func (s *pumpStatusSuite) TestCorrectlyGetStatusAndCommitTS(c *C) {
	origGetTSO := utilGetTSO
	utilGetTSO = func(cli pd.Client) (int64, error) { return 1024, nil }
	defer func() { utilGetTSO = origGetTSO }()

	server := &Server{node: nodeWithStatus{}}
	status := server.PumpStatus()
	c.Assert(status.ErrMsg, Equals, "")
	c.Assert(status.CommitTS, Equals, int64(1024))
	c.Assert(status.StatusMap, HasLen, 3)
	c.Assert(status.StatusMap, HasKey, "pump1")
	c.Assert(status.StatusMap, HasKey, "pump2")
	c.Assert(status.StatusMap, HasKey, "pump4")
}

type commitStatusSuite struct{}

var _ = Suite(commitStatusSuite{})

type nodeWithState struct {
	node.Node
	state    string
	newState string
}

func (n *nodeWithState) NodeStatus() *node.Status {
	return &node.Status{
		State:  n.state,
		NodeID: "test",
		Addr:   "192.168.1.1",
	}
}
func (n *nodeWithState) RefreshStatus(ctx context.Context, status *node.Status) error {
	n.newState = status.State
	return nil
}

func (s commitStatusSuite) TestShouldChangeToCorrectState(c *C) {
	tests := map[string]string{
		node.Pausing: node.Paused,
		node.Online:  node.Paused,
		"unknown":    "unknown",
	}
	for from, to := range tests {
		server := &Server{
			node:    &nodeWithState{state: from},
			storage: &dummyStorage{},
		}
		server.commitStatus()
		c.Assert(server.node.(*nodeWithState).newState, Equals, to)
	}
}

type closeSuite struct{}

var _ = Suite(&closeSuite{})

func (s *closeSuite) TestSkipIfAlreadyClosed(c *C) {
	var hook util.LogHook
	hook.SetUp()
	defer hook.TearDown()

	server := Server{isClosed: 1}
	server.Close()

	c.Assert(len(hook.Entrys), Less, 2)
}

type gcBinlogFileSuite struct{}

var _ = Suite(&gcBinlogFileSuite{})

func (s *gcBinlogFileSuite) TestShouldGCMinDrainerTSO(c *C) {
	storage := dummyStorage{}

	ctx, cancel := context.WithCancel(context.Background())

	cli := etcd.NewClient(testEtcdCluster.RandClient(), "drainers")
	registry := node.NewEtcdRegistry(cli, time.Second)
	registry.UpdateNode(ctx, "drainers/1", &node.Status{MaxCommitTS: 1003, State: node.Online})
	registry.UpdateNode(ctx, "drainers/2", &node.Status{MaxCommitTS: 1002, State: node.Online})
	// drainers/3 is set to be offline, so its MaxCommitTS is expected to be ignored
	registry.UpdateNode(ctx, "drainers/3", &node.Status{MaxCommitTS: 1001, State: node.Offline})

	server := Server{
		ctx:        ctx,
		storage:    &storage,
		node:       &pumpNode{EtcdRegistry: registry},
		gcDuration: time.Hour,
	}

	// Set a shorter interval because we don't really want to wait 1 hour
	origInterval := gcInterval
	gcInterval = 100 * time.Microsecond
	defer func() {
		gcInterval = origInterval
	}()

	server.wg.Add(1)
	go server.gcBinlogFile()

	// Give the GC goroutine some time to do the job,
	// the latency of the underlying etcd query can be much larger than gcInterval
	time.Sleep(1000 * gcInterval)
	cancel()

	c.Assert(storage.gcTS, Equals, int64(1002))
}

type waitCommitTSSuite struct{}

var _ = Suite(&waitCommitTSSuite{})

func (s *waitCommitTSSuite) TestShouldStoppedWhenDone(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	storage := dummyStorage{maxCommitTS: 1024}
	server := Server{
		storage: &storage,
		ctx:     ctx,
	}
	signal := make(chan struct{})
	go func() {
		err := server.waitUntilCommitTSSaved(ctx, int64(2000), time.Millisecond)
		close(signal)
		c.Assert(err, ErrorMatches, "context canceled")
	}()
	cancel()
	select {
	case <-signal:
	case <-time.After(time.Second):
		c.Fatal("Doesn't stop in time when done")
	}
}

func (s *waitCommitTSSuite) TestShouldWaitUntilTs(c *C) {
	storage := dummyStorage{maxCommitTS: 1024}
	server := Server{
		storage: &storage,
		ctx:     context.Background(),
	}
	signal := make(chan struct{})
	go func() {
		err := server.waitUntilCommitTSSaved(server.ctx, int64(2000), time.Millisecond)
		close(signal)
		c.Assert(err, IsNil)
	}()
	storage.maxCommitTS = 2000
	select {
	case <-signal:
	case <-time.After(time.Second):
		c.Fatal("Doesn't stop in time after ts is reached")
	}
}

type listenSuite struct{}

var _ = Suite(&listenSuite{})

func (s *listenSuite) TestWrongAddr(c *C) {
	_, err := listen("unix", "://asdf:1231:123:12")
	c.Assert(err, ErrorMatches, ".*invalid .* socket addr.*")
}

func (s *listenSuite) TestUnbindableAddr(c *C) {
	_, err := listen("tcp", "http://asdf;klj:7979/12")
	c.Assert(err, ErrorMatches, ".*fail to start.*")
}

func (s *listenSuite) TestReturnListener(c *C) {
	var l net.Listener
	l, err := listen("tcp", "http://localhost:17979")
	c.Assert(err, IsNil)
	defer l.Close()
	c.Assert(l, NotNil)
}
