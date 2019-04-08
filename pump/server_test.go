package pump

import (
	"fmt"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	. "github.com/pingcap/check"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-binlog/pkg/node"
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

type fakeStorage struct{}

func (s *fakeStorage) WriteBinlog(binlog *pb.Binlog) error        { return nil }
func (s *fakeStorage) GCTS(ts int64)                              {}
func (s *fakeStorage) MaxCommitTS() int64                         { return 0 }
func (s *fakeStorage) GetBinlog(ts int64) (*binlog.Binlog, error) { return nil, nil }
func (s *fakeStorage) PullCommitBinlog(ctx context.Context, last int64) <-chan []byte {
	return make(chan []byte)
}
func (s *fakeStorage) Close() error { return nil }

type fakePullable struct{ fakeStorage }

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
	go func() {
		server.wg.Add(1)
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
	fakeStorage
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
	go func() {
		server.wg.Add(1)
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
