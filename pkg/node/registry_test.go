package node

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
)

var _ = Suite(&testRegistrySuite{})

type testRegistrySuite struct{}

type RegisrerTestClient interface {
	Node(context.Context, string, string) (*NodeStatus, error)
}

func (t *testRegistrySuite) TestUpdateNodeInfo(c *C) {
	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), "binlog")
	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)
	latestPos := binlog.Pos{}
	ns := &NodeStatus{
		NodeID:        "test",
		Host:          "test",
		LatestFilePos: latestPos,
	}

	err := r.RegisterNode(context.Background(), nodePrefix, ns.NodeID, ns.Host)
	c.Assert(err, IsNil)
	mustEqualStatus(c, r, ns.NodeID, ns)

	ns.Host = "localhost:1234"
	err = r.UpdateNode(context.Background(), nodePrefix, ns.NodeID, ns.Host)
	c.Assert(err, IsNil)
	mustEqualStatus(c, r, ns.NodeID, ns)
	// use Nodes() to query node status
	ss, err := r.Nodes(context.Background(), nodePrefix)
	c.Assert(err, IsNil)
	c.Assert(ss, HasLen, 1)
}

func (t *testRegistrySuite) TestRegisterNode(c *C) {
	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), "binlog")
	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	ns := &NodeStatus{
		NodeID: "test",
		Host:   "test",
	}

	err := r.RegisterNode(context.Background(), nodePrefix, ns.NodeID, ns.Host)
	c.Assert(err, IsNil)
	mustEqualStatus(c, r, ns.NodeID, ns)

	err = r.MarkOfflineNode(context.Background(), nodePrefix, ns.NodeID, ns.Host)
	c.Assert(err, IsNil)
	ns.OfflineTS = latestTS
	ns.IsOffline = true
	mustEqualStatus(c, r, ns.NodeID, ns)

	err = r.UnregisterNode(context.Background(), nodePrefix, ns.NodeID)
	c.Assert(err, IsNil)
	exist, err := r.checkNodeExists(context.Background(), nodePrefix, ns.NodeID)
	c.Assert(err, IsNil)
	c.Assert(exist, IsFalse)
}

func (t *testRegistrySuite) TestRefreshNode(c *C) {
	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), "binlog")
	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	ns := &NodeStatus{
		NodeID: "test",
		Host:   "test",
	}

	err := r.RegisterNode(context.Background(), nodePrefix, ns.NodeID, ns.Host)
	c.Assert(err, IsNil)

	err = r.RefreshNode(context.Background(), nodePrefix, ns.NodeID, 1)
	c.Assert(err, IsNil)

	ns.IsAlive = true
	mustEqualStatus(c, r, ns.NodeID, ns)
	time.Sleep(2 * time.Second)
	ns.IsAlive = false
	mustEqualStatus(c, r, ns.NodeID, ns)
}

func mustEqualStatus(c *C, r RegisrerTestClient, nodeID string, status *NodeStatus) {
	ns, err := r.Node(context.Background(), nodePrefix, nodeID)
	// ignore the latestBinlogPos and alive
	status.LatestFilePos = ns.LatestFilePos
	c.Assert(err, IsNil)
	c.Assert(ns, DeepEquals, status)
}
