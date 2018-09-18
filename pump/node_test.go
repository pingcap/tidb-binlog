package pump

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	. "github.com/pingcap/check"
	pkgnode "github.com/pingcap/tidb-binlog/pkg/node"
	"golang.org/x/net/context"
)

var _ = Suite(&testNodeSuite{})

type testNodeSuite struct{}

type RegisrerTestClient interface {
	Node(context.Context, string, string) (*pkgnode.Status, error)
}

func (t *testNodeSuite) TestNode(c *C) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "nodetest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(tmpDir)

	etcdClient := testEtcdCluster.RandClient()
	listenAddr := "http://127.0.0.1:8250"
	hostName, err := os.Hostname()
	c.Assert(err, IsNil)
	exceptedNodeID := fmt.Sprintf("%s:%s", hostName, "8250")

	// test pump node
	cfg := &Config{
		DataDir:           tmpDir,
		EtcdURLs:          strings.Join(etcdClient.Endpoints(), ","),
		EtcdDialTimeout:   defaultEtcdDialTimeout,
		HeartbeatInterval: 1,
		ListenAddr:        listenAddr,
		AdvertiseAddr:     listenAddr,
	}

	node, err := NewPumpNode(cfg)
	c.Assert(err, IsNil)

	testCheckNodeID(c, node, exceptedNodeID)
	testInteracWithEtcd(c, node)
}

func testCheckNodeID(c *C, node pkgnode.Node, exceptedID string) {
	c.Assert(node.ID(), Equals, exceptedID)
	c.Assert(node.ShortID(), Equals, exceptedID[:shortIDLen])
}

func testInteracWithEtcd(c *C, node pkgnode.Node) {
	pn := node.(*pumpNode)
	ns := &pkgnode.Status{
		NodeID:  pn.status.NodeID,
		Addr:    pn.status.Addr,
		State:   pkgnode.Online,
		IsAlive: true,
	}

	// check register
	err := node.RefreshStatus(context.Background(), ns)
	c.Assert(err, IsNil)
	mustEqualStatus(c, node.(*pumpNode), pn.status.NodeID, ns)
}

func mustEqualStatus(c *C, r RegisrerTestClient, nodeID string, status *pkgnode.Status) {
	ns, err := r.Node(context.Background(), nodePrefix, nodeID)
	c.Assert(err, IsNil)
	c.Assert(ns, DeepEquals, status)
}
