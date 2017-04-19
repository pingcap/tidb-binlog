package pump

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	. "github.com/pingcap/check"
	"golang.org/x/net/context"
)

var _ = Suite(&testNodeSuite{})

type testNodeSuite struct{}

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

func testCheckNodeID(c *C, node Node, exceptedID string) {
	c.Assert(node.ID(), Equals, exceptedID)
	c.Assert(node.ShortID(), Equals, exceptedID[:shortIDLen])
}

func testInteracWithEtcd(c *C, node Node) {
	pn := node.(*pumpNode)
	ns := &NodeStatus{
		NodeID:  pn.id,
		Host:    pn.host,
		IsAlive: true,
	}

	// check register
	err := node.Register(context.Background())
	c.Assert(err, IsNil)
	mustEqualStatus(c, node.(*pumpNode), pn.id, ns)
	// check unregister
	ctx, cancel := context.WithCancel(context.Background())
	err = node.Unregister(ctx)
	c.Assert(err, IsNil)
	// cancel context
	cancel()
}
