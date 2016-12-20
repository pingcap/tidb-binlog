package pump

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"golang.org/x/net/context"
)

func TestNode(t *testing.T) {
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	tmpDir, err := ioutil.TempDir(os.TempDir(), "nodetest")
	if err != nil {
		t.Fatalf("mkdir tmp dir err:%v", err)
	}
	defer os.RemoveAll(tmpDir)

	etcdClient := cluster.RandClient()
	listenAddr := "http://127.0.0.1:8250"
	hostName, err := os.Hostname()
	if err != nil {
		t.Fatalf("get hostname err: %v", err)
	}
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
	if err != nil {
		t.Fatalf("new pump node err: %v", err)
	}

	testCheckNodeID(t, node, exceptedNodeID)
	testInteracWithEtcd(t, node)

	// test case that had nodeID in file
	tmpDir1, err := ioutil.TempDir(os.TempDir(), "nodetest")
	if err != nil {
		t.Fatalf("mkdir tmp dir err:%v", err)
	}
	defer os.RemoveAll(tmpDir1)

	_, err = generateLocalNodeID(tmpDir1, listenAddr)
	if err != nil {
		t.Fatalf("gen node ID err: %v", err)
	}

	cfg.DataDir = tmpDir1
	cfg.NodeID = "testID"
	_, err = NewPumpNode(cfg)
	if err != nil {
		t.Fatalf("newPumpNode err: %v", err)
	}
}

func testCheckNodeID(t *testing.T, node Node, excepted string) {
	if node.ID() != excepted {
		t.Fatalf("node ID %s, excepted %s", node.ID(), excepted)
	}

	if node.ShortID() != excepted[:shortIDLen] {
		t.Fatalf("node's short ID(%s), excepted %s", node.ShortID(), excepted[:shortIDLen])
	}
}

func testInteracWithEtcd(t *testing.T, node Node) {
	pn := node.(*pumpNode)
	ns := &NodeStatus{
		NodeID: pn.id,
		Host:   pn.host,
	}

	// check register
	err := node.Register(context.Background())
	if err != nil {
		t.Fatalf("register err: %v", err)
	}
	mustEqualStatus(t, pn, ns)

	// check heartbeat
	ctx, cancel := context.WithCancel(context.Background())
	node.Heartbeat(ctx)
	time.Sleep(2 * time.Second)
	ns.IsAlive = true
	mustEqualStatus(t, pn, ns)

	// test unregister
	err = node.Unregister(ctx)
	if err != nil {
		t.Fatal("unregister err: %v", err)
	}

	// cancel context
	cancel()
}

func mustEqualStatus(t *testing.T, node *pumpNode, status *NodeStatus) {
	ctx := context.Background()
	ns, err := node.Node(ctx, node.ID())
	if err != nil {
		t.Fatalf("get node status err: %v", err)
	}

	if ns.Host != status.Host || ns.NodeID != status.NodeID || ns.IsAlive != status.IsAlive {
		t.Fatalf("get status %v, excepted status %v", ns, status)
	}
}
