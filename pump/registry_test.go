package pump

import (
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"golang.org/x/net/context"
)

func TestUpdateNodeInfo(t *testing.T) {
	etcdclient, cluster := testSetup(t)
	defer cluster.Terminate(t)

	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	nodeID := "test1"
	host := "mytest"

	err := r.RegisterNode(context.Background(), nodeID, host)
	if err != nil {
		t.Fatal(err)
	}

	status, err := r.Node(context.Background(), nodeID)
	if err != nil {
		t.Fatal(err)
	}

	if status.NodeID != nodeID || status.Host != host {
		t.Fatalf("node info have error : %v", status)
	}

	host = "localhost:1234"
	err = r.UpdateNode(context.Background(), nodeID, host)
	if err != nil {
		t.Fatal(err)
	}

	status, err = r.Node(context.Background(), nodeID)
	if err != nil {
		t.Fatal(err)
	}

	if status.NodeID != nodeID || status.Host != host {
		t.Fatalf("node info have error : %v", status)
	}
}

func TestRefreshNode(t *testing.T) {
	etcdclient, cluster := testSetup(t)
	defer cluster.Terminate(t)

	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	nodeID := "test1"
	host := "mytest"

	err := r.RegisterNode(context.Background(), nodeID, host)
	if err != nil {
		t.Fatal(err)
	}

	err = r.RefreshNode(context.Background(), nodeID, 2)
	if err != nil {
		t.Fatal(err)
	}

	status, err := r.Node(context.Background(), nodeID)
	if err != nil {
		t.Fatal(err)
	}

	if status.NodeID != nodeID || !status.IsAlive {
		t.Fatalf("node info have error : %v", status)
	}

	time.Sleep(3 * time.Second)

	status, err = r.Node(context.Background(), nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if status.NodeID != nodeID || status.IsAlive {
		t.Fatalf("node info have error : %v", status)
	}
}

func testSetup(t *testing.T) (*etcd.Client, *integration.ClusterV3) {
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	etcdclient := etcd.NewClient(cluster.RandClient(), "binlog")
	return etcdclient, cluster
}
