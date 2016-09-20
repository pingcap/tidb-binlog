package pump

import (
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	pb "github.com/pingcap/tidb-binlog/proto"
	"golang.org/x/net/context"
)

func TestUpdateNodeInfo(t *testing.T) {
	etcdclient, cluster := testSetup(t)
	defer cluster.Terminate(t)

	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	nodeID := "test1"
	host := "mytest"

	ctx, cancel := context.WithTimeout(context.Background(), r.reqTimeout)
	defer cancel()
	err := r.RegisterNode(ctx, nodeID, host)
	if err != nil {
		t.Fatal(err)
	}

	status, err := r.Node(ctx, nodeID)
	if err != nil {
		t.Fatal(err)
	}

	if status.NodeID != nodeID || len(status.Offsets) != 0 {
		t.Fatalf("node info have error : %v", status)
	}

	newNodeStatus := &NodeStatus{
		NodeID:  nodeID,
		Host:    host,
		Offsets: make(map[string]*pb.Pos),
	}

	newNodeStatus.Offsets["cluster1"] = &pb.Pos{
		Suffix: 1,
		Offset: 1,
	}

	err = r.UpdateNodeStatus(ctx, nodeID, newNodeStatus)
	if err != nil {
		t.Fatal(err)
	}

	status, err = r.Node(ctx, nodeID)
	if err != nil {
		t.Fatal(err)
	}

	if status.NodeID != nodeID || status.Offsets["cluster1"].Suffix != 1 ||
		status.Offsets["cluster1"].Offset != 1 {
		t.Fatalf("node info have error : %v", status)
	}
}

func TestRefreshNode(t *testing.T) {
	etcdclient, cluster := testSetup(t)
	defer cluster.Terminate(t)

	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	nodeID := "test1"
	host := "mytest"

	ctx, cancel := context.WithTimeout(context.Background(), r.reqTimeout)
	defer cancel()
	err := r.RegisterNode(ctx, nodeID, host)
	if err != nil {
		t.Fatal(err)
	}

	err = r.RefreshNode(ctx, nodeID, 2)
	if err != nil {
		t.Fatal(err)
	}

	status, err := r.Node(ctx, nodeID)
	if err != nil {
		t.Fatal(err)
	}

	if status.NodeID != nodeID || len(status.Offsets) != 0 || !status.IsAlive {
		t.Fatalf("node info have error : %v", status)
	}

	time.Sleep(3 * time.Second)

	status, err = r.Node(ctx, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if status.NodeID != nodeID || len(status.Offsets) != 0 || status.IsAlive {
		t.Fatalf("node info have error : %v", status)
	}
}

func testSetup(t *testing.T) (*etcd.Client, *integration.ClusterV3) {
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	etcdclient := etcd.NewClient(cluster.RandClient(), "binlog")
	return etcdclient, cluster
}
