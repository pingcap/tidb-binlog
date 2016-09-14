package pump

import (
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	pb "github.com/pingcap/tidb-binlog/proto"
)

func TestUpdateMachineInfo(t *testing.T) {
	etcdclient, cluster := testSetup(t)
	defer cluster.Terminate(t)

	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	machID := "test1"
	host := "mytest"

	err := r.RegisterMachine(machID, host)
	if err != nil {
		t.Fatal(err)
	}

	status, err := r.Machine(machID)
	if err != nil {
		t.Fatal(err)
	}

	if status.MachID != machID || len(status.Offsets) != 0 {
		t.Fatalf("machine info have error : %v", status)
	}

	newMachineStatus := &MachineStatus{
		MachID:  machID,
		Host:    host,
		Offsets: make(map[string]*pb.Pos),
	}

	newMachineStatus.Offsets["cluster1"] = &pb.Pos{
		Suffix: 1,
		Offset: 1,
	}

	err = r.UpdateMeachineStatus(machID, newMachineStatus)
	if err != nil {
		t.Fatal(err)
	}

	status, err = r.Machine(machID)
	if err != nil {
		t.Fatal(err)
	}

	if status.MachID != machID || status.Offsets["cluster1"].Suffix != 1 ||
		status.Offsets["cluster1"].Offset != 1 {
		t.Fatalf("machine info have error : %v", status)
	}
}

func TestRefreshMachine(t *testing.T) {
	etcdclient, cluster := testSetup(t)
	defer cluster.Terminate(t)

	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	machID := "test1"
	host := "mytest"

	err := r.RegisterMachine(machID, host)
	if err != nil {
		t.Fatal(err)
	}

	err = r.RefreshMachine(machID, 2)
	if err != nil {
		t.Fatal(err)
	}

	status, err := r.Machine(machID)
	if err != nil {
		t.Fatal(err)
	}

	if status.MachID != machID || len(status.Offsets) != 0 || !status.IsAlive {
		t.Fatalf("machine info have error : %v", status)
	}

	time.Sleep(3 * time.Second)

	status, err = r.Machine(machID)
	if err != nil {
		t.Fatal(err)
	}
	if status.MachID != machID || len(status.Offsets) != 0 || status.IsAlive {
		t.Fatalf("machine info have error : %v", status)
	}
}

func testSetup(t *testing.T) (*etcd.Client, *integration.ClusterV3) {
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	etcdclient := etcd.NewClient(cluster.RandClient(), "binlog")
	return etcdclient, cluster
}
