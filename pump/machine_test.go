package pump

import (
       	"testing"
       	"time"

       	"github.com/pingcap/tidb-binlog/machine"
       	"github.com/pingcap/tidb-binlog/binlog/scheme"
       	"github.com/coreos/etcd/integration"
       	"github.com/pingcap/tidb-binlog/pkg/etcd"
)


func TestMachine(t *testing.T) {
       	etcdclient, cluster := testSetup(t)
       	defer cluster.Terminate(t)

       	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

       	machID := "test1"
       	host   := "mytest"

       	err := r.RegisterMachine(machID, host)
       	if err != nil {
       		t.Fatal(err)
       	}

       	_, err = r.Machine(machID)
       	if err != nil {
       		t.Fatal(err)
       	}

       	err = r.RegisterMachine(machID, host)
        if err == nil {
                t.Fatal(err)
        }
}

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

       	if status.MachID != machID || status.MachInfo.Offset.Offset != 0 ||
       	status.MachInfo.Offset.Suffix != 0 {
       		t.Fatalf("machine info have error : %v", status)
       	}

       	newMachineInfo := &machine.MachineInfo {
       		Host : 		host,
       		Offset:		scheme.BinlogOffset {
       			Suffix: 1,
       			Offset: 1,
       		},
       	}

       	err = r.UpdateMeachineInfo(machID, newMachineInfo)
       	if err != nil {
       		t.Fatal(err)
       	}

       	status, err = r.Machine(machID)
        if err != nil {
                t.Fatal(err)
        }

        if status.MachID != machID || status.MachInfo.Offset.Suffix != 1 ||
        status.MachInfo.Offset.Offset != 1 {
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

       	err =  r.RefreshMachine(machID, 2)
       	if err != nil {
                t.Fatal(err)
        }

        status, err := r.Machine(machID)
        if err != nil {
                t.Fatal(err)
        }

        if status.MachID != machID || status.MachInfo.Offset.Suffix != 0 ||
        status.MachInfo.Offset.Offset != 0 || !status.IsAlive {
                t.Fatalf("machine info have error : %v", status)
        }

       	time.Sleep(3*time.Second)

       	status, err = r.Machine(machID)
        if err != nil {
                t.Fatal(err)
        }
        if status.MachID != machID || status.MachInfo.Offset.Suffix != 0 ||
        status.MachInfo.Offset.Offset != 0 || status.IsAlive {
                t.Fatalf("machine info have error : %v", status)
        }
}

func testSetup(t *testing.T)  (*etcd.Etcd, *integration.ClusterV3)  {
       	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size:1})
       	etcdclient := etcd.NewEtcd(cluster.RandClient(), "binlog", time.Duration(5))
       	return etcdclient, cluster
}
