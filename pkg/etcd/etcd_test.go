package etcd

import (
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/integration"
	"github.com/juju/errors"
	"golang.org/x/net/context"
)

func TestCreate(t *testing.T) {
	ctx, etcdCli, cluster := testSetup(t)
	defer cluster.Terminate(t)
	etcdClient := cluster.RandClient()

	key := "binlog/testkey"
	obj := "test"

	// verify that kv pair is empty before set
	getResp, err := etcdClient.KV.Get(ctx, key)
	if err != nil {
		t.Fatalf("etcdClient.KV.Get failed: %v", err)
	}

	if len(getResp.Kvs) != 0 {
		t.Fatalf("expecting empty result on key: %s", key)
	}

	err = etcdCli.Create(ctx, key, obj, nil)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	getResp, err = etcdClient.KV.Get(ctx, key)
	if err != nil {
		t.Fatalf("etcdClient.KV.Get failed: %v", err)
	}

	if len(getResp.Kvs) == 0 {
		t.Fatalf("expecting non empty result on key: %s", key)
	}
}

func TestCreateWithTTL(t *testing.T) {
	ctx, etcdCli, cluster := testSetup(t)
	defer cluster.Terminate(t)

	key := "binlog/ttlkey"
	input := "ttltest"

	lcr, err := etcdCli.client.Lease.Grant(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	opts := []clientv3.OpOption{clientv3.WithLease(clientv3.LeaseID(lcr.ID))}

	if err := etcdCli.Create(ctx, key, input, opts); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	time.Sleep(2 * time.Second)
	_, err = etcdCli.Get(ctx, key)
	if err == nil || !errors.IsNotFound(err) {
		t.Fatalf("ttl failed: %v", err)
	}
}

func TestCreateWithKeyExist(t *testing.T) {
	ctx, etcdCli, cluster := testSetup(t)
	defer cluster.Terminate(t)
	obj := "existtest"
	key := "binlog/exist"

	etcdClient := cluster.RandClient()
	_, err := etcdClient.KV.Put(ctx, key, obj, nil...)
	if err != nil {
		t.Fatalf("etcdClient.KV.put failed: %v", err)
	}

	err = etcdCli.Create(ctx, key, obj, nil)
	if err == nil || !errors.IsAlreadyExists(err) {
		t.Errorf("expecting key exists error, but get: %s", err)
	}
}

func TestUpdate(t *testing.T) {
	ctx, etcdCli, cluster := testSetup(t)
	defer cluster.Terminate(t)

	input := "updatetest"
	input2 := "updatetest2"
	key := "binlog/updatekey"

	err := etcdCli.Update(ctx, key, input, 2)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	time.Sleep(time.Second)

	err = etcdCli.Update(ctx, key, input2, 3)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	res, err := etcdCli.Get(ctx, key)
	if err != nil || string(res) != input2 {
		t.Fatalf("ttl failed: %v", err)
	}

	time.Sleep(2 * time.Second)
	res, err = etcdCli.Get(ctx, key)
	if err == nil || !errors.IsNotFound(err) {
		t.Fatalf("ttl failed: %v", err)
	}
}

func TestList(t *testing.T) {
	ctx, etcdCli, cluster := testSetup(t)
	defer cluster.Terminate(t)

	key := "binlog/testkey"

	k1 := key + "/level1"
	k2 := key + "/level2"
	k3 := key + "/level3"
	k11 := key + "/level1/level1"

	err := etcdCli.Create(ctx, k1, k1, nil)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	err = etcdCli.Create(ctx, k2, k2, nil)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	err = etcdCli.Create(ctx, k3, k3, nil)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	err = etcdCli.Create(ctx, k11, k11, nil)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	root, err := etcdCli.List(ctx, key)
	if err != nil {
		t.Fatalf("etcdClient.KV.Get failed: %v", err)
	}

	if string(root.Childs["level1"].Value) != k1 || string(root.Childs["level1"].Childs["level1"].Value) != k11 || string(root.Childs["level2"].Value) != k2 || string(root.Childs["level3"].Value) != k3 {
		t.Fatalf("list result is error: %v", root)
	}
}

func testSetup(t *testing.T) (context.Context, *Client, *integration.ClusterV3) {
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	etcd := NewClient(cluster.RandClient(), "binlog")
	ctx := context.Background()
	return ctx, etcd, cluster
}
