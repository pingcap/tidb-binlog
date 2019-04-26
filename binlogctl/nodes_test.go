// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package binlogctl

import (
	"fmt"
	"time"
	"testing"
	"path"
	"context"

	"github.com/coreos/etcd/integration"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/node"
	. "github.com/pingcap/check"
)

type nodesSuite struct{}

var _ = Suite(&nodesSuite{})
var testEtcdCluster *integration.ClusterV3
var fakeRegistry *node.EtcdRegistry

func TestNode(t *testing.T) {
	testEtcdCluster = integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	createFakeRegistry("127.0.0.1:2379")
	TestingT(t)
}

func (s *nodesSuite) TestApplyAction(c *C) {
	err := ApplyAction("127.0.0.1:2379", "pump", "nodeID", PausePump)
	c.Assert(err, NotNil)
}

func (s *nodesSuite) TestQueryNodesByKind(c *C) {
	createRegistryFuc = createFakeRegistry
	defer func() {
		createRegistryFuc = createRegistry
	}()

	ns := &node.Status{
		NodeID:  "test",
		Addr:    "test",
		State:   node.Online,
		IsAlive: true,
	}

	c.Log("insert node")
	nodePrefix := path.Join(node.DefaultRootPath, node.NodePrefix["pumps"])
	err := fakeRegistry.UpdateNode(context.Background(), nodePrefix, ns)
	c.Assert(err, IsNil)

	c.Log("query node")
	err = QueryNodesByKind("127.0.0.1:2379", "pumps")
	c.Assert(err, IsNil)
}

func (s *nodesSuite) TestUpdateNodeState(c *C) {
	createRegistryFuc = createFakeRegistry
	defer func() {
		createRegistryFuc = createRegistry
	}()

	ns := &node.Status{
		NodeID:  "test",
		Addr:    "test",
		State:   node.Online,
		IsAlive: true,
	}

	nodePrefix := path.Join(node.DefaultRootPath, node.NodePrefix["pumps"])
	err := fakeRegistry.UpdateNode(context.Background(), nodePrefix, ns)
	c.Assert(err, IsNil)

	err = UpdateNodeState("127.0.0.1:2379", "pumps", "test", node.Paused)
	c.Assert(err, IsNil)
}

func createFakeRegistry(urls string) (*node.EtcdRegistry, error) {
	if fakeRegistry != nil {
		return fakeRegistry, nil
	}

	fmt.Println("create etcd client")
	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), node.DefaultRootPath)
	fmt.Println("create registry")
	fakeRegistry = node.NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)
	
	return fakeRegistry, nil
	/*
	ns := &Status{
		NodeID:  "test",
		Addr:    "test",
		State:   Online,
		IsAlive: true,
	}
	err := r.UpdateNode(context.Background(), nodePrefix, ns)
	c.Assert(err, IsNil)
	*/
}