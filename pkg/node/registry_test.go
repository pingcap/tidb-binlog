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

package node

import (
	"path"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"go.etcd.io/etcd/tests/v3/integration"
	"golang.org/x/net/context"

	"github.com/pingcap/tidb-binlog/pkg/etcd"
)

var _ = Suite(&testRegistrySuite{})
var nodePrefix = path.Join(DefaultRootPath, NodePrefix[PumpNode])

type testRegistrySuite struct{}

type RegisrerTestClient interface {
	Node(context.Context, string, string) (*Status, error)
}

var testEtcdCluster *integration.ClusterV3

func TestNode(t *testing.T) {
	testEtcdCluster = integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	TestingT(t)
}

func (t *testRegistrySuite) TestUpdateNodeInfo(c *C) {
	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), DefaultRootPath)
	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)
	ns := &Status{
		NodeID:  "test",
		Addr:    "test",
		State:   Online,
		IsAlive: true,
	}

	err := r.UpdateNode(context.Background(), nodePrefix, ns)
	c.Assert(err, IsNil)
	mustEqualStatus(c, r, ns.NodeID, ns)

	ns.Addr = "localhost:1234"
	err = r.UpdateNode(context.Background(), nodePrefix, ns)
	c.Assert(err, IsNil)
	mustEqualStatus(c, r, ns.NodeID, ns)
	// use Nodes() to query node status
	ss, err := r.Nodes(context.Background(), nodePrefix)
	c.Assert(err, IsNil)
	c.Assert(ss, HasLen, 1)
}

func (t *testRegistrySuite) TestRegisterNode(c *C) {
	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), DefaultRootPath)
	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	ns := &Status{
		NodeID:  "test",
		Addr:    "test",
		State:   Online,
		IsAlive: true,
	}
	err := r.UpdateNode(context.Background(), nodePrefix, ns)
	c.Assert(err, IsNil)
	mustEqualStatus(c, r, ns.NodeID, ns)

	ns.State = Offline
	err = r.UpdateNode(context.Background(), nodePrefix, ns)
	c.Assert(err, IsNil)
	mustEqualStatus(c, r, ns.NodeID, ns)

	// TODO: now don't have function to delete node, maybe do it later
	//err = r.UnregisterNode(context.Background(), nodePrefix, ns.NodeID)
	//c.Assert(err, IsNil)
	//exist, err := r.checkNodeExists(context.Background(), nodePrefix, ns.NodeID)
	//c.Assert(err, IsNil)
	//c.Assert(exist, IsFalse)
}

func (t *testRegistrySuite) TestRefreshNode(c *C) {
	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), DefaultRootPath)
	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	ns := &Status{
		NodeID:  "test",
		Addr:    "test",
		State:   Online,
		IsAlive: true,
	}
	err := r.UpdateNode(context.Background(), nodePrefix, ns)
	c.Assert(err, IsNil)

	ns.IsAlive = true
	mustEqualStatus(c, r, ns.NodeID, ns)

	// TODO: fix it later
	//time.Sleep(2 * time.Second)
	//ns.IsAlive = false
	//mustEqualStatus(c, r, ns.NodeID, ns)
}

func (t *testRegistrySuite) TestAnalyzeNodeID(c *C) {
	c.Assert(AnalyzeNodeID("/tidb-binlog/v1/pumps/v1NodeID"), Equals, "v1NodeID")
	c.Assert(AnalyzeNodeID("/tidb-binlog/pumps/legacyNodeID"), Equals, "legacyNodeID")
	c.Assert(AnalyzeNodeID("????"), Equals, "")
}

func mustEqualStatus(c *C, r RegisrerTestClient, nodeID string, status *Status) {
	ns, err := r.Node(context.Background(), nodePrefix, nodeID)
	c.Assert(err, IsNil)
	c.Assert(ns, DeepEquals, status)
}

type checkNodeExistsSuite struct{}

var _ = Suite(&checkNodeExistsSuite{})

func (s *checkNodeExistsSuite) TestNotExist(c *C) {
	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), DefaultRootPath)
	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	exist, err := r.checkNodeExists(context.Background(), "drainer", "404")
	c.Assert(err, IsNil)
	c.Assert(exist, IsFalse)
}

func (s *checkNodeExistsSuite) TestExist(c *C) {
	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), DefaultRootPath)
	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	ctx := context.Background()
	if err := r.client.Create(ctx, "/tidb-binlog/v1", "pump", nil); err != nil {
		c.Fatal("Can't create node for testing")
	}
	exist, err := r.checkNodeExists(ctx, "/tidb-binlog", "v1")
	c.Assert(err, IsNil)
	c.Assert(exist, IsTrue)
}
