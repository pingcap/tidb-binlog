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
	"time"
	"testing"
	"path"
	"context"
	"net/http"
	"net/http/httptest"

	"github.com/coreos/etcd/integration"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/node"
	. "github.com/pingcap/check"
)

type nodesSuite struct{}

var (
	_ = Suite(&nodesSuite{})
	testEtcdCluster *integration.ClusterV3
	fakeRegistry *node.EtcdRegistry
)

func TestNode(t *testing.T) {
	testEtcdCluster = integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	createMockRegistry("127.0.0.1:2379")
	TestingT(t)
}

func (s *nodesSuite) TestApplyAction(c *C) {
	createRegistryFuc = createMockRegistry
	defer func() {
		createRegistryFuc = createRegistry
	}()

	url := createMockPumpServer(c)

	err := ApplyAction("127.0.0.1:2379", "pumps", "test2", PausePump)
	c.Assert(err, ErrorMatches, "nodeID test2 not found")

	registerPumpForTest(c, "test", url)
	// TODO: handle log information and add check
	err = ApplyAction("127.0.0.1:2379", "pumps", "test", PausePump)
	c.Assert(err, IsNil)
}

func (s *nodesSuite) TestQueryNodesByKind(c *C) {
	createRegistryFuc = createMockRegistry
	defer func() {
		createRegistryFuc = createRegistry
	}()

	registerPumpForTest(c, "test", "127.0.0.1:8255")

	// TODO: handle log information and add check
	err := QueryNodesByKind("127.0.0.1:2379", "pumps")
	c.Assert(err, IsNil)
}

func (s *nodesSuite) TestUpdateNodeState(c *C) {
	createRegistryFuc = createMockRegistry
	defer func() {
		createRegistryFuc = createRegistry
	}()

	registerPumpForTest(c, "test", "127.0.0.1:8255")

	err := UpdateNodeState("127.0.0.1:2379", "pumps", "test", node.Paused)
	c.Assert(err, IsNil)

	// check node's state is changed to paused
	nodePrefix := path.Join(node.DefaultRootPath, node.NodePrefix["pumps"])
	nodes, err := fakeRegistry.Nodes(context.Background(), nodePrefix)
	c.Assert(err, IsNil)
	var findNode bool
	for _, n := range nodes {
		if n.NodeID == "test" {
			findNode = true
			c.Assert(n.State, Equals, node.Paused)
		}
	}
	c.Assert(findNode, IsTrue)
}

func createMockRegistry(urls string) (*node.EtcdRegistry, error) {
	if fakeRegistry != nil {
		return fakeRegistry, nil
	}

	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), node.DefaultRootPath)
	fakeRegistry = node.NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)
	
	return fakeRegistry, nil
}

func registerPumpForTest(c *C, nodeID, address string) {
	ns := &node.Status{
		NodeID:  nodeID,
		Addr:    address,
		State:   node.Online,
		IsAlive: true,
	}

	nodePrefix := path.Join(node.DefaultRootPath, node.NodePrefix["pumps"])
	err := fakeRegistry.UpdateNode(context.Background(), nodePrefix, ns)
	if err != nil {
		c.Fatal(err)
	}
}

type httpHandler struct {
}

func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	return
}

func createMockPumpServer(c *C) string {
	handler := &httpHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	return server.URL
}