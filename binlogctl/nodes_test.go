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
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"

	"github.com/coreos/etcd/integration"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/node"
)

func Test(t *testing.T) {
	testEtcdCluster = integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	TestingT(t)
}

var (
	_               = Suite(&testNodesSuite{})
	testEtcdCluster *integration.ClusterV3
	fakeRegistry    *node.EtcdRegistry
)

type testNodesSuite struct{}

func (s *testNodesSuite) SetUpTest(c *C) {
	newEtcdClientFromCfgFunc = newFakeEtcdClientFromCfg
	createRegistryFuc = createMockRegistry
	_, err := createMockRegistry("127.0.0.1:2379")
	c.Assert(err, IsNil)
}

func (s *testNodesSuite) TearDownTest(c *C) {
	newEtcdClientFromCfgFunc = etcd.NewClientFromCfg
	createRegistryFuc = createRegistry
}

func (s *testNodesSuite) TestApplyAction(c *C) {
	server, url := createMockPumpServer(c)
	defer server.Close()

	registerPumpForTest(c, "test", url)

	err := ApplyAction("127.0.0.1:2379", "pumps", "test2", PausePump)
	c.Assert(errors.IsNotFound(err), IsTrue)

	// TODO: handle log information and add check
	err = ApplyAction("127.0.0.1:2379", "pumps", "test", PausePump)
	c.Assert(err, IsNil)
}

func (s *testNodesSuite) TestQueryNodesByKind(c *C) {
	registerPumpForTest(c, "test", "127.0.0.1:8255")

	// TODO: handle log information and add check
	err := QueryNodesByKind("127.0.0.1:2379", "pumps")
	c.Assert(err, IsNil)
}

func (s *testNodesSuite) TestUpdateNodeState(c *C) {
	registerPumpForTest(c, "test", "127.0.0.1:8255")

	err := UpdateNodeState("127.0.0.1:2379", "pumps", "test2", node.Paused)
	c.Assert(err, ErrorMatches, ".*not found.*")

	err = UpdateNodeState("127.0.0.1:2379", "pumps", "test", node.Paused)
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

func (s *testNodesSuite) TestCreateRegistry(c *C) {
	urls := "127.0.0.1:2379"
	registry, err := createRegistry(urls)
	c.Assert(err, IsNil)
	c.Assert(registry, NotNil)

	ns := &node.Status{
		NodeID: "registry_test",
	}

	nodePrefix := path.Join(node.DefaultRootPath, node.NodePrefix["pumps"])
	err = registry.UpdateNode(context.Background(), nodePrefix, ns)
	if err != nil {
		c.Fatal(err)
	}

	nodes, err := registry.Nodes(context.Background(), nodePrefix)
	c.Assert(err, IsNil)

	var findNode bool
	for _, n := range nodes {
		if n.NodeID == "registry_test" {
			findNode = true
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
}

func createMockPumpServer(c *C) (*httptest.Server, string) {
	handler := &httpHandler{}
	server := httptest.NewServer(handler)

	return server, strings.TrimPrefix(server.URL, "http://")
}

func newFakeEtcdClientFromCfg([]string, time.Duration, string, *tls.Config) (*etcd.Client, error) {
	return etcd.NewClient(testEtcdCluster.RandClient(), node.DefaultRootPath), nil
}
