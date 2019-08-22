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

package pump

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/node"
	pkgnode "github.com/pingcap/tidb-binlog/pkg/node"
	"github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var _ = Suite(&testNodeSuite{})

type testNodeSuite struct{}

type RegisrerTestClient interface {
	Node(context.Context, string, string) (*pkgnode.Status, error)
}

func (t *testNodeSuite) TestNode(c *C) {
	tmpDir := c.MkDir()

	etcdClient := testEtcdCluster.RandClient()
	listenAddr := "http://127.0.0.1:8250"
	hostName, err := os.Hostname()
	c.Assert(err, IsNil)
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

	node, err := NewPumpNode(cfg, func() int64 { return 0 })
	c.Assert(err, IsNil)

	testCheckNodeID(c, node, exceptedNodeID)
	testInteracWithEtcd(c, node)
}

func testCheckNodeID(c *C, node pkgnode.Node, exceptedID string) {
	c.Assert(node.ID(), Equals, exceptedID)
	c.Assert(node.ShortID(), Equals, exceptedID[:shortIDLen])
}

func testInteracWithEtcd(c *C, node pkgnode.Node) {
	pn := node.(*pumpNode)
	ns := &pkgnode.Status{
		NodeID:  pn.status.NodeID,
		Addr:    pn.status.Addr,
		State:   pkgnode.Online,
		IsAlive: true,
	}

	// check register
	err := node.RefreshStatus(context.Background(), ns)
	c.Assert(err, IsNil)
	mustEqualStatus(c, node.(*pumpNode), pn.status.NodeID, ns)
}

func mustEqualStatus(c *C, r RegisrerTestClient, nodeID string, status *pkgnode.Status) {
	ns, err := r.Node(context.Background(), nodePrefix, nodeID)
	c.Assert(err, IsNil)
	c.Assert(ns, DeepEquals, status)
}

type ReadLocalNodeIDSuite struct{}

var _ = Suite(&ReadLocalNodeIDSuite{})

func (s *ReadLocalNodeIDSuite) TestReturnNotFoundErr(c *C) {
	dir := c.MkDir()
	_, err := readLocalNodeID(dir)
	c.Assert(errors.IsNotFound(err), IsTrue)
}

func (s *ReadLocalNodeIDSuite) TestIsDirectory(c *C) {
	dir := c.MkDir()
	nodeIDPath := filepath.Join(dir, nodeIDFile)
	if err := os.Mkdir(nodeIDPath, 0755); err != nil {
		c.Fatal("Fail to create dir for testing")
	}
	_, err := readLocalNodeID(dir)
	c.Assert(err, NotNil)
	c.Assert(errors.IsNotFound(err), IsFalse)
}

func (s *ReadLocalNodeIDSuite) TestCanReadNodeID(c *C) {
	dir := c.MkDir()
	nodeIDPath := filepath.Join(dir, nodeIDFile)
	if err := ioutil.WriteFile(nodeIDPath, []byte(" this-node\n"), 0644); err != nil {
		c.Fatal("Fail to write file for testing")
	}
	nodeID, err := readLocalNodeID(dir)
	c.Assert(err, IsNil)
	c.Assert(nodeID, Equals, "this-node")
}

type heartbeatSuite struct{}

var _ = Suite(&heartbeatSuite{})

func (s *heartbeatSuite) TestShouldCloseErrorChannel(c *C) {
	cli := etcd.NewClient(testEtcdCluster.RandClient(), "heartbeat")
	registry := node.NewEtcdRegistry(cli, time.Second)
	p := pumpNode{
		heartbeatInterval: time.Second,
		status:            &node.Status{},
		EtcdRegistry:      registry,
		getMaxCommitTs: func() int64 {
			return 42
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	errc := p.Heartbeat(ctx)
	cancel()
	select {
	case _, ok := <-errc:
		c.Assert(ok, IsFalse)
	case <-time.After(time.Second):
		c.Fatal("Doesn't close errc in time")
	}
}

func (s *heartbeatSuite) TestShouldUpdateStatus(c *C) {
	cli := etcd.NewClient(testEtcdCluster.RandClient(), "heartbeat")
	registry := node.NewEtcdRegistry(cli, time.Second)
	status := node.Status{}
	var maxCommitTs int64
	p := pumpNode{
		heartbeatInterval: 10 * time.Millisecond,
		status:            &status,
		EtcdRegistry:      registry,
		getMaxCommitTs: func() int64 {
			maxCommitTs++
			return maxCommitTs
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.Heartbeat(ctx)
	time.Sleep(3 * p.heartbeatInterval)
	cancel()
	c.Assert(p.status.MaxCommitTS, Greater, int64(0))
	c.Assert(p.status.MaxCommitTS, LessEqual, int64(3))
}

type notifyDrainerSuite struct{}

var _ = Suite(&notifyDrainerSuite{})

type mockDrainerServer struct {
	binlog.CisternServer
	notified bool
}

func (s *mockDrainerServer) Notify(ctx context.Context, in *binlog.NotifyReq) (*binlog.NotifyResp, error) {
	s.notified = true
	return nil, nil
}

func (s *notifyDrainerSuite) TestNotifyDrainer(c *C) {
	// start mock drainer server
	host := "127.0.0.1:8249"
	lis, err := net.Listen("tcp", host)
	c.Assert(err, IsNil)
	gs := grpc.NewServer()
	mockDrainerServerImpl := &mockDrainerServer{notified: false}
	binlog.RegisterCisternServer(gs, mockDrainerServerImpl)
	defer gs.Stop()
	go gs.Serve(lis)

	// update drainer info in etcd
	cli := etcd.NewClient(testEtcdCluster.RandClient(), "drainers")
	registry := node.NewEtcdRegistry(cli, time.Second)
	pNode := &pumpNode{
		EtcdRegistry: registry,
		status:       &node.Status{State: node.Online, NodeID: "pump_notify", Addr: "http://192.168.199.100:8260"},
		getMaxCommitTs: func() int64 {
			return 0
		},
	}
	drainerNodeStatus := &node.Status{
		NodeID: "pump_notify_test",
		State:  node.Online,
		Addr:   host,
	}
	err = registry.UpdateNode(context.Background(), "drainers", drainerNodeStatus)
	c.Assert(err, IsNil)

	// notify the mocked drainer
	err = pNode.Notify(context.Background())
	c.Assert(err, IsNil)
	c.Assert(mockDrainerServerImpl.notified, IsTrue)
}
