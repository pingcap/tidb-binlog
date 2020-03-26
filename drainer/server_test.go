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

package drainer

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"
	"time"

	"github.com/gorilla/mux"
	. "github.com/pingcap/check"
	pd "github.com/pingcap/pd/v3/client"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/node"
	"github.com/pingcap/tidb-binlog/pkg/security"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var _ = Suite(&testServerSuite{})

type testServerSuite struct{}

func (t *testServerSuite) TestGetLatestTS(c *C) {
	cp := dummyCheckpoint{commitTS: 1984}
	server := Server{
		syncer: &Syncer{
			cp: &cp,
		},
	}
	router := server.initAPIRouter()

	req := httptest.NewRequest("GET", "/commit_ts", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	body, _ := ioutil.ReadAll(resp.Body)
	var decoded util.Response
	err := json.Unmarshal(body, &decoded)
	c.Assert(err, IsNil)
	c.Assert(decoded.Code, Equals, 200)
	data, ok := decoded.Data.(map[string]interface{})
	c.Assert(ok, IsTrue)
	c.Assert(data, HasKey, "ts")
	ts, ok := data["ts"].(float64)
	c.Assert(ok, IsTrue)
	c.Assert(int64(ts), Equals, int64(1984))
}

func (t *testServerSuite) TestNotify(c *C) {
	server := Server{
		collector: &Collector{
			notifyChan: make(chan *notifyResult, 1),
		},
	}
	go func() {
		_, err := server.Notify(context.Background(), nil)
		c.Assert(err, IsNil)
	}()
	select {
	case <-server.collector.notifyChan:
	case <-time.After(time.Second):
		c.Fatal("Doesn't receive notify in time")
	}
}

func (t *testServerSuite) TestClose(c *C) {
	cli := etcd.NewClient(testEtcdCluster.RandClient(), node.DefaultRootPath)
	reg := node.NewEtcdRegistry(cli, 5*time.Second)
	var canceled bool
	cp := dummyCheckpoint{}
	server := Server{
		status:    &node.Status{MaxCommitTS: 1024},
		collector: &Collector{reg: reg},
		syncer: &Syncer{
			shutdown: make(chan struct{}),
			closed:   make(chan struct{}),
			cp:       &cp,
		},
		cp: &cp,
		cancel: func() {
			canceled = true
		},
		gs: grpc.NewServer(),
	}
	// Close the syncer closed signal channel manually to avoid blocking
	close(server.syncer.closed)
	server.Close()
	c.Assert(canceled, IsTrue)
	c.Assert(server.isClosed, Equals, int32(1))
}

func TestCommitStatus(t *testing.T) {
	cli := etcd.NewClient(testEtcdCluster.RandClient(), node.DefaultRootPath)
	reg := node.NewEtcdRegistry(cli, 5*time.Second)

	cases := []struct {
		input    string
		expected string
		ts       int64
	}{
		{input: node.Pausing, expected: node.Paused, ts: 2019},
		{input: node.Closing, expected: node.Offline, ts: 2042},
	}
	for _, cs := range cases {
		status := node.Status{State: cs.input}
		s := Server{
			status:    &status,
			collector: &Collector{reg: reg},
			syncer: &Syncer{
				cp: &dummyCheckpoint{commitTS: cs.ts},
			},
		}
		s.commitStatus()
		if s.status.State != cs.expected {
			t.Fatalf("Invalid state after commit: expect %s, get %s", cs.expected, s.status.State)
		}
		if s.status.MaxCommitTS != cs.ts {
			t.Fatalf("MaxCommitTS not updated correctly: expect %d, get %d", cs.ts, s.status.MaxCommitTS)
		}
	}
}

type applyActionSuite struct {
	server Server
	router *mux.Router
}

var _ = Suite(&applyActionSuite{})

func (s *applyActionSuite) SetUpTest(c *C) {
	s.server = Server{
		status: &node.Status{State: node.Online},
		ID:     "drainer-18",
		// Set `isClosed` to avoid actually running the `Close` method,
		// which involves a lot of components that's not directly relevant
		// to this test and difficult to mock.
		isClosed: 1,
	}
	s.router = s.server.initAPIRouter()
}

func (s *applyActionSuite) TestShouldCheckNodeID(c *C) {
	req := httptest.NewRequest("PUT", "/state/drainer-42/pause", nil)
	w := httptest.NewRecorder()

	s.router.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var decoded util.Response
	err := json.Unmarshal(body, &decoded)
	c.Assert(err, IsNil)
	c.Assert(decoded.Code, Equals, 3)
	c.Assert(decoded.Message, Matches, ".*invalid nodeID drainer-42.*")
}

func (s *applyActionSuite) TestShouldCheckState(c *C) {
	s.server.status.State = node.Paused

	req := httptest.NewRequest("PUT", "/state/drainer-18/close", nil)
	w := httptest.NewRecorder()

	s.router.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var decoded util.Response
	err := json.Unmarshal(body, &decoded)
	c.Assert(err, IsNil)
	c.Assert(decoded.Code, Equals, 3)
	c.Assert(decoded.Message, Matches, ".*apply close failed!.*")
}

func (s *applyActionSuite) TestShouldApplyAction(c *C) {
	cases := map[string]string{
		"pause": node.Pausing,
		"close": node.Closing,
		"run":   "",
	}
	for input, expect := range cases {
		s.server.status.State = node.Online
		req := httptest.NewRequest("PUT", "/state/drainer-18/"+input, nil)
		w := httptest.NewRecorder()

		s.router.ServeHTTP(w, req)

		resp := w.Result()
		body, _ := ioutil.ReadAll(resp.Body)

		c.Assert(resp.StatusCode, Equals, http.StatusOK)

		var decoded util.Response
		err := json.Unmarshal(body, &decoded)
		c.Assert(err, IsNil)
		if expect != "" {
			c.Assert(decoded.Code, Equals, 200)
			c.Assert(decoded.Message, Matches, ".*success.*")
			c.Assert(s.server.status.State, Equals, expect)
		} else {
			c.Assert(decoded.Code, Equals, 3)
			c.Assert(decoded.Message, Matches, ".*invalid action.*")
			c.Assert(s.server.status.State, Equals, node.Online) // State unchanged
		}
	}
}

type heartbeatSuite struct{}

var _ = Suite(&heartbeatSuite{})

func (s *heartbeatSuite) TestShouldStopWhenDone(c *C) {
	cli := etcd.NewClient(testEtcdCluster.RandClient(), node.DefaultRootPath)
	reg := node.NewEtcdRegistry(cli, 5*time.Second)

	server := Server{
		status:    &node.Status{},
		collector: &Collector{reg: reg},
		syncer: &Syncer{
			cp: &dummyCheckpoint{},
		},
		isClosed: 1, // Hack to avoid actually running close
	}

	ctx, cancel := context.WithCancel(context.Background())
	errc := server.heartbeat(ctx)
	cancel()
	select {
	case <-errc:
	case <-time.After(time.Second):
		c.Fatal("Doesn't stop in time")
	}
}

func (s *heartbeatSuite) TestShouldUpdateStatusPeriodically(c *C) {
	origInterval := heartbeatInterval
	heartbeatInterval = time.Millisecond
	defer func() {
		heartbeatInterval = origInterval
	}()
	cli := etcd.NewClient(testEtcdCluster.RandClient(), node.DefaultRootPath)
	reg := node.NewEtcdRegistry(cli, 5*time.Second)

	cp := dummyCheckpoint{commitTS: 1024}
	server := Server{
		status:    &node.Status{MaxCommitTS: 1024},
		collector: &Collector{reg: reg},
		syncer: &Syncer{
			cp: &cp,
		},
		isClosed: 1, // Hack to avoid actually running close
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = server.heartbeat(ctx)
	c.Assert(server.status.MaxCommitTS, Equals, int64(1024))
	cp.commitTS = 100200
	time.Sleep(heartbeatInterval * 3)
	c.Assert(server.status.MaxCommitTS, Equals, int64(100200))
}

type mockPdCli struct {
	pd.Client
}

func (pc *mockPdCli) GetClusterID(ctx context.Context) uint64 {
	return 8012
}

func (pc *mockPdCli) GetTS(ctx context.Context) (int64, int64, error) {
	return 112233, 12, nil
}

func (pc *mockPdCli) Close() {}

type newServerSuite struct {
	origGetPdCli func(string, security.Config) (pd.Client, error)
}

var _ = Suite(&newServerSuite{})

func (s *newServerSuite) SetUpTest(c *C) {
	s.origGetPdCli = getPdClient
}

func (s *newServerSuite) TearDownTest(c *C) {
	getPdClient = s.origGetPdCli
}

func (s *newServerSuite) TestCannotCreateDataDir(c *C) {
	tmpfile, err := ioutil.TempFile("", "test")
	if err != nil {
		c.Fatal("Failed to create temp file.")
	}
	defer os.Remove(tmpfile.Name())

	cfg := NewConfig()
	cfg.DataDir = tmpfile.Name()
	cfg.ListenAddr = "http://" + cfg.ListenAddr
	_, err = NewServer(cfg)
	c.Assert(err, ErrorMatches, ".*mkdir.*")
}

func (s *newServerSuite) TestCannotGetPdCli(c *C) {
	getPdClient = func(etcdURLs string, securityConfig security.Config) (pd.Client, error) {
		return nil, errors.New("Pd")
	}
	cfg := NewConfig()
	cfg.DataDir = path.Join(c.MkDir(), "drainer")
	cfg.ListenAddr = "http://" + cfg.ListenAddr
	_, err := NewServer(cfg)
	c.Assert(err, ErrorMatches, "Pd")
}

func (s *newServerSuite) TestInvalidDestDBType(c *C) {
	getPdClient = func(etcdURLs string, securityConfig security.Config) (pd.Client, error) {
		return &mockPdCli{}, nil
	}
	cfg := NewConfig()
	cfg.DataDir = path.Join(c.MkDir(), "drainer")
	cfg.ListenAddr = "http://" + cfg.ListenAddr
	cfg.SyncerCfg.DestDBType = "nothing"
	cfg.adjustConfig()
	_, err := NewServer(cfg)
	c.Assert(err, ErrorMatches, ".*unknown DestDBType.*")
	c.Assert(cfg.SyncerCfg.To.ClusterID, Equals, uint64(8012))
}
