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

package zk_test

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/zk"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testZKSuite{})

type testZKSuite struct {
	controller *gomock.Controller
	mockConn   *MockConn
	client     *zk.Client
}

// FIXME: Cannot use the real SetUpTest/TearDownTest to set up the mock
// otherwise the mock error will be ignored.

func (s *testZKSuite) setUpTest(c *C) {
	s.controller = gomock.NewController(c)
	s.mockConn = NewMockConn(s.controller)
	s.client = zk.NewWithConnection(s.mockConn, nil)
}

func (s *testZKSuite) tearDownTest() {
	s.client.Close()
	s.controller.Finish()
}

func (s *testZKSuite) TestParseConnectionString(c *C) {
	nodes, chroot := zk.ParseConnectionString("host1:2181,host2:2181/chroot")
	c.Assert(nodes, DeepEquals, []string{"host1:2181", "host2:2181"})
	c.Assert(chroot, Equals, "/chroot")

	nodes, chroot = zk.ParseConnectionString("foo:1234/ch/root,with/comma")
	c.Assert(nodes, DeepEquals, []string{"foo:1234"})
	c.Assert(chroot, Equals, "/ch/root,with/comma")

	nodes, chroot = zk.ParseConnectionString("host3:2181,host4:2181,host5:2181")
	c.Assert(nodes, DeepEquals, []string{"host3:2181", "host4:2181", "host5:2181"})
	c.Assert(chroot, Equals, "")
}

func (s *testZKSuite) TestConnectToVoid(c *C) {
	_, err := zk.New(nil, nil)
	c.Assert(err, ErrorMatches, ".*server list must not be empty.*")
}

func (s *testZKSuite) TestConnectToUnreachableNetwork(c *C) {
	_, err := zk.NewFromConnectionString("host.is.invalid:2181/ch", time.Nanosecond, time.Nanosecond)
	c.Assert(err, NotNil)
}

func (s *testZKSuite) TestTopics(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	topics := []string{"a", "b", "c"}

	s.mockConn.EXPECT().
		Children("/brokers/topics").
		Return(topics, nil, nil)
	s.mockConn.EXPECT().Close()

	t, err := s.client.Topics()
	c.Assert(err, IsNil)
	c.Assert(t, DeepEquals, topics)
}

func (s *testZKSuite) TestPartitions(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	s.mockConn.EXPECT().
		Get("/brokers/topics/a").
		Return([]byte(`{"version":1,"partitions":{"0":[0,1,3]}}`), nil, nil)
	s.mockConn.EXPECT().Close()

	p, err := s.client.Partitions("a")
	c.Assert(err, IsNil)
	c.Assert(p, DeepEquals, []int32{0})
}

func (s *testZKSuite) setUpMockBrokers() {
	getIDs := s.mockConn.EXPECT().
		Children("/brokers/ids").
		Return([]string{"0", "1"}, nil, nil)

	s.mockConn.EXPECT().
		Get("/brokers/ids/0").
		After(getIDs).
		Return([]byte(`{"version":2,"host":"192.0.2.1","port":9092}`), nil, nil)
	s.mockConn.EXPECT().
		Get("/brokers/ids/1").
		After(getIDs).
		Return([]byte(`{"version":2,"host":"192.0.2.2","port":9092}`), nil, nil)
	s.mockConn.EXPECT().Close()
}

func (s *testZKSuite) TestBrokers(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	s.setUpMockBrokers()

	br, err := s.client.Brokers()
	c.Assert(err, IsNil)
	c.Assert(br, DeepEquals, map[int32]string{
		0: "192.0.2.1:9092",
		1: "192.0.2.2:9092",
	})
}

func (s *testZKSuite) TestKafkaUrls(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	s.setUpMockBrokers()

	urls, err := s.client.KafkaUrls()
	c.Assert(err, IsNil)
	c.Assert(urls, Matches, `(192\.0\.2\.1:9092,192\.0\.2\.2:9092|192\.0\.2\.2:9092,192\.0\.2\.1:9092)`)
}

func (s *testZKSuite) TestNoKafka(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()

	s.mockConn.EXPECT().Children("/brokers/ids").Return(nil, nil, nil)
	s.mockConn.EXPECT().Close()

	_, err := s.client.KafkaUrls()
	c.Assert(err, ErrorMatches, "kafka brokers not found in zookeeper")
}
