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
	"bytes"
	"io/ioutil"
	"path"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/coreos/etcd/integration"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	dsync "github.com/pingcap/tidb-binlog/drainer/sync"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pkg/zk"
	gozk "github.com/samuel/go-zookeeper/zk"
)

var testEtcdCluster *integration.ClusterV3

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	testEtcdCluster = integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)
	TestingT(t)
}

var _ = Suite(&testDrainerSuite{})

type testDrainerSuite struct{}

func (t *testDrainerSuite) TestConfig(c *C) {
	args := []string{
		"-metrics-addr", "192.168.15.10:9091",
		"-txn-batch", "1",
		"-data-dir", "data.drainer",
		"-dest-db-type", "mysql",
		"-config", "../cmd/drainer/drainer.toml",
		"-addr", "192.168.15.10:8257",
		"-advertise-addr", "192.168.15.10:8257",
	}

	cfg := NewConfig()
	err := cfg.Parse(args)
	c.Assert(err, IsNil)
	c.Assert(cfg.MetricsAddr, Equals, "192.168.15.10:9091")
	c.Assert(cfg.DataDir, Equals, "data.drainer")
	c.Assert(cfg.SyncerCfg.TxnBatch, Equals, 1)
	c.Assert(cfg.SyncerCfg.DestDBType, Equals, "mysql")
	c.Assert(cfg.SyncerCfg.To.Host, Equals, "127.0.0.1")
	var strSQLMode *string
	c.Assert(cfg.SyncerCfg.StrSQLMode, Equals, strSQLMode)
	c.Assert(cfg.SyncerCfg.SQLMode, Equals, mysql.SQLMode(0))
}

func (t *testDrainerSuite) TestValidate(c *C) {
	cfg := NewConfig()

	cfg.ListenAddr = "http://123：9091"
	err := cfg.validate()
	c.Assert(err, ErrorMatches, ".*invalid addr.*")

	cfg.ListenAddr = "http://192.168.10.12:9091"
	err = cfg.validate()
	c.Assert(err, ErrorMatches, ".*invalid advertise-addr.*")

	cfg.AdvertiseAddr = "http://192.168.10.12:9091"
	cfg.EtcdURLs = "127.0.0.1:2379,127.0.0.1:2380"
	err = cfg.validate()
	c.Assert(err, ErrorMatches, ".*EtcdURLs.*")

	cfg.EtcdURLs = "http://127.0.0.1,http://192.168.12.12"
	err = cfg.validate()
	c.Assert(err, ErrorMatches, ".*EtcdURLs.*")

	cfg.EtcdURLs = "http://127.0.0.1:2379,http://192.168.12.12:2379"
	err = cfg.validate()
	c.Assert(err, IsNil)

	cfg.Compressor = "urada"
	err = cfg.validate()
	c.Assert(err, ErrorMatches, ".*Invalid compressor.*")

	cfg.Compressor = "gzip"
	err = cfg.validate()
	c.Assert(err, IsNil)
}

func (t *testDrainerSuite) TestAdjustConfig(c *C) {
	cfg := NewConfig()
	cfg.SyncerCfg.DestDBType = "pb"
	cfg.SyncerCfg.WorkerCount = 10
	cfg.SyncerCfg.DisableDispatch = false

	err := cfg.adjustConfig()
	c.Assert(err, IsNil)
	c.Assert(cfg.SyncerCfg.DestDBType, Equals, "file")
	c.Assert(cfg.SyncerCfg.WorkerCount, Equals, 1)
	c.Assert(cfg.SyncerCfg.DisableDispatch, IsTrue)

	cfg = NewConfig()
	err = cfg.adjustConfig()
	c.Assert(err, IsNil)
	c.Assert(cfg.ListenAddr, Equals, "http://"+util.DefaultListenAddr(8249))
	c.Assert(cfg.AdvertiseAddr, Equals, cfg.ListenAddr)

	cfg = NewConfig()
	cfg.ListenAddr = "0.0.0.0:8257"
	cfg.AdvertiseAddr = "192.168.15.12:8257"
	err = cfg.adjustConfig()
	c.Assert(err, IsNil)
	c.Assert(cfg.ListenAddr, Equals, "http://0.0.0.0:8257")
	c.Assert(cfg.AdvertiseAddr, Equals, "http://192.168.15.12:8257")
}

func (t *testDrainerSuite) TestConfigParsingFileWithInvalidOptions(c *C) {
	yc := struct {
		DataDir                string `toml:"data-dir" json:"data-dir"`
		ListenAddr             string `toml:"addr" json:"addr"`
		AdvertiseAddr          string `toml:"advertise-addr" json:"advertise-addr"`
		UnrecognizedOptionTest bool   `toml:"unrecognized-option-test" json:"unrecognized-option-test"`
	}{
		"data.drainer",
		"192.168.15.10:8257",
		"192.168.15.10:8257",
		true,
	}

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(yc)
	c.Assert(err, IsNil)

	configFilename := path.Join(c.MkDir(), "drainer_config_invalid.toml")
	err = ioutil.WriteFile(configFilename, buf.Bytes(), 0644)
	c.Assert(err, IsNil)

	args := []string{
		"--config",
		configFilename,
		"-L", "debug",
	}

	cfg := NewConfig()
	err = cfg.Parse(args)
	c.Assert(err, ErrorMatches, ".*contained unknown configuration options: unrecognized-option-test.*")
}

var _ = Suite(&testKafkaSuite{})

type testKafkaSuite struct {
	origNewZKFromConnectionString func(connectionString string, dialTimeout, sessionTimeout time.Duration) (*zk.Client, error)
}

func (t *testKafkaSuite) SetUpTest(c *C) {
	t.origNewZKFromConnectionString = newZKFromConnectionString
}

func (t *testKafkaSuite) TearDownTest(c *C) {
	newZKFromConnectionString = t.origNewZKFromConnectionString
}

type MockConn struct {
}

func (m *MockConn) Close() {
}
func (m *MockConn) Children(path string) ([]string, *gozk.Stat, error) {
	return []string{"0", "1"}, nil, nil
}
func (m *MockConn) Get(path string) ([]byte, *gozk.Stat, error) {
	if path[len(path)-1] == '0' {
		return []byte(`{"version":2,"host":"192.0.2.1","port":9092}`), nil, nil
	} else if path[len(path)-1] == '1' {
		return []byte(`{"version":2,"host":"192.0.2.2","port":9092}`), nil, nil
	}
	return nil, nil, nil
}

func (t *testKafkaSuite) TestConfigDestDBTypeKafka(c *C) {
	args := []string{
		"-metrics-addr", "192.168.15.10:9091",
		"-txn-batch", "1",
		"-data-dir", "data.drainer",
		"-dest-db-type", "kafka",
		"-config", "../cmd/drainer/drainer.toml",
		"-addr", "192.168.15.10:8257",
		"-advertise-addr", "192.168.15.10:8257",
	}
	newZKFromConnectionString = func(connectionString string, dialTimeout, sessionTimeout time.Duration) (client *zk.Client, e error) {
		return zk.NewWithConnection(&MockConn{}, nil), nil
	}

	// Without Zookeeper address
	cfg := NewConfig()
	err := cfg.Parse(args)
	c.Assert(err, IsNil)
	// kafka asserts
	c.Assert(cfg.SyncerCfg.To.KafkaAddrs, Matches, defaultKafkaAddrs)
	c.Assert(cfg.SyncerCfg.To.KafkaVersion, Equals, defaultKafkaVersion)
	c.Assert(cfg.SyncerCfg.To.KafkaMaxMessages, Equals, 1024)

	// With Zookeeper address
	cfg = NewConfig()
	cfg.SyncerCfg.To = new(dsync.DBConfig)
	cfg.SyncerCfg.To.ZKAddrs = "host1:2181"
	err = cfg.Parse(args)
	c.Assert(err, IsNil)
	c.Assert(cfg.MetricsAddr, Equals, "192.168.15.10:9091")
	c.Assert(cfg.DataDir, Equals, "data.drainer")
	c.Assert(cfg.SyncerCfg.TxnBatch, Equals, 1)
	c.Assert(cfg.SyncerCfg.DestDBType, Equals, "kafka")
	var strSQLMode *string
	c.Assert(cfg.SyncerCfg.StrSQLMode, Equals, strSQLMode)
	c.Assert(cfg.SyncerCfg.SQLMode, Equals, mysql.SQLMode(0))
	// kafka asserts
	c.Assert(cfg.SyncerCfg.To.KafkaAddrs, Matches, `(192\.0\.2\.1:9092,192\.0\.2\.2:9092|192\.0\.2\.2:9092,192\.0\.2\.1:9092)`)
	c.Assert(cfg.SyncerCfg.To.KafkaVersion, Equals, defaultKafkaVersion)
	c.Assert(cfg.SyncerCfg.To.KafkaMaxMessages, Equals, 1024)
}
