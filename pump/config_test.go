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
	"bytes"
	"io/ioutil"
	"os"

	"github.com/BurntSushi/toml"
	. "github.com/pingcap/check"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func (s *testConfigSuite) TestValidate(c *C) {
	cfg := Config{}
	cfg.GC = 1
	cfg.ListenAddr = "http://:8250"
	cfg.EtcdURLs = "http://192.168.10.23:7777"
	cfg.AdvertiseAddr = "http://:8250"
	err := cfg.validate()
	c.Check(err, ErrorMatches, ".*advertiseAddr.*")
	cfg.AdvertiseAddr = "http://0.0.0.0:8250"
	err = cfg.validate()
	c.Check(err, ErrorMatches, ".*advertiseAddr.*")
	cfg.AdvertiseAddr = "http://192.168.11.11:8250"
	err = cfg.validate()
	c.Check(err, IsNil)
}

func (s *testConfigSuite) TestConfigParsingCmdLineFlags(c *C) {
	args := []string{
		"--addr", "192.168.199.100:8260",
		"--pd-urls", "http://192.168.199.110:2379,http://hostname:2379",
		"--data-dir=/tmp/pump",
		"--heartbeat-interval=1500",
		"-L", "debug",
	}

	cfg := NewConfig()
	mustSuccess(c, cfg.Parse(args))
	validateConfig(c, cfg)
}

func (s *testConfigSuite) TestConfigParsingEnvFlags(c *C) {
	args := []string{
		"--addr", "192.168.199.100:8260",
		"-pd-urls", "http://192.168.199.110:2379,http://hostname:2379",
		"-heartbeat-interval=1500",
		"-L", "debug",
	}

	os.Clearenv()
	os.Setenv("PUMP_ADDR", "192.168.199.200:9000")
	os.Setenv("PUMP_PD_URLS", "http://127.0.0.1:2379,http://localhost:2379")
	os.Setenv("PUMP_DATA_DIR", "/tmp/pump")

	cfg := NewConfig()
	mustSuccess(c, cfg.Parse(args))
	validateConfig(c, cfg)
}

func (s *testConfigSuite) TestConfigParsingFileFlags(c *C) {
	yc := struct {
		ListenAddr        string `toml:"addr" json:"addr"`
		AdvertiseAddr     string `toml:"advertiser-addr" json:"advertise-addr"`
		EtcdURLs          string `toml:"pd-urls" json:"pd-urls"`
		BinlogDir         string `toml:"data-dir" json:"data-dir"`
		HeartbeatInterval uint   `toml:"heartbeat-interval" json:"heartbeat-interval"`
	}{
		"192.168.199.100:8260",
		"192.168.199.100:8260",
		"http://192.168.199.110:2379,http://hostname:2379",
		"/tmp/pump",
		1500,
	}

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(yc)
	c.Assert(err, IsNil)

	tmpfile := mustCreateCfgFile(c, buf.Bytes(), "pump_config")
	defer os.Remove(tmpfile.Name())

	args := []string{
		"--config",
		tmpfile.Name(),
		"-L", "debug",
	}

	os.Clearenv()
	cfg := NewConfig()
	mustSuccess(c, cfg.Parse(args))
	validateConfig(c, cfg)
}

func mustSuccess(c *C, err error) {
	c.Assert(err, IsNil)
}

func mustCreateCfgFile(c *C, b []byte, prefix string) *os.File {
	tmpfile, err := ioutil.TempFile("", prefix)
	mustSuccess(c, err)

	_, err = tmpfile.Write(b)
	mustSuccess(c, err)

	err = tmpfile.Close()
	mustSuccess(c, err)

	return tmpfile
}

func validateConfig(c *C, cfg *Config) {
	vcfg := &Config{
		ListenAddr:        "http://192.168.199.100:8260",
		AdvertiseAddr:     "http://192.168.199.100:8260",
		EtcdURLs:          "http://192.168.199.110:2379,http://hostname:2379",
		DataDir:           "/tmp/pump",
		HeartbeatInterval: 1500,
		LogLevel:          "debug",
	}

	c.Assert(cfg.ListenAddr, Equals, vcfg.ListenAddr)
	c.Assert(cfg.AdvertiseAddr, Equals, vcfg.AdvertiseAddr)
	c.Assert(cfg.EtcdURLs, Equals, vcfg.EtcdURLs)
	c.Assert(cfg.DataDir, Equals, vcfg.DataDir)
	c.Assert(cfg.HeartbeatInterval, Equals, vcfg.HeartbeatInterval)
	c.Assert(cfg.LogLevel, Equals, vcfg.LogLevel)
}
