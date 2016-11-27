package cistern

import (
	. "github.com/pingcap/check"
)

func (t *testCisternSuite) TestConfig(c *C) {
	args := []string{
		"-metrics-addr", "127.0.0.1:9091",
		"--addr", "192.168.199.100:8260",
		"--pd-urls", "http://192.168.199.110:2379,http://hostname:2379",
		"--data-dir", "",
		"--deposit-window-period", "0",
	}

	cfg := NewConfig()
	err := cfg.Parse(args)
	err = cfg.configFromFile("../cmd/drainer/config.toml")
	c.Assert(err, IsNil)
	c.Assert(err, IsNil)
	c.Assert(cfg.MetricsAddr, Equals, "127.0.0.1:9091")
	c.Assert(cfg.DataDir, Equals, "data.drainer")
	c.Assert(cfg.ListenAddr, Equals, "http://192.168.199.100:8260")
	c.Assert(cfg.EtcdURLs, Equals, "http://192.168.199.110:2379,http://hostname:2379")
}
