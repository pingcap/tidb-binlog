package drainer

import (
	"fmt"

	. "github.com/pingcap/check"
)

func (t *testDrainerSuite) TestConfig(c *C) {
	args := []string{
		"-metrics-addr", "127.0.0.1:9091",
		"-txn-batch", "1",
		"-data-dir", "data.drainer",
		"-dest-db-type", "mysql",
		"-config-file", "../cmd/drainer/config.toml",
	}

	cfg := NewConfig()
	err := cfg.Parse(args)
	c.Assert(err, IsNil)
	c.Assert(cfg.MetricsAddr, Equals, "127.0.0.1:9091")
	c.Assert(cfg.TxnBatch, Equals, 1)
	c.Assert(cfg.DataDir, Equals, "data.drainer")
	c.Assert(cfg.DestDBType, Equals, "mysql")
	c.Assert(cfg.To.Host, Equals, "127.0.0.1")
	c.Assert(fmt.Sprintf("%s", cfg), Equals, fmt.Sprintf("Config(%+v)", *cfg))
}
