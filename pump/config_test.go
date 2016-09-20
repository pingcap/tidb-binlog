package pump

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/ghodss/yaml"
	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func (s *testConfigSuite) TestConfigParsingCmdLineFlags(c *C) {
	args := []string{
		"--addr", "192.168.199.100:8260",
		"--pd-urls", "http://192.168.199.110:2379,http://hostname:2379",
		"--data-dir=/tmp/pump",
		"--heartbeat-interval=1500",
		"--debug",
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
		"--debug",
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
		ListenAddr    string `json:"addr"`
		AdvertiseAddr string `json:"advertise-addr"`
		EtcdURLs      string `json:"pd-urls"`
		BinlogDir     string `json:"data-dir"`
		HeartbeatMS   uint   `json:"heartbeat-interval"`
	}{
		"192.168.199.100:8260",
		"192.168.199.100:8260",
		"http://192.168.199.110:2379,http://hostname:2379",
		"/tmp/pump",
		1500,
	}

	b, err := yaml.Marshal(&yc)
	c.Assert(err, IsNil)

	tmpfile := mustCreateCfgFile(c, b, "pump_config")
	defer os.Remove(tmpfile.Name())

	args := []string{
		"--config-file",
		tmpfile.Name(),
		"--debug",
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
		ListenAddr:    "http://192.168.199.100:8260",
		AdvertiseAddr: "http://192.168.199.100:8260",
		EtcdURLs:      "http://192.168.199.110:2379,http://hostname:2379",
		BinlogDir:     "/tmp/pump",
		HeartbeatMS:   1500,
		Debug:         true,
	}

	c.Assert(cfg.ListenAddr, Equals, vcfg.ListenAddr)
	c.Assert(cfg.AdvertiseAddr, Equals, vcfg.AdvertiseAddr)
	c.Assert(cfg.EtcdURLs, Equals, vcfg.EtcdURLs)
	c.Assert(cfg.BinlogDir, Equals, vcfg.BinlogDir)
	c.Assert(cfg.HeartbeatMS, Equals, vcfg.HeartbeatMS)
	c.Assert(cfg.Debug, Equals, vcfg.Debug)
}
