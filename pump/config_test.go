package pump

import (
	"testing"

	"io/ioutil"
	"os"

	"github.com/ghodss/yaml"
	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func (s *testConfigSuite) SetUpSuite(c *C) {
}

func (s *testConfigSuite) TearDownSuite(c *C) {
}

func (s *testConfigSuite) TestConfigParsingCmdLineFlags(c *C) {
	args := []string{
		"-host", "192.168.199.100",
		"-port", "8260",
		"-pd-addrs", "http://192.168.199.113:2379,http://hostname:2379",
		"-heartbeat=500",
		"-binlog-dir=/tmp/pump",
		"--debug",
	}

	cfg := NewConfig()
	mustSuccess(c, cfg.Parse(args))
	validateConfig(c, cfg)
}

func (s *testConfigSuite) TestConfigParsingEnvFlags(c *C) {
	args := []string{
		"-host", "192.168.199.100",
		"-port", "8260",
		"-pd-addrs", "http://192.168.199.113:2379,http://hostname:2379",
		"-heartbeat=500",
		"--debug",
	}

	os.Clearenv()
	os.Setenv("PUMP_PORT", "9999")
	os.Setenv("PUMP_ETCD", "http://127.0.0.1:2379")
	os.Setenv("PUMP_BINLOG_DIR", "/tmp/pump")

	cfg := NewConfig()
	mustSuccess(c, cfg.Parse(args))
	validateConfig(c, cfg)
}

func (s *testConfigSuite) TestConfigParsingFileFlags(c *C) {
	yc := struct {
		Host          string   `json:"host"`
		Port          uint     `json:"port"`
		EtcdEndpoints []string `json:"pd-addrs"`
		BinlogDir     string   `json:"binlog-dir"`
		HeartbeatMS   uint     `json:"heartbeat"`
		Debug         bool     `json:"debug"`
	}{
		"192.168.199.100",
		8260,
		[]string{"http://192.168.199.113:2379", "http://hostname:2379"},
		"/tmp/pump",
		500,
		true,
	}

	b, err := yaml.Marshal(&yc)
	c.Assert(err, IsNil)

	tmpfile := mustCreateCfgFile(c, b, "pump_config")
	defer os.Remove(tmpfile.Name())

	args := []string{
		"--config-file",
		tmpfile.Name(),
	}

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
		Host:          "192.168.199.100",
		Port:          8260,
		EtcdEndpoints: []string{"http://192.168.199.113:2379", "http://hostname:2379"},
		BinlogDir:     "/tmp/pump",
		HeartbeatMS:   500,
		Debug:         true,
	}
	c.Assert(cfg.Host, Equals, vcfg.Host)
	c.Assert(cfg.Port, Equals, vcfg.Port)
	c.Assert(cfg.EtcdEndpoints, DeepEquals, vcfg.EtcdEndpoints)
	c.Assert(cfg.BinlogDir, Equals, vcfg.BinlogDir)
	c.Assert(cfg.HeartbeatMS, Equals, vcfg.HeartbeatMS)
	c.Assert(cfg.Debug, Equals, vcfg.Debug)
}
