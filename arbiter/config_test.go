package arbiter

import (
	"bytes"
	"io/ioutil"
	"path"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/check"
)

type TestConfigSuite struct {
}

var _ = check.Suite(&TestConfigSuite{})

func (t *TestConfigSuite) TestParseConfigFileWithInvalidArgs(c *check.C) {
	yc := struct {
		LogLevel               string `toml:"log-level" json:"log-level"`
		ListenAddr             string `toml:"addr" json:"addr"`
		LogFile                string `toml:"log-file" json:"log-file"`
		UnrecognizedOptionTest bool   `toml:"unrecognized-option-test" json:"unrecognized-option-test"`
	}{
		"debug",
		"127.0.0.1:8251",
		"/tmp/arbiter",
		true,
	}

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(yc)
	c.Assert(err, check.IsNil)

	configFilename := path.Join(c.MkDir(), "arbiter_config_invalid.toml")
	err = ioutil.WriteFile(configFilename, buf.Bytes(), 0644)
	c.Assert(err, check.IsNil)

	args := []string{
		"--config",
		configFilename,
	}

	cfg := NewConfig()
	err = cfg.Parse(args)
	c.Assert(err, check.ErrorMatches, ".*contained unknown configuration options: unrecognized-option-test.*")
}
