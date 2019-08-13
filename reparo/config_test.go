package reparo

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path"
	"runtime"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/check"
)

type testConfigSuite struct{}

var _ = check.Suite(&testConfigSuite{})

func (s *testConfigSuite) TestParseTemplateConfig(c *check.C) {
	config := NewConfig()

	arg := fmt.Sprintf("-config=%s", getTemplateConfigFilePath())
	err := config.Parse([]string{arg})
	c.Assert(err, check.IsNil, check.Commentf("arg: %s", arg))
}

func (s *testConfigSuite) TestParseConfigFileWithInvalidArgs(c *check.C) {
	yc := struct {
		Dir                    string `toml:"data-dir" json:"data-dir"`
		StartDatetime          string `toml:"start-datetime" json:"start-datetime"`
		StopDatetime           string `toml:"stop-datetime" json:"stop-datetime"`
		StartTSO               int64  `toml:"start-tso" json:"start-tso"`
		StopTSO                int64  `toml:"stop-tso" json:"stop-tso"`
		LogFile                string `toml:"log-file" json:"log-file"`
		LogLevel               string `toml:"log-level" json:"log-level"`
		UnrecognizedOptionTest bool   `toml:"unrecognized-option-test" json:"unrecognized-option-test"`
	}{
		"/tmp/reparo",
		"",
		"",
		0,
		0,
		"tmp/reparo/reparo.log",
		"debug",
		true,
	}

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(yc)
	c.Assert(err, check.IsNil)

	configFilename := path.Join(c.MkDir(), "reparo_config_invalid.toml")
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

func getTemplateConfigFilePath() string {
	// we put the template config file in "cmd/reapro/reparo.toml"
	_, filename, _, _ := runtime.Caller(0)
	path := path.Join(path.Dir(filename), "../cmd/reparo/reparo.toml")

	return path
}
