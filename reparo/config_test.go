package repora

import (
	"fmt"
	"path"
	"runtime"

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

func getTemplateConfigFilePath() string {
	// we put the template config file in "cmd/reapro/reparo.toml"
	_, filename, _, _ := runtime.Caller(0)
	path := path.Join(path.Dir(filename), "../cmd/reparo/reparo.toml")

	return path
}
