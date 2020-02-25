package plugin

import (
	"github.com/pingcap/check"
)

type PluginSuite struct {
}

var _ = check.Suite(&PluginSuite{})

func (ps *PluginSuite) SetUpTest(c *check.C) {
}

func (ps *PluginSuite) TearDownTest(c *check.C) {
}

type ITest1 interface {
	Do() int
}
type STest1 struct {
	a int
}

func (s *STest1) Do() int {
	return s.a
}

func (ps *PluginSuite) TestRegisterPlugin(c *check.C) {
	hook := &EventHooks{}
	s := STest1{32}

	RegisterPlugin(hook, "test1", s)
	p := hook.GetAllPluginsName()
	c.Assert(len(p), check.Equals, 1)

}
