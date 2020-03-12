package plugin

import (
	"fmt"
	"testing"

	"github.com/pingcap/check"
)

func Test(t *testing.T) { check.TestingT(t) }

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

func (s STest1) Do() int {
	return s.a
}

func (ps *PluginSuite) TestRegisterPlugin(c *check.C) {
	hook := &EventHooks{}
	s1 := STest1{32}

	RegisterPlugin(hook, "test1", s1)
	p := hook.GetAllPluginsName()
	c.Assert(len(p), check.Equals, 1)

	RegisterPlugin(hook, "test1", s1)
	p = hook.GetAllPluginsName()
	c.Assert(len(p), check.Equals, 1)

	s2 := STest1{64}
	RegisterPlugin(hook, "test2", s2)
	p = hook.GetAllPluginsName()
	c.Assert(len(p), check.Equals, 2)
	c.Assert(p[0], check.Equals, "test1")
	c.Assert(p[1], check.Equals, "test2")
}

func (ps *PluginSuite) TestTraversePlugin(c *check.C) {
	hook := &EventHooks{}

	s1 := STest1{32}
	RegisterPlugin(hook, "test1", s1)

	s2 := STest1{64}
	RegisterPlugin(hook, "test2", s2)

	s3 := STest1{128}
	RegisterPlugin(hook, "test3", s3)

	p := hook.GetAllPluginsName()
	c.Assert(len(p), check.Equals, 3)

	ret := 0
	hook.Range(func(k, val interface{}) bool {
		c, ok := val.(ITest1)
		if !ok {
			//ignore type incorrect error
			fmt.Printf("ok : %v\n", ok)
			return true
		}
		ret += c.Do()
		return true
	})
	c.Assert(ret, check.Equals, 32+64+128)
}
