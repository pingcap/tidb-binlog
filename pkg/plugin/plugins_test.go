package plugin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type ITest1 interface {
	Do() int
}
type STest1 struct {
	a int
}

func (s *STest1) Do() int {
	return s.a
}

func TestRegisterPlugin(t *testing.T) {
	hook := &EventHooks{}
	s := STest1{32}

	RegisterPlugin(hook, "test1", s)
	p := hook.GetAllPluginsName()
	assert.Equal(t, 1, len(p))
}
