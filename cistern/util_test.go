package cistern

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tipb/go-binlog"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (suite *testUtilSuite) TestPosToFloat(c *check.C) {
	pos := binlog.Pos{
		Suffix: 4,
		Offset: 3721,
	}
	f := posToFloat(&pos)
	c.Assert(f, check.Equals, 4.3721)
}
