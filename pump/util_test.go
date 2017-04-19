package pump

import (
	. "github.com/pingcap/check"
)

func (t *testPumpServerSuite) TestCheckBinlogNames(c *C) {
	names := []string{"binlog-0000000000000001", "test", "binlog-0000000000000002"}
	excepted := []string{"binlog-0000000000000001", "binlog-0000000000000002"}
	res := checkBinlogNames(names)
	c.Assert(res, HasLen, len(excepted))
	c.Assert(res, DeepEquals, excepted)
}
