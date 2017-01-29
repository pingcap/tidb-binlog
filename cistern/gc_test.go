package cistern

import (
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
)

func testGColdBinLog(c *C, keys []int64) {
	// remove binlog older than 7 days
	err := GCHistoryBinlog(testDS, 2*time.Hour*24)
	c.Check(err, IsNil)
	// check
	for i := 0; i <= 7; i++ {
		_, err = testDS.Get(keys[i])
		c.Check(errors.IsNotFound(err), IsTrue)
	}

	_, err = testDS.Get(keys[8])
	c.Assert(err, IsNil)
	mustCheckSegments(c, 1)
}
