package syncer

import (
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

type testUtilSuite struct{}

var _ = check.Suite(&testUtilSuite{})

func (s *testUtilSuite) TestFormatValue(c *check.C) {
	datetime, err := time.Parse("20060102150405", "20190415121212")
	c.Assert(err, check.IsNil)

	testCases := []struct {
		value     interface{}
		tp        byte
		changeTp  byte
		expectStr string
	}{
		{
			value:     1,
			tp:        mysql.TypeInt24,
			changeTp:  mysql.TypeFloat,
			expectStr: "1",
		},
		{
			value:     1.11,
			tp:        mysql.TypeFloat,
			changeTp:  mysql.TypeDouble,
			expectStr: "1.11",
		},
		{
			value:     1.11,
			tp:        mysql.TypeDouble,
			changeTp:  mysql.TypeFloat,
			expectStr: "1.11",
		},
		{
			value:     "a",
			tp:        mysql.TypeVarchar,
			changeTp:  mysql.TypeString,
			expectStr: "a",
		},
		{
			value:     "a",
			tp:        mysql.TypeString,
			changeTp:  mysql.TypeVarchar,
			expectStr: "a",
		},
		{
			value:     datetime,
			tp:        mysql.TypeDatetime,
			changeTp:  mysql.TypeDatetime,
			expectStr: "2019-04-15 12:12:12 +0000 UTC",
		},
		{
			value:     time.Duration(time.Second),
			tp:        mysql.TypeDuration,
			changeTp:  mysql.TypeDuration,
			expectStr: "1s",
		},
	}

	for _, testCase := range testCases {
		datum := types.NewDatum(testCase.value)
		str := formatValueToString(datum, testCase.tp)
		c.Assert(str, check.Equals, testCase.expectStr)

		newDatum := formatValue(datum, testCase.changeTp)
		newStr := formatValueToString(newDatum, testCase.changeTp)
		c.Assert(newStr, check.Equals, testCase.expectStr)
	}
}
