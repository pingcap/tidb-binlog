package syncer

import (
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

type testUtilSuite struct{}

var _ = check.Suite(&testUtilSuite{})

func (s *testUtilSuite) TestFormatValue(c *check.C) {
	datetime, err := time.Parse("20060102150405", "20190415121212")
	c.Assert(err, check.IsNil)
	bitVal, err2 := types.NewBitLiteral("b'10010'")
	c.Assert(err2, check.IsNil)

	testCases := []struct {
		value     interface{}
		tp        byte
		expectStr string
		expectVal interface{}
	}{
		{
			value:     1,
			tp:        mysql.TypeInt24,
			expectStr: "1",
			expectVal: int64(1),
		},
		{
			value:     1.11,
			tp:        mysql.TypeFloat,
			expectStr: "1.11",
			expectVal: float64(1.11),
		},
		{
			value:     1.11,
			tp:        mysql.TypeDouble,
			expectStr: "1.11",
			expectVal: float64(1.11),
		},
		{
			value:     "a",
			tp:        mysql.TypeVarchar,
			expectStr: "a",
			expectVal: "a",
		},
		{
			value:     "a",
			tp:        mysql.TypeString,
			expectStr: "a",
			expectVal: "a",
		},
		{
			value:     datetime,
			tp:        mysql.TypeDatetime,
			expectStr: "2019-04-15 12:12:12 +0000 UTC",
			expectVal: "2019-04-15 12:12:12 +0000 UTC",
		},
		{
			value:     time.Duration(time.Second),
			tp:        mysql.TypeDuration,
			expectStr: "1s",
			expectVal: "1s",
		},
		{
			value:     types.Enum{Name: "a", Value: 1},
			tp:        mysql.TypeEnum,
			expectStr: "a",
			expectVal: uint64(1),
		},
		{
			value:     types.Set{Name: "a", Value: 1},
			tp:        mysql.TypeSet,
			expectStr: "a",
			expectVal: uint64(1),
		},
		{
			value:     bitVal,
			tp:        mysql.TypeBit,
			expectStr: "0x12",
			expectVal: types.BinaryLiteral([]byte{0x12}),
		},
	}

	for _, testCase := range testCases {
		datum := types.NewDatum(testCase.value)
		str := formatValueToString(datum, testCase.tp)
		c.Assert(str, check.Equals, testCase.expectStr)

		fv, err := formatValue(datum, testCase.tp, true)
		c.Assert(err, check.IsNil)
		c.Assert(fv.GetValue(), check.DeepEquals, testCase.expectVal)
	}
}

const TimeLayout = "2006-01-02 15:04:05"

func (s *testUtilSuite) TestUtcZone(c *check.C) {
	datetime, err := time.Parse("20060102150405", "20190415121212")
	c.Assert(err, check.IsNil)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	loaclTime, err := types.ParseTimestampFromNum(sc, 19961120012345)
	c.Assert(err, check.IsNil)

	sc.TimeZone = time.UTC
	utcTime, err := types.ParseTimestampFromNum(sc, 19961120012345)
	c.Assert(err, check.IsNil)

	localToUtc := func(timeStr string) string {
		t, err := time.ParseInLocation(TimeLayout, timeStr, time.Local)
		c.Assert(err, check.IsNil)
		return t.In(time.UTC).Format(TimeLayout)
	}
	testCases := []struct {
		value     interface{}
		tp        byte
		expectStr string
		expectVal interface{}
		utcTz     bool
	}{
		{
			value:     datetime,
			tp:        mysql.TypeDatetime,
			expectStr: "2019-04-15 12:12:12 +0000 UTC",
			expectVal: "2019-04-15 12:12:12 +0000 UTC",
			utcTz:     false,
		},
		{
			value:     datetime,
			tp:        mysql.TypeDatetime,
			expectStr: "2019-04-15 12:12:12 +0000 UTC",
			expectVal: "2019-04-15 12:12:12 +0000 UTC",
			utcTz:     true,
		},
		{
			value:     loaclTime,
			tp:        mysql.TypeTimestamp,
			expectStr: "1996-11-20 01:23:45",
			expectVal: localToUtc("1996-11-20 01:23:45"),
			utcTz:     false,
		},
		{
			value:     utcTime,
			tp:        mysql.TypeTimestamp,
			expectStr: "1996-11-20 01:23:45",
			expectVal: "1996-11-20 01:23:45",
			utcTz:     true,
		},
	}

	for _, testCase := range testCases {
		datum := types.NewDatum(testCase.value)
		str := formatValueToString(datum, testCase.tp)
		c.Assert(str, check.Equals, testCase.expectStr)

		fv, err := formatValue(datum, testCase.tp, testCase.utcTz)
		c.Assert(err, check.IsNil)
		c.Assert(fv.GetValue(), check.DeepEquals, testCase.expectVal)
	}
}
