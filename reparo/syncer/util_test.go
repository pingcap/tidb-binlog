package syncer

import (
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
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

		fv := formatValue(datum, testCase.tp)
		c.Assert(fv.GetValue(), check.DeepEquals, testCase.expectVal)
	}
}
