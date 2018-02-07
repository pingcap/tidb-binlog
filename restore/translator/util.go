package translator

import (
	"fmt"

	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

func formatValueToString(data types.Datum, tp byte) string {
	val := data.GetValue()
	switch tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeDecimal, mysql.TypeNewDecimal, mysql.TypeVarchar, mysql.TypeString, mysql.TypeJSON:
		if val != nil {
			return fmt.Sprintf("%s", val)
		}
		fallthrough
	default:
		return fmt.Sprintf("%v", val)
	}
}

// TODO: test it.
func formatValue(value types.Datum, tp byte) types.Datum {
	if value.GetValue() == nil {
		return value
	}

	switch tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeDecimal, mysql.TypeNewDecimal, mysql.TypeVarchar, mysql.TypeString, mysql.TypeJSON:
		value = types.NewDatum(fmt.Sprintf("%s", value.GetValue()))
	case mysql.TypeEnum:
		value = types.NewDatum(value.GetMysqlEnum().Value)
	case mysql.TypeSet:
		value = types.NewDatum(value.GetMysqlSet().Value)
	case mysql.TypeBit:
		value = types.NewDatum(value.GetMysqlBit())
	}

	return value
}

func isAcceptableBinlog(binlog *pb.Binlog, startTs, endTs int64) bool {
	// no limit
	if startTs == 0 && endTs == 0 {
		return true
	}
	if startTs != 0 && binlog.CommitTs < startTs {
		return false
	}
	if endTs != 0 && binlog.CommitTs > endTs {
		return false
	}
	return true
}
