// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package syncer

import (
	"fmt"

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
