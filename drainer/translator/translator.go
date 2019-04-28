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

package translator

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

var sqlMode mysql.SQLMode

// SetSQLMode set the sql mode of parser
func SetSQLMode(mode mysql.SQLMode) {
	sqlMode = mode
}

func getParser() (p *parser.Parser) {
	p = parser.New()
	p.SetSQLMode(sqlMode)

	return
}

func insertRowToDatums(table *model.TableInfo, row []byte) (pk types.Datum, datums map[int64]types.Datum, err error) {
	colsTypeMap := util.ToColumnTypeMap(table.Columns)

	// decode the pk value
	var remain []byte
	remain, pk, err = codec.DecodeOne(row)
	if err != nil {
		return types.Datum{}, nil, errors.Trace(err)
	}

	datums, err = tablecodec.DecodeRow(remain, colsTypeMap, time.Local)
	if err != nil {
		return types.Datum{}, nil, errors.Trace(err)
	}

	// if only one column and IsPKHandleColumn then datums contains no any columns.
	if datums == nil {
		datums = make(map[int64]types.Datum)
	}

	for _, col := range table.Columns {
		if IsPKHandleColumn(table, col) {
			datums[col.ID] = pk
		}
	}

	return
}

func getDefaultOrZeroValue(col *model.ColumnInfo) types.Datum {
	// see https://github.com/pingcap/tidb/issues/9304
	// must use null if TiDB not write the column value when default value is null
	// and the value is null
	if !mysql.HasNotNullFlag(col.Flag) {
		return types.NewDatum(nil)
	}

	if col.GetDefaultValue() != nil {
		return types.NewDatum(col.GetDefaultValue())
	}

	if col.Tp == mysql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		return types.NewDatum(col.FieldType.Elems[0])
	}

	return table.GetZeroValue(col)
}

// DecodeOldAndNewRow decodes a byte slice into datums with a existing row map.
// Row layout: colID1, value1, colID2, value2, .....
func DecodeOldAndNewRow(b []byte, cols map[int64]*types.FieldType, loc *time.Location) (map[int64]types.Datum, map[int64]types.Datum, error) {
	if b == nil {
		return nil, nil, nil
	}
	if b[0] == codec.NilFlag {
		return nil, nil, nil
	}

	cnt := 0
	var (
		data   []byte
		err    error
		oldRow = make(map[int64]types.Datum, len(cols))
		newRow = make(map[int64]types.Datum, len(cols))
	)
	for len(b) > 0 {
		// Get col id.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		_, cid, err := codec.DecodeOne(data)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		// Get col value.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		id := cid.GetInt64()
		ft, ok := cols[id]
		if ok {
			v, err := tablecodec.DecodeColumnValue(data, ft, loc)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}

			if _, ok := oldRow[id]; ok {
				newRow[id] = v
			} else {
				oldRow[id] = v
			}

			cnt++
			if cnt == len(cols)*2 {
				// Get enough data.
				break
			}
		}
	}

	if cnt != len(cols)*2 || len(newRow) != len(oldRow) {
		return nil, nil, errors.Errorf("row data is corrupted %v", b)
	}

	return oldRow, newRow, nil
}

type updateDecoder struct {
	colsTypes map[int64]*types.FieldType
}

func newUpdateDecoder(table *model.TableInfo) updateDecoder {
	columns := writableColumns(table)
	return updateDecoder{
		colsTypes: util.ToColumnTypeMap(columns),
	}
}

// decode decodes a byte slice into datums with a existing row map.
// Row layout: colID1, value1, colID2, value2, .....
func (ud updateDecoder) decode(b []byte, loc *time.Location) (map[int64]types.Datum, map[int64]types.Datum, error) {
	return DecodeOldAndNewRow(b, ud.colsTypes, loc)
}
