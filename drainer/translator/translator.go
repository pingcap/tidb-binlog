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
	"reflect"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-binlog/pkg/util"
)

var sqlMode mysql.SQLMode

// SetSQLMode set the sql mode of parser
func SetSQLMode(mode mysql.SQLMode) {
	sqlMode = mode
}

func insertRowToDatums(table *model.TableInfo, row []byte) (datums map[int64]types.Datum, err error) {
	colsTypeMap := util.ToColumnTypeMap(table.Columns)

	var (
		commonPKInfo *model.IndexInfo
		pkLen        = 1
	)
	if table.IsCommonHandle {
		for _, idx := range table.Indices {
			if idx.Primary {
				commonPKInfo = idx
				break
			}
		}
		if commonPKInfo == nil {
			err = errors.New("Unsupported clustered index without primary key")
			return
		}
		pkLen = len(commonPKInfo.Columns)
	}
	var (
		pk     []types.Datum
		remain = row
	)
	for i := 0; i < pkLen; i++ {
		var aPK types.Datum
		remain, aPK, err = codec.DecodeOne(remain)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if table.IsCommonHandle {
			// clustered index could be complex type that need Unflatten from raw datum.
			aPK, err = tablecodec.Unflatten(aPK, &table.Columns[commonPKInfo.Columns[i].Offset].FieldType, time.Local)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		pk = append(pk, aPK)
	}

	datums, err = tablecodec.DecodeRowToDatumMap(remain, colsTypeMap, time.Local)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// if only one column and IsPKHandleColumn then datums contains no any columns.
	if datums == nil {
		datums = make(map[int64]types.Datum)
	}

	if table.IsCommonHandle {
		for idxColOrdinal, idxCol := range commonPKInfo.Columns {
			if idxCol.Length != types.UnspecifiedLength {
				// primary key's prefixed column already in row data.
				continue
			}
			tblIdxCol := table.Columns[idxCol.Offset]
			if _, exists := datums[tblIdxCol.ID]; exists {
				// use row column instead of pk column if row column exists
				// e.g. new collation's pk just be sortKey, but row column have full data.
				continue
			}
			datums[tblIdxCol.ID] = pk[idxColOrdinal]
		}
	} else {
		for _, col := range table.Columns {
			if (table.PKIsHandle && mysql.HasPriKeyFlag(col.Flag)) || col.ID == implicitColID {
				// If pk is handle, the datums TiDB write will always be Int64 type.
				// https://github.com/pingcap/tidb/blob/cd10bca6660937beb5d6de11d49ec50e149fe083/table/tables/tables.go#L721
				//
				// create table pk(id BIGINT UNSIGNED);
				// insert into pk(id) values(18446744073709551615)
				//
				// Will get -1 here, note: uint64(int64(-1)) = 18446744073709551615
				// so we change it to uint64 if the column type is unsigned
				datums[col.ID] = fixType(pk[0], col)
			}
		}
	}

	log.S().Debugf("get insert row pk: %v, datums: %+v", pk, datums)

	return
}

func getEnumDatum(getCol *model.ColumnInfo) (data types.Datum, err error) {
	ivalue := getCol.GetOriginDefaultValue()
	switch value := ivalue.(type) {
	case string:
		enum, err := types.ParseEnumName(getCol.Elems, value, "")
		if err != nil {
			return types.Datum{}, errors.AddStack(err)
		}
		return types.NewDatum(enum), nil
	}

	return types.Datum{}, errors.Errorf("unknown type: %v", reflect.TypeOf(ivalue))
}

func getSetDatum(getCol *model.ColumnInfo) (data types.Datum, err error) {
	ivalue := getCol.GetOriginDefaultValue()
	switch value := ivalue.(type) {
	case string:
		enum, err := types.ParseSetName(getCol.Elems, value, "")
		if err != nil {
			return types.Datum{}, errors.AddStack(err)
		}
		return types.NewDatum(enum), nil
	}

	return types.Datum{}, errors.Errorf("unknown type: %v", reflect.TypeOf(ivalue))
}

func transTimestampToLocal(getCol *model.ColumnInfo) (string, error) {
	ivalue := getCol.GetOriginDefaultValue()
	value, ok := ivalue.(string)
	if !ok {
		return "", errors.New("not string value")
	}

	t, err := time.Parse(types.TimeFormat, value)
	if err != nil {
		return "", errors.AddStack(err)
	}

	value = t.Local().Format(types.TimeFormat)
	return value, nil
}

func getDefaultOrZeroValue(tableInfo *model.TableInfo, col *model.ColumnInfo) types.Datum {
	getCol := col
	if tableInfo != nil {
		for _, c := range tableInfo.Columns {
			if c.ID == col.ID {
				getCol = c
			}
		}
	}

	if getCol.GetOriginDefaultValue() != nil {
		// ref https://github.com/pingcap/tidb/blob/release-4.0/ddl/column.go#L675
		// trans value from UTC to local timezone value format.
		if getCol.Tp == mysql.TypeTimestamp {
			value, err := transTimestampToLocal(getCol)
			if err != nil {
				log.Warn("failed to transTimestampToLocal",
					zap.Reflect("value", getCol.GetOriginDefaultValue()),
					zap.Error(err))
			} else {
				return types.NewDatum(value)
			}
		} else if getCol.Tp == mysql.TypeEnum {
			data, err := getEnumDatum(getCol)
			if err != nil {
				log.Warn("failed to get enum datam", zap.Reflect("value", getCol.GetOriginDefaultValue()),
					zap.Error(err))
			} else {
				return data
			}
		} else if getCol.Tp == mysql.TypeSet {
			data, err := getSetDatum(getCol)
			if err != nil {
				log.Warn("failed to get set datam", zap.Reflect("value", getCol.GetOriginDefaultValue()),
					zap.Error(err))
			} else {
				return data
			}
		}

		return types.NewDatum(getCol.GetOriginDefaultValue())
	}

	// see https://github.com/pingcap/tidb/issues/9304
	// must use null if TiDB not write the column value when default value is null
	// and the value is null
	if !mysql.HasNotNullFlag(col.Flag) {
		return types.NewDatum(nil)
	}

	// if !mysql.HasNotNullFlag(col.Flag) {
	if col.Tp == mysql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		return types.NewDatum(col.FieldType.Elems[0])
	}

	return table.GetZeroValue(col)
}

// DecodeOldAndNewRow decodes a byte slice into datums with a existing row map.
// Row layout: colID1, value1, colID2, value2, .....
func DecodeOldAndNewRow(b []byte,
	cols map[int64]*model.ColumnInfo,
	loc *time.Location,
	canAppendDefaultValue bool,
	pinfo *model.TableInfo,
) (map[int64]types.Datum, map[int64]types.Datum, error) {
	if b == nil {
		return nil, nil, nil
	}
	if b[0] == codec.NilFlag {
		return nil, nil, nil
	}

	var (
		cnt    int
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
		col, ok := cols[id]
		if ok {
			v, err := tablecodec.DecodeColumnValue(data, &col.FieldType, loc)
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

	parsedCols := cnt / 2
	isInvalid := len(newRow) != len(oldRow)
	if isInvalid {
		return nil, nil, errors.Errorf("row data is corrupted cols num: %d, oldRow: %v, newRow: %v", len(cols), oldRow, newRow)
	}
	if parsedCols < len(cols) {
		if !canAppendDefaultValue {
			return nil, nil, errors.Errorf("row data is corrupted cols num: %d, oldRow: %v, newRow: %v", len(cols), oldRow, newRow)
		}

		var missingCols []*model.ColumnInfo
		for colID, col := range cols {
			_, inOld := oldRow[colID]
			_, inNew := newRow[colID]
			if !inOld && !inNew {
				missingCols = append(missingCols, col)
			}
		}

		// We can't find columns that's missing in both old and new
		if len(missingCols) != len(cols)-parsedCols {
			return nil, nil, errors.Errorf("row data is corrupted %v", b)
		}

		for _, missingCol := range missingCols {
			v := getDefaultOrZeroValue(pinfo, missingCol)
			oldRow[missingCol.ID] = v
			newRow[missingCol.ID] = v

			log.S().Debugf("missing col: %+v", *missingCol)

			log.Info(
				"fill missing col with default val",
				zap.String("name", missingCol.Name.O),
				zap.Int64("id", missingCol.ID),
				zap.Int("Tp", int(missingCol.FieldType.Tp)),
				zap.Reflect("value", v))
		}
	}

	return oldRow, newRow, nil
}

type updateDecoder struct {
	columns               map[int64]*model.ColumnInfo
	canAppendDefaultValue bool
	ptable                *model.TableInfo
}

func newUpdateDecoder(ptable, table *model.TableInfo, canAppendDefaultValue bool) updateDecoder {
	columns := writableColumns(table)
	return updateDecoder{
		columns:               util.ToColumnMap(columns),
		canAppendDefaultValue: canAppendDefaultValue,
		ptable:                ptable,
	}
}

// decode decodes a byte slice into datums with a existing row map.
// Row layout: colID1, value1, colID2, value2, .....
func (ud updateDecoder) decode(b []byte, loc *time.Location) (map[int64]types.Datum, map[int64]types.Datum, error) {
	return DecodeOldAndNewRow(b, ud.columns, loc, ud.canAppendDefaultValue, ud.ptable)
}

func fixType(data types.Datum, col *model.ColumnInfo) types.Datum {
	if mysql.HasUnsignedFlag(col.Flag) {
		switch oldV := data.GetValue().(type) {
		case int64:
			log.Debug("convert int64 type to uint64", zap.Int64("value", oldV))
			return types.NewDatum(uint64(oldV))
		}
	}
	return data
}
