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
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-binlog/pkg/util"
	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	pb "github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

// TiBinlogToSlaveBinlog translates the format to slave binlog
func TiBinlogToSlaveBinlog(
	infoGetter TableInfoGetter,
	schema string,
	table string,
	tiBinlog *pb.Binlog,
	pv *pb.PrewriteValue,
) (*obinlog.Binlog, error) {
	if tiBinlog.DdlJobId > 0 { // DDL
		slaveBinlog := &obinlog.Binlog{
			Type:     obinlog.BinlogType_DDL,
			CommitTs: tiBinlog.GetCommitTs(),
			DdlData: &obinlog.DDLData{
				SchemaName: proto.String(schema),
				TableName:  proto.String(table),
				DdlQuery:   tiBinlog.GetDdlQuery(),
			},
		}
		return slaveBinlog, nil
	}
	slaveBinlog := &obinlog.Binlog{
		Type:     obinlog.BinlogType_DML,
		CommitTs: tiBinlog.GetCommitTs(),
		DmlData:  new(obinlog.DMLData),
	}

	for _, mut := range pv.GetMutations() {
		info, ok := infoGetter.TableByID(mut.GetTableId())
		if !ok {
			return nil, errors.Errorf("TableByID empty table id: %d", mut.GetTableId())
		}
		isTblDroppingCol := infoGetter.IsDroppingColumn(mut.GetTableId())

		schema, _, ok = infoGetter.SchemaAndTableName(mut.GetTableId())
		if !ok {
			return nil, errors.Errorf("SchemaAndTableName empty table id: %d", mut.GetTableId())
		}

		iter := newSequenceIterator(&mut)
		for {
			table, err := nextRow(schema, info, isTblDroppingCol, iter)
			if err != nil {
				if errors.Cause(err) == io.EOF {
					break
				}
				return nil, errors.Trace(err)
			}
			slaveBinlog.DmlData.Tables = append(slaveBinlog.DmlData.Tables, table)
		}
	}
	return slaveBinlog, nil
}

func genTable(schema string, tableInfo *model.TableInfo) (table *obinlog.Table) {
	table = new(obinlog.Table)
	table.SchemaName = proto.String(schema)
	table.TableName = proto.String(tableInfo.Name.O)
	// get obinlog.ColumnInfo
	columnInfos := make([]*obinlog.ColumnInfo, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		info := new(obinlog.ColumnInfo)
		info.Name = col.Name.O
		info.MysqlType = types.TypeToStr(col.Tp, col.Charset)
		info.IsPrimaryKey = mysql.HasPriKeyFlag(col.Flag)
		columnInfos = append(columnInfos, info)
	}
	table.ColumnInfo = columnInfos

	// If PKIsHandle, tableInfo.Indices *will not* contains the primary key
	// so we add it here.
	// If !PKIsHandle tableInfo.Indices *will* contains the primary key
	if tableInfo.PKIsHandle {
		pkName := tableInfo.GetPkName()
		key := &obinlog.Key{
			Name:        proto.String("PRIMARY"),
			ColumnNames: []string{pkName.O},
		}
		table.UniqueKeys = append(table.UniqueKeys, key)
	}

	for _, index := range tableInfo.Indices {
		if !index.Unique && !index.Primary {
			continue
		}

		// just a protective check
		if tableInfo.PKIsHandle && index.Name.O == "PRIMARY" {
			log.Warn("PKIsHandle and also contains PRIMARY index TableInfo.Indices")
			continue
		}

		key := new(obinlog.Key)
		table.UniqueKeys = append(table.UniqueKeys, key)

		key.Name = proto.String(index.Name.O)

		names := make([]string, len(index.Columns))
		for i, col := range index.Columns {
			names[i] = col.Name.O
		}

		key.ColumnNames = names
	}

	return
}

func insertRowToRow(tableInfo *model.TableInfo, raw []byte) (row *obinlog.Row, err error) {
	_, columnValues, err := insertRowToDatums(tableInfo, raw)
	columns := tableInfo.Columns

	row = new(obinlog.Row)

	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if !ok {
			val = getDefaultOrZeroValue(col)
		}

		column := DatumToColumn(col, val)
		row.Columns = append(row.Columns, column)
	}

	return
}

func deleteRowToRow(tableInfo *model.TableInfo, raw []byte) (row *obinlog.Row, err error) {
	columns := tableInfo.Columns

	colsTypeMap := util.ToColumnTypeMap(tableInfo.Columns)
	columnValues, err := tablecodec.DecodeRow(raw, colsTypeMap, time.Local)
	if err != nil {
		return nil, errors.Annotate(err, "DecodeRow failed")
	}

	// log.Debugf("delete decodeRow: %+v\n", columnValues)

	row = new(obinlog.Row)

	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if !ok {
			val = getDefaultOrZeroValue(col)
		}

		column := DatumToColumn(col, val)
		row.Columns = append(row.Columns, column)
	}

	return
}

func updateRowToRow(tableInfo *model.TableInfo, raw []byte, isTblDroppingCol bool) (row *obinlog.Row, changedRow *obinlog.Row, err error) {
	updtDecoder := newUpdateDecoder(tableInfo, isTblDroppingCol)
	oldDatums, newDatums, err := updtDecoder.decode(raw, time.Local)
	if err != nil {
		return
	}

	row = new(obinlog.Row)
	changedRow = new(obinlog.Row)
	for _, col := range tableInfo.Columns {
		var val types.Datum
		var ok bool

		if val, ok = newDatums[col.ID]; !ok {
			getDefaultOrZeroValue(col)
		}
		column := DatumToColumn(col, val)
		row.Columns = append(row.Columns, column)

		if val, ok = oldDatums[col.ID]; !ok {
			getDefaultOrZeroValue(col)
		}
		column = DatumToColumn(col, val)
		changedRow.Columns = append(changedRow.Columns, column)
	}

	return
}

// DatumToColumn convert types.Datum to obinlog.Column
func DatumToColumn(colInfo *model.ColumnInfo, datum types.Datum) (col *obinlog.Column) {
	col = new(obinlog.Column)

	if datum.IsNull() {
		col.IsNull = proto.Bool(true)
		return
	}

	switch types.TypeToStr(colInfo.Tp, colInfo.Charset) {
	// date and time type
	case "date", "datetime", "time", "timestamp", "year":
		str := fmt.Sprintf("%v", datum.GetValue())
		col.StringValue = proto.String(str)

	// numeric type
	case "int", "bigint", "smallint", "tinyint":
		str := fmt.Sprintf("%v", datum.GetValue())
		if mysql.HasUnsignedFlag(colInfo.Flag) {
			val, err := strconv.ParseUint(str, 10, 64)
			if err != nil {
				log.Fatal("ParseUint failed", zap.String("str", str), zap.Error(err))
			}
			col.Uint64Value = proto.Uint64(val)
		} else {
			val, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				log.Fatal("ParseInt failed", zap.String("str", str), zap.Error(err))
			}
			col.Int64Value = proto.Int64(val)
		}

	case "float", "double":
		col.DoubleValue = proto.Float64(datum.GetFloat64())
	case "decimal":
		str := fmt.Sprintf("%v", datum.GetValue())
		col.StringValue = proto.String(str)
	case "bit":
		col.BytesValue = datum.GetBytes()

	// string type
	case "text", "longtext", "mediumtext", "char", "tinytext", "varchar", "var_string":
		col.StringValue = proto.String(datum.GetString())
	case "blob", "longblob", "mediumblob", "binary", "tinyblob", "varbinary":
		col.BytesValue = datum.GetBytes()
	case "enum":
		col.Uint64Value = proto.Uint64(datum.GetMysqlEnum().Value)
	case "set":
		col.Uint64Value = proto.Uint64(datum.GetMysqlSet().Value)

	// TiDB don't suppose now
	case "geometry":
		log.Warn("unknown mysql type", zap.Uint8("type", colInfo.Tp))
		str := fmt.Sprintf("%v", datum.GetValue())
		col.StringValue = proto.String(str)

	case "json":
		col.BytesValue = []byte(datum.GetMysqlJSON().String())

	default:
		log.Warn("unknown mysql type", zap.Uint8("type", colInfo.Tp))
		str := fmt.Sprintf("%v", datum.GetValue())
		col.StringValue = proto.String(str)

	}

	return
}

func createTableMutation(tp pb.MutationType, info *model.TableInfo, isTblDroppingCol bool, row []byte) (*obinlog.TableMutation, error) {
	var err error
	mut := new(obinlog.TableMutation)
	switch tp {
	case pb.MutationType_Insert:
		mut.Type = obinlog.MutationType_Insert.Enum()
		mut.Row, err = insertRowToRow(info, row)
		if err != nil {
			return nil, err
		}
	case pb.MutationType_Update:
		mut.Type = obinlog.MutationType_Update.Enum()
		mut.Row, mut.ChangeRow, err = updateRowToRow(info, row, isTblDroppingCol)
		if err != nil {
			return nil, err
		}
	case pb.MutationType_DeleteRow:
		mut.Type = obinlog.MutationType_Delete.Enum()
		mut.Row, err = deleteRowToRow(info, row)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.Errorf("unknown mutation type: %v", tp)
	}
	return mut, nil
}

func nextRow(schema string, info *model.TableInfo, isTblDroppingCol bool, iter *sequenceIterator) (*obinlog.Table, error) {
	mutType, row, err := iter.next()
	if err != nil {
		return nil, errors.Trace(err)
	}

	tableMutation, err := createTableMutation(mutType, info, isTblDroppingCol, row)
	if err != nil {
		return nil, errors.Trace(err)
	}
	table := genTable(schema, info)
	table.Mutations = append(table.Mutations, tableMutation)
	return table, nil
}
