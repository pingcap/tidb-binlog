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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
)

func pbBinlogToTxn(binlog *pb.Binlog) (txn *loader.Txn, err error) {
	txn = new(loader.Txn)
	switch binlog.Tp {
	case pb.BinlogType_DDL:
		txn.DDL = new(loader.DDL)
		// for table DDL, pb.Binlog.DdlQuery will be "use <db>; create..."
		txn.DDL.SQL = string(binlog.DdlQuery)
	case pb.BinlogType_DML:
		data := binlog.DmlData
		for _, event := range data.GetEvents() {
			dml := new(loader.DML)
			dml.Database = event.GetSchemaName()
			dml.Table = event.GetTableName()
			txn.DMLs = append(txn.DMLs, dml)

			switch event.GetTp() {
			case pb.EventType_Insert:
				dml.Tp = loader.InsertDMLType

				cols, args, err := genColsAndArgs(event.Row)
				if err != nil {
					return nil, errors.Trace(err)
				}

				dml.Values = make(map[string]interface{})
				for i := 0; i < len(cols); i++ {
					dml.Values[cols[i]] = args[i]
				}
			case pb.EventType_Update:
				dml.Tp = loader.UpdateDMLType
				dml.Values = make(map[string]interface{})
				dml.OldValues = make(map[string]interface{})

				for _, c := range event.GetRow() {
					col := &pb.Column{}
					err := col.Unmarshal(c)
					if err != nil {
						return nil, errors.Trace(err)
					}

					_, newDatum, err := codec.DecodeOne(col.Value)
					if err != nil {
						return nil, errors.Trace(err)
					}
					_, oldDatum, err := codec.DecodeOne(col.ChangedValue)
					if err != nil {
						return nil, errors.Trace(err)
					}

					tp := col.Tp[0]
					newDatum = formatValue(newDatum, tp)
					newValue := newDatum.GetValue()
					oldDatum = formatValue(oldDatum, tp)
					oldValue := oldDatum.GetValue()

					log.Debug("translate update event",
						zap.String("col name", col.Name),
						zap.String("col mysql type", col.MysqlType),
						zap.Uint8("tp", tp),
						zap.Reflect("old value", oldValue),
						zap.Reflect("new value", newValue))

					dml.Values[col.Name] = newValue
					dml.OldValues[col.Name] = oldValue
				}
			case pb.EventType_Delete:
				dml.Tp = loader.DeleteDMLType

				cols, args, err := genColsAndArgs(event.Row)
				if err != nil {
					return nil, errors.Trace(err)
				}

				dml.Values = make(map[string]interface{})
				for i := 0; i < len(cols); i++ {
					dml.Values[cols[i]] = args[i]
				}
			default:
				return nil, errors.Errorf("unknown type: %v", event.GetTp())
			}
		}
	default:
		return nil, errors.Errorf("unknown type: %v", binlog.Tp)
	}

	return
}

func genColsAndArgs(row [][]byte) (cols []string, args []interface{}, err error) {
	cols = make([]string, 0, len(row))
	args = make([]interface{}, 0, len(row))
	for _, c := range row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		cols = append(cols, col.Name)

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		tp := col.Tp[0]
		val = formatValue(val, tp)
		log.Debug("format value",
			zap.String("col name", col.Name),
			zap.String("mysql type", col.MysqlType),
			zap.Reflect("value", val.GetValue()))
		args = append(args, val.GetValue())
	}

	return
}
