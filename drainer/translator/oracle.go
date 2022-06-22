package translator

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
	router "github.com/pingcap/tidb/util/table-router"
	tipb "github.com/pingcap/tipb/go-binlog"

	"github.com/pingcap/tidb-binlog/pkg/loader"
)

// TiBinlogToOracleTxn translate the format to loader.Txn
func TiBinlogToOracleTxn(infoGetter TableInfoGetter, schema string, table string, tiBinlog *tipb.Binlog, pv *tipb.PrewriteValue, shouldSkip bool, tableRouter *router.Table) (txn *loader.Txn, err error) {
	txn = new(loader.Txn)

	if tiBinlog.DdlJobId > 0 {
		downStreamSchema, downStreamTable, routeErr := tableRouter.Route(schema, table)
		if routeErr != nil {
			return nil, errors.Annotatef(routeErr, "when binlog to oracle ddl txn, route schema and table failed. schema=%s, table=%s", schema, table)
		}
		txn.DDL = &loader.DDL{
			Database: downStreamSchema,
			Table:    downStreamTable,
			// TODO: don't route DDL here because we will rewrite DDLs in loader
			SQL:        string(tiBinlog.GetDdlQuery()),
			ShouldSkip: shouldSkip,
		}
	} else {
		tableIDColumnsMap := make(map[int64]map[string]*model.ColumnInfo)
		for _, mut := range pv.GetMutations() {
			var info *model.TableInfo
			var ok bool
			info, ok = infoGetter.TableByID(mut.GetTableId())
			if !ok {
				return nil, errors.Errorf("TableByID empty table id: %d", mut.GetTableId())
			}

			if _, ok := tableIDColumnsMap[mut.GetTableId()]; !ok {
				tableIDColumnsMap[mut.GetTableId()] = genColumnInfoMap(info)
			}

			pinfo, _ := infoGetter.TableBySchemaVersion(mut.GetTableId(), pv.SchemaVersion)

			canAppendDefaultValue := infoGetter.CanAppendDefaultValue(mut.GetTableId(), pv.SchemaVersion)

			schema, table, ok = infoGetter.SchemaAndTableName(mut.GetTableId())
			if !ok {
				return nil, errors.Errorf("SchemaAndTableName empty table id: %d", mut.GetTableId())
			}
			downStreamSchema, downStreamTable, routeErr := tableRouter.Route(schema, table)
			if routeErr != nil {
				return nil, errors.Annotate(routeErr, fmt.Sprintf("when binlog to oracle dml txn, route schema and table failed. schema=%s, table=%s", schema, table))
			}
			iter := newSequenceIterator(&mut)
			for {
				mutType, row, err := iter.next()
				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, errors.Trace(err)
				}

				switch mutType {
				case tipb.MutationType_Insert:
					names, args, err := genDBInsert(schema, pinfo, info, row, loader.OracleDB, time.Local)
					if err != nil {
						return nil, errors.Annotate(err, "gen insert fail")
					}

					dml := &loader.DML{
						Tp:               loader.InsertDMLType,
						Database:         downStreamSchema,
						Table:            downStreamTable,
						Values:           make(map[string]interface{}),
						UpColumnsInfoMap: tableIDColumnsMap[mut.GetTableId()],
						DestDBType:       loader.OracleDB,
					}
					txn.DMLs = append(txn.DMLs, dml)
					for i, name := range names {
						dml.Values[strings.ToUpper(name)] = args[i]
					}
				case tipb.MutationType_Update:
					names, args, oldArgs, err := genDBUpdate(schema, pinfo, info, row, canAppendDefaultValue, loader.OracleDB, time.Local)
					if err != nil {
						return nil, errors.Annotate(err, "gen update fail")
					}

					dml := &loader.DML{
						Tp:               loader.UpdateDMLType,
						Database:         downStreamSchema,
						Table:            downStreamTable,
						Values:           make(map[string]interface{}),
						OldValues:        make(map[string]interface{}),
						UpColumnsInfoMap: tableIDColumnsMap[mut.GetTableId()],
						DestDBType:       loader.OracleDB,
					}
					txn.DMLs = append(txn.DMLs, dml)
					for i, name := range names {
						dml.Values[strings.ToUpper(name)] = args[i]
						dml.OldValues[strings.ToUpper(name)] = oldArgs[i]
					}

				case tipb.MutationType_DeleteRow:
					names, args, err := genDBDelete(schema, info, row, loader.OracleDB, time.Local)
					if err != nil {
						return nil, errors.Annotate(err, "gen delete fail")
					}

					dml := &loader.DML{
						Tp:               loader.DeleteDMLType,
						Database:         downStreamSchema,
						Table:            downStreamTable,
						Values:           make(map[string]interface{}),
						UpColumnsInfoMap: tableIDColumnsMap[mut.GetTableId()],
						DestDBType:       loader.OracleDB,
					}
					txn.DMLs = append(txn.DMLs, dml)
					for i, name := range names {
						dml.Values[strings.ToUpper(name)] = args[i]
					}

				default:
					return nil, errors.Errorf("unknown mutation type: %v", mutType)
				}
			}
		}
	}

	return
}

func genColumnInfoMap(table *model.TableInfo) map[string]*model.ColumnInfo {
	colsMap := make(map[string]*model.ColumnInfo)
	for _, column := range table.Columns {
		//for oracle downstream db, column name should be upper case.
		colsMap[strings.ToUpper(column.Name.O)] = column
	}
	return colsMap
}
