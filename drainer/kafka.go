package drainer

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/util"
	obinlog "github.com/pingcap/tidb-tools/binlog_proto/go-binlog"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	pb "github.com/pingcap/tipb/go-binlog"
)

func updateRowsToRows(table *model.TableInfo, rawRows [][]byte) (rows []*obinlog.Row, changedRows []*obinlog.Row, err error) {
	for _, raw := range rawRows {
		row, changedRow, err := updateRowToRow(table, raw)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		rows = append(rows, row)
		changedRows = append(changedRows, changedRow)
	}
	return
}

func insertRowsToRows(table *model.TableInfo, rawRows [][]byte) (rows []*obinlog.Row, err error) {
	for _, raw := range rawRows {
		row, err := insertRowToRow(table, raw)
		if err != nil {
			return nil, errors.Trace(err)
		}

		rows = append(rows, row)
	}
	return
}

func deleteRowsToRows(table *model.TableInfo, rawRows [][]byte) (rows []*obinlog.Row, err error) {
	for _, raw := range rawRows {
		row, err := deleteRowToRow(table, raw)
		if err != nil {
			return nil, errors.Trace(err)
		}

		rows = append(rows, row)
	}
	return
}

func nullColumn() (col *obinlog.Column) {
	col = new(obinlog.Column)
	col.IsNull = proto.Bool(true)

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
				log.Fatal(err)
			}
			col.Uint64Value = proto.Uint64(val)
		} else {
			val, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				log.Fatal(err)
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
		log.Warn("unknow mysql type: ", colInfo.Tp)
		str := fmt.Sprintf("%v", datum.GetValue())
		col.StringValue = proto.String(str)

	case "json":
		col.BytesValue = []byte(datum.GetMysqlJSON().String())

	default:
		log.Warn("unknow mysql type: ", colInfo.Tp)
		str := fmt.Sprintf("%v", datum.GetValue())
		col.StringValue = proto.String(str)

	}

	return
}

func updateRowToRow(tableInfo *model.TableInfo, raw []byte) (row *obinlog.Row, changedRow *obinlog.Row, err error) {
	colsTypeMap := util.ToColumnTypeMap(tableInfo.Columns)
	oldDatums, newDatums, err := translator.DecodeOldAndNewRow(raw, colsTypeMap, time.Local)
	if err != nil {
		return
	}

	row = new(obinlog.Row)
	changedRow = new(obinlog.Row)
	for _, col := range tableInfo.Columns {
		if val, ok := newDatums[col.ID]; ok {
			column := DatumToColumn(col, val)
			row.Columns = append(row.Columns, column)
		} else {
			if col.DefaultValue == nil {
				column := nullColumn()
				row.Columns = append(row.Columns, column)
			} else {
				log.Fatal("can't find value col: ", col, "default value: ", col.DefaultValue)
			}
		}
		if val, ok := oldDatums[col.ID]; ok {
			column := DatumToColumn(col, val)
			changedRow.Columns = append(changedRow.Columns, column)
		} else {
			if col.DefaultValue == nil {
				column := nullColumn()
				row.Columns = append(row.Columns, column)
			} else {
				log.Fatal("can't find value col: ", col, "default value: ", col.DefaultValue)
			}
		}
	}

	return
}

func deleteRowToRow(tableInfo *model.TableInfo, raw []byte) (row *obinlog.Row, err error) {
	columns := tableInfo.Columns

	colsTypeMap := util.ToColumnTypeMap(tableInfo.Columns)
	columnValues, err := tablecodec.DecodeRow(raw, colsTypeMap, time.Local)
	if err != nil {
		log.Error(err)
		err = errors.Trace(err)
		return
	}

	// log.Debugf("delete decodeRow: %+v\n", columnValues)

	row = new(obinlog.Row)

	for _, col := range columns {
		var column *obinlog.Column
		val, ok := columnValues[col.ID]
		if ok {
			column = DatumToColumn(col, val)
		} else {
			if col.DefaultValue == nil {
				column = nullColumn()
			} else {
				log.Fatal("can't find value col: ", col, "default value: ", col.DefaultValue)
			}
		}
		row.Columns = append(row.Columns, column)
	}

	return
}

func insertRowToRow(tableInfo *model.TableInfo, raw []byte) (row *obinlog.Row, err error) {
	columns := tableInfo.Columns

	remain, pk, err := codec.DecodeOne(raw)
	if err != nil {
		log.Error(err)
		err = errors.Trace(err)
		return
	}

	log.Debugf("decode pk: %+v", pk)

	colsTypeMap := util.ToColumnTypeMap(tableInfo.Columns)
	columnValues, err := tablecodec.DecodeRow(remain, colsTypeMap, time.Local)
	if err != nil {
		log.Error(err)
		err = errors.Trace(err)
		return
	}

	log.Debugf("decodeRow: %+v\n", columnValues)
	// maybe only the pk column value
	if columnValues == nil {
		columnValues = make(map[int64]types.Datum)
	}

	row = new(obinlog.Row)

	for _, col := range columns {
		if translator.IsPKHandleColumn(tableInfo, col) {
			columnValues[col.ID] = pk
		}

		var column *obinlog.Column
		val, ok := columnValues[col.ID]
		if ok {
			column = DatumToColumn(col, val)
		} else {
			if col.DefaultValue == nil {
				column = nullColumn()
			} else {
				log.Fatal("can't find value col: ", col, "default value: ", col.DefaultValue)
			}
		}
		row.Columns = append(row.Columns, column)
	}

	return
}

func mutationsToBinlog(schema *Schema, commitTs int64, mutations []pb.TableMutation) (*obinlog.Binlog, error) {
	binlog := new(obinlog.Binlog)
	binlog.Type = obinlog.BinlogType_DML
	binlog.CommitTs = commitTs
	binlog.DmlData = new(obinlog.DMLData)

	for _, mutation := range mutations {
		tableInfo, ok := schema.TableByID(mutation.GetTableId())
		if !ok {
			log.Warn("not found table id: ", mutation.GetTableId())
			continue
		}

		dbName, tableName, ok := schema.SchemaAndTableName(mutation.GetTableId())
		if !ok {
			log.Warn("not found table id: ", mutation.GetTableId())
			continue
		}

		// get obinlog.ColumnInfo
		var columnInfos []*obinlog.ColumnInfo
		for _, col := range tableInfo.Columns {
			info := new(obinlog.ColumnInfo)
			info.Name = col.Name.O
			info.MysqlType = types.TypeToStr(col.Tp, col.Charset)
			if mysql.HasPriKeyFlag(col.Flag) {
				info.IsPrimaryKey = true
			}
			columnInfos = append(columnInfos, info)
		}

		for _, mtype := range mutation.Sequence {
			table := new(obinlog.Table)
			table.ColumnInfo = columnInfos
			table.SchemaName = proto.String(dbName)
			table.TableName = proto.String(tableName)

			switch mtype {
			case pb.MutationType_Insert:
				table.Type = obinlog.MutationType_Insert.Enum()
				rows, err := insertRowsToRows(tableInfo, mutation.InsertedRows)
				if err != nil {
					return nil, errors.Trace(err)
				}
				table.Rows = rows
				binlog.DmlData.Tables = append(binlog.DmlData.Tables, table)

			case pb.MutationType_Update:
				table.Type = obinlog.MutationType_Update.Enum()
				rows, changedRow, err := updateRowsToRows(tableInfo, mutation.UpdatedRows)
				if err != nil {
					return nil, errors.Trace(err)
				}
				table.Rows = rows
				table.ChangedRows = changedRow
				binlog.DmlData.Tables = append(binlog.DmlData.Tables, table)
			case pb.MutationType_DeleteRow:
				table.Type = obinlog.MutationType_Delete.Enum()
				rows, err := deleteRowsToRows(tableInfo, mutation.DeletedRows)
				if err != nil {
					return nil, errors.Trace(err)
				}
				table.Rows = rows
				binlog.DmlData.Tables = append(binlog.DmlData.Tables, table)
			default:
				log.Warn("unknow sequecne type: ", mtype)
			}
		}
	}

	return binlog, nil
}

// Kafka is the syncer to kafka
type Kafka struct {
	addr   []string
	schema *Schema

	stop          chan struct{}
	items         chan *binlogItem
	topic         string
	clusterID     string
	checkPoint    checkpoint.CheckPoint
	positions     map[string]pb.Pos
	commitTs      int64
	ignoreSchemas map[string]struct{}

	wg sync.WaitGroup
}

// NewKafka return a instance of Kafka
func NewKafka(kafkaAddr string, clusterID string, schema *Schema, checkPoint checkpoint.CheckPoint, ignoreSchemas map[string]struct{}) *Kafka {
	commitTs, pos := checkPoint.Pos()
	return &Kafka{
		addr:          strings.Split(kafkaAddr, ","),
		schema:        schema,
		clusterID:     clusterID,
		topic:         clusterID + "_obinlog",
		items:         make(chan *binlogItem, 1024),
		checkPoint:    checkPoint,
		commitTs:      commitTs,
		positions:     pos,
		ignoreSchemas: ignoreSchemas,
	}
}

// Stop stop the kafka syncker
func (k *Kafka) Stop() {
	close(k.items)
	k.wg.Wait()
}

func (k *Kafka) binlogToBinlog(item *binlogItem) (binlog *obinlog.Binlog, err error) {
	pbBinlog := item.binlog
	if pbBinlog.DdlJobId == 0 {
		preWrite := &pb.PrewriteValue{}
		err = preWrite.Unmarshal(pbBinlog.GetPrewriteValue())
		if err != nil {
			return nil, err
		}
		binlog, err = mutationsToBinlog(k.schema, pbBinlog.CommitTs, preWrite.GetMutations())
		if err != nil {
			return
		}
	} else {
		binlog = new(obinlog.Binlog)
		binlog.Type = obinlog.BinlogType_DDL
		binlog.CommitTs = pbBinlog.CommitTs

		var database, sql string
		database, _, sql, err = k.schema.handleDDL(item.job, nil)
		if err != nil {
			return
		}
		binlog.DdlData = new(obinlog.DDLData)
		binlog.DdlData.SchemaName = &database
		binlog.DdlData.DdlQuery = []byte(sql)
	}

	return
}

func (k *Kafka) isIgnoreSchema(schema string) bool {
	_, ok := k.ignoreSchemas[strings.ToLower(schema)]

	return ok
}

// may change binlog, if the return value is nil, the whold binlog should be ignore
func (k *Kafka) filter(binlog *obinlog.Binlog) *obinlog.Binlog {
	switch binlog.Type {
	case obinlog.BinlogType_DDL:
		if k.isIgnoreSchema(binlog.DdlData.GetSchemaName()) {
			return nil
		}
	case obinlog.BinlogType_DML:
		var tables []*obinlog.Table
		for _, table := range binlog.DmlData.Tables {
			if k.isIgnoreSchema(table.GetSchemaName()) {
				continue
			}
			tables = append(tables, table)
		}
		if len(tables) == 0 {
			return nil
		}
		binlog.DmlData.Tables = tables
		return binlog
	}

	return binlog
}

// Run start sync binlog to kafka
func (k *Kafka) Run() error {
	log.Debug("start run...")
	k.wg.Add(1)
	defer k.wg.Done()

	config := sarama.NewConfig()
	config.Producer.MaxMessageBytes = 1 << 30 // 1G
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner

	producer, err := sarama.NewSyncProducer(k.addr, config)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Error(err)
		}
	}()

	for {
		saveCheckPoint := time.Tick(time.Second * 10)
		select {
		case <-k.stop:
			k.checkPoint.Save(k.commitTs, k.positions)
			return nil
		case <-saveCheckPoint:
			k.checkPoint.Save(k.commitTs, k.positions)
		case item := <-k.items:
			log.Debugf("job id: %v handle %v item : %+v", item.binlog.DdlJobId, item.binlog.Tp, *item)
			binlog, err := k.binlogToBinlog(item)
			if err != nil {
				log.Fatal(err)
			}

			binlog = k.filter(binlog)
			if binlog == nil {
				continue
			}

			data, err := binlog.Marshal()
			if err != nil {
				log.Fatal(err)
			}
			msg := &sarama.ProducerMessage{Topic: k.topic, Key: nil, Value: sarama.ByteEncoder(data), Partition: 0}
			for {
				_, _, err := producer.SendMessage(msg)
				if err != nil {
					log.Error(err)
					select {
					case <-k.stop:
						k.checkPoint.Save(k.commitTs, k.positions)
						return nil
					case <-time.After(time.Second):
					}
				} else {
					if pos, ok := k.positions[item.nodeID]; !ok || ComparePos(item.pos, pos) > 0 {
						k.positions[item.nodeID] = item.pos
					}
					if binlog.CommitTs > k.commitTs {
						k.commitTs = binlog.CommitTs
					}

					if binlog.DdlData != nil {
						k.checkPoint.Save(k.commitTs, k.positions)
					}
					break
				}
			}
		}
	}
}

func (k *Kafka) pushPBBinlog(item *binlogItem) {
	log.Debug("push item: ", *item)
	k.items <- item
}
