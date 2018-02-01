package restore

import (
	"fmt"

	pb "github.com/pingcap/pbReadder/pb_binlog"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

func decode(payload []byte) {
	binlog := &pb.Binlog{}
	err := binlog.Unmarshal(payload)
	if err != nil {
		log.Fatalf("payload %s unmarshal error %v", payload, err)
	}
	outPutBinlogHeader(binlog)
	switch binlog.Tp {
	case pb.BinlogType_DML:
		outputDML(binlog)
	case pb.BinlogType_DDL:
		outputDDL(binlog)
	default:
		panic("unreachable")
	}
}

func outPutBinlogHeader(binlog *pb.Binlog) {
	fmt.Printf("\n\nbinlog type: %s; commit ts: %d\n", binlog.Tp, binlog.CommitTs)
}

func outputDDL(binlog *pb.Binlog) {
	fmt.Printf("DDL query: %s\n", binlog.DdlQuery)
}

func outputDML(binlog *pb.Binlog) {
	dml := binlog.DmlData
	if dml == nil {
		log.Fatalf("dml binlog's data can't be empty")
	}

	for _, event := range dml.Events {
		e := &event
		outputEventHeader(e)
		tp := e.GetTp()
		row := e.GetRow()
		switch tp {
		case pb.EventType_Insert:
			outputInsertAndDeleteEvent(row)
		case pb.EventType_Update:
			outputUpdateEvent(row)
		case pb.EventType_Delete:
			outputInsertAndDeleteEvent(row)
		default:
			panic("unreachable")
		}
	}
}

func outputEventHeader(event *pb.Event) {
	fmt.Printf("schema: %s; table: %s; type: %s\n", event.GetSchemaName(), event.GetTableName(), event.GetTp())
}

func outputUpdateEvent(row [][]byte) {
	for _, c := range row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			log.Errorf("unmarshal error %v", err)
			return
		}

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			log.Errorf("decode row error %v", err)
			return
		}

		_, changedVal, err := codec.DecodeOne(col.ChangedValue)
		if err != nil {
			log.Errorf("decode row error %v", err)
			return
		}

		tp := col.Tp[0]
		fmt.Printf("%s(%s): %s => %s\n", col.Name, col.MysqlType, formatValue(val, tp), formatValue(changedVal, tp))
	}
}

func outputInsertAndDeleteEvent(row [][]byte) {
	for _, c := range row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			log.Errorf("unmarshal error %v", err)
			return
		}

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			log.Errorf("decode row error %v", err)
			return
		}

		tp := col.Tp[0]
		fmt.Printf("%s(%s): %s \n", col.Name, col.MysqlType, formatValue(val, tp))
	}
}

func formatValue(data types.Datum, tp byte) string {
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
