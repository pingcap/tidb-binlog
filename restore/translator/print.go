package translator

import (
	"fmt"

	"github.com/ngaut/log"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/util/codec"
)

type printTranslator struct {
}

func newPrintTranslator() Translator {
	return &printTranslator{}
}

func (p *printTranslator) TransInsert(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error) {
	printHeader(binlog, event)
	printInsertAndDeleteEvent(row)
	return "", nil, nil
}

func (p *printTranslator) TransUpdate(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error) {
	printHeader(binlog, event)
	printUpdateEvent(row)
	return "", nil, nil

}

func (p *printTranslator) TransDelete(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error) {
	printHeader(binlog, event)
	printInsertAndDeleteEvent(row)
	return "", nil, nil

}

func (p *printTranslator) TransUpdateSafeMode(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error) {
	printHeader(binlog, event)
	printUpdateEvent(row)
	return "", nil, nil
}

func (p *printTranslator) TransDDL(binlog *pb.Binlog) (string, []interface{}, error) {
	printBinlogHeader(binlog)
	printDDL(binlog)
	return "", nil, nil
}

func printHeader(binlog *pb.Binlog, event *pb.Event) {
	printBinlogHeader(binlog)
	printEventHeader(event)
}

func printBinlogHeader(binlog *pb.Binlog) {
	fmt.Printf("\n\nbinlog type: %s; commit ts: %d\n", binlog.Tp, binlog.CommitTs)
}

func printDDL(binlog *pb.Binlog) {
	fmt.Printf("DDL query: %s\n", binlog.DdlQuery)
}

func printEventHeader(event *pb.Event) {
	fmt.Printf("schema: %s; table: %s; type: %s\n", event.GetSchemaName(), event.GetTableName(), event.GetTp())
}

func printUpdateEvent(row [][]byte) {
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
		fmt.Printf("%s(%s): %s => %s\n", col.Name, col.MysqlType, formatValueToString(val, tp), formatValueToString(changedVal, tp))
	}
}

func printInsertAndDeleteEvent(row [][]byte) {
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
		fmt.Printf("%s(%s): %s \n", col.Name, col.MysqlType, formatValueToString(val, tp))
	}
}
