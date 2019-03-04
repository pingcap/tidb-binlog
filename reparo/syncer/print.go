package syncer

import (
	"fmt"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/util/codec"
)

type printSyncer struct{}

var _ Syncer = &printSyncer{}

func newPrintSyncer() (*printSyncer, error) {
	return &printSyncer{}, nil
}

func (p *printSyncer) Sync(pbBinlog *pb.Binlog) error {
	switch pbBinlog.Tp {
	case pb.BinlogType_DDL:
		printDDL(pbBinlog)
	case pb.BinlogType_DML:
		for _, event := range pbBinlog.GetDmlData().GetEvents() {
			printEvent(&event)
		}
	default:
		return errors.Errorf("unknown type: %v", pbBinlog.Tp)

	}

	return nil
}

func (p *printSyncer) Close() error {
	return nil
}

func printEvent(event *pb.Event) {
	printHeader(event)

	switch event.GetTp() {
	case pb.EventType_Insert:
		printInsertOrDeleteEvent(event.Row)
	case pb.EventType_Update:
		printUpdateEvent(event.Row)
	case pb.EventType_Delete:
		printInsertOrDeleteEvent(event.Row)
	}
}

func printHeader(event *pb.Event) {
	printEventHeader(event)
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

func printInsertOrDeleteEvent(row [][]byte) {
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
