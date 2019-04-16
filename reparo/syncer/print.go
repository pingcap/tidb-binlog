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

func (p *printSyncer) Sync(pbBinlog *pb.Binlog, cb func(binlog *pb.Binlog)) error {
	var info string
	switch pbBinlog.Tp {
	case pb.BinlogType_DDL:
		info = getDDLStr(pbBinlog)
	case pb.BinlogType_DML:
		for _, event := range pbBinlog.GetDmlData().GetEvents() {
			header := getEventHeaderStr(&event)
			info = header + getEventDataStr(&event)
		}
	default:
		return errors.Errorf("unknown type: %v", pbBinlog.Tp)

	}

	fmt.Print(info)
	cb(pbBinlog)

	return nil
}

func (p *printSyncer) Close() error {
	return nil
}

func getEventDataStr(event *pb.Event) string {
	switch event.GetTp() {
	case pb.EventType_Insert:
		return getInsertOrDeleteEventStr(event.Row)
	case pb.EventType_Update:
		return getUpdateEventStr(event.Row)
	case pb.EventType_Delete:
		return getInsertOrDeleteEventStr(event.Row)
	}

	return ""
}

func getDDLStr(binlog *pb.Binlog) string {
	return fmt.Sprintf("DDL query: %s\n", binlog.DdlQuery)
}

func getEventHeaderStr(event *pb.Event) string {
	return fmt.Sprintf("schema: %s; table: %s; type: %s\n", event.GetSchemaName(), event.GetTableName(), event.GetTp())
}

func getUpdateEventStr(rows [][]byte) string {
	var eventStr string
	for _, row := range rows {
		eventStr += getUpdateRowStr(row)
	}

	return eventStr
}

func getUpdateRowStr(row []byte) string {
	col := &pb.Column{}
	err := col.Unmarshal(row)
	if err != nil {
		log.Errorf("unmarshal error %v", err)
		return ""
	}

	_, val, err := codec.DecodeOne(col.Value)
	if err != nil {
		log.Errorf("decode row error %v", err)
		return ""
	}

	_, changedVal, err := codec.DecodeOne(col.ChangedValue)
	if err != nil {
		log.Errorf("decode row error %v", err)
		return ""
	}

	tp := col.Tp[0]
	return fmt.Sprintf("%s(%s): %s => %s\n", col.Name, col.MysqlType, formatValueToString(val, tp), formatValueToString(changedVal, tp))
}

func getInsertOrDeleteEventStr(rows [][]byte) string {
	var eventStr string
	for _, row := range rows {
		eventStr += getInsertOrDeleteRowStr(row)
	}

	return eventStr
}

func getInsertOrDeleteRowStr(row []byte) string {
	col := &pb.Column{}
		err := col.Unmarshal(row)
		if err != nil {
			log.Errorf("unmarshal error %v", err)
			return ""
		}

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			log.Errorf("decode row error %v", err)
			return ""
		}

		tp := col.Tp[0]
		return fmt.Sprintf("%s(%s): %s \n", col.Name, col.MysqlType, formatValueToString(val, tp))
}
