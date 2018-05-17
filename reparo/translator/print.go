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

func (p *printTranslator) TransInsert(event *pb.Event) (*TranslateResult, error) {
	printHeader(event)
	printInsertAndDeleteEvent(event.GetRow())
	return nil, nil
}

func (p *printTranslator) TransUpdate(event *pb.Event) (*TranslateResult, error) {
	printHeader(event)
	printUpdateEvent(event.GetRow())
	return nil, nil
}

func (p *printTranslator) TransDelete(event *pb.Event) (*TranslateResult, error) {
	printHeader(event)
	printInsertAndDeleteEvent(event.GetRow())
	return nil, nil

}

func (p *printTranslator) TransDDL(ddl string) (*TranslateResult, error) {
	printDDL(ddl)
	return nil, nil
}

func printHeader(event *pb.Event) {
	printEventHeader(event)
}

func printDDL(ddl string) {
	fmt.Printf("DDL query: %s\n", ddl)
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
