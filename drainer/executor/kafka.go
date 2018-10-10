package executor

import (
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/util"
	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
)

type kafkaExecutor struct {
	addr      []string
	version   string
	clusterID string
	producer  sarama.SyncProducer
	topic     string
}

func newKafka(cfg *DBConfig) (Executor, error) {
	clusterIDStr := strconv.FormatUint(cfg.ClusterID, 10)
	executor := &kafkaExecutor{
		addr:      strings.Split(cfg.KafkaAddrs, ","),
		version:   cfg.KafkaVersion,
		clusterID: clusterIDStr,
		topic:     clusterIDStr + "_obinlog",
	}

	var err error
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = time.Second
	executor.producer, err = util.CreateKafkaProducer(config, executor.addr, executor.version, 1<<30, "kafka.")
	if err != nil {
		return nil, err
	}

	return executor, nil
}

func (p *kafkaExecutor) Execute(sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	if len(sqls) == 0 {
		return nil
	}

	binlog := new(obinlog.Binlog)
	if isDDL {
		binlog.Type = obinlog.BinlogType_DDL
		binlog.CommitTs = commitTSs[0]
		binlog.DdlData = new(obinlog.DDLData)
		binlog.DdlData.SchemaName = proto.String(args[0][0].(string))
		binlog.DdlData.TableName = proto.String(args[0][1].(string))
		binlog.DdlData.DdlQuery = []byte(sqls[0])
	} else {
		binlog.Type = obinlog.BinlogType_DML
		binlog.CommitTs = commitTSs[0]
		binlog.DmlData = new(obinlog.DMLData)

		var tables []*obinlog.Table
		for i := range args {
			table := args[i][0].(*obinlog.Table)
			var idx int
			var preTable *obinlog.Table
			for idx, preTable = range tables {
				if preTable.GetSchemaName() == table.GetSchemaName() && preTable.GetTableName() == table.GetTableName() {
					preTable.Mutations = append(preTable.Mutations, table.Mutations...)
					break
				}
			}

			if idx == len(tables) {
				tables = append(tables, table)
			}
		}

		binlog.DmlData.Tables = tables
	}

	return errors.Trace(p.saveBinlog(binlog))
}

func (p *kafkaExecutor) Close() error {
	return p.producer.Close()
}

func (p *kafkaExecutor) saveBinlog(binlog *obinlog.Binlog) error {
	log.Debug(binlog.String())
	data, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	msg := &sarama.ProducerMessage{Topic: p.topic, Key: nil, Value: sarama.ByteEncoder(data), Partition: 0}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
