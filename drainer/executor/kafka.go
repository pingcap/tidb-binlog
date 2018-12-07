package executor

import (
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/pkg/util"
	obinlog "github.com/pingcap/tidb-tools/tidb_binlog/slave_binlog_proto/go-binlog"
)

var maxWaitTimeToSendMSG = time.Second * 30

type kafkaExecutor struct {
	addr      []string
	version   string
	clusterID string
	producer  sarama.AsyncProducer
	topic     string

	meta *checkpoint.KafkaMeta

	toBeAckCommitTSMu sync.Mutex
	toBeAckCommitTS   map[int64]struct{}

	lastSuccessTime time.Time

	*baseError
}

func newKafka(cfg *DBConfig) (Executor, error) {
	var topic string
	if len(cfg.TopicName) == 0 {
		clusterIDStr := strconv.FormatUint(cfg.ClusterID, 10)
		topic = clusterIDStr + "_obinlog"
	} else {
		topic = cfg.TopicName
	}

	executor := &kafkaExecutor{
		addr:            strings.Split(cfg.KafkaAddrs, ","),
		version:         cfg.KafkaVersion,
		topic:           topic,
		meta:            checkpoint.GetKafkaMeta(),
		toBeAckCommitTS: make(map[int64]struct{}),
		baseError:       newBaseError(),
	}

	config, err := util.NewSaramaConfig(executor.version, "kafka.")
	if err != nil {
		return nil, errors.Trace(err)
	}

	config.Producer.Flush.MaxMessages = cfg.KafkaMaxMessages

	// maintain minimal set that has been necessary so far
	// this also avoid take too much time in NewAsyncProducer if kafka is down
	// because it will fetch metadata right away if setting Full = true, and we set
	// config.Metadata.Retry.Max to be a pretty hight value
	// maybe when this issue if fixed: https://github.com/Shopify/sarama/issues/1145
	// we can avoid setting Metadata.Retry to be a pretty hight value too
	config.Metadata.Full = false
	config.Metadata.Retry.Max = 10000
	config.Metadata.Retry.Backoff = 500 * time.Millisecond

	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.MaxMessageBytes = 1 << 30
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	// just set to a pretty high retry num, so we will not drop some msg and
	// continue to push the laster msg, we will quit if we find msg fail to push after `maxWaitTimeToSendMSG`
	// aim to avoid not continuity msg sent to kafka.. see: https://github.com/Shopify/sarama/issues/838
	config.Producer.Retry.Max = 10000
	config.Producer.Retry.Backoff = 500 * time.Millisecond

	executor.producer, err = sarama.NewAsyncProducer(executor.addr, config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	go executor.run()

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
			for idx = 0; idx < len(tables); idx++ {
				preTable = tables[idx]
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
	log.Debug("save binlog: ", binlog.String())
	data, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	msg := &sarama.ProducerMessage{Topic: p.topic, Key: nil, Value: sarama.ByteEncoder(data), Partition: 0}
	msg.Metadata = binlog.CommitTs

	p.toBeAckCommitTSMu.Lock()
	if len(p.toBeAckCommitTS) == 0 {
		p.lastSuccessTime = time.Now()
		p.meta.SetSafeTS(binlog.CommitTs - 1)
	}
	p.toBeAckCommitTS[binlog.CommitTs] = struct{}{}
	p.toBeAckCommitTSMu.Unlock()

	select {
	case p.producer.Input() <- msg:
		return nil
	case <-p.errCh:
		return errors.Trace(p.err)
	}

}

func (p *kafkaExecutor) run() {
	checkTick := time.NewTicker(time.Second)
	defer checkTick.Stop()

	for {
		select {
		case msg := <-p.producer.Successes():
			commitTs := msg.Metadata.(int64)
			log.Debug("commitTs: ", commitTs, " return success from kafka")
			p.lastSuccessTime = time.Now()

			p.toBeAckCommitTSMu.Lock()
			delete(p.toBeAckCommitTS, commitTs)
			if len(p.toBeAckCommitTS) == 0 {
				p.meta.SetSafeTS(math.MaxInt64)
			} else {
				p.meta.SetSafeTS(commitTs)
			}
			p.toBeAckCommitTSMu.Unlock()
		case err := <-p.producer.Errors():
			panic(err)
		case <-checkTick.C:
			p.toBeAckCommitTSMu.Lock()
			if len(p.toBeAckCommitTS) > 0 && time.Since(p.lastSuccessTime) > maxWaitTimeToSendMSG {
				log.Debug("fail to push to kafka")
				err := errors.Errorf("fail to push msg to kafka after %v, check if kafka is up and working", maxWaitTimeToSendMSG)
				p.SetErr(err)
				p.toBeAckCommitTSMu.Unlock()
				return
			}
			p.toBeAckCommitTSMu.Unlock()
		}
	}
}
