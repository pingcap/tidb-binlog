package executor

import (
	"context"
	"math"
	"strconv"
	"sync"
	"time"
	"fmt"
	"runtime"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
)

type pulsarExecutor struct {
	addr     string
	producer pulsar.Producer
	topic    string

	meta *checkpoint.KafkaMeta

	toBeAckCommitTSMu sync.Mutex
	toBeAckCommitTS   map[int64]struct{}

	successes       chan pulsar.ProducerMessage
	lastSuccessTime time.Time

	*baseError
}

func newPulsar(cfg *DBConfig) (Executor, error) {
	var topic string
	if len(cfg.TopicName) == 0 {
		clusterIDStr := strconv.FormatUint(cfg.ClusterID, 10)
		topic = clusterIDStr + "_obinlog"
	} else {
		topic = cfg.TopicName
	}
	log.Debugf("Pulsar Config: topic[%v], addr[%v]", cfg.TopicName, cfg.PulsaAddr)
	executor := &pulsarExecutor{
		addr:            cfg.PulsaAddr,
		topic:           topic,
		meta:            checkpoint.GetKafkaMeta(),
		toBeAckCommitTS: make(map[int64]struct{}),
		baseError:       newBaseError(),
		successes:       make(chan pulsar.ProducerMessage, 128),
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                     fmt.Sprintf("pulsar://%s", executor.addr),
		OperationTimeoutSeconds: 5,
		MessageListenerThreads:  runtime.NumCPU(),
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: executor.topic,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	executor.producer = producer
	go executor.run()

	return executor, nil
}

func (p *pulsarExecutor) Execute(sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	if len(sqls) == 0 {
		return nil
	}

	binlog := new(obinlog.Binlog)
	if isDDL {
		binlog.Type = obinlog.BinlogType_DDL
		binlog.CommitTs = commitTSs[0]
		binlog.DdlData = new(obinlog.DDLData)
		log.Debugf("pulsarExecutor.Execute: sqls [%v]", sqls)
		log.Debugf("pulsarExecutor.Execute: args [%v]", args)
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

func (p *pulsarExecutor) Close() error {
	return p.producer.Close()
}

func (p *pulsarExecutor) saveBinlog(binlog *obinlog.Binlog) error {
	data, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	msg := pulsar.ProducerMessage{
		Payload:    data,
		Properties: map[string]string{"CommitTS": strconv.FormatInt(binlog.CommitTs, 10)},
	}

	p.toBeAckCommitTSMu.Lock()
	if len(p.toBeAckCommitTS) == 0 {
		p.lastSuccessTime = time.Now()
		p.meta.SetSafeTS(binlog.CommitTs - 1)
	}
	p.toBeAckCommitTS[binlog.CommitTs] = struct{}{}
	p.toBeAckCommitTSMu.Unlock()

	p.producer.SendAsync(context.Background(), msg, func(msg pulsar.ProducerMessage, err error) {
		if err != nil {
			p.SetErr(err)
			return
		}
		p.successes <- msg
	})

	select {
	case <-p.errCh:
		return errors.Trace(p.err)
	default:
		return nil
	}
}

func (p *pulsarExecutor) run() {
	checkTick := time.NewTicker(time.Second)
	defer checkTick.Stop()

	for {
		select {
		case msg := <-p.successes:
			commitTs, _ := strconv.ParseInt(msg.Properties["CommitTS"], 10, 64)
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
