package sync

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/util"
	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
)

var maxWaitTimeToSendMSG = time.Second * 30

var _ Syncer = &KafkaSyncer{}

// KafkaSyncer sync data to kafka
type KafkaSyncer struct {
	addr     []string
	producer sarama.AsyncProducer
	topic    string

	toBeAckCommitTSMu sync.Mutex
	toBeAckCommitTS   map[int64]struct{}

	lastSuccessTime time.Time

	shutdown chan struct{}
	*baseSyncer
}

// newAsyncProducer will only be changed in unit test for mock
var newAsyncProducer = sarama.NewAsyncProducer

// NewKafka returns a instance of KafkaSyncer
func NewKafka(cfg *DBConfig, tableInfoGetter translator.TableInfoGetter) (*KafkaSyncer, error) {
	var topic string
	if len(cfg.TopicName) == 0 {
		clusterIDStr := strconv.FormatUint(cfg.ClusterID, 10)
		topic = clusterIDStr + "_obinlog"
	} else {
		topic = cfg.TopicName
	}

	executor := &KafkaSyncer{
		addr:            strings.Split(cfg.KafkaAddrs, ","),
		topic:           topic,
		toBeAckCommitTS: make(map[int64]struct{}),
		shutdown:        make(chan struct{}),
		baseSyncer:      newBaseSyncer(tableInfoGetter),
	}

	config, err := util.NewSaramaConfig(cfg.KafkaVersion, "kafka.")
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

	executor.producer, err = newAsyncProducer(executor.addr, config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	go executor.run()

	return executor, nil
}

// Sync implements Syncer interface
func (p *KafkaSyncer) Sync(item *Item) error {
	slaveBinlog, err := translator.TiBinlogToSlaveBinlog(p.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue)
	if err != nil {
		return errors.Trace(err)
	}

	err = p.saveBinlog(slaveBinlog, item)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Close implements Syncer interface
func (p *KafkaSyncer) Close() error {
	close(p.shutdown)

	err := <-p.Error()

	return err
}

func (p *KafkaSyncer) saveBinlog(binlog *obinlog.Binlog, item *Item) error {
	// log.Debug("save binlog: ", binlog.String())
	data, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	msg := &sarama.ProducerMessage{Topic: p.topic, Key: nil, Value: sarama.ByteEncoder(data), Partition: 0}
	msg.Metadata = item

	p.toBeAckCommitTSMu.Lock()
	if len(p.toBeAckCommitTS) == 0 {
		p.lastSuccessTime = time.Now()
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

func (p *KafkaSyncer) run() {
	var wg sync.WaitGroup

	// handle successs from producer
	wg.Add(1)
	go func() {
		defer wg.Done()

		for msg := range p.producer.Successes() {
			item := msg.Metadata.(*Item)
			commitTs := item.Binlog.GetCommitTs()
			log.Debug("commitTs: ", commitTs, " return success from kafka")

			p.toBeAckCommitTSMu.Lock()
			p.lastSuccessTime = time.Now()
			delete(p.toBeAckCommitTS, commitTs)
			p.toBeAckCommitTSMu.Unlock()

			p.success <- item
		}
		close(p.success)
	}()

	// handle errors from producer
	wg.Add(1)
	go func() {
		defer wg.Done()

		for err := range p.producer.Errors() {
			panic(err)
		}
	}()

	checkTick := time.NewTicker(time.Second)
	defer checkTick.Stop()

	for {
		select {
		case <-checkTick.C:
			p.toBeAckCommitTSMu.Lock()
			if len(p.toBeAckCommitTS) > 0 && time.Since(p.lastSuccessTime) > maxWaitTimeToSendMSG {
				log.Debug("fail to push to kafka")
				err := errors.Errorf("fail to push msg to kafka after %v, check if kafka is up and working", maxWaitTimeToSendMSG)
				p.setErr(err)
				p.toBeAckCommitTSMu.Unlock()
				return
			}
			p.toBeAckCommitTSMu.Unlock()
		case <-p.shutdown:
			err := p.producer.Close()
			p.setErr(err)

			wg.Wait()
			return
		}
	}
}
