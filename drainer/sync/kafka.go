// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package sync

import (
	"fmt"
	"hash"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/util"
	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
	"go.uber.org/zap"
)

var maxWaitTimeToSendMSG = time.Second * 30

var _ Syncer = &KafkaSyncer{}

// PartitionMode Kafka partition mode
type PartitionMode byte

const (
	// PartitionFixed choose fixed partition 0
	PartitionFixed PartitionMode = iota + 1
	// PartitionBySchema choose partition by schema
	PartitionBySchema
	// PartitionByTable choose partition by table
	PartitionByTable
)

// KafkaSyncer sync data to kafka
type KafkaSyncer struct {
	addr     []string
	producer sarama.AsyncProducer
	cli      sarama.Client
	topic    string

	msgTracker *msgTracker

	lastSuccessTime time.Time

	shutdown chan struct{}
	*baseSyncer

	partitionMode     PartitionMode
	includeResolvedTs bool
}

func partitionMode(mode string) PartitionMode {
	switch strings.ToLower(mode) {
	case "schema":
		return PartitionBySchema
	case "table":
		return PartitionByTable
	default:
		return PartitionFixed
	}
}

// newAsyncProducer will only be changed in unit test for mock
var newAsyncProducer = sarama.NewAsyncProducerFromClient

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
		addr:              strings.Split(cfg.KafkaAddrs, ","),
		topic:             topic,
		msgTracker:        newMsgTracker(),
		shutdown:          make(chan struct{}),
		baseSyncer:        newBaseSyncer(tableInfoGetter),
		partitionMode:     partitionMode(cfg.KafkaPartitionMode),
		includeResolvedTs: cfg.IncludeResolvedTs,
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

	if executor.partitionMode == PartitionFixed {
		config.Producer.Partitioner = sarama.NewManualPartitioner
	} else {
		config.Producer.Partitioner = newHashPartitioner
	}
	config.Producer.MaxMessageBytes = 1 << 30
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	// just set to a pretty high retry num, so we will not drop some msg and
	// continue to push the laster msg, we will quit if we find msg fail to push after `maxWaitTimeToSendMSG`
	// aim to avoid not continuity msg sent to kafka.. see: https://github.com/Shopify/sarama/issues/838
	config.Producer.Retry.Max = 10000
	config.Producer.Retry.Backoff = 500 * time.Millisecond

	executor.cli, err = sarama.NewClient(executor.addr, config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	executor.producer, err = newAsyncProducer(executor.cli)
	if err != nil {
		return nil, errors.Trace(err)
	}

	go executor.run()

	return executor, nil
}

func (p *KafkaSyncer) getPartitions() ([]int32, error) {
	return p.cli.Partitions(p.topic)
}

// Sync implements Syncer interface
func (p *KafkaSyncer) Sync(item *Item) error {
	slaveBinlog, err := translator.TiBinlogToSlaveBinlog(p.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue)
	if err != nil {
		return errors.Trace(err)
	}
	if p.partitionMode == PartitionFixed {
		return errors.Trace(p.saveBinlog(slaveBinlog, item, nil))
	}

	var msgs []*sarama.ProducerMessage
	switch p.partitionMode {
	case PartitionBySchema:
		msgs, err = p.splitBinlogBySchema(slaveBinlog, item)
		if err != nil {
			return errors.Trace(err)
		}
	case PartitionByTable:
		msgs, err = p.splitBinlogByTable(slaveBinlog, item)
		if err != nil {
			return errors.Trace(err)
		}
	default:
		return errors.Errorf("Not supported partition mode: %v", p.partitionMode)
	}
	if p.includeResolvedTs {
		resolvedMsgs, err := p.createResolvedMsgs(slaveBinlog.CommitTs, item)
		if err != nil {
			return errors.Trace(err)
		}
		msgs = append(msgs, resolvedMsgs...)
	}
	p.msgTracker.SentN(slaveBinlog.CommitTs, len(msgs))
	for _, m := range msgs {
		if err := p.sendMsg(m); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (p *KafkaSyncer) createResolvedMsgs(ts int64, item *Item) ([]*sarama.ProducerMessage, error) {
	partitions, err := p.getPartitions()
	if err != nil {
		return nil, errors.Trace(err)
	}
	msgs := make([]*sarama.ProducerMessage, 0, len(partitions))
	for i, _ := range partitions {
		m, err := p.newResolvedMsg(ts, int32(i), item)
		if err != nil {
			return nil, errors.Trace(err)
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

func (p *KafkaSyncer) splitBinlogBySchema(binlog *obinlog.Binlog, item *Item) ([]*sarama.ProducerMessage, error) {
	if binlog.Type == obinlog.BinlogType_DDL {
		partitionKey := sarama.StringEncoder(*binlog.DdlData.SchemaName)
		m, err := p.newBinlogMsg(binlog, item, partitionKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return []*sarama.ProducerMessage{m}, nil
	}

	tablesBySchema := make(map[string][]*obinlog.Table)
	for _, table := range binlog.DmlData.Tables {
		schemaName := *table.SchemaName
		tablesBySchema[schemaName] = append(tablesBySchema[schemaName], table)
	}
	msgs := make([]*sarama.ProducerMessage, 0, len(tablesBySchema))
	for schema, tables := range tablesBySchema {
		l := obinlog.Binlog{
			Type:     binlog.Type,
			CommitTs: binlog.CommitTs,
			DmlData: &obinlog.DMLData{
				Tables: tables,
			},
		}
		m, err := p.newBinlogMsg(&l, item, sarama.StringEncoder(schema))
		if err != nil {
			return nil, errors.Trace(err)
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

func (p *KafkaSyncer) splitBinlogByTable(binlog *obinlog.Binlog, item *Item) ([]*sarama.ProducerMessage, error) {
	if binlog.Type == obinlog.BinlogType_DDL {
		partitionKey := sarama.StringEncoder(*binlog.DdlData.SchemaName + "." + *binlog.DdlData.TableName)
		m, err := p.newBinlogMsg(binlog, item, partitionKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return []*sarama.ProducerMessage{m}, nil
	}

	msgs := make([]*sarama.ProducerMessage, 0, len(binlog.DmlData.Tables))
	for _, table := range binlog.DmlData.Tables {
		l := obinlog.Binlog{
			Type:     binlog.Type,
			CommitTs: binlog.CommitTs,
			DmlData: &obinlog.DMLData{
				Tables: []*obinlog.Table{table},
			},
		}
		m, err := p.newBinlogMsg(&l, item, sarama.StringEncoder(*table.SchemaName+"."+*table.TableName))
		if err != nil {
			return nil, errors.Trace(err)
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

// Close implements Syncer interface
func (p *KafkaSyncer) Close() error {
	close(p.shutdown)

	err := <-p.Error()

	return err
}

func (p *KafkaSyncer) newBinlogMsg(bl *obinlog.Binlog, it *Item, k sarama.Encoder) (*sarama.ProducerMessage, error) {
	data, err := bl.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}

	msg := &sarama.ProducerMessage{Topic: p.topic, Key: k, Value: sarama.ByteEncoder(data), Partition: -1}
	msg.Metadata = it
	return msg, nil
}

func (p *KafkaSyncer) newResolvedMsg(ts int64, partition int32, item *Item) (*sarama.ProducerMessage, error) {
	bl := obinlog.Binlog{
		Type:     obinlog.BinlogType_RESOLVED,
		CommitTs: ts,
	}
	data, err := bl.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}

	msg := &sarama.ProducerMessage{Topic: p.topic, Value: sarama.ByteEncoder(data), Partition: partition}
	msg.Metadata = item
	return msg, nil
}

func (p *KafkaSyncer) saveBinlog(binlog *obinlog.Binlog, item *Item, key sarama.Encoder) error {
	msg, err := p.newBinlogMsg(binlog, item, key)
	if err != nil {
		return errors.Trace(err)
	}
	p.msgTracker.Sent(binlog.CommitTs)
	return p.sendMsg(msg)
}

func (p *KafkaSyncer) sendMsg(msg *sarama.ProducerMessage) error {
	select {
	case p.producer.Input() <- msg:
		return nil
	case <-p.errCh:
		return errors.Trace(p.err)
	}
}

func (p *KafkaSyncer) run() {
	var wg sync.WaitGroup

	// handle successes from producer
	wg.Add(1)
	go func() {
		defer wg.Done()

		for msg := range p.producer.Successes() {
			item := msg.Metadata.(*Item)
			commitTs := item.Binlog.GetCommitTs()
			log.Debug("get success msg from producer", zap.Int64("ts", commitTs))
			isLastOne := p.msgTracker.Acked(commitTs)
			if isLastOne {
				p.success <- item
			}
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
			if p.msgTracker.HasWaitedTooLongForAck(maxWaitTimeToSendMSG) {
				log.Debug("fail to push to kafka")
				err := errors.Errorf("fail to push msg to kafka after %v, check if kafka is up and working", maxWaitTimeToSendMSG)
				p.setErr(err)
				return
			}
		case <-p.shutdown:
			err := p.producer.Close()
			if err == nil {
				err = p.cli.Close()
			}
			p.setErr(err)

			wg.Wait()
			return
		}
	}
}

type hashPartitioner struct {
	hasher hash.Hash32
}

func newHashPartitioner(topic string) sarama.Partitioner {
	return &hashPartitioner{
		hasher: fnv.New32a(),
	}
}

func (p *hashPartitioner) PartitionByKey(key sarama.Encoder, numPartitions int32) (int32, error) {
	bytes, err := key.Encode()
	if err != nil {
		return -1, err
	}
	p.hasher.Reset()
	_, err = p.hasher.Write(bytes)
	if err != nil {
		return -1, err
	}
	partition := int32(p.hasher.Sum32()) % numPartitions
	if partition < 0 {
		partition = -partition
	}
	return partition, nil
}

func (p *hashPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Partition >= 0 {
		if message.Partition >= numPartitions {
			return -1, fmt.Errorf("invalid partition %d (maximum: %d)", message.Partition, numPartitions-1)
		}
		return message.Partition, nil
	}
	return p.PartitionByKey(message.Key, numPartitions)
}

func (p *hashPartitioner) RequiresConsistency() bool {
	return true
}

type msgTracker struct {
	sync.Mutex
	msgsToBeAcked   map[int64]int
	lastSuccessTime time.Time
}

func newMsgTracker() *msgTracker {
	return &msgTracker{
		msgsToBeAcked:   make(map[int64]int),
		lastSuccessTime: time.Now(),
	}
}

func (mt *msgTracker) SentN(commitTs int64, n int) {
	mt.Lock()
	if !mt.hasPendingUnlocked() {
		mt.lastSuccessTime = time.Now()
	}
	mt.msgsToBeAcked[commitTs] += n
	mt.Unlock()
}

func (mt *msgTracker) Sent(commitTs int64) {
	mt.SentN(commitTs, 1)
}

func (mt *msgTracker) Acked(commitTs int64) (isLastOne bool) {
	mt.Lock()
	mt.lastSuccessTime = time.Now()
	mt.msgsToBeAcked[commitTs] -= 1
	if mt.msgsToBeAcked[commitTs] == 0 {
		delete(mt.msgsToBeAcked, commitTs)
		isLastOne = true
	}
	mt.Unlock()
	return
}

func (mt *msgTracker) HasPending() bool {
	mt.Lock()
	defer mt.Unlock()
	return mt.hasPendingUnlocked()
}

func (mt *msgTracker) HasWaitedTooLongForAck(timeout time.Duration) bool {
	mt.Lock()
	defer mt.Unlock()
	return mt.hasPendingUnlocked() && time.Since(mt.lastSuccessTime) > timeout
}

func (mt *msgTracker) hasPendingUnlocked() bool {
	return len(mt.msgsToBeAcked) > 0
}
