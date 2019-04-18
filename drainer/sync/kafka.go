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
	"bytes"
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

type PartitionMode byte

const (
	// choose fixed partition 0
	PARTITION_FIXED = iota + 1
	// choose partition by schema
	PARTITION_BY_SCHEMA
	// choose partition by table
	PARTITION_BY_TABLE
)

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

	partitionMode PartitionMode
}

func partitionMode(mode string) PartitionMode {
	switch strings.ToLower(mode) {
	case "schema":
		return PARTITION_BY_SCHEMA
	case "table":
		return PARTITION_BY_TABLE
	default:
		return PARTITION_FIXED
	}
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
		partitionMode:   partitionMode(cfg.KafkaPartitionMode),
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

	if executor.partitionMode == PARTITION_FIXED {
		config.Producer.Partitioner = sarama.NewManualPartitioner
	} else {
		config.Producer.Partitioner = sarama.NewReferenceHashPartitioner
	}
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
	switch slaveBinlog.Type {
	case obinlog.BinlogType_DDL:
		return errors.Trace(p.saveBinlog(slaveBinlog, item, nil))
	case obinlog.BinlogType_DML:
		tables := slaveBinlog.DmlData.Tables
		if len(tables) == 1 || p.partitionMode == PARTITION_FIXED {
			return errors.Trace(p.saveBinlog(slaveBinlog, item, nil))
		} else if p.partitionMode == PARTITION_BY_SCHEMA {
			return errors.Trace(p.executeDMLPartitionBySchema(slaveBinlog, item))
		} else if p.partitionMode == PARTITION_BY_TABLE {
			return errors.Trace(p.executeDMLPartitionByTable(slaveBinlog, item))
		} else {
			return errors.Errorf("Invalid partition mode: %v", p.partitionMode)
		}
	}
	return errors.Errorf("Invalid binlog type: %v", slaveBinlog.Type)
}

func (p *KafkaSyncer) executeDMLPartitionBySchema(binlog *obinlog.Binlog, item *Item) error {
	tablesBySchema := make(map[string][]*obinlog.Table)
	for _, table := range binlog.DmlData.Tables {
		schemaName := *table.SchemaName
		if _, ok := tablesBySchema[schemaName]; !ok {
			tablesBySchema[schemaName] = make([]*obinlog.Table, 0, 1)
		}
		tablesBySchema[schemaName] = append(tablesBySchema[schemaName], table)
	}

	var err error
	for schema, tables := range tablesBySchema {
		binlog.DmlData.Tables = tables
		if err = p.saveBinlog(binlog, item, sarama.StringEncoder(schema)); err != nil {
			break
		}
	}
	return errors.Trace(err)
}

func (p *KafkaSyncer) executeDMLPartitionByTable(binlog *obinlog.Binlog, item *Item) error {
	tables := binlog.DmlData.Tables
	binlog.DmlData.Tables = make([]*obinlog.Table, 1)
	var err error
	var buffer bytes.Buffer
	for _, table := range tables {
		binlog.DmlData.Tables[0] = table
		buffer.Reset()
		buffer.WriteString(*table.SchemaName)
		buffer.WriteRune('.')
		buffer.WriteString(*table.TableName)
		if err = p.saveBinlog(binlog, item, sarama.StringEncoder(buffer.String())); err != nil {
			break
		}
	}
	return errors.Trace(err)
}

// Close implements Syncer interface
func (p *KafkaSyncer) Close() error {
	close(p.shutdown)

	err := <-p.Error()

	return err
}

func (p *KafkaSyncer) saveBinlog(binlog *obinlog.Binlog, item *Item, key sarama.Encoder) error {
	// log.Debug("save binlog: ", binlog.String())
	data, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	msg := &sarama.ProducerMessage{Topic: p.topic, Key: key, Value: sarama.ByteEncoder(data), Partition: 0}
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

	// handle successes from producer
	wg.Add(1)
	go func() {
		defer wg.Done()

		for msg := range p.producer.Successes() {
			item := msg.Metadata.(*Item)
			commitTs := item.Binlog.GetCommitTs()
			log.Debug("get success msg from producer", zap.Int64("ts", commitTs))

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
