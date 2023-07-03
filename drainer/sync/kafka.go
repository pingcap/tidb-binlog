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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	obinlog "github.com/pingcap/tidb/tidb-binlog/proto/go-binlog"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/util"
)

var maxWaitTimeToSendMSG = time.Second * 30
var stallWriteSize = 90 * 1024 * 1024

var _ Syncer = &KafkaSyncer{}

// KafkaSyncer sync data to kafka
type KafkaSyncer struct {
	addr     []string
	producer sarama.AsyncProducer
	topic    string

	ackWindowMu            sync.Mutex
	ackWindow              *ackWindow
	resumeProduce          chan struct{}
	resumeProduceCloseOnce sync.Once

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
		addr:       strings.Split(cfg.KafkaAddrs, ","),
		topic:      topic,
		ackWindow:  newAckWindow(),
		shutdown:   make(chan struct{}),
		baseSyncer: newBaseSyncer(tableInfoGetter, nil),
	}

	config, err := util.NewSaramaConfig(cfg.KafkaVersion, "kafka.")
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(cfg.KafkaClientID) > 0 {
		config.ClientID = cfg.KafkaClientID
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

// SetSafeMode should be ignore by KafkaSyncer
func (p *KafkaSyncer) SetSafeMode(mode bool) bool {
	return false
}

// Sync implements Syncer interface
func (p *KafkaSyncer) Sync(item *Item) error {
	secondaryBinlog, err := translator.TiBinlogToSecondaryBinlog(p.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue)
	if err != nil {
		return errors.Trace(err)
	}

	err = p.saveBinlog(secondaryBinlog, item)
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

	waitResume := false

	p.ackWindowMu.Lock()
	if p.ackWindow.unackedCount == 0 {
		p.lastSuccessTime = time.Now()
	}
	p.ackWindow.appendTS(binlog.CommitTs, len(data))
	if p.ackWindow.unackedSize >= stallWriteSize && p.ackWindow.unackedCount > 1 {
		p.resumeProduce = make(chan struct{})
		p.resumeProduceCloseOnce = sync.Once{}
		waitResume = true
	}
	p.ackWindowMu.Unlock()

	if waitResume {
		select {
		case <-p.resumeProduce:
		case <-p.errCh:
			return errors.Trace(p.err)
		}
	}

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

		var readyItems []*Item
		for msg := range p.producer.Successes() {
			item := msg.Metadata.(*Item)
			commitTs := item.Binlog.GetCommitTs()
			log.Debug("get success msg from producer", zap.Int64("ts", commitTs))

			p.ackWindowMu.Lock()
			p.lastSuccessTime = time.Now()
			p.ackWindow.handleSuccess(item)
			if p.ackWindow.unackedSize < stallWriteSize && p.resumeProduce != nil {
				p.resumeProduceCloseOnce.Do(func() {
					close(p.resumeProduce)
				})
			}
			for {
				item, ok := p.ackWindow.getReadyItem()
				if ok {
					readyItems = append(readyItems, item)
				} else {
					break
				}
			}
			p.ackWindowMu.Unlock()

			for i, item := range readyItems {
				p.success <- item
				readyItems[i] = nil // GC
			}
			readyItems = readyItems[:0]
		}
		close(p.success)
	}()

	// handle errors from producer
	wg.Add(1)
	go func() {
		defer wg.Done()

		for err := range p.producer.Errors() {
			log.Fatal("fail to produce message to kafka, please check the state of kafka server", zap.Error(err))
		}
	}()

	checkTick := time.NewTicker(time.Second)
	defer checkTick.Stop()

	for {
		select {
		case <-checkTick.C:
			p.ackWindowMu.Lock()
			if p.ackWindow.unackedCount > 0 && time.Since(p.lastSuccessTime) > maxWaitTimeToSendMSG {
				log.Debug("fail to push to kafka")
				err := errors.Errorf("fail to push msg to kafka after %v, check if kafka is up and working", maxWaitTimeToSendMSG)
				p.setErr(err)
				p.ackWindowMu.Unlock()
				return
			}
			p.ackWindowMu.Unlock()
		case <-p.shutdown:
			err := p.producer.Close()
			p.setErr(err)

			wg.Wait()
			return
		}
	}
}

type ackItem struct {
	ts   int64
	item *Item
	size int
}

// ackWindow is used to handle success message from sarama.AsyncProducer in order.
//
// The messages from sarama.AsyncProducer.Success() may not stay in the same order as the messages we send to
// sarama.AsyncProducer.Input(). Here we use a window sliding window to record the order in which messages are sent.
// And delivery success messages strictly in this order.
type ackWindow struct {
	win          []*ackItem
	items        map[int64]*ackItem
	unackedCount int
	unackedSize  int
}

func newAckWindow() *ackWindow {
	return &ackWindow{
		items: make(map[int64]*ackItem),
	}
}

// appendTS appends the new binlog commit ts and size before it is sent to sarama.AsyncProducer.
func (a *ackWindow) appendTS(ts int64, size int) {
	if len(a.win) > 0 && a.win[len(a.win)-1].ts >= ts {
		log.Warn(
			"binlog commit ts is out of order, skip it",
			zap.Int64("last-commit-ts", a.win[len(a.win)-1].ts),
			zap.Int64("current-commit-ts", a.win[len(a.win)-1].ts),
		)
		return
	}
	item := &ackItem{
		ts:   ts,
		size: size,
	}
	a.win = append(a.win, item)
	a.items[ts] = item
	a.unackedCount++
	a.unackedSize += size
}

// handleSuccess handles the success item from sarama.AsyncProducer.Success().
func (a *ackWindow) handleSuccess(item *Item) {
	ts := item.Binlog.GetCommitTs()
	if v, ok := a.items[ts]; ok {
		v.item = item
		a.unackedCount--
		a.unackedSize -= v.size
	} else {
		log.Warn("get unknown success msg from producer, skip it", zap.Int64("ts", ts))
	}
}

// getReadyItem checks whether the first ackItem is acked. If it is acked, pop it from the window,
// and then return the *Item its holding and true. Otherwise, return nil and false.
func (a *ackWindow) getReadyItem() (*Item, bool) {
	if len(a.win) == 0 || a.win[0].item == nil {
		return nil, false
	}
	delete(a.items, a.win[0].ts)
	item := a.win[0].item
	a.win[0] = nil // GC
	a.win = a.win[1:]
	return item, true
}
