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
	"sync"
	"time"

	"github.com/Shopify/sarama"

	"github.com/pingcap/check"
)

type msgTrackerSuite struct{}

var _ = check.Suite(&msgTrackerSuite{})

func (ts *msgTrackerSuite) TestShouldBeThreadSafe(c *check.C) {
	tracker := newMsgTracker()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var i int64
		for ; i < 10; i++ {
			tracker.Sent(i)
			tracker.Sent(i)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		var i int64
		for ; i < 10; i++ {
			tracker.Acked(i)
		}
	}()
	wg.Wait()
	c.Assert(tracker.HasPending(), check.IsTrue)
	var i int64
	for ; i < 10; i++ {
		tracker.Acked(i)
	}
	c.Assert(tracker.HasPending(), check.IsFalse)
}

func (ts *msgTrackerSuite) TestIsLastAck(c *check.C) {
	tracker := newMsgTracker()
	tracker.SentN(1, 3)
	c.Assert(tracker.Acked(1), check.IsFalse)
	c.Assert(tracker.Acked(1), check.IsFalse)
	c.Assert(tracker.Acked(1), check.IsTrue)
}

func (ts *msgTrackerSuite) TestWaitingTooLongForAck(c *check.C) {
	tracker := newMsgTracker()
	tracker.Sent(1)
	c.Assert(tracker.HasWaitedTooLongForAck(time.Second), check.IsFalse)
	tracker.Acked(1)
	time.Sleep(10 * time.Nanosecond)
	c.Assert(tracker.HasWaitedTooLongForAck(time.Nanosecond), check.IsFalse)
	tracker.Sent(2)
	time.Sleep(10 * time.Microsecond)
	c.Assert(tracker.HasWaitedTooLongForAck(time.Millisecond), check.IsFalse)
}

type hashPartitionerSuite struct{}

var _ = check.Suite(&hashPartitionerSuite{})

func (hs *hashPartitionerSuite) TestDifferentInstancesShouldReturnSamePartition(c *check.C) {
	partitioners := []sarama.Partitioner{
		newHashPartitioner(""),
		newHashPartitioner(""),
		newHashPartitioner(""),
	}
	msg := &sarama.ProducerMessage{Key: sarama.StringEncoder("hello")}
	var numPartitions int32 = 7
	selected, err := partitioners[0].Partition(msg, numPartitions)
	c.Assert(err, check.IsNil)
	for _, p := range partitioners[1:] {
		partition, err := p.Partition(msg, numPartitions)
		c.Assert(err, check.IsNil)
		c.Assert(partition, check.Equals, selected)
	}
}

func (hs *hashPartitionerSuite) TestCanSpecifyPartitionDirectly(c *check.C) {
	const numPartitions int32 = 10
	partitioner := newHashPartitioner("")

	msg := &sarama.ProducerMessage{Key: sarama.StringEncoder("hello"), Partition: -1}
	p, err := partitioner.Partition(msg, numPartitions)
	c.Assert(err, check.IsNil)
	c.Assert(p, check.GreaterEqual, int32(0))

	msg.Partition = 3
	p, err = partitioner.Partition(msg, numPartitions)
	c.Assert(err, check.IsNil)
	c.Assert(p, check.Equals, int32(3))

	msg.Partition = 10
	_, err = partitioner.Partition(msg, numPartitions)
	c.Assert(err, check.NotNil)
}

type findSelectedPartitionsSuite struct{}

var _ = check.Suite(&findSelectedPartitionsSuite{})

func (fs *findSelectedPartitionsSuite) TestFindCorrectPartitions(c *check.C) {
	ks := KafkaSyncer{topic: "Hello"}
	partitioner := newHashPartitioner(ks.topic).(*hashPartitioner)
	var nPartitions int32 = 5
	msgs := []*sarama.ProducerMessage{
		{Key: sarama.StringEncoder("a"), Partition: -1},
		{Key: sarama.StringEncoder("b"), Partition: -1},
		{Key: sarama.StringEncoder("c"), Partition: -1},
		{Key: sarama.StringEncoder("d"), Partition: -1},
		{Key: sarama.StringEncoder("e"), Partition: -1},
	}
	result := make(map[int32]struct{})
	for _, m := range msgs {
		p, err := partitioner.Partition(m, nPartitions)
		c.Assert(err, check.IsNil)
		result[p] = struct{}{}
	}

	selected, err := ks.findSelectedPartitions(nPartitions, msgs)
	c.Assert(err, check.IsNil)

	c.Logf("Result: %v", selected)
	c.Assert(len(selected), check.Equals, len(result))
	for _, s := range selected {
		_, ok := result[s]
		c.Assert(ok, check.IsTrue)
	}
}
