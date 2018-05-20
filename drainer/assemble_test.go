package drainer

import (
	"encoding/binary"
	"hash/crc32"
	"time"

	"github.com/Shopify/sarama"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pump"
)

func (t *testDrainerSuite) TestGetKeyFromComsumerMessageHeader(c *C) {
	data := []byte("1")
	message := &sarama.ConsumerMessage{
		Headers: []*sarama.RecordHeader{{
			Key:   pump.MessageID,
			Value: data,
		}, {
			Key:   pump.No,
			Value: data,
		}},
	}

	c.Assert(getKeyFromComsumerMessageHeader(pump.No, message), DeepEquals, data)
	c.Assert(getKeyFromComsumerMessageHeader(pump.Total, message), IsNil)
}

func (t *testDrainerSuite) TestAssembleBinlog(c *C) {
	asm := newAssembler()
	defer asm.close()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// [complete binlog slices]
	messages := t.testGenerateConsumerMessage("t1", 4, nil)
	for _, message := range messages {
		asm.append(message)
	}
	var binlog *assembledBinlog
	select {
	case binlog = <-asm.messages():
	case <-ticker.C:
		c.Fatalf("assembler was wrong")
	}
	c.Assert(binlog, NotNil)
	c.Assert(asm.bms, HasLen, 0)
	c.Assert(asm.slices, HasLen, 0)
	c.Assert(asm.input, HasLen, 0)
	c.Assert(asm.msgs, HasLen, 0)

	// [unsplit binlog]
	binlog = nil
	messages = t.testGenerateConsumerMessage("t1", 1, nil)
	messages[0].Headers = nil
	asm.append(messages[0])
	select {
	case binlog = <-asm.messages():
	case <-ticker.C:
		c.Fatalf("assembler was wrong")
	}
	c.Assert(binlog, NotNil)
	c.Assert(asm.bms, HasLen, 0)
	c.Assert(asm.slices, HasLen, 0)
	c.Assert(asm.input, HasLen, 0)
	c.Assert(asm.msgs, HasLen, 0)

	// [incomplete binlog, unsplit binlog]
	binlog = nil
	messages = t.testGenerateConsumerMessage("t1", 4, []int{1})
	for _, message := range messages {
		asm.append(message)
	}
	time.Sleep(time.Second)
	c.Assert(asm.slices, HasLen, 3)
	messages = t.testGenerateConsumerMessage("t2", 1, nil)
	messages[0].Headers = nil
	asm.append(messages[0])
	select {
	case binlog = <-asm.messages():
	case <-ticker.C:
		c.Fatalf("assembler was wrong")
	}
	c.Assert(binlog, NotNil)
	c.Assert(asm.bms, HasLen, 0)
	c.Assert(asm.slices, HasLen, 0)
	c.Assert(asm.input, HasLen, 0)
	c.Assert(asm.msgs, HasLen, 0)

	// [incomplete binlog,  binlog slices]
	binlog = nil
	messages = t.testGenerateConsumerMessage("t1", 4, []int{1})
	for _, message := range messages {
		asm.append(message)
	}
	time.Sleep(time.Second)
	c.Assert(asm.slices, HasLen, 3)
	messages = t.testGenerateConsumerMessage("t2", 2, nil)
	asm.append(messages[0])
	time.Sleep(time.Second)
	c.Assert(asm.slices, HasLen, 1)
	// duplicate binlog slices
	asm.append(messages[0])
	time.Sleep(time.Second)
	c.Assert(asm.slices, HasLen, 1)
	asm.append(messages[1])
	time.Sleep(time.Second)
	// sleep sometime above, try fetch ticker once (maybe a *reset* is better)
	<-ticker.C
	select {
	case binlog = <-asm.messages():
	case <-ticker.C:
		c.Fatalf("assembler was wrong")
	}
	c.Assert(binlog, NotNil)
	c.Assert(asm.bms, HasLen, 0)
	c.Assert(asm.slices, HasLen, 0)
	c.Assert(asm.input, HasLen, 0)
	c.Assert(asm.msgs, HasLen, 0)

	// cache overflow
	// [incomplete binlog,  binlog slices]
	binlog = nil
	messages = t.testGenerateConsumerMessage("t1", 4, nil)
	asm.cacheSize = 3
	for _, message := range messages {
		asm.append(message)
	}
	time.Sleep(time.Second)
	c.Assert(asm.slices, HasLen, 1)
	c.Assert(asm.input, HasLen, 0)
	c.Assert(asm.msgs, HasLen, 0)
	c.Assert(asm.bms, HasLen, 1)
}

func (t *testDrainerSuite) testGenerateConsumerMessage(id string, size int, loss []int) []*sarama.ConsumerMessage {
	lossM := make(map[int]bool)
	for _, value := range loss {
		lossM[value] = true
	}

	var (
		basicData = []byte("test")
		data      = make([]byte, 0, len(basicData)*size)
	)
	for i := 0; i < size; i++ {
		data = append(data, basicData...)
	}
	checksum := crc32.Checksum(data, crcTable)
	checksumByte := make([]byte, 4)
	binary.LittleEndian.PutUint32(checksumByte, checksum)

	messages := make([]*sarama.ConsumerMessage, 0, size-len(loss))

	totalByte := make([]byte, 4)
	binary.LittleEndian.PutUint32(totalByte, uint32(size))

	for i := 0; i < size; i++ {
		if lossM[i] {
			continue
		}

		noByte := make([]byte, 4)
		binary.LittleEndian.PutUint32(noByte, uint32(i))
		messages = append(messages, &sarama.ConsumerMessage{
			Topic:     "test",
			Partition: 0,
			Value:     sarama.ByteEncoder(basicData),
			Headers: []*sarama.RecordHeader{
				{
					Key:   pump.MessageID,
					Value: []byte(id),
				}, {
					Key:   pump.No,
					Value: noByte,
				}, {
					Key:   pump.Total,
					Value: totalByte,
				}, {
					Key:   pump.Checksum,
					Value: checksumByte,
				},
			},
		})
	}

	return messages
}
