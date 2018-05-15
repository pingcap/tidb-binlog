package drainer

import (
	"encoding/binary"
	"hash/crc32"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pump"
	pb "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
)

// unsupported concurrency

// slice cache size is to cache non-complete binlog slices to assemble binlog
// * large binlog - upper limit sliceCacheSize * MaxsliceSize, default 5G now
const sliceCacheSize = 5000

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// assembler is to assmeble binlog slices to binlog
type assembler struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// store binlog slices bitmap to check whether it's complete binlog
	bms map[string]*bitmap
	// cache all slices
	slices chan *sarama.ConsumerMessage
	// output complete binlog channel
	msgs  chan *assembledBinlog
	input chan *sarama.ConsumerMessage

	cacheSize int
}

func newAssembler() *assembler {
	ctx, cancel := context.WithCancel(context.Background())

	asm := &assembler{
		ctx:       ctx,
		cancel:    cancel,
		cacheSize: sliceCacheSize,
		bms:       make(map[string]*bitmap, sliceCacheSize/10),
		msgs:      make(chan *assembledBinlog, sliceCacheSize/10),
		input:     make(chan *sarama.ConsumerMessage, sliceCacheSize/10),
		slices:    make(chan *sarama.ConsumerMessage, sliceCacheSize),
	}

	asm.wg.Add(1)
	go func() {
		defer asm.wg.Done()
		asm.do()
	}()

	return asm
}

func (a *assembler) append(msg *sarama.ConsumerMessage) {
	select {
	case <-a.ctx.Done():
		log.Warningf("assembler was canceled: %v", a.ctx.Err())
	case a.input <- msg:
	}
}

func (a *assembler) messages() chan *assembledBinlog {
	return a.msgs
}

func (a *assembler) close() {
	a.cancel()
	a.wg.Wait()
}

func (a *assembler) do() {
	var (
		msg     *sarama.ConsumerMessage
		binlogs = make([]*assembledBinlog, 0, 16)
	)
	for {
		if len(binlogs) > 0 {
			binlog := binlogs[0]
			select {
			case <-a.ctx.Done():
				log.Warningf("assembler was canceled: %v", a.ctx.Err())
				return
			case a.msgs <- binlog:
				log.Infof("assemble a binlog %+v, size %d", binlog.entity.Pos, len(binlog.entity.Payload))
				binlogs = binlogs[1:]
			case msg = <-a.input:
				binlog := a.assemble(msg)
				if binlog != nil {
					binlogs = append(binlogs, binlog)
				}
			}
		} else {
			select {
			case <-a.ctx.Done():
				log.Warningf("assembler was canceled: %v", a.ctx.Err())
				return
			case msg = <-a.input:
				binlog := a.assemble(msg)
				if binlog != nil {
					binlogs = append(binlogs, binlog)
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (a *assembler) assemble(msg *sarama.ConsumerMessage) *assembledBinlog {
	// unsplit binlog, just return
	if len(msg.Headers) == 0 {
		if len(a.bms) != 0 {
			log.Error("[assembler] meet corruption binlog, pop corrpution binlog and skip it")
			errorBinlogCount.Add(1)
			a.popBinlogSlices()
		}

		b := constructAssembledBinlog(false)
		b.entity = &pb.Entity{
			Payload: msg.Value,
			Pos: pb.Pos{
				Offset: msg.Offset,
			},
		}

		return b
	}

	// if slices channel is full, skip non-complete binlog slice that is in the front of slices channel
	// maybe we loss some binlog slices, issue an alert
	if len(a.slices) == a.cacheSize {
		// fetch message id
		// dont do any check here, i know it's right and bothered to handle error
		// just ignore it now, refine it later
		log.Error("[assembler] cache is full, pop binlog slice in the front of slices")
		errorBinlogCount.Add(1)
		a.popBinlogSlices()
	}

	// get total of binlog slices and no from consumerMessage header
	var (
		totalByte = getKeyFromComsumerMessageHeader(pump.Total, msg)
		total     = int(binary.LittleEndian.Uint32(totalByte))
		noByte    = getKeyFromComsumerMessageHeader(pump.No, msg)
		no        = int(binary.LittleEndian.Uint32(noByte))
		messageID = string(getKeyFromComsumerMessageHeader(pump.MessageID, msg))
	)

	_, ok := a.bms[messageID]
	// check whether new binlog slice arrive
	if ok {
		// only one binlog in slices
		// check completed or append
		if len(a.bms) == 1 {
			isNew := a.bms[messageID].set(no)
			// duplicate slice arrives, just ignore
			if !isNew {
				return nil
			}

			a.slices <- msg
			if !a.bms[messageID].completed() {
				return nil
			}
			messages := a.peekBinlogSlices()
			b, err := assembleBinlog(messages)
			if err != nil {
				log.Errorf("[pump] assemble messages error %v", err)
				errorBinlogCount.Add(1)
				return nil
			}

			return b
		}
	}

	if len(a.bms) >= 1 {
		// meet incontinuity binlogs slices
		// pump must have sent duplicate binlog slices or lose some binlog in kafka
		// just ingnore all slices before it and issue an alert
		log.Error("[assembler] meet corruption binlog, pop corrpution binlog and skip it")
		errorBinlogCount.Add(1)
		a.popBinlogSlices()
	}

	// just append slices
	a.slices <- msg
	a.bms[messageID] = newBitmap(total)
	a.bms[messageID].set(no)
	return nil
}

func (a *assembler) peekBinlogSlices() []*sarama.ConsumerMessage {
	skippedMsg := <-a.slices
	skippedID := string(getKeyFromComsumerMessageHeader(pump.MessageID, skippedMsg))
	skippedBitmap := a.bms[string(skippedID)]

	messages := make([]*sarama.ConsumerMessage, 0, skippedBitmap.current)
	messages = append(messages, skippedMsg)
	for i := 0; i < skippedBitmap.current-1; i++ {
		messages = append(messages, <-a.slices)
	}
	delete(a.bms, skippedID)

	return messages
}

func (a *assembler) popBinlogSlices() {
	skippedMsg := <-a.slices
	skippedID := string(getKeyFromComsumerMessageHeader(pump.MessageID, skippedMsg))
	skippedBitmap := a.bms[string(skippedID)]
	for i := 0; i < skippedBitmap.current-1; i++ {
		<-a.slices
	}
	delete(a.bms, skippedID)
}

func getKeyFromComsumerMessageHeader(key []byte, message *sarama.ConsumerMessage) []byte {
	for _, record := range message.Headers {
		if string(record.Key) == string(key) {
			return record.Value
		}
	}

	return nil
}

func assembleBinlog(messages []*sarama.ConsumerMessage) (*assembledBinlog, error) {
	slices := make([]*sarama.ConsumerMessage, len(messages))
	totalSize := 0
	for _, msg := range messages {
		no := int(binary.LittleEndian.Uint32(getKeyFromComsumerMessageHeader(pump.No, msg)))
		slices[no] = msg
		totalSize += len(msg.Value)
	}

	b := constructAssembledBinlog(true)
	b.entity.Pos = pb.Pos{
		Offset: slices[0].Offset,
	}
	for _, slice := range slices {
		b.entity.Payload = append(b.entity.Payload, slice.Value...)
	}

	checksumByte := getKeyFromComsumerMessageHeader(pump.Checksum, slices[len(slices)-1])
	originChecksum := binary.LittleEndian.Uint32(checksumByte)
	checksum := crc32.Checksum(b.entity.Payload, crcTable)
	if checksum != originChecksum {
		return nil, pump.ErrCRCMismatch
	}

	return b, nil
}
