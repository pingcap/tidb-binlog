package drainer

import (
	"encoding/binary"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pump"
	pb "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
)

// unsupported concurrency

// slice cache size is to cache non-complete binlog slices, two use cases
// * large binlog - upper limit sliceCacheSize * MaxsliceSize, default 5G now
// * ease loss binlog slice alert
const sliceCacheSize = 5000

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
	msgs  chan *pb.Entity
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
		msgs:      make(chan *pb.Entity, sliceCacheSize/10),
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

func (a *assembler) messages() chan *pb.Entity {
	return a.msgs
}

func (a *assembler) close() {
	a.cancel()
	a.wg.Wait()
}

func (a *assembler) do() {
	var (
		msg    *sarama.ConsumerMessage
		binlog *pb.Entity
	)
	for {
		select {
		case <-a.ctx.Done():
			log.Warningf("assembler was canceled: %v", a.ctx.Err())
			return
		case msg = <-a.input:
			binlog = a.assemble(msg)
		}

		if binlog == nil {
			continue
		}

		select {
		case <-a.ctx.Done():
			log.Warningf("assembler was canceled: %v", a.ctx.Err())
			return
		case a.msgs <- binlog:
			log.Infof("assemble a binlog %+v, size %d", binlog.Pos, len(binlog.Payload))
		}

	}
}

func (a *assembler) assemble(msg *sarama.ConsumerMessage) *pb.Entity {
	if len(msg.Headers) == 0 && len(a.bms) == 0 {
		return &pb.Entity{
			Payload: msg.Value,
			Pos: pb.Pos{
				Offset: msg.Offset,
			},
		}
	}

	// skip non-complete binlog slice in the front of slices channel
	// maybe we loss some binlog slices, issue an alert
	if len(a.slices) == a.cacheSize {
		// fetch message id
		// dont do any check here, i know it's right and bothered to handle error
		// just ignore it now, refine it later
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
			a.bms[messageID].set(no)
			if !a.bms[messageID].completed() {
				a.slices <- msg
				return nil
			}

			messages := append(a.peekBinlogSlices(), msg)
			entity, err := assembleBinlog(messages)
			if err != nil {
				log.Errorf("[pump] assemble messages error %v", err)
				return nil
			}

			return entity
		}
		// meet same and incontinuity binlogs slices
		// we must have sent duplicate binlog slices
		// just ingnore all slices before it
		for len(a.slices) > 0 {
			a.popBinlogSlices()
		}
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

func assembleBinlog(messages []*sarama.ConsumerMessage) (*pb.Entity, error) {
	return nil, nil
}
