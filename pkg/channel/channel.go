package channel

import (
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/mem"
	"github.com/pingcap/tidb-binlog/pkg/flow"
)

type ChannelPro struct {
	//inValue  chan interface{}
	//outValue chan interface{}
	channel  chan interface{}
	sizeChan chan uint64
	//chanSize uint64
	//memSize  uint64
	memControl *mem.MemoryControl
	speedControl *flow.SpeedControl

	// if is the data source, we need control the input speed and memory
	isSource bool

	// if is end, we need not control the output speed
	isEnd    bool

	memUsedcount uint64
}

func NewChannelPro(chanSize, memSize, rate, maxToken, interval uint64, isSource, isEnd bool) *ChannelPro {
	c := &ChannelPro{
		//inValue:   make(chan interface{}),
		//outValue:  make(chan interface{}),
		channel:   make(chan interface{}, chanSize),
		sizeChan:  make(chan uint64, chanSize),
		//chanSize:  chanSize,
		//memSize:   memSize,
		isSource: isSource,
		isEnd:    isEnd,
		
		memControl: mem.NewMemoryControl(memSize),	
	}

	if !isEnd {
		c.speedControl = flow.NewSpeedControl(rate, maxToken, interval)
	}

	return c
}

func (c *ChannelPro) Push(value interface{}, size uint64) {
	reach, usedPercent := c.memControl.AllocMemory(size)
	if reach {
		time.Sleep(time.Duration(usedPercent-1)*time.Second)
	}
	c.channel <-value
	c.sizeChan <-size
	c.memUsedcount += size
	log.Debugf("push data to channel, usedPercent: %v, memoryUsedcount: %d", usedPercent, c.memUsedcount)
}

func (c *ChannelPro) Pop() interface{} {
	if !c.isEnd {
		c.speedControl.ApplyTokenSync()
	}

	value := <- c.channel
	size := <- c.sizeChan
	c.memControl.FreeMemory(size)
	return value
}

func (c *ChannelPro) Close() {
	close(c.channel)
	close(c.sizeChan)
}

func (c *ChannelPro) GetMemoryUsedCount() uint64 {
	return c.memUsedcount
}
