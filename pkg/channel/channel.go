package channel

import (
	"time"
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

func NewChannelPro(chanSize, memSize, rate, token, maxToken, interval uint64, isSource, isEnd bool) *ChannelPro {
	return &ChannelPro{
		//inValue:   make(chan interface{}),
		//outValue:  make(chan interface{}),
		channel:   make(chan interface{}, chanSize),
		sizeChan:  make(chan uint64, chanSize),
		//chanSize:  chanSize,
		//memSize:   memSize,
		isSource: isSource,
		isEnd:    isEnd,
		
		memControl: mem.NewMemControl(memSize),
		if !isEnd {
			speedControl: flow.NewSpeedControl(rate, token, maxToken, interval uint64)
		}
	}
}

func (c *ChannelPro) Push(value interface{}, size uint64) {
	/*
	size := EstimateSize(value)
	reach := c.memControl.AllocMemory(size)
	if reach {
		// TODO: count the wait time
		time.Sleep(time.Second)
	}
	*/
	//c.inValue <-value
	reach, usedPercent := c.memControl.AllocMemory(size)
	if reach {
		time.Sleep(time.Duration(usedPercent-1)*time.Second)
	}
	c.channel <-value
	c.sizeChan <-size
	c.memUsedcount += size
}

//func (c *ChannelPro) Pop() <-chan interface{} {
func (c *ChannelPro) Pop() interface{} {
	if !c.isEnd {
		c.speedControl.ApplyTokenSync()
	}

	value := <- c.outValue:
	size := <- c.sizeChan
	c.memControl.FreeMemory(size)
	return value
	//return c.channel
}

func (c *ChannelPro) Close() {
	c.channel.Close()
	c.sizeChan.Close()
}

func (c *ChannelPro) GetMemoryUsedCount() uint64 {
	return c.memUsedcount
}

func EstimateSize(value interface{}) uint64 {


	return 0
}

func (c *ChannelPro) Run() {
	// push inValue to channel, and pop channel value to outValue
	for {



	}
}

// 
func (c *ChannelPro) pushToChannel() {

}

func (c *ChannelPro) popFromChannel() {

}