package channel

import (
	"time"
	"github.com/pingcap/tidb-binlog/pkg/mem"
	"github.com/pingcap/tidb-binlog/pkg/flow"
)

type ChannelPro struct {
	inValue  chan interface{}
	outValue chan interface{}
	channel  chan interface{}
	//chanSize uint64
	//memSize  uint64
	memControl *mem.MemoryControl
	speedControl *flow.SpeedControl
}

func NewChannelPro(chanSize, memSize, rate, token, maxToken, interval uint64) *ChannelPro {
	return &ChannelPro{
		inValue:   make(chan interface{}),
		outValue:  make(chan interface{}),
		channel:   make(chan interface{}, chanSize),
		//chanSize:  chanSize,
		//memSize:   memSize,
		
		memControl: mem.NewChannelPro(memSize),
		speedControl: flow.NewSpeedControl(rate, token, maxToken, interval uint64)
	}
}

func (c *ChannelPro) Push(value interface{}) {
	/*
	size := EstimateSize(value)
	reach := c.memControl.AllocMemory(size)
	if reach {
		// TODO: count the wait time
		time.Sleep(time.Second)
	}
	*/
	c.inValue <-value
}

func (c *ChannelPro) Pop() interface{} {
	for {
		select {
		case value<- c.outValue:
			return value
		}
	}
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