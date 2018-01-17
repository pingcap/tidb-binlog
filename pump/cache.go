package pump

import "sync"

type binlogBuffer struct {
	cache []byte
}

var binlogBufferPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		return &binlogBuffer{
			cache: make([]byte, mib),
		}
	},
}
