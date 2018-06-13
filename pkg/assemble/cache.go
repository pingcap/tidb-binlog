package assemble

import (
	"sync"

	pb "github.com/pingcap/tipb/go-binlog"
)

const mib = 1024 * 1024

// AssembledBinlog is an assembled binlog
type AssembledBinlog struct {
	Entity *pb.Entity
	inPool bool
}

// ConstructAssembledBinlog constructs a binlog
func ConstructAssembledBinlog(fromPool bool) *AssembledBinlog {
	if fromPool {
		return assembledBinlogPool.Get().(*AssembledBinlog)
	}
	return &AssembledBinlog{}
}

// DestructAssembledBinlog destructs binlog and put to pool (if b.inPool)
func DestructAssembledBinlog(b *AssembledBinlog) {
	if b.inPool {
		b.Entity.Payload = b.Entity.Payload[0:0]
		assembledBinlogPool.Put(b)
	}
}

var assembledBinlogPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		return &AssembledBinlog{
			Entity: &pb.Entity{
				Payload: make([]byte, 0, mib),
			},
			inPool: true,
		}
	},
}
