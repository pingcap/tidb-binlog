package drainer

import (
	"sync"

	pb "github.com/pingcap/tipb/go-binlog"
)

const mib = 1024 * 1024

type assembledBinlog struct {
	entity *pb.Entity
	inPool bool
}

func constructAssembledBinlog(fromPool bool) *assembledBinlog {
	if fromPool {
		return assembledBinlogPool.Get().(*assembledBinlog)
	}
	return &assembledBinlog{}
}

func destructAssembledBinlog(b *assembledBinlog) {
	if b.inPool {
		b.entity.Payload = b.entity.Payload[0:0]
		assembledBinlogPool.Put(b)
	}
}

var assembledBinlogPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		return &assembledBinlog{
			entity: &pb.Entity{
				Payload: make([]byte, 0, mib),
			},
			inPool: true,
		}
	},
}
