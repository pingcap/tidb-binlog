package storage

import (
	"fmt"
	"bytes"
	"encoding/binary"
	"sync/atomic"

	"github.com/juju/errors"
	pb "github.com/pingcap/tipb/go-binlog"
)

var tsKeyPrefix = []byte("ts:")

func decodeTSKey(key []byte) int64 {
	// check bound
	_ = key[len(tsKeyPrefix)+8-1]

	return int64(binary.BigEndian.Uint64(key[len(tsKeyPrefix):]))
}

func encodeTSKey(ts int64) []byte {
	buf := make([]byte, 8+len(tsKeyPrefix))
	copy(buf, tsKeyPrefix)

	b := buf[len(tsKeyPrefix):]

	binary.BigEndian.PutUint64(b, uint64(ts))

	return buf
}

// test helper
type memOracle struct {
	ts int64
}

func newMemOracle() *memOracle {
	return &memOracle{
		ts: 0,
	}
}

func (o *memOracle) getTS() int64 {
	return atomic.AddInt64(&o.ts, 1)
}


func binlogInfo(binlog *pb.Binlog) ([]byte, error) {
	var b bytes.Buffer

	b.WriteString(fmt.Sprintf("{ Type: %s, ", binlog.Tp))
	b.WriteString(fmt.Sprintf("startTs: %d, ", binlog.StartTs))
	b.WriteString(fmt.Sprintf("commitTs: %d, ", binlog.CommitTs))
	if len(binlog.PrewriteValue) != 0 {
		b.WriteString("prewriteValue: { mutations: { ")
		preWrite := &pb.PrewriteValue{}
		err := preWrite.Unmarshal(binlog.PrewriteValue)
		if err != nil {
			return nil, errors.Errorf("prewrite unmarshal error %v", err)
		}

		b.WriteString(fmt.Sprintf("SchemaVersion: %d, ", preWrite.SchemaVersion))

		for _, mutation := range preWrite.Mutations {
			b.WriteString(fmt.Sprintf("[ tableID: %d, %d insertedRows, %d updatedRows, %d deletedRows], ", 
				mutation.TableId, len(mutation.InsertedRows), len(mutation.UpdatedRows), len(mutation.DeletedRows)))
		}

		b.WriteString(" } }")
	}

	if len(binlog.DdlQuery) != 0 {
		b.WriteString(fmt.Sprintf("DDlQuery: %d, ", binlog.DdlQuery))
	}

	if binlog.DdlJobId != 0 {
		b.WriteString(fmt.Sprintf("DDLJobID: %d, ", binlog.DdlJobId))
	}



	return b.Bytes(), nil
}
