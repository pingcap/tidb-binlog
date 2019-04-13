package translator

import (
	"io"

	"github.com/pingcap/errors"
	ti "github.com/pingcap/tipb/go-binlog"
)

// sequenceIterator is a helper to iterate row event by sequence
type sequenceIterator struct {
	mutation  *ti.TableMutation
	idx       int
	insertIdx int
	deleteIdx int
	updateIdx int
}

func newSequenceIterator(mutation *ti.TableMutation) *sequenceIterator {
	return &sequenceIterator{mutation: mutation}
}

func (si *sequenceIterator) next() (tp ti.MutationType, row []byte, err error) {
	if si.idx >= len(si.mutation.Sequence) {
		err = io.EOF
		return
	}

	tp = si.mutation.Sequence[si.idx]
	si.idx++

	switch tp {
	case ti.MutationType_Insert:
		row = si.mutation.InsertedRows[si.insertIdx]
		si.insertIdx++
	case ti.MutationType_Update:
		row = si.mutation.UpdatedRows[si.updateIdx]
		si.updateIdx++
	case ti.MutationType_DeleteRow:
		row = si.mutation.DeletedRows[si.deleteIdx]
		si.deleteIdx++
	default:
		err = errors.Errorf("unknown mutation type: %v", tp)
		return
	}

	return
}
