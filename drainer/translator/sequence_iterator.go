// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package translator

import (
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/tipb/go-binlog"
)

// sequenceIterator is a helper to iterate row event by sequence
type sequenceIterator struct {
	mutation  *binlog.TableMutation
	idx       int
	insertIdx int
	deleteIdx int
	updateIdx int
}

func newSequenceIterator(mutation *binlog.TableMutation) *sequenceIterator {
	return &sequenceIterator{mutation: mutation}
}

func (si *sequenceIterator) next() (tp binlog.MutationType, row []byte, err error) {
	if si.idx >= len(si.mutation.Sequence) {
		err = io.EOF
		return
	}

	tp = si.mutation.Sequence[si.idx]
	si.idx++

	switch tp {
	case binlog.MutationType_Insert:
		row = si.mutation.InsertedRows[si.insertIdx]
		si.insertIdx++
	case binlog.MutationType_Update:
		row = si.mutation.UpdatedRows[si.updateIdx]
		si.updateIdx++
	case binlog.MutationType_DeleteRow:
		row = si.mutation.DeletedRows[si.deleteIdx]
		si.deleteIdx++
	default:
		err = errors.Errorf("unknown mutation type: %v", tp)
		return
	}

	return
}
