package translator

import (
	"io"

	. "github.com/pingcap/check"
	ti "github.com/pingcap/tipb/go-binlog"
)

type testSequenceIteratorSuite struct{}

var _ = Suite(&testSequenceIteratorSuite{})

func (t *testSequenceIteratorSuite) TestIterator(c *C) {
	mut := new(ti.TableMutation)
	var tps []ti.MutationType
	var rows [][]byte

	// generate test data
	for i := 0; i < 10; i++ {
		row := []byte{byte(i)}
		rows = append(rows, row)
		switch i % 3 {
		case 0:
			mut.Sequence = append(mut.Sequence, ti.MutationType_Insert)
			mut.InsertedRows = append(mut.InsertedRows, row)
			tps = append(tps, ti.MutationType_Insert)
		case 1:
			mut.Sequence = append(mut.Sequence, ti.MutationType_Update)
			mut.UpdatedRows = append(mut.UpdatedRows, row)
			tps = append(tps, ti.MutationType_Update)
		case 2:
			mut.Sequence = append(mut.Sequence, ti.MutationType_DeleteRow)
			mut.DeletedRows = append(mut.DeletedRows, row)
			tps = append(tps, ti.MutationType_DeleteRow)
		}
	}

	// get back by iterator
	iter := newSequenceIterator(mut)
	var getTps []ti.MutationType
	var getRows [][]byte

	for {
		tp, row, err := iter.next()
		if err == io.EOF {
			break
		}

		c.Assert(err, IsNil)

		getTps = append(getTps, tp)
		getRows = append(getRows, row)
	}

	c.Assert(getTps, DeepEquals, tps)
	c.Assert(getRows, DeepEquals, rows)
}
