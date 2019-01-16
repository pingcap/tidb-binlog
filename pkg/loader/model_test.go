package loader

import (
	"strings"

	check "github.com/pingcap/check"
)

type dmlSuite struct {
}

var _ = check.Suite(&dmlSuite{})

func getDML(key bool, tp DMLType) *DML {
	info := &tableInfo{
		columns: []string{"id", "a1"},
	}

	if key {
		info.uniqueKeys = append(info.uniqueKeys, indexInfo{"PRIMARY", []string{"id"}})
	}

	dml := new(DML)
	dml.info = info
	dml.Database = "test"
	dml.Table = "test"
	dml.Tp = tp

	return dml
}

func (d *dmlSuite) TestWhere(c *check.C) {
	d.testWhere(c, InsertDMLType)
	d.testWhere(c, UpdateDMLType)
	d.testWhere(c, DeleteDMLType)
}

func (d *dmlSuite) testWhere(c *check.C, tp DMLType) {
	dml := getDML(true, tp)
	var values = map[string]interface{}{
		"id": 1,
		"a1": 1,
	}

	if tp == UpdateDMLType {
		dml.OldValues = values
	} else {
		dml.Values = values
	}

	names, args := dml.whereSlice()
	c.Assert(names, check.DeepEquals, []string{"id"})
	c.Assert(args, check.DeepEquals, []interface{}{1})

	builder := new(strings.Builder)
	args = dml.buildWhere(builder)
	c.Assert(args, check.DeepEquals, []interface{}{1})
	c.Assert(strings.Count(builder.String(), "?"), check.Equals, len(args))

	// no pk
	dml = getDML(false, tp)
	if tp == UpdateDMLType {
		dml.OldValues = values
	} else {
		dml.Values = values
	}

	names, args = dml.whereSlice()
	c.Assert(names, check.DeepEquals, []string{"id", "a1"})
	c.Assert(args, check.DeepEquals, []interface{}{1, 1})

	builder.Reset()
	args = dml.buildWhere(builder)
	c.Assert(args, check.DeepEquals, []interface{}{1, 1})
	c.Assert(strings.Count(builder.String(), "?"), check.Equals, len(args))

	// set a1 to NULL value
	values["a1"] = nil
	builder.Reset()
	args = dml.buildWhere(builder)
	c.Assert(args, check.DeepEquals, []interface{}{1})
	c.Assert(strings.Count(builder.String(), "?"), check.Equals, len(args))
}
