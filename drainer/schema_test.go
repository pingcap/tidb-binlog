package drainer

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

func (t *testDrainerSuite) TestSchema(c *C) {
	var jobs []*model.Job
	dbName := model.NewCIStr("Test")
	// db and ignoreDB info
	dbInfo := &model.DBInfo{
		ID:    1,
		Name:  dbName,
		State: model.StatePublic,
	}
	// `createSchema` job
	job := &model.Job{
		ID:         3,
		State:      model.JobStateSynced,
		SchemaID:   1,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{1, dbInfo, nil, 123},
		Query:      "create database test",
	}
	jobDup := &model.Job{
		ID:         3,
		State:      model.JobStateSynced,
		SchemaID:   1,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{2, dbInfo, nil, 123},
		Query:      "create database test",
	}
	jobs = append(jobs, job)

	// construct a rollbackdone job
	jobs = append(jobs, &model.Job{ID: 5, State: model.JobStateRollbackDone})

	// reconstruct the local schema
	schema, err := NewSchema(jobs, false)
	c.Assert(err, IsNil)
	err = schema.handlePreviousDDLJobIfNeed(2)
	c.Assert(err, IsNil)

	// test drop schema
	jobs = append(jobs, &model.Job{ID: 6, State: model.JobStateSynced, SchemaID: 1, Type: model.ActionDropSchema, BinlogInfo: &model.HistoryInfo{3, nil, nil, 123}, Query: "drop database test"})
	schema, err = NewSchema(jobs, false)
	c.Assert(err, IsNil)
	err = schema.handlePreviousDDLJobIfNeed(3)
	c.Assert(err, IsNil)

	// test create schema already exist error
	jobs = jobs[:0]
	jobs = append(jobs, job)
	jobs = append(jobs, jobDup)
	schema, err = NewSchema(jobs, false)
	c.Assert(err, IsNil)
	err = schema.handlePreviousDDLJobIfNeed(2)
	c.Log(err)
	c.Assert(errors.IsAlreadyExists(err), IsTrue)

	// test schema drop schema error
	jobs = jobs[:0]
	jobs = append(jobs, &model.Job{ID: 9, State: model.JobStateSynced, SchemaID: 1, Type: model.ActionDropSchema, BinlogInfo: &model.HistoryInfo{1, nil, nil, 123}, Query: "drop database test"})
	schema, err = NewSchema(jobs, false)
	c.Assert(err, IsNil)
	err = schema.handlePreviousDDLJobIfNeed(1)
	c.Assert(errors.IsNotFound(err), IsTrue)
}

func (*testDrainerSuite) TestTable(c *C) {
	var jobs []*model.Job
	dbName := model.NewCIStr("Test")
	tbName := model.NewCIStr("T")
	colName := model.NewCIStr("A")
	idxName := model.NewCIStr("idx")
	// column info
	colInfo := &model.ColumnInfo{
		ID:        1,
		Name:      colName,
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     model.StatePublic,
	}
	// index info
	idxInfo := &model.IndexInfo{
		Name:  idxName,
		Table: tbName,
		Columns: []*model.IndexColumn{
			{
				Name:   colName,
				Offset: 0,
				Length: 10,
			},
		},
		Unique:  true,
		Primary: true,
		State:   model.StatePublic,
	}
	// table info
	tblInfo := &model.TableInfo{
		ID:    2,
		Name:  tbName,
		State: model.StatePublic,
	}
	// db info
	dbInfo := &model.DBInfo{
		ID:    3,
		Name:  dbName,
		State: model.StatePublic,
	}

	// `createSchema` job
	job := &model.Job{
		ID:         5,
		State:      model.JobStateSynced,
		SchemaID:   3,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{1, dbInfo, nil, 123},
		Query:      "create database " + dbName.O,
	}
	jobs = append(jobs, job)

	// `createTable` job
	job = &model.Job{
		ID:         6,
		State:      model.JobStateSynced,
		SchemaID:   3,
		TableID:    2,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{2, nil, tblInfo, 123},
		Query:      "create table " + tbName.O,
	}
	jobs = append(jobs, job)

	// `addColumn` job
	tblInfo.Columns = []*model.ColumnInfo{colInfo}
	job = &model.Job{
		ID:         7,
		State:      model.JobStateSynced,
		SchemaID:   3,
		TableID:    2,
		Type:       model.ActionAddColumn,
		BinlogInfo: &model.HistoryInfo{3, nil, tblInfo, 123},
		Query:      "alter table " + tbName.O + " add column " + colName.O,
	}
	jobs = append(jobs, job)

	// construct a historical `addIndex` job
	tblInfo.Indices = []*model.IndexInfo{idxInfo}
	job = &model.Job{
		ID:         8,
		State:      model.JobStateSynced,
		SchemaID:   3,
		TableID:    2,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{4, nil, tblInfo, 123},
		Query:      fmt.Sprintf("alter table %s add index %s(%s)", tbName, idxName, colName),
	}
	jobs = append(jobs, job)

	// reconstruct the local schema
	schema, err := NewSchema(jobs, false)
	c.Assert(err, IsNil)
	err = schema.handlePreviousDDLJobIfNeed(4)
	c.Assert(err, IsNil)

	// check the historical db that constructed above whether in the schema list of local schema
	_, ok := schema.SchemaByID(dbInfo.ID)
	c.Assert(ok, IsTrue)
	// check the historical table that constructed above whether in the table list of local schema
	table, ok := schema.TableByID(tblInfo.ID)
	c.Assert(ok, IsTrue)
	c.Assert(table.Columns, HasLen, 1)
	c.Assert(table.Indices, HasLen, 1)
	// check truncate table
	tblInfo1 := &model.TableInfo{
		ID:    9,
		Name:  tbName,
		State: model.StatePublic,
	}
	jobs = append(jobs, &model.Job{ID: 9, State: model.JobStateSynced, SchemaID: 3, TableID: 2, Type: model.ActionTruncateTable, BinlogInfo: &model.HistoryInfo{5, nil, tblInfo1, 123}, Query: "truncate table " + tbName.O})
	schema1, err := NewSchema(jobs, false)
	c.Assert(err, IsNil)
	err = schema1.handlePreviousDDLJobIfNeed(5)
	c.Assert(err, IsNil)
	_, ok = schema1.TableByID(tblInfo1.ID)
	c.Assert(ok, IsTrue)

	_, ok = schema1.TableByID(2)
	c.Assert(ok, IsFalse)
	// check drop table
	jobs = append(jobs, &model.Job{ID: 9, State: model.JobStateSynced, SchemaID: 3, TableID: 9, Type: model.ActionDropTable, BinlogInfo: &model.HistoryInfo{6, nil, nil, 123}, Query: "drop table " + tbName.O})
	schema2, err := NewSchema(jobs, false)
	c.Assert(err, IsNil)
	err = schema2.handlePreviousDDLJobIfNeed(6)
	c.Assert(err, IsNil)

	_, ok = schema2.TableByID(tblInfo.ID)
	c.Assert(ok, IsFalse)
	// test schemaAndTableName
	_, _, ok = schema1.SchemaAndTableName(9)
	c.Assert(ok, IsTrue)
	// drop schema
	_, err = schema1.DropSchema(3)
	c.Assert(err, IsNil)
	// test schema version
	c.Assert(schema.SchemaMetaVersion(), Equals, int64(0))
}
