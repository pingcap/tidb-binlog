package drainer

import (
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

func (t *testDrainerSuite) TestSchema(c *C) {
	var jobs []*model.Job
	dbName := model.NewCIStr("Test")
	ignoreDBName := model.NewCIStr("ignoreTest")
	// db and ignoreDB info
	dbInfo := &model.DBInfo{
		ID:    1,
		Name:  dbName,
		State: model.StatePublic,
	}
	ingnoreDBInfo := &model.DBInfo{
		ID:    2,
		Name:  ignoreDBName,
		State: model.StatePublic,
	}
	// `createSchema` job
	job := &model.Job{
		ID:         3,
		SchemaID:   1,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{123, dbInfo, nil, 123},
	}
	jobs = append(jobs, job)
	// `createIgnoreSchema` job
	job1 := &model.Job{
		ID:         4,
		SchemaID:   2,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{123, ingnoreDBInfo, nil, 123},
	}
	jobs = append(jobs, job1)
	// construct a cancelled job
	jobs = append(jobs, &model.Job{ID: 5, State: model.JobStateCancelled})
	// construct ignore db list
	ignoreNames := make(map[string]struct{})
	ignoreNames[ignoreDBName.L] = struct{}{}
	// reconstruct the local schema
	schema, err := NewSchema(jobs, ignoreNames, false)
	c.Assert(err, IsNil)
	// check ignore DB
	_, ok := schema.IgnoreSchemaByID(ingnoreDBInfo.ID)
	c.Assert(ok, IsTrue)
	// test drop schema and drop ignore schema
	jobs = append(jobs, &model.Job{ID: 6, SchemaID: 1, Type: model.ActionDropSchema})
	jobs = append(jobs, &model.Job{ID: 7, SchemaID: 2, Type: model.ActionDropSchema})
	_, err = NewSchema(jobs, ignoreNames, false)
	c.Assert(err, IsNil)
	// test create schema already exist error
	jobs = jobs[:0]
	jobs = append(jobs, job)
	jobs = append(jobs, job)
	_, err = NewSchema(jobs, ignoreNames, false)
	c.Assert(errors.IsAlreadyExists(err), IsTrue)

	// test schema drop schema error
	jobs = jobs[:0]
	jobs = append(jobs, &model.Job{ID: 9, SchemaID: 1, Type: model.ActionDropSchema})
	_, err = NewSchema(jobs, ignoreNames, false)
	c.Assert(errors.IsNotFound(err), IsTrue)
}

func (*testDrainerSuite) TestTable(c *C) {
	var jobs []*model.Job
	dbName := model.NewCIStr("Test")
	ignoreDBName := model.NewCIStr("ignoreTest")
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
		SchemaID:   3,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{123, dbInfo, nil, 123},
	}
	jobs = append(jobs, job)

	// `createTable` job
	job = &model.Job{
		ID:         6,
		SchemaID:   3,
		TableID:    2,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{123, nil, tblInfo, 123},
	}
	jobs = append(jobs, job)

	// `addColumn` job
	tblInfo.Columns = []*model.ColumnInfo{colInfo}
	job = &model.Job{
		ID:         7,
		SchemaID:   3,
		TableID:    2,
		Type:       model.ActionAddColumn,
		BinlogInfo: &model.HistoryInfo{123, nil, tblInfo, 123},
	}
	jobs = append(jobs, job)

	// construct a historical `addIndex` job
	tblInfo.Indices = []*model.IndexInfo{idxInfo}
	job = &model.Job{
		ID:         8,
		SchemaID:   3,
		TableID:    2,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{123, nil, tblInfo, 123},
	}
	jobs = append(jobs, job)

	// construct ignore db list
	ignoreNames := make(map[string]struct{})
	ignoreNames[ignoreDBName.O] = struct{}{}
	// reconstruct the local schema
	schema, err := NewSchema(jobs, ignoreNames, false)
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
	jobs = append(jobs, &model.Job{ID: 9, SchemaID: 3, TableID: 2, Type: model.ActionTruncateTable, BinlogInfo: &model.HistoryInfo{123, nil, tblInfo1, 123}})
	schema1, err := NewSchema(jobs, ignoreNames, false)
	c.Assert(err, IsNil)
	table, ok = schema1.TableByID(tblInfo1.ID)
	c.Assert(ok, IsTrue)
	table, ok = schema1.TableByID(2)
	c.Assert(ok, IsFalse)

	// test rename table in ignore schema
	ingnoreDBInfo := &model.DBInfo{
		ID:    4,
		Name:  ignoreDBName,
		State: model.StatePublic,
	}

	createIgnoreSchemaJob := &model.Job{
		ID:         8,
		SchemaID:   ingnoreDBInfo.ID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{123, ingnoreDBInfo, nil, 123},
	}

	createTableJob := &model.Job{
		ID:         9,
		SchemaID:   ingnoreDBInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{123, ingnoreDBInfo, &model.TableInfo{ID: 3}, 123},
	}

	jobs = append(jobs, createIgnoreSchemaJob)
	jobs = append(jobs, createTableJob)
	jobs = append(jobs, &model.Job{
		ID:         10,
		SchemaID:   ingnoreDBInfo.ID,
		TableID:    3,
		Type:       model.ActionRenameTable,
		BinlogInfo: &model.HistoryInfo{123, ingnoreDBInfo, &model.TableInfo{ID: 4}, 123},
	})
	_, err = NewSchema(jobs, ignoreNames, false)
	// rename table in ignore schema will not return error
	c.Assert(err, IsNil)

	// check drop table
	jobs = append(jobs, &model.Job{ID: 11, SchemaID: 3, TableID: 9, Type: model.ActionDropTable})
	schema2, err := NewSchema(jobs, ignoreNames, false)
	c.Assert(err, IsNil)
	table, ok = schema2.TableByID(tblInfo.ID)
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
