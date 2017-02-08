package drainer

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

func (t *testDrainerSuite) TestHandleDDL(c *C) {
	var err error
	d := &Executor{}
	d.ignoreSchemaNames = make(map[string]struct{})
	d.schema, err = NewSchema(nil, nil)
	c.Assert(err, IsNil)
	dbName := model.NewCIStr("Test")
	ignoreDBName := model.NewCIStr("ignoreTest")
	colName := model.NewCIStr("A")
	tbName := model.NewCIStr("T")

	// check cancelled job
	job := &model.Job{ID: 1, State: model.JobCancelled}
	_, _, sql, err := d.handleDDL(job)
	c.Assert(err, IsNil)
	c.Assert(sql, Equals, "")

	// check job.Query is empty
	job = &model.Job{ID: 1, State: model.JobDone}
	_, _, sql, err = d.handleDDL(job)
	c.Assert(sql, Equals, "")
	c.Assert(err, NotNil, Commentf("should return not found job.Query"))

	// db info
	dbInfo := &model.DBInfo{
		ID:    2,
		Name:  dbName,
		State: model.StatePublic,
	}
	// ignoreDB info
	ingnoreDBInfo := &model.DBInfo{
		ID:    4,
		Name:  ignoreDBName,
		State: model.StatePublic,
	}
	// table Info
	tblInfo := &model.TableInfo{
		ID:    6,
		Name:  tbName,
		State: model.StatePublic,
	}
	// column info
	colInfo := &model.ColumnInfo{
		ID:        8,
		Name:      colName,
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     model.StatePublic,
	}
	tblInfo.Columns = []*model.ColumnInfo{colInfo}

	d.ignoreSchemaNames[ingnoreDBInfo.Name.L] = struct{}{}

	testCases := []struct {
		name        string
		jobID       int64
		schemaID    int64
		tableID     int64
		jobType     model.ActionType
		binlogInfo  *model.HistoryInfo
		query       string
		resultQuery string
		schemaName  string
		tableName   string
	}{
		{"createSchema", 3, 2, 0, model.ActionCreateSchema, &model.HistoryInfo{123, dbInfo, nil}, "create database Test", "create database Test", dbInfo.Name.L, ""},
		{"createIgnoreSchema", 5, 4, 0, model.ActionCreateSchema, &model.HistoryInfo{123, ingnoreDBInfo, nil}, "create database ignoreTest", "", "", ""},
		{"createTable", 7, 2, 6, model.ActionCreateTable, &model.HistoryInfo{123, nil, tblInfo}, "create table T(id int);", "create table T(id int);", dbInfo.Name.L, tblInfo.Name.L},
		{"addColumn", 9, 2, 6, model.ActionAddColumn, &model.HistoryInfo{123, nil, tblInfo}, "alter table t add a varchar(45);", "alter table t add a varchar(45);", dbInfo.Name.L, tblInfo.Name.L},
		{"truncateTable", 11, 2, 6, model.ActionTruncateTable, &model.HistoryInfo{123, nil, tblInfo}, "truncate table t;", "truncate table t;", dbInfo.Name.L, tblInfo.Name.L},
		{"dropTable", 12, 2, 10, model.ActionDropTable, nil, "drop table t;", "drop table t;", dbInfo.Name.L, "t"},
		{"dropSchema", 13, 2, 0, model.ActionDropSchema, nil, "drop database test;", "drop database test;", dbInfo.Name.L, ""},
	}

	for _, testCase := range testCases {
		// prepare for ddl
		switch testCase.name {
		case "addColumn":
			tblInfo.Columns = []*model.ColumnInfo{colInfo}
		case "truncateTable":
			tblInfo.ID = 10
		}

		job = &model.Job{
			ID:         testCase.jobID,
			SchemaID:   testCase.schemaID,
			TableID:    testCase.tableID,
			Type:       testCase.jobType,
			BinlogInfo: testCase.binlogInfo,
			Query:      testCase.query,
		}
		testDoDDLAndCheck(c, d, job, false, testCase.resultQuery, testCase.schemaName, testCase.tableName)

		// custom check after ddl
		switch testCase.name {
		case "createSchema":
			_, ok := d.schema.SchemaByID(dbInfo.ID)
			c.Assert(ok, IsTrue)
		case "createTable":
			_, ok := d.schema.TableByID(tblInfo.ID)
			c.Assert(ok, IsTrue)
		case "addColumn", "truncateTable":
			tb, ok := d.schema.TableByID(tblInfo.ID)
			c.Assert(ok, IsTrue)
			c.Assert(tb.Columns, HasLen, 1)
		case "dropTable":
			_, ok := d.schema.TableByID(tblInfo.ID)
			c.Assert(ok, IsFalse)
		case "dropSchema":
			_, ok := d.schema.SchemaByID(job.SchemaID)
			c.Assert(ok, IsFalse)
		}
	}
}

func testDoDDLAndCheck(c *C, d *Executor, job *model.Job, isErr bool, sql string, schema string, table string) {
	schemaName, tableName, s, err := d.handleDDL(job)
	c.Assert(err != nil, Equals, isErr)
	c.Assert(sql, Equals, s)
	c.Assert(schemaName, Equals, schema)
	c.Assert(tableName, Equals, table)
}
