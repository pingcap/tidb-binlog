package drainer

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

func (t *testDrainerSuite) TestHandleDDL(c *C) {
	var err error
	cfg := &SyncerConfig{DestDBType: "mysql"}
	s := &Syncer{cfg: cfg}
	s.schema, err = NewSchema(nil, false)
	c.Assert(err, IsNil)
	dbName := model.NewCIStr("Test")
	colName := model.NewCIStr("A")
	tbName := model.NewCIStr("T")

	// check cancelled job
	job := &model.Job{ID: 1, State: model.JobStateCancelled}
	_, _, sql, err := s.schema.handleDDL(job)
	c.Assert(err, IsNil)
	c.Assert(sql, Equals, "")

	// check job.Query is empty
	job = &model.Job{ID: 1, State: model.JobStateDone}
	_, _, sql, err = s.schema.handleDDL(job)
	c.Assert(sql, Equals, "")
	c.Assert(err, NotNil, Commentf("should return not found job.Query"))

	// db info
	dbInfo := &model.DBInfo{
		ID:    2,
		Name:  dbName,
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
		{"createSchema", 3, 2, 0, model.ActionCreateSchema, &model.HistoryInfo{1, dbInfo, nil, 123}, "create database Test", "create database Test", dbInfo.Name.O, ""},
		{"createTable", 7, 2, 6, model.ActionCreateTable, &model.HistoryInfo{3, nil, tblInfo, 123}, "create table T(id int);", "create table T(id int);", dbInfo.Name.O, tblInfo.Name.O},
		{"addColumn", 9, 2, 6, model.ActionAddColumn, &model.HistoryInfo{4, nil, tblInfo, 123}, "alter table T add a varchar(45);", "alter table T add a varchar(45);", dbInfo.Name.O, tblInfo.Name.O},
		{"truncateTable", 11, 2, 6, model.ActionTruncateTable, &model.HistoryInfo{5, nil, tblInfo, 123}, "truncate table T;", "truncate table T;", dbInfo.Name.O, tblInfo.Name.O},
		{"dropTable", 12, 2, 10, model.ActionDropTable, &model.HistoryInfo{6, nil, nil, 123}, "drop table T;", "drop table T;", dbInfo.Name.O, tblInfo.Name.O},
		{"dropSchema", 13, 2, 0, model.ActionDropSchema, &model.HistoryInfo{7, nil, nil, 123}, "drop database test;", "drop database test;", dbInfo.Name.O, ""},
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
		testDoDDLAndCheck(c, s, job, false, testCase.resultQuery, testCase.schemaName, testCase.tableName)

		// custom check after ddl
		switch testCase.name {
		case "createSchema":
			_, ok := s.schema.SchemaByID(dbInfo.ID)
			c.Assert(ok, IsTrue)
		case "createTable":
			_, ok := s.schema.TableByID(tblInfo.ID)
			c.Assert(ok, IsTrue)
		case "addColumn", "truncateTable":
			tb, ok := s.schema.TableByID(tblInfo.ID)
			c.Assert(ok, IsTrue)
			c.Assert(tb.Columns, HasLen, 1)
		case "dropTable":
			_, ok := s.schema.TableByID(tblInfo.ID)
			c.Assert(ok, IsFalse)
		case "dropSchema":
			_, ok := s.schema.SchemaByID(job.SchemaID)
			c.Assert(ok, IsFalse)
		}
	}
}

func testDoDDLAndCheck(c *C, s *Syncer, job *model.Job, isErr bool, sql string, schema string, table string) {
	schemaName, tableName, resSQL, err := s.schema.handleDDL(job)
	c.Logf("handle: %s", job.Query)
	c.Logf("result: %s, %s, %s, %v", schemaName, tableName, resSQL, err)
	c.Assert(err != nil, Equals, isErr)
	c.Assert(sql, Equals, resSQL)
	c.Assert(schemaName, Equals, schema)
	c.Assert(tableName, Equals, table)
}
