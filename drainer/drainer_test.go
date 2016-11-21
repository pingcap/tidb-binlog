package drainer

import (
	"os"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testDrainerSuite{})

type testDrainerSuite struct{}

func (t *testDrainerSuite) TestNewDrainer(c *C) {
	args := []string{
		"-metrics-addr", "127.0.0.1:9091",
		"-config-file", "../cmd/drainer/config.toml",
	}

	cfg := NewConfig()
	err := cfg.Parse(args)
	c.Assert(err, IsNil)
	d, err := NewDrainer(cfg, nil)
	defer os.RemoveAll(cfg.DataDir)
	c.Assert(err, IsNil)

	// test save point
	d.savePoint(12)
	c.Assert(d.meta.Pos(), Equals, int64(12))
}

func (t *testDrainerSuite) TestBatch(c *C) {
	b := newBatch(true, false, 12)
	b.addJob([]string{"drop table test"}, [][]interface{}{{}})
	c.Assert(b.sqls, HasLen, 1)
	c.Assert(b.args, HasLen, 1)
	c.Assert(b.commitTS, Equals, int64(12))
	c.Assert(b.isDDL, Equals, true)
	c.Assert(b.retry, Equals, false)
}

func (t *testDrainerSuite) TestHandleDDL(c *C) {
	var err error
	d := &Drainer{}
	d.jobs = make(map[int64]*model.Job)
	d.ignoreSchemaNames = make(map[string]struct{})
	d.schema, err = NewSchema(nil, nil)
	c.Assert(err, IsNil)
	dbName := model.NewCIStr("Test")
	ignoreDBName := model.NewCIStr("ignoreTest")
	colName := model.NewCIStr("A")
	tbName := model.NewCIStr("T")

	// check not found job
	_, sql, err := d.handleDDL(0)
	c.Assert(sql, Equals, "")
	c.Assert(err, NotNil, Commentf("should return not found job error"))

	// check cancelled job
	job := &model.Job{ID: 1, State: model.JobCancelled}
	d.jobs[job.ID] = job
	_, sql, err = d.handleDDL(job.ID)
	c.Assert(err, IsNil)
	c.Assert(sql, Equals, "")

	// check job.Query is empty
	job = &model.Job{ID: 1, State: model.JobDone}
	d.jobs[job.ID] = job
	_, sql, err = d.handleDDL(job.ID)
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
		args        []interface{}
		query       string
		resultQuery string
		schemaName  string
	}{
		{"createSchema", 3, 2, 0, model.ActionCreateSchema, []interface{}{123, dbInfo}, "create database Test", "create database Test", dbInfo.Name.L},
		{"createIgnoreSchema", 5, 4, 0, model.ActionCreateSchema, []interface{}{123, ingnoreDBInfo}, "create database ignoreTest", "", ""},
		{"createTable", 7, 2, 6, model.ActionCreateTable, []interface{}{123, tblInfo}, "create table T(id int);", "create table T(id int);", dbInfo.Name.L},
		{"addColumn", 9, 2, 6, model.ActionAddColumn, []interface{}{123, tblInfo}, "alter table t add a varchar(45);", "alter table t add a varchar(45);", dbInfo.Name.L},
		{"truncateTable", 11, 2, 6, model.ActionTruncateTable, []interface{}{123, tblInfo}, "truncate table t;", "truncate table t;", dbInfo.Name.L},
		{"dropTable", 12, 2, 10, model.ActionDropTable, []interface{}{}, "drop table t;", "drop table t;", dbInfo.Name.L},
		{"dropSchema", 13, 2, 0, model.ActionDropSchema, []interface{}{}, "drop database test;", "drop database test;", dbInfo.Name.L},
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
			ID:       testCase.jobID,
			SchemaID: testCase.schemaID,
			TableID:  testCase.tableID,
			Type:     testCase.jobType,
			Args:     testCase.args,
			Query:    testCase.query,
		}
		testDoDDLAndCheck(c, d, job, false, testCase.resultQuery, testCase.schemaName)

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

func testDoDDLAndCheck(c *C, d *Drainer, job *model.Job, isErr bool, sql string, schema string) {
	jobs := mustAppendJob(c, nil, job)
	d.jobs[job.ID] = jobs[0]
	schemaName, s, err := d.handleDDL(job.ID)
	c.Assert(err != nil, Equals, isErr)
	c.Assert(sql, Equals, s)
	c.Assert(schemaName, Equals, schema)
}
