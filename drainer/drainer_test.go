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
	b.addJob([]string{"select * from test"}, [][]interface{}{{}})
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
	if err == nil {
		c.Fatal("should return not found job error")
	}

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
	if err == nil {
		c.Fatal("should return not found job.Query")
	}

	// db info
	dbInfo := &model.DBInfo{
		ID:    2,
		Name:  dbName,
		State: model.StatePublic,
	}
	// `createSchema` job
	job = &model.Job{
		ID:       3,
		SchemaID: 2,
		Type:     model.ActionCreateSchema,
		Args:     []interface{}{123, dbInfo},
		Query:    "create database Test",
	}
	testDoDDLAndCheck(c, d, job, false, job.Query, dbInfo.Name.L)
	_, ok := d.schema.SchemaByID(dbInfo.ID)
	c.Assert(ok, IsTrue)

	// ignoreDB info
	ingnoreDBInfo := &model.DBInfo{
		ID:    4,
		Name:  ignoreDBName,
		State: model.StatePublic,
	}
	// `createSchema` job
	job = &model.Job{
		ID:       5,
		SchemaID: 4,
		Type:     model.ActionCreateSchema,
		Args:     []interface{}{123, ingnoreDBInfo},
		Query:    "create database ignoreTest",
	}
	d.ignoreSchemaNames[ingnoreDBInfo.Name.L] = struct{}{}
	testDoDDLAndCheck(c, d, job, false, "", "")

	// `createTable`
	tblInfo := &model.TableInfo{
		ID:    6,
		Name:  tbName,
		State: model.StatePublic,
	}
	job = &model.Job{
		ID:       7,
		SchemaID: 2,
		TableID:  6,
		Type:     model.ActionCreateTable,
		Args:     []interface{}{123, tblInfo},
		Query:    "create table T(id int);",
	}
	testDoDDLAndCheck(c, d, job, false, job.Query, dbInfo.Name.L)
	_, ok = d.schema.TableByID(tblInfo.ID)
	c.Assert(ok, IsTrue)

	// `addColumn`
	colInfo := &model.ColumnInfo{
		ID:        8,
		Name:      colName,
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     model.StatePublic,
	}
	tblInfo.Columns = []*model.ColumnInfo{colInfo}
	job = &model.Job{
		ID:       9,
		SchemaID: 2,
		TableID:  6,
		Type:     model.ActionAddColumn,
		Args:     []interface{}{123, tblInfo},
		Query:    "alter table t add a varchar(45);",
	}
	testDoDDLAndCheck(c, d, job, false, job.Query, dbInfo.Name.L)
	tb, ok := d.schema.TableByID(tblInfo.ID)
	c.Assert(ok, IsTrue)
	c.Assert(tb.Columns, HasLen, 1)

	// `truncate table`
	tblInfo.ID = 10
	job = &model.Job{
		ID:       11,
		SchemaID: 2,
		TableID:  6,
		Type:     model.ActionTruncateTable,
		Args:     []interface{}{123, tblInfo},
		Query:    "truncate table t;",
	}
	testDoDDLAndCheck(c, d, job, false, job.Query, dbInfo.Name.L)
	tb, ok = d.schema.TableByID(tblInfo.ID)
	c.Assert(ok, IsTrue)
	c.Assert(tb.Columns, HasLen, 1)

	// `drop table`
	job = &model.Job{
		ID:       12,
		SchemaID: 2,
		TableID:  10,
		Type:     model.ActionDropTable,
		Query:    "drop table t;",
	}
	testDoDDLAndCheck(c, d, job, false, job.Query, dbInfo.Name.L)
	_, ok = d.schema.TableByID(tblInfo.ID)
	c.Assert(ok, IsFalse)

	// `drop schema`
	job = &model.Job{
		ID:       13,
		SchemaID: 2,
		Type:     model.ActionDropSchema,
		Query:    "drop database test;",
	}
	testDoDDLAndCheck(c, d, job, false, job.Query, dbInfo.Name.L)
	_, ok = d.schema.SchemaByID(job.SchemaID)
	c.Assert(ok, IsFalse)
}

func testDoDDLAndCheck(c *C, d *Drainer, job *model.Job, isErr bool, sql string, schema string) {
	jobs := testAppendJob(c, nil, job)
	d.jobs[job.ID] = jobs[0]
	schemaName, s, err := d.handleDDL(job.ID)
	if isErr && err == nil {
		c.Fatal("should return error")
	} else if !isErr {
		c.Assert(err, IsNil)
	}
	c.Assert(sql, Equals, s)
	c.Assert(schemaName, Equals, schema)
}
