package drainer

import (
	"os"
	"testing"

	"golang.org/x/net/context"
	grpc "google.golang.org/grpc"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
	pb "github.com/pingcap/tipb/go-binlog"
)

type errMockType int64

const (
	dumpBinlogErr errMockType = iota
	dumpDDLJobsErr
	getLatestCommitTSErr
	recvErr
	beginTxErr
	queryErr
	prepareErr
	execErr
	closeConnErr
	commitTxErr
	rollbackTxErr
	genInsertSQLErr
	genUpdateSQLErr
	genDeleteSQLErr
	genDeleteSQLByIDErr
	genDDLSQLErr
)

var isErrMocks = make(map[errMockType]bool)
var errMock = errors.New("mock error")

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testDrainerSuite{})

type testDrainerSuite struct{}

type mockStreamClient struct {
	// 0: generate dml 1:generate dml 2: generate ddl
	index int64
	grpc.ClientStream
}

type mockCisternClient struct{}

type mockTranslator struct{}

func (s *mockStreamClient) Recv() (*pb.DumpBinlogResp, error) {
	if isErr, ok := isErrMocks[recvErr]; ok && isErr {
		return nil, errMock
	}
	s.index++

	// generate dml binlog
	if s.index <= 1 {
		preWriteValue := pb.PrewriteValue{
			Mutations: testGenTableMutation(),
		}
		preWrite, err := preWriteValue.Marshal()
		if err != nil {
			return nil, errMock
		}

		binlog := pb.Binlog{
			DdlJobId:      0,
			PrewriteValue: preWrite,
		}
		rawData, err := binlog.Marshal()
		if err != nil {
			return nil, errMock
		}

		resp := &pb.DumpBinlogResp{
			Payload: rawData,
		}
		return resp, nil
	}

	// generate ddl binlog
	binlog := pb.Binlog{
		DdlJobId: 100,
		CommitTs: 2,
	}
	rawData, err := binlog.Marshal()
	if err != nil {
		return nil, errMock
	}

	ddlJob := &model.Job{
		ID:       100,
		SchemaID: 3,
		TableID:  2,
		Query:    "truncate tablt test",
		Type:     model.ActionDropTable,
	}
	rawJob, err := ddlJob.Encode()
	if err != nil {
		return nil, errMock
	}

	resp := &pb.DumpBinlogResp{
		Payload:  rawData,
		Ddljob:   rawJob,
		CommitTS: 2,
	}

	return resp, nil
}

func (c *mockCisternClient) DumpBinlog(ctx context.Context, in *pb.DumpBinlogReq, opts ...grpc.CallOption) (pb.Cistern_DumpBinlogClient, error) {
	if isErr, ok := isErrMocks[dumpBinlogErr]; ok && isErr {
		return nil, errMock
	}

	return &mockStreamClient{index: 0}, nil
}

func (c *mockCisternClient) DumpDDLJobs(ctx context.Context, in *pb.DumpDDLJobsReq, opts ...grpc.CallOption) (*pb.DumpDDLJobsResp, error) {
	if isErr, ok := isErrMocks[dumpDDLJobsErr]; ok && isErr {
		isErrMocks[dumpDDLJobsErr] = false
		return nil, errMock
	}

	jobs, _, _ := testConstructJobs()
	var rawJobs [][]byte
	for _, job := range jobs {
		rawJob, err := job.Encode()
		if err != nil {
			return nil, err
		}
		rawJobs = append(rawJobs, rawJob)
	}

	return &pb.DumpDDLJobsResp{Ddljobs: rawJobs}, nil
}

func (c *mockCisternClient) GetLatestCommitTS(ctx context.Context, in *pb.GetLatestCommitTSReq, opts ...grpc.CallOption) (*pb.GetLatestCommitTSResp, error) {
	if isErr, ok := isErrMocks[getLatestCommitTSErr]; ok && isErr {
		return nil, errMock
	}

	return nil, nil
}

func (t *mockTranslator) GenInsertSQLs(schema string, tb *model.TableInfo, data [][]byte) ([]string, [][]interface{}, error) {
	if isErr, ok := isErrMocks[genInsertSQLErr]; ok && isErr {
		return nil, nil, errMock
	}

	return []string{"sql_insert"}, [][]interface{}{{}}, nil
}

func (t *mockTranslator) GenUpdateSQLs(schema string, tb *model.TableInfo, data [][]byte) ([]string, [][]interface{}, error) {
	if isErr, ok := isErrMocks[genUpdateSQLErr]; ok && isErr {
		return nil, nil, errMock
	}
	return []string{"sql_update"}, [][]interface{}{{}}, nil
}

func (t *mockTranslator) GenDeleteSQLsByID(schema string, tb *model.TableInfo, data []int64) ([]string, [][]interface{}, error) {
	if isErr, ok := isErrMocks[genDeleteSQLByIDErr]; ok && isErr {
		return nil, nil, errMock
	}
	return []string{"sql_delete_id"}, [][]interface{}{{}}, nil
}

func (t *mockTranslator) GenDeleteSQLs(schema string, tb *model.TableInfo, ty translator.OpType, data [][]byte) ([]string, [][]interface{}, error) {
	if isErr, ok := isErrMocks[genDeleteSQLErr]; ok && isErr {
		return nil, nil, errMock
	}
	return []string{"sql_delete"}, [][]interface{}{{}}, nil
}

func (t *mockTranslator) GenDDLSQL(schema string, sql string) (string, error) {
	if isErr, ok := isErrMocks[genDDLSQLErr]; ok && isErr {
		return "", errMock
	}

	return "", nil
}

func (t *testDrainerSuite) TestNewDrainer(c *C) {
	args := []string{
		"-metrics-addr", "127.0.0.1:9091",
		"-dest-db-type", "mockSQL",
		"-init-commit-ts", "1",
		"-config-file", "../cmd/drainer/drainer.toml",
	}

	cfg := NewConfig()
	err := cfg.Parse(args)
	c.Assert(err, IsNil)
	d, err := NewDrainer(cfg, &mockCisternClient{})
	defer os.RemoveAll(cfg.DataDir)
	c.Assert(err, IsNil)

	// test save point
	d.savePoint(12)
	c.Assert(d.meta.Pos(), Equals, int64(12))

	// test get history job and open err
	isErrMocks[dumpDDLJobsErr] = true
	err = d.Start()
	c.Assert(err, NotNil)

	// test new translator err
	d.cfg.DestDBType = "mockTestSQL"
	err = d.Start()
	c.Assert(err, NotNil)

	// test generate DML
	d.translator = &mockTranslator{}
	mutations := testGenTableMutation()
	b := newBatch(false, true, 12)
	err = d.translateSqls(mutations, b)
	c.Assert(err, IsNil)
	c.Assert(b.sqls, DeepEquals, []string{"sql_delete", "sql_update", "sql_insert", "sql_delete_id", "sql_delete"})

	// test translator
	translator.Register(d.cfg.DestDBType, d.translator)
	err = d.Start()
	c.Assert(err, NotNil)
}

func (t *testDrainerSuite) TestBatch(c *C) {
	b := newBatch(true, false, 12)
	b.addJob("drop table test", []interface{}{})
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
	var jobs []*model.Job
	jobs = append(jobs, mustTranslateJob(c, job))
	d.jobs[job.ID] = jobs[0]
	schemaName, s, err := d.handleDDL(job.ID)
	c.Assert(err != nil, Equals, isErr)
	c.Assert(sql, Equals, s)
	c.Assert(schemaName, Equals, schema)
}

func testGenTableMutation() []pb.TableMutation {
	return []pb.TableMutation{{
		TableId:      2,
		InsertedRows: [][]byte{{}},
		UpdatedRows:  [][]byte{{}},
		DeletedIds:   []int64{3},
		DeletedPks:   [][]byte{{}},
		DeletedRows:  [][]byte{{}},
		Sequence:     []pb.MutationType{pb.MutationType_DeletePK, pb.MutationType_Update, pb.MutationType_Insert, pb.MutationType_DeleteID, pb.MutationType_DeleteRow},
	}}
}
