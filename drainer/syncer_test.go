package drainer

import (
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	pb "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
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

	// check rollback done job
	job := &model.Job{ID: 1, State: model.JobStateRollbackDone}
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
			State:      model.JobStateDone,
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

type checkWaitSuite struct{}

var _ = Suite(&checkWaitSuite{})

func (s *checkWaitSuite) TestFalseIfIncomplete(c *C) {
	syncer := Syncer{}
	j := job{binlogTp: translator.DML}
	c.Assert(syncer.checkWait(&j), IsFalse)
}

type fakeCP struct{}

func (cp *fakeCP) Load() error         { return nil }
func (cp *fakeCP) Save(ts int64) error { return nil }
func (cp *fakeCP) Check(ts int64) bool { return false }
func (cp *fakeCP) TS() int64           { return 0 }
func (cp *fakeCP) Close() error        { return nil }
func (cp *fakeCP) String() string      { return "" }

type passingCP struct{ fakeCP }

func (cp *passingCP) Check(ts int64) bool { return true }

func (s *checkWaitSuite) TestTrueIfPassCheckpoint(c *C) {
	syncer := Syncer{cp: &passingCP{}}
	j := job{binlogTp: translator.FAKE}
	c.Assert(syncer.checkWait(&j), IsTrue)
}

func (s *checkWaitSuite) TestTrueForDDLJob(c *C) {
	syncer := Syncer{cp: &fakeCP{}}
	j := job{binlogTp: translator.DDL}
	c.Assert(syncer.checkWait(&j), IsTrue)
}

func (s *checkWaitSuite) TestShouldBeFalse(c *C) {
	syncer := Syncer{cp: &fakeCP{}}
	j := job{binlogTp: translator.FAKE}
	c.Assert(syncer.checkWait(&j), IsFalse)
}

type newJobChansSuite struct{}

var _ = Suite(&newJobChansSuite{})

func (s *newJobChansSuite) TestShouldCreateJobChans(c *C) {
	chns := newJobChans(3)
	c.Assert(len(chns), Equals, 3)
	size := maxBinlogItemCount / 3
	for _, ch := range chns {
		c.Assert(cap(ch), Equals, size)
	}
}

type jobStringSuite struct{}

var _ = Suite(&jobStringSuite{})

func (s *jobStringSuite) TestEmptyArgs(c *C) {
	j := job{
		binlogTp:   translator.FAKE,
		mutationTp: pb.MutationType_Insert,
		sql:        "",
		args:       []interface{}{}, // Empty
		key:        "test",
		commitTS:   134,
		nodeID:     "node",
	}
	expected := "{binlogTp: 4, mutationTp: Insert, sql: , args: [], key: test, commitTS: 134, nodeID: node}"
	c.Assert(j.String(), Equals, expected)
}

func (s *jobStringSuite) TestLongArgsAreTruncated(c *C) {
	j := job{
		binlogTp:   translator.DML,
		mutationTp: pb.MutationType_Insert,
		sql:        "",
		key:        "test",
		commitTS:   134,
		nodeID:     "node",
	}
	j.args = []interface{}{
		strings.Repeat("x", 50),
		798,
	}
	expected := "{binlogTp: 1, mutationTp: Insert, sql: , " +
		"args: [xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx..., 798], " +
		"key: test, commitTS: 134, nodeID: node}"
	c.Assert(j.String(), Equals, expected)
}

func BenchmarkJobString(b *testing.B) {
	j := job{
		args: []interface{}{
			strings.Repeat("x", 50),
			798,
			strings.Repeat("y", 100),
		},
	}
	b.ResetTimer()
	var d string
	for i := 0; i < b.N; i++ {
		d = j.String()
	}
	if len(d) == 0 {
		b.Fatal("String is empty")
	}
}

type addJobSuite struct{}

var _ = Suite(&addJobSuite{})

func (s *addJobSuite) TestFlushShouldSendJobToWorkers(c *C) {
	syncer := Syncer{cfg: &SyncerConfig{WorkerCount: 2}}
	syncer.jobCh = newJobChans(syncer.cfg.WorkerCount)
	j := job{binlogTp: translator.FLUSH}

	signal := make(chan struct{})
	go func() {
		syncer.addJob(&j)
		close(signal)
	}()

	for _, chl := range syncer.jobCh {
		<-chl
		syncer.jobWg.Done()
	}

	select {
	case <-signal:
	case <-time.After(1 * time.Second):
		c.Fatal("syncer.addJob doesn't return in time")
	}
}

func (s *addJobSuite) TestShouldAddToOneWorker(c *C) {
	syncer, err := NewSyncer(context.Background(), &fakeCP{}, &SyncerConfig{WorkerCount: 3})
	c.Assert(err, IsNil)
	j := job{binlogTp: translator.DDL, nodeID: "local", commitTS: 173}

	signal := make(chan struct{})
	go func() {
		syncer.addJob(&j)
		close(signal)
	}()

	select {
	case <-syncer.jobCh[0]:
	case <-syncer.jobCh[1]:
	case <-syncer.jobCh[2]:
	case <-time.After(50 * time.Millisecond):
		c.Fatal("syncer.addJob add no job!")
	}
	syncer.jobWg.Done()

	select {
	case <-signal:
	case <-time.After(1 * time.Second):
		c.Fatal("syncer.addJob doesn't return in time")
	}

	c.Assert(syncer.positions[j.nodeID], Equals, j.commitTS)
}

