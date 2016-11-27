package cistern

import (
	"os"
	"testing"

	"golang.org/x/net/context"
	grpc "google.golang.org/grpc"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tipb/go-binlog"
)

func Test(t *testing.T) {
	windowNamespace = []byte("testWindow")
	binlogNamespace = []byte("testBinlog")
	savepointNamespace = []byte("testSavePoint")
	ddlJobNamespace = []byte("testDDL")
	TestingT(t)
}

var _ = Suite(&testCisternSuite{})
var mockSendErr bool

type testCisternSuite struct{}

type mockStreamService struct {
	grpc.ServerStream
}

func (s *mockStreamService) Send(out *binlog.DumpBinlogResp) error {
	if mockSendErr {
		return errors.New("send error")
	}
	mockSendErr = true
	return nil
}

func (t *testCisternSuite) TestServer(c *C) {
	// init window and store
	b, err := store.NewBoltStore("./server.test", [][]byte{windowNamespace, ddlJobNamespace, binlogNamespace, savepointNamespace})
	c.Assert(err, IsNil)
	defer func() {
		b.Close()
		os.Remove("./server.test")
	}()
	w, err := NewDepositWindow(b)
	c.Assert(err, IsNil)
	w.SaveLower(9)
	w.SaveUpper(10)
	// init data
	col := &Collector{
		boltdb: b,
		window: w,
	}
	bins := make(map[int64]*binlog.Binlog)
	bins[2] = &binlog.Binlog{
		Tp:      binlog.BinlogType_Commit,
		StartTs: 1,
	}
	bins[4] = &binlog.Binlog{
		Tp:       binlog.BinlogType_Commit,
		StartTs:  2,
		DdlJobId: 8,
	}
	bins[5] = &binlog.Binlog{
		Tp:      binlog.BinlogType_Commit,
		StartTs: 3,
	}
	c.Assert(col.store(bins), IsNil)
	// test store ddl job
	jobs := make(map[int64]*model.Job)
	jobs[6] = &model.Job{
		ID:   7,
		Type: model.ActionCreateTable,
		Args: []interface{}{0, &model.TableInfo{}},
	}
	jobs[7] = &model.Job{
		ID:   7,
		Type: model.ActionCreateTable,
		Args: []interface{}{1, &model.TableInfo{}},
	}
	jobs[8] = &model.Job{
		ID:   8,
		Type: model.ActionCreateTable,
		Args: []interface{}{2, &model.TableInfo{}},
	}
	for index := range jobs {
		rawJob, err1 := jobs[index].Encode()
		c.Assert(err1, IsNil)
		err1 = jobs[index].Decode(rawJob)
		c.Assert(err1, IsNil)
	}
	c.Assert(col.storeDDLJobs(jobs), IsNil)
	// init server
	s := &Server{
		boltdb:    b,
		collector: col,
		window:    w,
	}
	// test DumpBinlog
	c.Assert(s.DumpBinlog(&binlog.DumpBinlogReq{BeginCommitTS: 1}, &mockStreamService{}), NotNil)
	// testGetLatestCommitTS
	col.updateStatus(true)
	res, err := s.GetLatestCommitTS(context.Background(), nil)
	c.Assert(res.IsSynced, Equals, true)
	c.Assert(res.CommitTS, Equals, int64(10))
	// testDumpDDLJobs
	//get ddl jobs by ts
	ddlRes, err := s.DumpDDLJobs(context.Background(), &binlog.DumpDDLJobsReq{BeginCommitTS: 0})
	c.Assert(err, IsNil)
	c.Assert(ddlRes.Ddljobs, HasLen, 1)
	// get ddl job by job ID
	bins[1] = &binlog.Binlog{
		Tp:       binlog.BinlogType_Commit,
		StartTs:  1,
		DdlJobId: 7,
	}
	c.Assert(col.store(bins), IsNil)
	ddlRes, err = s.DumpDDLJobs(context.Background(), &binlog.DumpDDLJobsReq{BeginCommitTS: 0})
	c.Assert(err, IsNil)
	c.Assert(ddlRes.Ddljobs, HasLen, 1)
	ddlRes, err = s.DumpDDLJobs(context.Background(), &binlog.DumpDDLJobsReq{BeginCommitTS: 5})
	c.Assert(err, IsNil)
	c.Assert(ddlRes.Ddljobs, HasLen, 3)
}
