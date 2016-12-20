package cistern

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/net/context"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-binlog"
)

func (t *testCisternSuite) TestCollector(c *C) {
	// init window
	s, err := store.NewBoltStore("./collector.test", [][]byte{windowNamespace, ddlJobNamespace, binlogNamespace, savepointNamespace})
	c.Assert(err, IsNil)
	defer func() {
		s.Close()
		os.Remove("./collector.test")
	}()
	w, err := NewDepositWindow(s)
	c.Assert(err, IsNil)
	w.SaveUpper(3)
	// test update status
	col := &Collector{}
	col.pumps = make(map[string]*Pump)
	col.pumps["testNode1"] = &Pump{current: binlog.Pos{Suffix: 1, Offset: 1}}
	col.window = w
	col.updateStatus(true)
	status := col.HTTPStatus()
	c.Assert(status.PumpPos, HasLen, 1)
	c.Assert(status.Synced, Equals, true)
	c.Assert(status.DepositWindow.Upper, Equals, int64(3))

	// test update savePoint
	col.boltdb = s
	savePoints := make(map[string]binlog.Pos)
	savePoints["testNode1"] = binlog.Pos{Suffix: 1, Offset: 1}
	err = col.updateSavepoints(savePoints)
	c.Assert(err, IsNil)
	pos, err := col.getSavePoints("testNode1")
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, savePoints["testNode1"])
	pos, err = col.getSavePoints("testNode2")
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, binlog.Pos{})

	// test store binlog
	bin := &binlog.Binlog{
		Tp:      binlog.BinlogType_Prewrite,
		StartTs: 1,
	}
	bins := make(map[int64]*binlog.Binlog)
	bins[1] = bin
	c.Assert(col.store(bins), IsNil)
	key := codec.EncodeInt([]byte{}, 1)
	_, err = col.boltdb.Get(binlogNamespace, key)
	c.Assert(err, IsNil)

	// test store ddl job
	job := &model.Job{
		ID:   1,
		Type: model.ActionCreateTable,
		Args: []interface{}{123, &model.TableInfo{}},
	}
	rawJob, err := job.Encode()
	c.Assert(err, IsNil)
	err = job.Decode(rawJob)
	c.Assert(err, IsNil)
	jobs := make(map[int64]*model.Job)
	jobs[1] = job
	c.Assert(col.storeDDLJobs(jobs), IsNil)
	_, err = col.boltdb.Get(ddlJobNamespace, key)
	c.Assert(err, IsNil)

	// test load history ddl job
	store, err := tidb.NewStore(fmt.Sprintf("memory://testkv"))
	c.Assert(err, IsNil)
	defer store.Close()
	col.tiStore = store
	// start a transation
	txn, err := store.Begin()
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	// add history ddl job
	job.ID = 2
	err = m.AddHistoryDDLJob(job)
	c.Assert(err, IsNil)
	// add history canceled ddl job
	job.ID = 3
	job.State = model.JobCancelled
	err = m.AddHistoryDDLJob(job)
	c.Assert(err, IsNil)
	txn.Commit()
	err = col.LoadHistoryDDLJobs()
	c.Assert(err, IsNil)
	key = codec.EncodeInt([]byte{}, 2)
	_, err = col.boltdb.Get(ddlJobNamespace, key)
	c.Assert(err, IsNil)
	key = codec.EncodeInt([]byte{}, 3)
	_, err = col.boltdb.Get(ddlJobNamespace, key)
	c.Assert(errors.IsNotFound(err), IsTrue)

	// test grab ddl jobs
	col.timeout = time.Second
	bins[2] = &binlog.Binlog{
		Tp:       binlog.BinlogType_Prewrite,
		DdlJobId: 3,
	}
	bins[4] = &binlog.Binlog{
		Tp:       binlog.BinlogType_Prewrite,
		DdlJobId: 4,
	}
	go func() {
		txn, err = store.Begin()
		c.Assert(err, IsNil)
		m = meta.NewMeta(txn)
		job.ID = 4
		job.State = model.JobDone
		err = m.AddHistoryDDLJob(job)
		c.Assert(err, IsNil)
		txn.Commit()
	}()

	ddlJobs, err := col.grabDDLJobs(context.Background(), bins)
	c.Assert(ddlJobs, HasLen, 1)
	c.Assert(err, IsNil)

	// test update latest commitTS
	col.updateLatestCommitTS(bins)
	c.Assert(col.window.LoadUpper(), Equals, int64(4))
}
