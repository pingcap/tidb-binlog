// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package drainer

import (
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/pkg/filter"
	pb "github.com/pingcap/tipb/go-binlog"
)

type syncerSuite struct{}

var _ = check.Suite(&syncerSuite{})

func (s *syncerSuite) TestFilterTable(c *check.C) {
	schema, err := NewSchema(nil, false)
	c.Assert(err, check.IsNil)

	var dropID int64 = 1
	schema.tableIDToName[dropID] = TableName{Schema: "test", Table: "test"}
	// ignore "test" db
	filter := filter.NewFilter([]string{"test"}, nil, nil, nil)

	var pv = &pb.PrewriteValue{
		Mutations: []pb.TableMutation{
			{TableId: dropID},
		},
	}

	ignore, err := filterTable(pv, filter, schema)
	c.Assert(err, check.IsNil)
	c.Assert(ignore, check.IsTrue)

	// append one mutation that will not be dropped
	var keepID int64 = 2
	schema.tableIDToName[keepID] = TableName{Schema: "keep", Table: "keep"}
	pv.Mutations = append(pv.Mutations, pb.TableMutation{TableId: keepID})

	ignore, err = filterTable(pv, filter, schema)
	c.Assert(err, check.IsNil)
	c.Assert(ignore, check.IsFalse)
	c.Assert(len(pv.Mutations), check.Equals, 1)
}

func (s *syncerSuite) TestFilterMarkDatas(c *check.C) {
	var dmls []*loader.DML
	dml := loader.DML{
		Database: "retl",
		Table:    "_drainer_repl_mark",
		Tp:       1,
		Values:   make(map[string]interface{}),
	}
	dml.Values["channel_id"] = int64(100)
	dmls = append(dmls, &dml)
	dml1 := loader.DML{
		Database: "retl",
		Table:    "retl_mark9",
		Tp:       1,
		Values:   make(map[string]interface{}),
	}
	dml1.Values["status"] = 100
	dmls = append(dmls, &dml1)
	loopBackSyncInfo := loopbacksync.LoopBackSync{
		ChannelID:  100,
		DdlSync:    true,
		MarkStatus: true,
	}
	status, err := filterMarkDatas(dmls, &loopBackSyncInfo)
	c.Assert(status, check.IsTrue)
	c.Assert(err, check.IsNil)
}

func (s *syncerSuite) TestNewSyncer(c *check.C) {
	cfg := &SyncerConfig{
		DestDBType: "_intercept",
		DdlSync:    true,
	}

	cpFile := c.MkDir() + "/checkpoint"
	cp, err := checkpoint.NewFile(&checkpoint.Config{CheckPointFile: cpFile})
	c.Assert(err, check.IsNil)

	syncer, err := NewSyncer(cp, cfg, nil)
	c.Assert(err, check.IsNil)

	// run syncer
	go func() {
		err := syncer.Start()
		c.Assert(err, check.IsNil, check.Commentf(errors.ErrorStack(err)))
	}()

	var commitTS int64
	var jobID int64

	// create database test
	commitTS++
	jobID++
	binlog := &pb.Binlog{
		Tp:       pb.BinlogType_Commit,
		CommitTs: commitTS,
		DdlQuery: []byte("create database test"),
		DdlJobId: jobID,
	}
	job := &model.Job{
		ID:    jobID,
		Type:  model.ActionCreateSchema,
		State: model.JobStateSynced,
		Query: "create database test",
		BinlogInfo: &model.HistoryInfo{
			SchemaVersion: 1,
			DBInfo: &model.DBInfo{
				ID:   1,
				Name: model.CIStr{O: "test", L: "test"},
			},
		},
	}
	syncer.Add(&binlogItem{
		binlog: binlog,
		job:    job,
	})

	// create table test.test
	commitTS++
	jobID++
	var testTableID int64 = 2
	binlog = &pb.Binlog{
		Tp:       pb.BinlogType_Commit,
		CommitTs: commitTS,
		DdlQuery: []byte("create table test.test(id int)"),
		DdlJobId: jobID,
	}
	job = &model.Job{
		ID:       jobID,
		SchemaID: 1, // must be the previous create schema id of `test`
		Type:     model.ActionCreateTable,
		State:    model.JobStateSynced,
		Query:    "create table test.test(id int)",
		BinlogInfo: &model.HistoryInfo{
			SchemaVersion: 2,
			TableInfo: &model.TableInfo{
				ID:   testTableID,
				Name: model.CIStr{O: "test", L: "test"},
			},
		},
	}
	syncer.Add(&binlogItem{
		binlog: binlog,
		job:    job,
	})

	// Add dml
	commitTS++
	binlog = &pb.Binlog{
		Tp:            pb.BinlogType_Commit,
		CommitTs:      commitTS,
		PrewriteValue: getEmptyPrewriteValue(2, testTableID),
	}
	syncer.Add(&binlogItem{
		binlog: binlog,
	})

	// Add fake binlog
	for i := 0; i < 3; i++ {
		time.Sleep(time.Second)
		commitTS++
		binlog = &pb.Binlog{
			StartTs:  commitTS,
			CommitTs: commitTS,
		}

		syncer.Add(&binlogItem{
			binlog: binlog,
		})
	}

	syncer.Close()

	// should get 3 binlog item
	interceptSyncer := syncer.dsyncer.(*interceptSyncer)
	c.Assert(interceptSyncer.items, check.HasLen, 3)

	// check checkpoint ts
	// the latest fake binlog may or may not be saved
	var lastNoneFakeTS int64 = 3
	c.Assert(cp.TS(), check.Greater, lastNoneFakeTS)
	c.Assert(syncer.GetLatestCommitTS(), check.Greater, lastNoneFakeTS)
}

func (s *syncerSuite) TestIsIgnoreTxnCommitTS(c *check.C) {
	c.Assert(isIgnoreTxnCommitTS(nil, 1), check.IsFalse)
	c.Assert(isIgnoreTxnCommitTS([]int64{1, 3}, 1), check.IsTrue)
	c.Assert(isIgnoreTxnCommitTS([]int64{1, 3}, 2), check.IsFalse)
	c.Assert(isIgnoreTxnCommitTS([]int64{1, 3}, 3), check.IsTrue)
}

func getEmptyPrewriteValue(schemaVersion int64, tableID int64) (data []byte) {
	pv := &pb.PrewriteValue{
		SchemaVersion: schemaVersion,
	}
	pv.Mutations = append(pv.Mutations, pb.TableMutation{
		TableId:      tableID,
		Sequence:     []pb.MutationType{pb.MutationType_Insert, pb.MutationType_Update, pb.MutationType_DeleteRow},
		InsertedRows: [][]byte{[]byte("dummy")},
		UpdatedRows:  [][]byte{[]byte("dummy")},
		DeletedRows:  [][]byte{[]byte("dummy")},
	})

	var err error
	data, err = pv.Marshal()
	if err != nil {
		panic(err)
	}
	return
}
