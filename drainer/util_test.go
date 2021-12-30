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
	"crypto/tls"
	"fmt"

	. "github.com/pingcap/check"
	dsync "github.com/pingcap/tidb-binlog/drainer/sync"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
)

type taskGroupSuite struct{}

var _ = Suite(&taskGroupSuite{})

/* May only get one log entry
func (s *taskGroupSuite) TestShouldRecoverFromPanic(c *C) {
	var logHook util.LogHook
	logHook.SetUp()
	defer logHook.TearDown()

	var called bool
	var g taskGroup
	g.GoNoPanic("test", func() {
		called = true
		panic("Evil Smile")
	})
	g.Wait()
	c.Assert(called, IsTrue)
	c.Assert(logHook.Entrys, HasLen, 2)
	c.Assert(logHook.Entrys[0].Message, Matches, ".*Recovered.*")
	c.Assert(logHook.Entrys[1].Message, Matches, ".*Exit.*")
}
*/

func (t *taskGroupSuite) TestCombineFilterRules(c *C) {
	filterRules := []*bf.BinlogEventRule{
		{
			Action:        bf.Ignore,
			SchemaPattern: "do_not_drop_database*",
			TablePattern:  "*",
			Events:        []bf.EventType{"drop database"},
			SQLPattern:    nil,
		},
		{
			Action:        bf.Ignore,
			SchemaPattern: "do_not_drop_database*",
			TablePattern:  "*",
			Events:        nil,
			SQLPattern:    []string{"alter table .* add column aaa int"},
		},
		{
			Action:        bf.Ignore,
			SchemaPattern: "do_not_drop_database*",
			TablePattern:  "*",
			Events:        []bf.EventType{"delete"},
			SQLPattern:    nil,
		},
		{
			Action:        bf.Ignore,
			SchemaPattern: "do_not_add_col_database*",
			TablePattern:  "do_not_add_col_table*",
			Events:        nil,
			SQLPattern:    []string{"alter table .* add column aaa int"},
		},
		{
			Action:        bf.Ignore,
			SchemaPattern: "do_not_delete_database*",
			TablePattern:  "do_not_delete_table*",
			Events:        []bf.EventType{"delete"},
			SQLPattern:    nil,
		},
	}
	expectRules := map[string]*bf.BinlogEventRule{
		"`do_not_drop_database*`.`*`": {
			Action:        bf.Ignore,
			SchemaPattern: "do_not_drop_database*",
			TablePattern:  "*",
			Events:        []bf.EventType{"drop database", "delete"},
			SQLPattern:    []string{"alter table .* add column aaa int"},
		},
		"`do_not_add_col_database*`.`do_not_add_col_table*`": {
			Action:        bf.Ignore,
			SchemaPattern: "do_not_add_col_database*",
			TablePattern:  "do_not_add_col_table*",
			Events:        []bf.EventType{},
			SQLPattern:    []string{"alter table .* add column aaa int"},
		},
		"`do_not_delete_database*`.`do_not_delete_table*`": {
			Action:        bf.Ignore,
			SchemaPattern: "do_not_delete_database*",
			TablePattern:  "do_not_delete_table*",
			Events:        []bf.EventType{"delete"},
			SQLPattern:    []string{},
		},
	}
	combinedFilters := combineFilterRules(filterRules)
	for _, filterRule := range combinedFilters {
		expectFilter, ok := expectRules[fmt.Sprintf("`%s`.`%s`", filterRule.SchemaPattern, filterRule.TablePattern)]
		c.Assert(ok, IsTrue)
		c.Assert(expectFilter.Action, Equals, filterRule.Action)
		c.Assert(expectFilter.SchemaPattern, Equals, filterRule.SchemaPattern)
		c.Assert(expectFilter.TablePattern, Equals, filterRule.TablePattern)
		c.Assert(expectFilter.Events, DeepEquals, filterRule.Events)
		c.Assert(expectFilter.SQLPattern, DeepEquals, filterRule.SQLPattern)
	}
}

type checkPointSuite struct{}

var _ = Suite(&checkPointSuite{})

func (t *checkPointSuite) TestGenTidbCheckPointCfg(c *C) {
	cfg := &Config{
		DataDir:         "checkpoint_dir",
		InitialCommitTS: 2000,
	}
	tls := &tls.Config{}
	cfg.SyncerCfg = &SyncerConfig{
		To: &dsync.DBConfig{
			Checkpoint: dsync.CheckpointConfig{
				Type:     "tidb",
				Host:     "host-1",
				User:     "user-1",
				Password: "password-1",
				TLS:      tls,
				Table:    "new_checkpoint_table",
			},
		},
	}
	id := uint64(1000)
	cp, err := GenCheckPointCfg(cfg, id)
	c.Assert(err, IsNil)
	c.Assert(cp.ClusterID, Equals, id)
	c.Assert(cp.InitialCommitTS, Equals, cfg.InitialCommitTS)
	c.Assert(cp.CheckPointFile, Equals, "checkpoint_dir/savepoint")
	c.Assert(cp.CheckpointType, Equals, cfg.SyncerCfg.To.Checkpoint.Type)
	c.Assert(cp.Db.Host, Equals, cfg.SyncerCfg.To.Checkpoint.Host)
	c.Assert(cp.Db.User, Equals, cfg.SyncerCfg.To.Checkpoint.User)
	c.Assert(cp.Db.Password, Equals, cfg.SyncerCfg.To.Checkpoint.Password)
	c.Assert(cp.Db.TLS, Equals, tls)
	c.Assert(cp.Table, Equals, "new_checkpoint_table")

	//checkpoint have not config case
	cfg.SyncerCfg = &SyncerConfig{
		DestDBType: "tidb",
		To: &dsync.DBConfig{
			Host:     "host-1",
			User:     "user-1",
			Password: "password-1",
			TLS:      tls,
		},
	}
	cp, err = GenCheckPointCfg(cfg, id)
	c.Assert(err, IsNil)
	c.Assert(cp.ClusterID, Equals, id)
	c.Assert(cp.InitialCommitTS, Equals, cfg.InitialCommitTS)
	c.Assert(cp.CheckPointFile, Equals, "checkpoint_dir/savepoint")
	c.Assert(cp.CheckpointType, Equals, cfg.SyncerCfg.DestDBType)
	c.Assert(cp.Db.Host, Equals, cfg.SyncerCfg.To.Host)
	c.Assert(cp.Db.User, Equals, cfg.SyncerCfg.To.User)
	c.Assert(cp.Db.Password, Equals, cfg.SyncerCfg.To.Password)
	c.Assert(cp.Db.TLS, Equals, tls)
	c.Assert(cp.Table, Equals, "")
}

func (t *checkPointSuite) TestGenOracleCheckPointCfg(c *C) {
	cfg := &Config{
		DataDir:         "checkpoint_dir",
		InitialCommitTS: 2000,
	}
	tls := &tls.Config{}
	cfg.SyncerCfg = &SyncerConfig{
		To: &dsync.DBConfig{
			Checkpoint: dsync.CheckpointConfig{
				Type:                "oracle",
				Host:                "host-1",
				User:                "user-1",
				Password:            "password-1",
				TLS:                 tls,
				OracleServiceName:   "oracle-service-name-1",
				OracleConnectString: "oracle-connect-string-1",
				Table:               "new_checkpoint_table",
			},
		},
	}
	id := uint64(1000)
	cp, err := GenCheckPointCfg(cfg, id)
	c.Assert(err, IsNil)
	c.Assert(cp.ClusterID, Equals, id)
	c.Assert(cp.InitialCommitTS, Equals, cfg.InitialCommitTS)
	c.Assert(cp.CheckPointFile, Equals, "checkpoint_dir/savepoint")
	c.Assert(cp.CheckpointType, Equals, cfg.SyncerCfg.To.Checkpoint.Type)
	c.Assert(cp.Db.Host, Equals, cfg.SyncerCfg.To.Checkpoint.Host)
	c.Assert(cp.Db.User, Equals, cfg.SyncerCfg.To.Checkpoint.User)
	c.Assert(cp.Db.Password, Equals, cfg.SyncerCfg.To.Checkpoint.Password)
	c.Assert(cp.Db.TLS, Equals, tls)
	c.Assert(cp.Table, Equals, "new_checkpoint_table")

	//checkpoint have not config case
	cfg.SyncerCfg = &SyncerConfig{
		DestDBType: "oracle",
		To: &dsync.DBConfig{
			Host:                "host-1",
			User:                "user-1",
			Password:            "password-1",
			TLS:                 tls,
			OracleServiceName:   "oracle-service-name-1",
			OracleConnectString: "oracle-connect-string-1",
		},
	}
	cp, err = GenCheckPointCfg(cfg, id)
	c.Assert(err, IsNil)
	c.Assert(cp.ClusterID, Equals, id)
	c.Assert(cp.InitialCommitTS, Equals, cfg.InitialCommitTS)
	c.Assert(cp.CheckPointFile, Equals, "checkpoint_dir/savepoint")
	c.Assert(cp.CheckpointType, Equals, cfg.SyncerCfg.DestDBType)
	c.Assert(cp.Db.Host, Equals, cfg.SyncerCfg.To.Host)
	c.Assert(cp.Db.User, Equals, cfg.SyncerCfg.To.User)
	c.Assert(cp.Db.Password, Equals, cfg.SyncerCfg.To.Password)
	c.Assert(cp.Db.TLS, Equals, tls)
	c.Assert(cp.Table, Equals, "")
}
