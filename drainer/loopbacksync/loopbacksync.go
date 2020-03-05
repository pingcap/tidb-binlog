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

package loopbacksync

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/plugin"
	"go.uber.org/zap"
)

const (
	//MarkTableName mark table name
	MarkTableName = "retl._drainer_repl_mark"
	//ID syncer worker coroutine id
	ID = "id"
	//ChannelID channel id
	ChannelID = "channel_id"
	//Val val
	Val = "val"
	//ChannelInfo channel info
	ChannelInfo = "channel_info"
)

//LoopBackSync loopback sync info
type LoopBackSync struct {
	ChannelID       int64
	LoopbackControl bool
	MarkDBName      string
	MarkTableName   string
	SyncDDL         bool
	Index           int64
	PluginPath      string
	PluginNames     []string
	Hooks           []*plugin.EventHooks
	SupportPlugin   bool
	RecordId        int
}

//NewLoopBackSyncInfo return LoopBackSyncInfo objec
func NewLoopBackSyncInfo(ChannelID int64, LoopbackControl, SyncDDL bool, path string, names []string, SupportPlug bool, mdbname, mtablename string) *LoopBackSync {
	l := &LoopBackSync{
		ChannelID:       ChannelID,
		LoopbackControl: LoopbackControl,
		SyncDDL:         SyncDDL,
		Index:           0,
		PluginPath:      path,
		PluginNames:     names,
		SupportPlugin:   SupportPlug,
		MarkDBName:      strings.TrimSpace(mdbname),
		MarkTableName:   strings.TrimSpace(mtablename),
	}
	if l.SupportPlugin {
		l.Hooks = make([]*plugin.EventHooks, 2)
		l.Hooks[plugin.SyncerPlugin] = &plugin.EventHooks{}
		l.Hooks[plugin.LoaderPlugin] = &plugin.EventHooks{}
	}
	return l
}

// CreateMarkTable create the db and table if need.
func CreateMarkTable(db *sql.DB, mdbname, mtablename string) error {
	// CreateMarkDBDDL is DDL to create the database of mark table.
	var err error
	if len(mdbname) == 0 {
		// CreateMarkTableDDL is the DDL to create the mark table.
		var CreateMarkTableDDL string = fmt.Sprintf("CREATE TABLE If Not Exists %s (%s bigint not null,%s bigint not null DEFAULT 0, %s bigint DEFAULT 0, %s varchar(64) ,PRIMARY KEY (%s,%s));", mtablename, ID, ChannelID, Val, ChannelInfo, ID, ChannelID)
		_, err = db.Exec(CreateMarkTableDDL)
		if err != nil {
			return errors.Annotate(err, "failed to create mark table")
		}
	} else {
		var CreateMarkDBDDL = fmt.Sprintf("create database IF NOT EXISTS %s;", mdbname)
		_, err = db.Exec(CreateMarkDBDDL)
		if err != nil {
			return errors.Annotate(err, "failed to create mark db")
		}

		// CreateMarkTableDDL is the DDL to create the mark table.
		var CreateMarkTableDDL string = fmt.Sprintf("CREATE TABLE If Not Exists %s.%s (%s bigint not null,%s bigint not null DEFAULT 0, %s bigint DEFAULT 0, %s varchar(64) ,PRIMARY KEY (%s,%s));", mdbname, mtablename, ID, ChannelID, Val, ChannelInfo, ID, ChannelID)
		_, err = db.Exec(CreateMarkTableDDL)
		if err != nil {
			return errors.Annotate(err, "failed to create mark table")
		}
	}

	return nil
}

// InitMarkTableData init rowNum rows in the mark table for channelID.
func InitMarkTableData(db *sql.DB, rowNum int, channelID int64) error {
	var builder strings.Builder
	holder := "(?,?,?,?)"
	columns := fmt.Sprintf("(%s,%s,%s,%s) ", ID, ChannelID, Val, ChannelInfo)
	builder.WriteString("REPLACE INTO " + MarkTableName + columns + " VALUES ")
	for i := 0; i < rowNum; i++ {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(holder)
	}

	var args []interface{}
	for id := 0; id < rowNum; id++ {
		args = append(args, id, channelID, 1 /* value */, "" /*channel_info*/)
	}

	query := builder.String()
	if _, err := db.Exec(query, args...); err != nil {
		log.Error("Exec fail", zap.String("query", query), zap.Reflect("args", args), zap.Error(err))
		return errors.Trace(err)
	}

	return nil
}

// CleanMarkTableData clean up the data in mark table.
func CleanMarkTableData(db *sql.DB, channelID int64) error {
	sql := fmt.Sprintf("delete from %s where %s = ? ", MarkTableName, ChannelID)
	_, err := db.Exec(sql, channelID)

	if err != nil {
		return errors.Annotate(err, "failed t clean mark table data")
	}

	return nil
}

// UpdateMark update the mark table.
func UpdateMark(tx *sql.Tx, id int64, channelID int64) error {
	sql := fmt.Sprintf("update %s set %s=%s+1 where %s=? and %s=? limit 1;", MarkTableName, Val, Val, ID, ChannelID)
	_, err := tx.Exec(sql, id, channelID)

	return errors.Trace(err)
}
