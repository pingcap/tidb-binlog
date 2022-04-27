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

package sync

import (
	"fmt"
	"time"

	pb "github.com/pingcap/tipb/go-binlog"

	"github.com/pingcap/tidb-binlog/drainer/translator"
)

// Item contains information about binlog
type Item struct {
	Binlog        *pb.Binlog
	PrewriteValue *pb.PrewriteValue // only for DML
	Schema        string
	Table         string
	RelayLogPos   pb.Pos

	// Each item has a schemaVersion. with amend txn feature the prewrite DML's SchemaVersion could change.
	// which makes restart & reload history DDL with previous SchemaVersion not reliable.
	// so we should save this version as checkpoint.
	SchemaVersion int64

	// the applied TS executed in downstream, only for tidb
	AppliedTS int64
	// should skip to replicate this item at downstream
	// currently only used for signal the syncer to learn that the downstream schema is changed
	// when we don't replicate DDL.
	ShouldSkip bool
}

func (i *Item) String() string {
	return fmt.Sprintf("commit ts: %v", i.Binlog.CommitTs)
}

// Syncer sync binlog item to downstream
type Syncer interface {
	// Sync the binlog item to downstream
	Sync(item *Item) error
	// will be close if Close normally or meet error, call Error() to check it
	Successes() <-chan *Item
	// Return not nil if fail to sync data to downstream or nil if closed normally
	Error() <-chan error
	// Close the Syncer, no more item can be added by `Sync`
	// will drain all items and return nil if all successfully sync into downstream
	Close() error
	// SetSafeMode make the Syncer to use safe mode or not. If no need to handle, it should return false
	SetSafeMode(mode bool) bool
}

type baseSyncer struct {
	*baseError
	success         chan *Item
	tableInfoGetter translator.TableInfoGetter
	timeZone        *time.Location
}

func newBaseSyncer(tableInfoGetter translator.TableInfoGetter, timeZone *time.Location) *baseSyncer {
	if timeZone == nil {
		timeZone = time.Local
	}
	return &baseSyncer{
		baseError:       newBaseError(),
		success:         make(chan *Item, 8),
		tableInfoGetter: tableInfoGetter,
		timeZone:        timeZone,
	}
}

// Successes implements Syncer interface
func (s *baseSyncer) Successes() <-chan *Item {
	return s.success
}

// Error implements Syncer interface
func (s *baseSyncer) Error() <-chan error {
	return s.error()
}
