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
	"github.com/pingcap/tidb-binlog/drainer/translator"
	pb "github.com/pingcap/tipb/go-binlog"
)

// Item contains information about binlog
type Item struct {
	Binlog        *pb.Binlog
	PrewriteValue *pb.PrewriteValue // only for DML
	Schema        string
	Table         string
	RelayLogPos   pb.Pos

	// the applied TS executed in downstream, only for tidb
	AppliedTS int64
	// should skip to replicate this item at downstream
	// currently only used for signal the syncer to learn that the downstream schema is changed
	// when we don't replicate DDL.
	ShouldSkip bool
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
}

type BaseSyncer struct {
	*baseError
	success         chan *Item
	tableInfoGetter translator.TableInfoGetter
}

func newBaseSyncer(tableInfoGetter translator.TableInfoGetter) *BaseSyncer {
	return &BaseSyncer{
		baseError:       newBaseError(),
		success:         make(chan *Item, 8),
		tableInfoGetter: tableInfoGetter,
	}
}

// Successes implements Syncer interface
func (s *BaseSyncer) Successes() <-chan *Item {
	return s.success
}

// Error implements Syncer interface
func (s *BaseSyncer) Error() <-chan error {
	return s.error()
}
