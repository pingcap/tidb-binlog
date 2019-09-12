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

	// the applied TS executed in downstream, only for tidb
	AppliedTS int64
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
	Close() error
}

type baseSyncer struct {
	*baseError
	success         chan *Item
	tableInfoGetter translator.TableInfoGetter
}

func newBaseSyncer(tableInfoGetter translator.TableInfoGetter) *baseSyncer {
	return &baseSyncer{
		baseError:       newBaseError(),
		success:         make(chan *Item),
		tableInfoGetter: tableInfoGetter,
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
