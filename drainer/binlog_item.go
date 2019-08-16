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
	"fmt"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	pb "github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

type binlogItem struct {
	binlog *pb.Binlog
	nodeID string
	job    *model.Job
}

// GetCommitTs implements Item interface in merger.go
func (b *binlogItem) GetCommitTs() int64 {
	return b.binlog.CommitTs
}

// GetSourceID implements Item interface in merger.go
func (b *binlogItem) GetSourceID() string {
	return b.nodeID
}

// String returns the string of this binlogItem
func (b *binlogItem) String() string {
	return fmt.Sprintf("{startTS: %d, commitTS: %d, node: %s}", b.binlog.StartTs, b.binlog.CommitTs, b.nodeID)
}

func (b *binlogItem) Size() int64 {
	if b.binlog == nil {
		return 0
	}
	return int64(len(b.binlog.DdlQuery) + len(b.binlog.PrewriteKey) + len(b.binlog.PrewriteValue) + len(b.binlog.XXX_unrecognized))
}

func newBinlogItem(b *pb.Binlog, nodeID string) *binlogItem {
	itemp := &binlogItem{
		binlog: b,
		nodeID: nodeID,
	}

	return itemp
}

//
func (b *binlogItem) SetJob(job *model.Job) {
	b.job = job
}

type binlogItemCache struct {
	cachedChan         chan *binlogItem
	cachedSize         int64
	maxBinlogCacheSize int64
	cond               *sync.Cond
	quiting            bool
}

func newBinlogItemCache(maxBinlogItemCount int, maxBinlogCacheSize int64) (bc *binlogItemCache) {
	return &binlogItemCache{
		cachedChan:         make(chan *binlogItem, maxBinlogItemCount),
		maxBinlogCacheSize: maxBinlogCacheSize,
		cond:               sync.NewCond(new(sync.Mutex)),
	}
}

func (bc *binlogItemCache) Push(b *binlogItem, shutdown chan struct{}) chan struct{} {
	finished := make(chan struct{})
	go func() {
		bc.cond.L.Lock()
		if b.Size() >= bc.maxBinlogCacheSize {
			for bc.cachedSize != 0 && !bc.quiting {
				bc.cond.Wait()
			}
		} else {
			for bc.cachedSize+b.Size() > bc.maxBinlogCacheSize && !bc.quiting {
				bc.cond.Wait()
			}
		}
		bc.cond.L.Unlock()
		select {
		case <-shutdown:
		case bc.cachedChan <- b:
			bc.cond.L.Lock()
			bc.cachedSize += b.Size()
			bc.cond.L.Unlock()
			log.Debug("receive publish binlog item", zap.Stringer("item", b))
		}
		close(finished)
	}()
	return finished
}

func (bc *binlogItemCache) Pop() chan *binlogItem {
	result := make(chan *binlogItem)
	go func() {
		select {
		case b := <-bc.cachedChan:
			result <- b
			bc.cond.L.Lock()
			// has popped new binlog item, minus cachedSize
			bc.cachedSize -= b.Size()
			bc.cond.Signal()
			bc.cond.L.Unlock()
		}
	}()
	return result
}

func (bc *binlogItemCache) Close() {
	bc.quiting = true
	bc.cond.Signal()
}

func (bc *binlogItemCache) Len() int {
	return len(bc.cachedChan)
}
