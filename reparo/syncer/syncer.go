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

package syncer

import (
	"fmt"

	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

// Syncer is the interface for executing binlog event to the target.
type Syncer interface {
	// Sync the binlog into target database.
	Sync(pbBinlog *pb.Binlog, successCB func(binlog *pb.Binlog)) error

	// Close closes the Syncer
	Close() error
}

// New creates a new executor based on the name.
func New(name string, cfg *DBConfig) (Syncer, error) {
	switch name {
	case "mysql":
		return newMysqlSyncer(cfg)
	case "print":
		return newPrintSyncer()
	case "memory":
		return newMemSyncer()
	}
	panic(fmt.Sprintf("unknown syncer %s", name))
}
