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

package checkpoint

import (
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
)

var (
	// ErrCheckPointClosed indicates the CheckPoint already closed.
	ErrCheckPointClosed = errors.New("CheckPoint already closed")
)

// CheckPoint is the binlog sync pos meta.
// When syncer restarts, we should reload meta info to guarantee continuous transmission.
type CheckPoint interface {
	// Load loads checkpoint information.
	Load() error

	// Save saves checkpoint information.
	Save(int64) error

	// Check checks whether we should save checkpoint.
	Check(int64) bool

	// Pos gets position information.
	TS() int64

	// Close closes the CheckPoint and release resources, after closed other methods should not be called again.
	Close() error

	// String returns CommitTS and Offset
	String() string
}

// NewCheckPoint returns a CheckPoint instance by giving name
func NewCheckPoint(name string, cfg *Config) (CheckPoint, error) {
	var (
		cp  CheckPoint
		err error
	)
	switch name {
	case "mysql", "tidb":
		cp, err = newMysql(name, cfg)
	case "pb":
		cp, err = newPb(cfg)
	case "kafka":
		cp, err = newKafka(cfg)
	case "flash":
		cp, err = newFlash(cfg)
	default:
		err = errors.Errorf("unsupported checkpoint type %s", name)
	}
	if err != nil {
		return nil, errors.Annotatef(err, "initialize %s type checkpoint with config %+v", name, cfg)
	}

	log.Infof("initialize %s type checkpoint %s with config %+v", name, cp, cfg)
	return cp, nil
}
