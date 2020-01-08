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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	// StatusNormal means server quit normally, data <= ts is synced to downstream
	StatusNormal int = 0

	// StatusRunning means server running or quit abnormally, part of data may or may not been synced to downstream
	StatusRunning int = 1
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
	Save(commitTS int64, slaveTS int64, status int) error

	// TS gets checkpoint commit timestamp.
	TS() int64

	// Status return the status saved.
	Status() int

	// Close closes the CheckPoint and release resources, after closed other methods should not be called again.
	Close() error
}

// NewCheckPoint returns a CheckPoint instance by giving name
func NewCheckPoint(cfg *Config) (CheckPoint, error) {
	var (
		cp  CheckPoint
		err error
	)
	switch cfg.CheckpointType {
	case "mysql", "tidb":
		cp, err = newMysql(cfg)
	case "file":
		cp, err = NewFile(cfg)
	default:
		err = errors.Errorf("unsupported checkpoint type %s", cfg.CheckpointType)
	}
	if err != nil {
		return nil, errors.Annotatef(err, "initialize %s type checkpoint with config %+v", cfg.CheckpointType, cfg)
	}

	log.Info("initialize checkpoint", zap.String("type", cfg.CheckpointType), zap.Int64("checkpoint", cp.TS()), zap.Reflect("cfg", cfg))

	return cp, nil
}
