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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// KafkaCheckpoint is local CheckPoint struct.
type KafkaCheckpoint struct {
	sync.Mutex
	*PbCheckPoint
}

func newKafka(cfg *Config) (CheckPoint, error) {
	pb, err := NewPb(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cp := &KafkaCheckpoint{
		PbCheckPoint: pb.(*PbCheckPoint),
	}

	return cp, nil
}

// Save implements CheckPoint.Save()
func (cp *KafkaCheckpoint) Save(ts int64) error {
	cp.Lock()
	defer cp.Unlock()

	if cp.closed {
		return errors.Trace(ErrCheckPointClosed)
	}

	if ts <= cp.CommitTS {
		log.Error("ignore save checkpoint", zap.Int64("ts", ts))
		return nil
	}

	return cp.PbCheckPoint.Save(ts)
}
