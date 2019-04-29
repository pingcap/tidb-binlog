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

package util

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

var (
	slowDist               = 30 * time.Millisecond
	physicalShiftBits uint = 18
)

// GetApproachTS get a approach ts by ts and time
func GetApproachTS(ts int64, tm time.Time) int64 {
	if ts == 0 {
		return 0
	}
	second := int64(time.Since(tm).Seconds())
	return ts + (second*1000)<<physicalShiftBits
}

// GetTSO get tso from pd
func GetTSO(pdCli pd.Client) (int64, error) {
	now := time.Now()
	physical, logical, err := pdCli.GetTS(context.Background())
	if err != nil {
		return 0, errors.Trace(err)
	}
	dist := time.Since(now)
	if dist > slowDist {
		log.Warn("get timestamp too slow", zap.Duration("take", dist))
	}

	ts := int64(oracle.ComposeTS(physical, logical))

	return ts, nil
}

// TSOToRoughTime translates tso to rough time that used to display
func TSOToRoughTime(ts int64) time.Time {
	t := time.Unix(ts>>18/1000, 0)
	return t
}
