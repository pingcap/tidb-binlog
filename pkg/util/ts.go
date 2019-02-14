package util

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"golang.org/x/net/context"
)

var (
	slowDist               = 30 * time.Millisecond
	physicalShiftBits uint = 18
)

// TsToTimestamp translate ts to timestamp
func TsToTimestamp(ts int64) int64 {
	return ts >> physicalShiftBits / 1000
}

// GetApproachTS get a approach ts by ts and time
func GetApproachTS(ts int64, tm time.Time) int64 {
	if ts == 0 {
		return 0
	}
	second := int64(time.Since(tm).Seconds())
	return ts + (second*1000)<<18
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
		log.Warnf("get timestamp too slow: %s", dist)
	}

	ts := int64(oracle.ComposeTS(physical, logical))

	return ts, nil
}
