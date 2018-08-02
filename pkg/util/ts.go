package util

import (
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/pd-client"
	"golang.org/x/net/context"
)

var (
	slowDist      = 30 * time.Millisecond
	physicalShiftBits uint = 18
)

// TsToTimestamp translate ts to timestamp
func TsToTimestamp(ts int64) int64 {
	return ts >> 18 / 1000
}

// GetApproachTs get a approach ts by ts and time
func GetApproachTS(ts int64, tm time.Time) int64 {
	second := int64(time.Since(tm).Seconds())
	return ts + (second*1000)<<18
}

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

	ts := int64(composeTS(physical, logical))

	return ts, nil
}

func composeTS(physical, logical int64) uint64 {
	return uint64((physical << physicalShiftBits) + logical)
}