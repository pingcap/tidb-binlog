package drainer

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	drainerServiceSafePointPrefix = "drainer"
	defaultDrainerGCSafePointTTL  = 5 * 60
)

func updateServiceSafePoint(ctx context.Context, pdClient pd.Client, cpt checkpoint.CheckPoint, ttl int64) {
	updateInterval := time.Duration(ttl/2) * time.Second
	tick := time.NewTicker(updateInterval)
	defer tick.Stop()
	dumplingServiceSafePointID := fmt.Sprintf("%s_%d", drainerServiceSafePointPrefix, time.Now().UnixNano())
	log.Info("generate drainer gc safePoint id", zap.String("id", dumplingServiceSafePointID))

	for {
		snapshotTS := uint64(cpt.TS())
		log.Debug("update PD safePoint limit with ttl",
			zap.Uint64("safePoint", snapshotTS),
			zap.Int64("ttl", ttl))
		for retryCnt := 0; retryCnt <= 10; retryCnt++ {
			_, err := pdClient.UpdateServiceGCSafePoint(ctx, dumplingServiceSafePointID, ttl, snapshotTS)
			if err == nil {
				break
			}
			log.Debug("update PD safePoint failed", zap.Error(err), zap.Int("retryTime", retryCnt))
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
