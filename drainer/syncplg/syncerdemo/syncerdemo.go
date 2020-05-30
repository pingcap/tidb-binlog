package main

import (
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/sync"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/prometheus/client_golang/prometheus"
)

//NewSyncerPlugin return A syncer instance which implemented interface of sync.Syncer
func NewSyncerPlugin (
	cfg *sync.DBConfig,
	file string,
	tableInfoGetter translator.TableInfoGetter,
	worker int,
	batchSize int,
	queryHistogramVec *prometheus.HistogramVec,
	sqlMode *string,
	destDBType string,
	relayer relay.Relayer,
	info *loopbacksync.LoopBackSync,
) (sync.Syncer, error) {
	return sync.NewSyncerDemo(cfg, file, tableInfoGetter, worker, batchSize, queryHistogramVec, sqlMode,
		destDBType, relayer, info)
}
