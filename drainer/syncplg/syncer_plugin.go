package syncplg

import (
	"errors"
	"fmt"
	"plugin"

	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/sync"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	NewSyncerPlugin = "NewSyncerPlugin"
)

type NewSyncerFunc func(
	cfg *sync.DBConfig,
	tableInfoGetter translator.TableInfoGetter,
	worker int,
	batchSize int,
	queryHistogramVec *prometheus.HistogramVec,
	sqlMode *string,
	destDBType string,
	relayer relay.Relayer,
	info *loopbacksync.LoopBackSync,
	enableDispatch bool,
	enableCausility bool) (dsyncer sync.Syncer, err error)

func LoadPlugin(path, name string) (NewSyncerFunc, error) {
	fp := path + "/" + name
	p, err := plugin.Open(fp)
	if err != nil {
		return nil, fmt.Errorf("faile to Open %s . err: %s", fp, err.Error())
	}

	sym, err := p.Lookup(NewSyncerPlugin)
	if err != nil {
		return nil, err
	}
	newSyncer, ok := sym.(func(
		cfg *sync.DBConfig,
		tableInfoGetter translator.TableInfoGetter,
		worker int,
		batchSize int,
		queryHistogramVec *prometheus.HistogramVec,
		sqlMode *string,
		destDBType string,
		relayer relay.Relayer,
		info *loopbacksync.LoopBackSync,
		enableDispatch bool,
		enableCausility bool) (dsyncer sync.Syncer, err error))
	if !ok {
		return nil, errors.New("function type is incorrect")
	}
	return newSyncer, nil
}
