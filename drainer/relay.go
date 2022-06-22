package drainer

import (
	"database/sql"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/sync"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	obinlog "github.com/pingcap/tidb/tidb-binlog/proto/go-binlog"
	router "github.com/pingcap/tidb/util/table-router"
	"go.uber.org/zap"
)

func feedByRelayLogIfNeed(cfg *Config) error {
	if !cfg.SyncerCfg.Relay.IsEnabled() {
		return nil
	}

	// for the mysql type checkpoint
	// clusterID will be use as the key
	// we can't get the cluster id from pd so we just set 0
	// and the checkpoint will use the clusterID exist at the checkpoint table.
	cpCfg, err := GenCheckPointCfg(cfg, 0 /* clusterID */)
	if err != nil {
		return errors.Trace(err)
	}

	scfg := cfg.SyncerCfg

	cp, err := checkpoint.NewCheckPoint(cpCfg)
	if err != nil {
		return errors.Trace(err)
	}

	defer cp.Close()

	if cp.IsConsistent() {
		return nil
	}

	reader, err := relay.NewReader(scfg.Relay.LogDir, 1 /* readBufferSize */)
	if err != nil {
		return errors.Annotate(err, "failed to create reader")
	}
	var db *sql.DB
	if cfg.SyncerCfg.DestDBType == "oracle" {
		db, err = loader.CreateOracleDB(cfg.SyncerCfg.To.User, cfg.SyncerCfg.To.Password, scfg.To.Host, scfg.To.Port, cfg.SyncerCfg.To.OracleServiceName, cfg.SyncerCfg.To.OracleConnectString)
	} else {
		db, err = loader.CreateDBWithSQLMode(scfg.To.User, scfg.To.Password, scfg.To.Host, scfg.To.Port, scfg.To.TLS, scfg.StrSQLMode, scfg.To.Params, scfg.To.ReadTimeout)
	}
	if err != nil {
		return errors.Annotate(err, "failed to create SQL db")
	}
	defer db.Close()

	ld, err := sync.CreateLoader(db, scfg.To, scfg.WorkerCount, scfg.TxnBatch,
		queryHistogramVec, scfg.StrSQLMode, scfg.DestDBType, nil, /*loopbacksync.LoopBackSync*/
		scfg.EnableDispatch(), scfg.EnableCausality())
	if err != nil {
		return errors.Annotate(err, "failed to create loader")
	}

	err = feedByRelayLog(reader, ld, cp, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// feedByRelayLog will take over the `ld loader.Loader`.
func feedByRelayLog(r relay.Reader, ld loader.Loader, cp checkpoint.CheckPoint, cfg *Config) error {
	checkpointTS := cp.TS()
	lastSuccessTS := checkpointTS
	r.Run()

	loaderQuit := make(chan struct{})
	var loaderErr error
	go func() {
		ld.SetSafeMode(true)
		loaderErr = ld.Run()
		close(loaderQuit)
	}()

	var readerTxnsC <-chan *obinlog.Binlog
	var toPushLoaderTxn *loader.Txn
	var loaderInputC chan<- *loader.Txn
	successTxnC := ld.Successes()

	readerTxnsC = r.Binlogs()
	readerTxnsCClosed := false

	loaderClosed := false

	var tableRouter *router.Table = nil
	upperColName := false
	var routerErr error
	if cfg.SyncerCfg.DestDBType == "oracle" {
		upperColName = true
		tableRouter, _, routerErr = genRouterAndBinlogEvent(cfg.SyncerCfg)
		if routerErr != nil {
			return errors.Annotate(routerErr, "when feed by relay log, gen router and filter failed")
		}
	}

	for {
		// when reader is drained and all txn has been push into loader
		// we close cloader.
		if readerTxnsC == nil && loaderInputC == nil && !loaderClosed {
			ld.Close()
			loaderClosed = true
		}

		// break once we drainer the success items return by loader.
		if loaderClosed && successTxnC == nil {
			break
		}

		select {
		case sbinlog, ok := <-readerTxnsC:
			if !ok {
				log.Info("readerTxnsC closed")
				readerTxnsC = nil
				readerTxnsCClosed = true
				continue
			}
			if sbinlog.CommitTs <= checkpointTS {
				continue
			}
			var txn *loader.Txn
			var err error
			txn, err = loader.SecondaryBinlogToTxn(sbinlog, tableRouter, upperColName)
			if err != nil {
				return errors.Trace(err)
			}

			readerTxnsC = nil
			txn.Metadata = sbinlog.CommitTs
			toPushLoaderTxn = txn
			loaderInputC = ld.Input()
		case loaderInputC <- toPushLoaderTxn:
			loaderInputC = nil
			toPushLoaderTxn = nil
			if !readerTxnsCClosed {
				readerTxnsC = r.Binlogs()
			}
		case success, ok := <-successTxnC:
			if !ok {
				successTxnC = nil
				log.Info("success closed")
				continue
			}
			lastSuccessTS = success.Metadata.(int64)
		case <-loaderQuit:
			if loaderErr != nil {
				return errors.Trace(loaderErr)
			}
		}
	}

	log.Info("finish feed by relay log")

	readerErr := <-r.Error()

	if readerErr != nil {
		return errors.Trace(readerErr)
	}

	err := cp.Save(lastSuccessTS, 0 /* secondaryTS */, true /*consistent*/, 0)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("update status as normal", zap.Int64("ts", lastSuccessTS))

	return nil
}
