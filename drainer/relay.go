package drainer

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/sync"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
	"go.uber.org/zap"
)

func feedByRelayLogIfNeed(cfg *Config) error {
	if !cfg.SyncerCfg.Relay.SwitchOn() {
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

	if cp.Status() == checkpoint.StatusNormal {
		return nil
	}

	reader, err := relay.NewReader(scfg.Relay.LogDir, 1 /* readBufferSize */)
	if err != nil {
		return errors.Annotate(err, "failed to create reader")
	}

	db, ld, err := sync.CreateLoader(scfg.To, scfg.WorkerCount, scfg.TxnBatch, nil, scfg.StrSQLMode, scfg.DestDBType, scfg.To.SyncMode)
	if err != nil {
		return errors.Annotate(err, "faild to create loader")
	}

	defer db.Close()

	err = feedByRelayLog(reader, ld, cp)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// feedByRelayLog will take over the `ld loader.Loader`.
func feedByRelayLog(r relay.Reader, ld loader.Loader, cp checkpoint.CheckPoint) error {
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

	var readerTxns <-chan *obinlog.Binlog
	// var readerInputClosed bool
	var toPushLoaderTxn *loader.Txn
	var loaderInput chan<- *loader.Txn
	successTxnC := ld.Successes()

	readerTxns = r.Txns()

	loaderClosed := false
loop:
	for {
		if readerTxns == nil && loaderInput == nil && !loaderClosed {
			ld.Close()
			loaderClosed = true
		}

		if loaderClosed && successTxnC == nil {
			break
		}

		select {
		case sbinlog, ok := <-readerTxns:
			if !ok {
				log.Info("readerTxns closed")
				readerTxns = nil
				continue
			}
			txn, err := loader.SlaveBinlogToTxn(sbinlog)
			if err != nil {
				return errors.Trace(err)
			}

			if sbinlog.CommitTs <= checkpointTS {
				continue
			}

			txn.Metadata = sbinlog.CommitTs
			toPushLoaderTxn = txn
			loaderInput = ld.Input()
		case loaderInput <- toPushLoaderTxn:
			loaderInput = nil
			toPushLoaderTxn = nil
		case success, ok := <-successTxnC:
			if !ok {
				successTxnC = nil
				log.Info("success closed")
				break loop
			}
			lastSuccessTS = success.Metadata.(int64)
		case <-loaderQuit:
			if loaderErr != nil {
				return errors.Trace(loaderErr)
			}
			loaderQuit = nil
		}
	}

	log.Info("finish feed by relay log")

	readerErr := <-r.Error()
	<-loaderQuit

	if readerErr != nil {
		return errors.Trace(readerErr)
	}

	if loaderErr != nil {
		return errors.Trace(loaderErr)
	}

	err := cp.Save(lastSuccessTS, 0 /* slaveTS */, checkpoint.StatusNormal)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("update status as normal", zap.Int64("ts", lastSuccessTS))

	return nil
}
