package sync

import (
	"database/sql"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	router "github.com/pingcap/tidb/util/table-router"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

var _ Syncer = &OracleSyncer{}

// OracleSyncer sync binlog to Oracle
type OracleSyncer struct {
	db      *sql.DB
	loader  loader.Loader
	relayer relay.Relayer
	*baseSyncer
	tableRouter *router.Table
}

// NewOracleSyncer returns a instance of OracleSyncer
func NewOracleSyncer(
	cfg *DBConfig,
	tableInfoGetter translator.TableInfoGetter,
	worker int,
	batchSize int,
	queryHistogramVec *prometheus.HistogramVec,
	sqlMode *string,
	destDBType string,
	relayer relay.Relayer,
	enableDispatch bool,
	enableCausility bool,
	tableRouter *router.Table,
) (*OracleSyncer, error) {
	if cfg.TLS != nil {
		log.Info("enable TLS to connect downstream MySQL/TiDB")
	}

	db, err := loader.CreateOracleDB(cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.OracleServiceName, cfg.OracleConnectString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	loader, err := CreateLoader(db, cfg, worker, batchSize, queryHistogramVec, sqlMode, destDBType, nil, enableDispatch, enableCausility)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s := &OracleSyncer{
		db:          db,
		loader:      loader,
		relayer:     relayer,
		baseSyncer:  newBaseSyncer(tableInfoGetter, nil),
		tableRouter: tableRouter,
	}

	go s.run()

	return s, nil
}

// SetSafeMode make the OracleSyncer to use safe mode or not
func (m *OracleSyncer) SetSafeMode(mode bool) bool {
	m.loader.SetSafeMode(mode)
	return true
}

// Sync implements Syncer interface
func (m *OracleSyncer) Sync(item *Item) error {
	// `relayer` is nil if relay log is disabled.
	if m.relayer != nil {
		pos, err := m.relayer.WriteBinlog(item.Schema, item.Table, item.Binlog, item.PrewriteValue)
		if err != nil {
			return err
		}
		item.RelayLogPos = pos
	}

	txn, err := translator.TiBinlogToOracleTxn(m.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue, item.ShouldSkip, m.tableRouter)
	if err != nil {
		return errors.Trace(err)
	}
	txn.Metadata = item

	select {
	case <-m.errCh:
		return m.err
	case m.loader.Input() <- txn:
		return nil
	}
}

// Close implements Syncer interface
func (m *OracleSyncer) Close() error {
	m.loader.Close()

	err := <-m.Error()

	if m.relayer != nil {
		closeRelayerErr := m.relayer.Close()
		if err == nil {
			err = closeRelayerErr
		}
	}

	return err
}

func (m *OracleSyncer) run() {
	var wg sync.WaitGroup

	// handle success
	wg.Add(1)
	go func() {
		defer wg.Done()

		for txn := range m.loader.Successes() {
			item := txn.Metadata.(*Item)
			item.AppliedTS = txn.AppliedTS
			if m.relayer != nil {
				m.relayer.GCBinlog(item.RelayLogPos)
			}
			m.success <- item
		}
		close(m.success)
		log.Info("Successes chan quit")
	}()

	// run loader
	err := m.loader.Run()

	wg.Wait()
	m.db.Close()
	m.setErr(err)
}
