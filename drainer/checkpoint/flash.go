package checkpoint

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/flash"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
)

// FlashCheckPoint is a local savepoint struct for flash
type FlashCheckPoint struct {
	sync.RWMutex
	clusterID       uint64
	initialCommitTS int64

	db       *sql.DB
	schema   string
	table    string
	metaCP   *flash.MetaCheckpoint
	saveTime time.Time

	CommitTS int64 `toml:"commitTS" json:"commitTS"`
}

func checkFlashConfig(cfg *Config) error {
	if cfg == nil {
		cfg = new(Config)
	}
	if cfg.Db == nil {
		cfg.Db = new(DBConfig)
	}
	if cfg.Db.Host == "" {
		cfg.Db.Host = "127.0.0.1"
	}
	if cfg.Db.Port == 0 {
		cfg.Db.Port = 9000
	}
	if cfg.Schema == "" {
		cfg.Schema = "tidb_binlog"
	}
	if cfg.Table == "" {
		cfg.Table = "checkpoint"
	}

	return nil
}

func newFlash(cfg *Config) (CheckPoint, error) {
	if err := checkFlashConfig(cfg); err != nil {
		log.Errorf("Checkpoint config is invaild %v", err)
		return nil, errors.Trace(err)
	}

	hostAndPorts, err := pkgsql.ParseCHAddr(cfg.Db.Host)
	if err != nil {
		return nil, errors.Trace(err)
	}

	db, err := pkgsql.OpenCH(hostAndPorts[0].Host, hostAndPorts[0].Port, cfg.Db.User, cfg.Db.Password, "")
	if err != nil {
		log.Errorf("open database error %v", err)
		return nil, errors.Trace(err)
	}

	sp := &FlashCheckPoint{
		db:              db,
		clusterID:       cfg.ClusterID,
		initialCommitTS: cfg.InitialCommitTS,
		schema:          cfg.Schema,
		table:           cfg.Table,
		metaCP:          flash.GetInstance(),
	}

	sql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", sp.schema)
	_, err = execSQL(db, sql)
	if err != nil {
		log.Errorf("Create database error %v", err)
		return sp, errors.Trace(err)
	}

	sql = fmt.Sprintf("ATTACH TABLE IF NOT EXISTS `%s`.`%s`(`clusterid` UInt64, `checkpoint` String) ENGINE MutableMergeTree((`clusterid`), 8192)", sp.schema, sp.table)
	_, err = execSQL(db, sql)
	if err != nil {
		log.Errorf("Create table error %v", err)
		return nil, errors.Trace(err)

	}

	err = sp.Load()
	return sp, errors.Trace(err)
}

// Load implements CheckPoint.Load interface
func (sp *FlashCheckPoint) Load() error {
	sp.Lock()
	defer sp.Unlock()

	sql := fmt.Sprintf("SELECT `checkpoint` from `%s`.`%s` WHERE `clusterid` = %d", sp.schema, sp.table, sp.clusterID)
	rows, err := querySQL(sp.db, sql)
	if err != nil {
		log.Errorf("select checkPoint error %v", err)
		return errors.Trace(err)
	}

	var str string
	for rows.Next() {
		err = rows.Scan(&str)
		if err != nil {
			log.Errorf("rows Scan error %v", err)
			return errors.Trace(err)
		}
	}

	if len(str) == 0 {
		sp.CommitTS = sp.initialCommitTS
		return nil
	}

	err = json.Unmarshal([]byte(str), sp)
	if err != nil {
		return errors.Trace(err)
	}

	if sp.CommitTS == 0 {
		sp.CommitTS = sp.initialCommitTS
	}
	return nil
}

// Save implements checkpoint.Save interface
func (sp *FlashCheckPoint) Save(ts int64) error {
	sp.Lock()
	defer sp.Unlock()

	sp.saveTime = time.Now()

	// Init CP using metaCP's safe CP.
	forceSave, ok, safeTS := sp.metaCP.PopSafeCP()
	if forceSave {
		// If force save, use the CP passed in.
		safeTS = ts
	} else if !ok {
		return nil
	}

	sp.CommitTS = safeTS

	b, err := json.Marshal(sp)
	if err != nil {
		log.Errorf("Json Marshal error %v", err)
		return errors.Trace(err)
	}

	sql := fmt.Sprintf("IMPORT INTO `%s`.`%s` (`clusterid`, `checkpoint`) VALUES(?, ?)", sp.schema, sp.table)
	sqls := []string{sql}
	args := [][]interface{}{{sp.clusterID, b}}
	err = pkgsql.ExecuteSQLs(sp.db, sqls, args, false)

	return errors.Trace(err)
}

// Check implements CheckPoint.Check interface
func (sp *FlashCheckPoint) Check(ts int64) bool {
	sp.RLock()
	defer sp.RUnlock()

	sp.metaCP.PushPendingCP(ts)

	return time.Since(sp.saveTime) >= maxSaveTime
}

// TS implements CheckPoint.TS interface
func (sp *FlashCheckPoint) TS() int64 {
	sp.RLock()
	defer sp.RUnlock()

	return sp.CommitTS
}

// String implements CheckPoint.String interface
func (sp *FlashCheckPoint) String() string {
	ts := sp.TS()
	return fmt.Sprintf("binlog commitTS = %d", ts)
}
