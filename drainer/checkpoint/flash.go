package checkpoint

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	pb "github.com/pingcap/tipb/go-binlog"
)

// TODO: re-engineer flash checkpoint to co-work with executor's batch write.

// FlashCheckPoint is a local savepoint struct for flash
type FlashCheckPoint struct {
	sync.RWMutex
	clusterID       uint64
	initialCommitTS int64

	db       *sql.DB
	schema   string
	table    string
	saveTime time.Time

	CommitTS  int64             `toml:"commitTS" json:"commitTS"`
	Positions map[string]pb.Pos `toml:"positions" json:"positions"`
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

	db, err := pkgsql.OpenCH("clickhouse", hostAndPorts[0].Host, hostAndPorts[0].Port, cfg.Db.User, cfg.Db.Password)
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
		Positions:       make(map[string]pb.Pos),
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
func (sp *FlashCheckPoint) Save(ts int64, poss map[string]pb.Pos) error {
	sp.Lock()
	defer sp.Unlock()

	for nodeID, pos := range poss {
		newPos := pb.Pos{}
		if pos.Offset > 5000 {
			newPos.Suffix = pos.Suffix
			newPos.Offset = pos.Offset - 5000
		}
		sp.Positions[nodeID] = newPos
	}

	sp.CommitTS = ts
	sp.saveTime = time.Now()

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
func (sp *FlashCheckPoint) Check() bool {
	sp.RLock()
	defer sp.RUnlock()

	return time.Since(sp.saveTime) >= maxSaveTime
}

// Pos implements CheckPoint.Pos interface
func (sp *FlashCheckPoint) Pos() (int64, map[string]pb.Pos) {
	sp.RLock()
	defer sp.RUnlock()

	poss := make(map[string]pb.Pos)
	for nodeID, pos := range sp.Positions {
		poss[nodeID] = pb.Pos{
			Suffix: pos.Suffix,
			Offset: pos.Offset,
		}
	}

	return sp.CommitTS, poss
}

// String inplements CheckPoint.String interface
func (sp *FlashCheckPoint) String() string {
	ts, poss := sp.Pos()
	return fmt.Sprintf("binlog commitTS = %d and positions = %+v", ts, poss)
}
