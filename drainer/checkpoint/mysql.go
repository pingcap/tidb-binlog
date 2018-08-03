package checkpoint

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	// mysql driver
	_ "github.com/go-sql-driver/mysql"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	tmysql "github.com/pingcap/tidb/mysql"
)

// MysqlCheckPoint is a local savepoint struct for mysql
type MysqlCheckPoint struct {
	sync.RWMutex
	clusterID       uint64
	initialCommitTS int64

	db       *sql.DB
	schema   string
	table    string
	saveTime time.Time
	// type, tidb or mysql
	tp string

	CommitTS  int64             `toml:"commitTS" json:"commitTS"`
	TsMap     map[string]int64  `toml:"ts-map" json:"ts-map"`

	snapshot time.Time
}

func newMysql(tp string, cfg *Config) (CheckPoint, error) {
	if res := checkConfig(cfg); res != nil {
		log.Errorf("Argument cfg is Invaild %v", res)
		return &MysqlCheckPoint{}, errors.Trace(res)
	}

	db, err := pkgsql.OpenDB("mysql", cfg.Db.Host, cfg.Db.Port, cfg.Db.User, cfg.Db.Password)
	if err != nil {
		log.Errorf("open database error %v", err)
		return &MysqlCheckPoint{}, errors.Trace(err)
	}

	sp := &MysqlCheckPoint{
		db:              db,
		clusterID:       cfg.ClusterID,
		initialCommitTS: cfg.InitialCommitTS,
		schema:          cfg.Schema,
		table:           cfg.Table,
		tp:              tp,
		TsMap:           make(map[string]int64),
	}

	sql := genCreateSchema(sp)
	_, err = execSQL(db, sql)
	if err != nil {
		log.Errorf("Create schema error %v", err)
		return sp, errors.Trace(err)
	}

	sql = genCreateTable(sp)
	_, err = execSQL(db, sql)
	if err != nil {
		log.Errorf("Create table error %v", err)
		return sp, errors.Trace(err)

	}

	err = sp.Load()
	return sp, errors.Trace(err)
}

// Load implements CheckPoint.Load interface
func (sp *MysqlCheckPoint) Load() error {
	sp.Lock()
	defer func() {
		if sp.CommitTS == 0 {
			sp.CommitTS = sp.initialCommitTS
		}
		sp.Unlock()
	}()

	sql := genSelectSQL(sp)
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

	return nil
}

// Save implements checkpoint.Save interface
func (sp *MysqlCheckPoint) Save(ts int64) error {
	sp.Lock()
	defer sp.Unlock()

	sp.CommitTS = ts
	sp.saveTime = time.Now()

	// we don't need update tsMap every time.
	if sp.tp == "tidb" && time.Since(sp.snapshot) > time.Minute {
		sp.snapshot = time.Now()
		slaveTS, err := pkgsql.GetTidbPosition(sp.db)
		if err != nil {
			// if tidb dont't support `show master status`, will return 1105 ErrUnknown error
			errCode, ok := pkgsql.GetSQLErrCode(err)
			if !ok || int(errCode) != tmysql.ErrUnknown {
				log.Warnf("get ts from slave cluster error %v", err)
			}
		} else {
			sp.TsMap["master-ts"] = ts
			sp.TsMap["slave-ts"] = slaveTS
		}
	}

	b, err := json.Marshal(sp)
	if err != nil {
		log.Errorf("Json Marshal error %v", err)
		return errors.Trace(err)
	}

	sql := genReplaceSQL(sp, string(b))
	_, err = execSQL(sp.db, sql)

	return errors.Trace(err)
}

// Check implements CheckPoint.Check interface
func (sp *MysqlCheckPoint) Check(int64) bool {
	sp.RLock()
	defer sp.RUnlock()

	return time.Since(sp.saveTime) >= maxSaveTime
}

// Pos implements CheckPoint.Pos interface
func (sp *MysqlCheckPoint) Pos() (int64) {
	sp.RLock()
	defer sp.RUnlock()

	return sp.CommitTS
}

// String inplements CheckPoint.String interface
func (sp *MysqlCheckPoint) String() string {
	ts := sp.Pos()
	return fmt.Sprintf("binlog commitTS = %d", ts)
}
