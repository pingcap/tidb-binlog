package checkpoint

import (
	"database/sql"
	"encoding/json"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	// mysql driver
	_ "github.com/go-sql-driver/mysql"
	pb "github.com/pingcap/tipb/go-binlog"
)

// MysqlCheckPoint is a local savepoint struct for mysql
type MysqlCheckPoint struct {
	sync.RWMutex
	clusterID string

	db       *sql.DB
	schema   string
	table    string
	saveTime time.Time

	CommitTS  int64             `toml:"commitTS" json:"commitTS"`
	Positions map[string]pb.Pos `toml:"positions" json:"positions"`
}

func newMysql(cfg *Config) (CheckPoint, error) {
	db, err := openDB("mysql", cfg.db.Host, cfg.db.Port, cfg.db.User, cfg.db.Password)
	if err != nil {
		log.Errorf("open database error %v", err)
		return &MysqlCheckPoint{}, errors.Trace(err)
	}

	sp := &MysqlCheckPoint{
		db:        db,
		clusterID: cfg.ClusterID,
		schema:    cfg.Schema,
		table:     cfg.Table,
		CommitTS:  0,
		Positions: make(map[string]pb.Pos),
	}

	sql := genCreateSchema(sp)
	_, err = execSQL(db, sql)
	if err != nil {
		log.Errorf("Create schema error")
		return sp, errors.Trace(err)

	}

	sql = genCreateTable(sp)
	_, err = execSQL(db, sql)
	if err != nil {
		log.Errorf("Create table error %v", err)
		return sp, errors.Trace(err)

	}

	err = sp.Load()
	return sp, err
}

// Save saves checkpoint into mysql
func (sp *MysqlCheckPoint) Save(ts int64, poss map[string]pb.Pos) error {
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

	sql := genInsertSQL(sp, string(b))
	_, err = execSQL(sp.db, sql)

	return err
}

// Check we should save checkpoint
func (sp *MysqlCheckPoint) Check() bool {
	sp.RLock()
	defer sp.RUnlock()

	return time.Since(sp.saveTime) >= maxSaveTime
}

// Pos return Meta information
func (sp *MysqlCheckPoint) Pos() (int64, map[string]pb.Pos) {
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

// Load implements checkpoint.Load interface
func (sp *MysqlCheckPoint) Load() error {
	sp.Lock()
	defer sp.Unlock()

	sql := genSelectCountSQL(sp)
	rows, err := querySQL(sp.db, sql)
	if !rows.Next() {
		return nil
	}

	sql = genSelectSQL(sp)
	rows, err = querySQL(sp.db, sql)
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
		return nil
	}

	return json.Unmarshal([]byte(str), sp)
}
