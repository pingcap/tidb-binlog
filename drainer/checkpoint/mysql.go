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
	pb "github.com/pingcap/tipb/go-binlog"
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

	CommitTS  int64             `toml:"commitTS" json:"commitTS"`
	Positions map[string]pb.Pos `toml:"positions" json:"positions"`
}

func newMysql(cfg *Config) (CheckPoint, error) {
	if res := checkConfig(cfg); res != nil {
		log.Errorf("Argument cfg is Invaild %v", res)
		return &MysqlCheckPoint{}, errors.Trace(res)
	}

	db, err := openDB("mysql", cfg.Db.Host, cfg.Db.Port, cfg.Db.User, cfg.Db.Password)
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
		Positions:       make(map[string]pb.Pos),
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
	defer sp.Unlock()

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

	if sp.CommitTS == 0 {
		sp.CommitTS = sp.initialCommitTS
	}
	return nil
}

// Save implements checkpoint.Save interface
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

	sql := genReplaceSQL(sp, string(b))
	_, err = execSQL(sp.db, sql)

	return errors.Trace(err)
}

// Check implements CheckPoint.Check interface
func (sp *MysqlCheckPoint) Check() bool {
	sp.RLock()
	defer sp.RUnlock()

	return time.Since(sp.saveTime) >= maxSaveTime
}

// Pos implements CheckPoint.Pos interface
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

// String inplements CheckPoint.String interface
func (sp *MysqlCheckPoint) String() string {
	ts, poss := sp.Pos()
	return fmt.Sprintf("binlog commitTS = %d and positions = %+v", ts, poss)
}
