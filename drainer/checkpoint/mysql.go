package checkpoint
import (
    "fmt"
    "sync"
    "time"
    "database/sql"
    pb "github.com/pingcap/tipb/go-binlog"
    "github.com/juju/errors"
    "github.com/ngaut/log"

    "encoding/json"
    _ "github.com/go-sql-driver/mysql"
)

type mysqlSavePoint struct {
    db             *sql.DB
    classId        string
    schema         string
    table          string
    commitTs       int64
    positions      map[string]pb.Pos
    saveTime       time.Time
    sync.RWMutex
}

func ExecSQL(db *sql.DB, sql string) error {
    stmt, err := db.Prepare(sql)
    if err != nil {
        log.Errorf("prepare sql error")
        return errors.Trace(err)
    }

    _, err = stmt.Exec()
    if err != nil {
        log.Errorf("Exec sql error")
        return errors.Trace(err)
    }

    return nil
}


func (sp *mysqlSavePoint) Save(ts int64, poss map[string]pb.Pos) error{
    sp.RLock()
    defer sp.RUnlock()
    savePoint := make(map[int64]map[string]pb.Pos)
    for nodeID, pos := range poss {
        newPos := pb.Pos{}
        if pos.Offset > 5000 {
            newPos.Suffix = pos.Suffix
            newPos.Offset = pos.Offset - 5000
        }
        sp.positions[nodeID] = newPos
    }
    
    sp.commitTs = ts
    sp.saveTime = time.Now()
    savePoint[ts] = sp.positions
    b, _ := json.Marshal(savePoint)
    str := string(b)
    sql := fmt.Sprintf("insert into %s.%s values(%s, '%s')", sp.schema, sp.table, sp.classId, str)
    return  ExecSQL(sp.db, sql)
}

func (sp *mysqlSavePoint) Check() bool {
    sp.RLock()
    defer sp.RUnlock()

    if time.Since(sp.saveTime) >= maxSaveTime {
        return true
    }

    return false
}

func (sp *mysqlSavePoint) Pos() (int64, map[string]pb.Pos) {

    sp.RLock()
    defer sp.RUnlock()

    poss := make(map[string]pb.Pos)
    for nodeID, pos := range sp.positions {
        poss[nodeID] = pb.Pos{
            Suffix: pos.Suffix,
            Offset: pos.Offset,
        }
    }
    return sp.commitTs, poss
}

func newMysqlSavePoint(cfg *DBConfig) (SaveCheckPoint, error) {
    db, err := openDB("mysql", cfg.Host, cfg.Port, cfg.User, cfg.Password)
    if err != nil {
    log.Errorf("open database error %v", err)
        return &mysqlSavePoint{}, errors.Trace(err)
    }


    sql := fmt.Sprintf("create schema if not exists %s", cfg.Schema)
    rows, ero := db.Query(sql)
    if ero != nil {
        log.Errorf("create schema error")
        return &mysqlSavePoint{}, errors.Trace(ero)

    }
    var ts int64
    poss := make(map[string]pb.Pos)
    if !rows.Next() {
	    sql := fmt.Sprintf("select savePoint from %s.%s where classId = %s", cfg.Schema, cfg.Table, cfg.ClassId)
            rows, err = db.Query(sql)
            if err != nil {
        	    sql = fmt.Sprintf("create table %s.%s(classId varchar(255) primary key, savePoint varchar(255))", cfg.Schema, cfg.Table)
        	    rows, err = db.Query(sql)
        	    if err != nil {
            		log.Errorf("Exec error %v;", err)
                	return &mysqlSavePoint{}, errors.Trace(err)
		        }
	        }
            if rows.Next(){
                var str string
                savePoint := make(map[int64]map[string]pb.Pos)
                err = rows.Scan(&str)
                if err != nil {
                    log.Errorf("Scan error %v;", err)
                    return &mysqlSavePoint{}, errors.Trace(err)
                }
                if err := json.Unmarshal([]byte(str), &savePoint); err != nil{
                    log.Errorf("Json Unmarshal error %v;", err)
                    return &mysqlSavePoint{}, errors.Trace(err)
                }
                for k, v := range savePoint{
                    ts = k
                    poss = v
                }
            }
   }

    return &mysqlSavePoint{
        db: db,
        classId: cfg.ClassId,
        schema: cfg.Schema,
        table:  cfg.Table,
        commitTs: ts,
        positions: poss,
    }, nil
}


func (sp *mysqlSavePoint) Load() error {
    sp.RLock()
    defer sp.RUnlock()
    sql := fmt.Sprintf("select savePoint from %s.%s where classId = %s", sp.schema, sp.table, sp.classId)
    rows, err := sp.db.Query(sql)
    if err != nil {
	log.Errorf("select savePoint error %v", err)
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
    tmp := make(map[int64] map[string]pb.Pos)
    if err = json.Unmarshal([]byte(str), &tmp); err != nil{
	    log.Errorf("json Unmarshal error %v", err)
	    return errors.Trace(err)
    }
    
    for k, v := range tmp {
	sp.commitTs = k
	sp.positions = v
    }
    return nil
}
