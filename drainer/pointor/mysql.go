package pointor
import (
    "fmt"
    "sync"
    "time"
    "database/sql"
    pb "github.com/pingcap/tipb/go-binlog"
    "github.com/juju/errors"
    "github.com/ngaut/log"
)

type mysqlPoint struct {
    db             *sql.DB
    schema         string
    table          string
    commitTS       int64
    offset         int64
    saveTime       time.Time
    sync.RWMutex
}

func newMysqlPoint(cfg *DBConfig) (Point, error) {
    db, err := openDB("mysql", cfg.Host, cfg.Port, cfg.User, cfg.Password)
    if err != nil {
        return nil, errors.Trace(err)
    }
    db.Query("drop databasae if exists test")
    db.Query("create database test")
    return &mysqlPoint{
        db: db,
        schema: "test",
        table:  "mytest",
    }, nil
}

/*
func (sp *mysqlPoint)InsertData(ts int64, offset int64) error {
    sp.RLock()
    defer sp.RUnlock()

    stmt, err := sp.db.Prepare("insert into ? values(?, ?)")
    if err != nil{
        return errors.Trace(err)
    }

    _, err := stmt.Exec(sp.table, ts, offset)
    if err != nil{
        log.Errorf("save checkPoint to table %s err %v", sp.table, errors.ErrorStack(err))
        return errors.Trace(err)
    }
    return nil
}
*/

func (sp *mysqlPoint) Save(ts int64, poss map[string]pb.Pos) error{
    sp.RLock()
    defer sp.RUnlock()

    for _, pos := range poss {
        // for safe restart, we should forward two binlog files
        // make sure drainer can get binlogs larger than commitTS
        // this is a simple way , if meet problem we would replace by an accurate algorithm
        newPos := pb.Pos{}
        if pos.Offset > 5000 {
            newPos.Offset = pos.Offset - 5000
        }
        sp.offset = newPos.Offset
    }
    
    sp.commitTS = ts
    sp.saveTime = time.Now()
    fmt.Println(ts, sp.offset)
    fmt.Println(sp.db)
    /*
    sql := fmt.Sprintf("create table test.mytest (commitTS int(64), offset int(64);")
    stmt, err := sp.db.Prepare(sql)
    if err != nil {
        return errors.Trace(err)
    }
    stmt.Exec(sp.table)
*/
    _, err := sp.db.Query("create table test.mytest(commitTS int(64), offset int(64))")
    if err != nil {
	    fmt.Println(err)
	    return err
    }
    //stmt, err = sp.db.Prepare("insert into test.? values(?, ?)")
    sql := fmt.Sprintf("insert into test.%s values(%d, %d);", sp.table, sp.commitTS, sp.offset)
    fmt.Println(sql)
    stmt, ero := sp.db.Prepare(sql)
    if ero != nil {
	    fmt.Println(err)
    }
    _, ero = stmt.Exec()
    if ero != nil{
        log.Errorf("save checkPoint to table %s err %v", sp.table, errors.ErrorStack(err))
        return errors.Trace(ero)
    }
    return nil
}

func (sp *mysqlPoint) Check() bool {
    sp.RLock()
    defer sp.RUnlock()

    if time.Since(sp.saveTime) >= maxSaveTime {
        return true
    }

    return false
}

func (sp *mysqlPoint) Pos() (int64, map[string]pb.Pos) {
    poss := make(map[string]pb.Pos)
    poss["test"] = pb.Pos{0, sp.offset}
/*    for nodeID, pos := range lm.Positions {
        poss[nodeID] = pb.Pos{
            Suffix: pos.Suffix,
            Offset: pos.Offset,
        }
    } 
*/
    return sp.commitTS, poss
}


func (sp *mysqlPoint) Load() error {
    return nil
}
