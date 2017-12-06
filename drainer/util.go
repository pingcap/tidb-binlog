package drainer

import (
	"database/sql"
	"fmt"
	"hash/crc32"
	"net"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/drainer/executor"
	tddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	tmysql "github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tipb/go-binlog"
)

const (
	lengthOfBinaryTime = 15
	shiftBits          = 18
	subTime            = 20 * 60 * 1000
)

// InitLogger initalizes Pump's logger.
func InitLogger(cfg *Config) {
	log.SetLevelByString(cfg.LogLevel)

	if len(cfg.LogFile) > 0 {
		log.SetOutputByName(cfg.LogFile)

		if cfg.LogRotate == "hour" {
			log.SetRotateByHour()
		} else {
			log.SetRotateByDay()
		}
	}
}

// ComparePos compares the two positions of binlog items, return 0 when the left equal to the right,
// return -1 if the left is ahead of the right, oppositely return 1.
func ComparePos(left, right binlog.Pos) int {
	if left.Suffix < right.Suffix {
		return -1
	} else if left.Suffix > right.Suffix {
		return 1
	} else if left.Offset < right.Offset {
		return -1
	} else if left.Offset > right.Offset {
		return 1
	} else {
		return 0
	}
}

// CalculateNextPos calculates the position of binlog item next to the given one.
func CalculateNextPos(item binlog.Entity) binlog.Pos {
	pos := item.Pos
	// 4 bytes(magic) + 8 bytes(size) + length of payload + 4 bytes(CRC)
	pos.Offset += int64(len(item.Payload) + 16)
	return pos
}

// GenCheckPointCfg returns an CheckPoint config instance
func GenCheckPointCfg(cfg *Config, id uint64) *checkpoint.Config {
	dbCfg := checkpoint.DBConfig{
		Host:     cfg.SyncerCfg.To.Host,
		User:     cfg.SyncerCfg.To.User,
		Password: cfg.SyncerCfg.To.Password,
		Port:     cfg.SyncerCfg.To.Port,
	}
	return &checkpoint.Config{
		Db:            &dbCfg,
		ClusterID:     id,
		BinlogFileDir: path.Join(cfg.DataDir, "savepoint"),
	}
}

func getSafeTS(ts int64) int64 {
	ts = ts >> shiftBits
	ts -= subTime
	if ts < int64(0) {
		ts = int64(0)
	}

	return ts
}

// combine suffix offset in one float, the format would be suffix.offset
func posToFloat(pos *binlog.Pos) float64 {
	var decimal float64
	decimal = float64(pos.Suffix)
	for decimal > 1 {
		decimal = decimal / 10
	}
	return float64(pos.Offset) + decimal
}

func genDrainerID(listenAddr string) (string, error) {
	urllis, err := url.Parse(listenAddr)
	if err != nil {
		return "", errors.Trace(err)
	}

	_, port, err := net.SplitHostPort(urllis.Host)
	if err != nil {
		return "", errors.Trace(err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "", errors.Trace(err)
	}

	return fmt.Sprintf("%s:%s", hostname, port), nil
}

func execute(executor executor.Executor, sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	// compute txn duration
	beginTime := time.Now()
	defer func() {
		txnHistogram.Observe(time.Since(beginTime).Seconds())
	}()

	return executor.Execute(sqls, args, commitTSs, isDDL)
}

func appleTxn(db *sql.DB, sqls []string, args [][]interface{}) error {
	txn, err := db.Begin()
	if err != nil {
		log.Errorf("exec sqls[%v] begin failed %v", sqls, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	for i := range sqls {
		log.Debugf("[exec][sql]%s[args]%v", sqls[i], args[i])

		_, err = txn.Exec(sqls[i], args[i]...)
		if err != nil {
			log.Warnf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], err)
			rerr := txn.Rollback()
			if rerr != nil {
				log.Errorf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], rerr)
			}
			return errors.Trace(err)
		}
	}

	err = txn.Commit()
	if err != nil {
		log.Errorf("exec sqls[%v] commit failed %v", sqls, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	return nil
}

func ignoreDDLError(err error) bool {
	mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
	if !ok {
		return false
	}

	errCode := terror.ErrCode(mysqlErr.Number)
	// we can get error code from:
	// infoschema's error definition: https://github.com/pingcap/tidb/blob/master/infoschema/infoschema.go
	// DDL's error definition: https://github.com/pingcap/tidb/blob/master/ddl/ddl.go
	// tidb/mysql error code definition: https://github.com/pingcap/tidb/blob/master/mysql/errcode.go
	switch errCode {
	case infoschema.ErrDatabaseExists.Code(), infoschema.ErrDatabaseNotExists.Code(), infoschema.ErrDatabaseDropExists.Code(),
		infoschema.ErrTableExists.Code(), infoschema.ErrTableNotExists.Code(), infoschema.ErrTableDropExists.Code(),
		infoschema.ErrColumnExists.Code(), infoschema.ErrColumnNotExists.Code(), infoschema.ErrIndexExists.Code(),
		tddl.ErrCantDropFieldOrKey.Code(), tmysql.ErrDupKeyName:
		return true
	default:
		return false
	}
}

func closeExecutors(executors ...executor.Executor) {
	for _, e := range executors {
		err := e.Close()
		if err != nil {
			log.Errorf("close db failed - %v", err)
		}
	}
}

func createExecutors(destDBType string, cfg *executor.DBConfig, count int) ([]executor.Executor, error) {
	executors := make([]executor.Executor, 0, count)
	for i := 0; i < count; i++ {
		executor, err := executor.New(destDBType, cfg)
		if err != nil {
			return nil, errors.Trace(err)
		}

		executors = append(executors, executor)
	}

	return executors, nil
}

func genHashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func formatIgnoreSchemas(ignoreSchemas string) map[string]struct{} {
	ignoreSchemas = strings.ToLower(ignoreSchemas)
	schemas := strings.Split(ignoreSchemas, ",")

	ignoreSchemaNames := make(map[string]struct{})
	for _, schema := range schemas {
		ignoreSchemaNames[schema] = struct{}{}
	}

	return ignoreSchemaNames
}

func filterIgnoreSchema(schema *model.DBInfo, ignoreSchemaNames map[string]struct{}) bool {
	_, ok := ignoreSchemaNames[schema.Name.L]
	return ok
}
