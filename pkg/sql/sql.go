package sql

import (
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	tddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	tmysql "github.com/pingcap/tidb/mysql"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// MaxDMLRetryCount defines maximum number of times of DML retrying.
	MaxDMLRetryCount = 100
	// MaxDDLRetryCount defines maximum number of times of DDL retrying.
	MaxDDLRetryCount = 5
	// RetryWaitTime defines wait time when retrying.
	RetryWaitTime = 3 * time.Second

	// SlowWarnLog defines the duration to log warn log of sql when exec time greater than
	SlowWarnLog = 100 * time.Millisecond

	defaultTiDBTxnMode = "optimistic"
)

// ExecuteSQLs execute sqls in a transaction with retry.
func ExecuteSQLs(db *sql.DB, sqls []string, args [][]interface{}, isDDL bool) error {
	return ExecuteSQLsWithHistogram(db, sqls, args, isDDL, nil)
}

// ExecuteSQLsWithHistogram execute sqls in a transaction with retry.
func ExecuteSQLsWithHistogram(db *sql.DB, sqls []string, args [][]interface{}, isDDL bool, hist *prometheus.HistogramVec) error {
	if len(sqls) == 0 {
		return nil
	}

	retryCount := MaxDMLRetryCount
	if isDDL {
		retryCount = MaxDDLRetryCount
	}

	var err error
	for i := 0; i < retryCount; i++ {
		if i > 0 {
			//log.Warnf("exec sql retry %d - %v", i, sqls)
			time.Sleep(RetryWaitTime)
		}

		err = ExecuteTxnWithHistogram(db, sqls, args, hist)
		if err == nil {
			return nil
		}
	}

	return errors.Trace(err)
}

// ExecuteTxn executes transaction
func ExecuteTxn(db *sql.DB, sqls []string, args [][]interface{}) error {
	return ExecuteTxnWithHistogram(db, sqls, args, nil)
}

// ExecuteTxnWithHistogram executes transaction
func ExecuteTxnWithHistogram(db *sql.DB, sqls []string, args [][]interface{}, hist *prometheus.HistogramVec) error {
	txn, err := db.Begin()
	if err != nil {
		log.Errorf("exec sqls[%v] begin failed %v", sqls, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	for i := range sqls {
		//log.Debugf("[exec][sql]%s[args]%v", sqls[i], args[i])

		startTime := time.Now()

		_, err = txn.Exec(sqls[i], args[i]...)
		if err != nil {
			log.Errorf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], err)
			rerr := txn.Rollback()
			if rerr != nil {
				log.Errorf("[rollback][error]%v", rerr)
			}
			return errors.Trace(err)
		}

		takeDuration := time.Since(startTime)
		if hist != nil {
			hist.WithLabelValues("exec").Observe(takeDuration.Seconds())
		}
		if takeDuration > SlowWarnLog {
			log.Warnf("[exec slow log take %v][sql]%s[args]%v", takeDuration, sqls[i], args[i])
		}
	}

	startTime := time.Now()

	err = txn.Commit()
	if err != nil {
		log.Errorf("exec sqls[%v] commit failed %v", sqls, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	if hist != nil {
		takeDuration := time.Since(startTime)
		hist.WithLabelValues("commit").Observe(takeDuration.Seconds())
	}

	return nil
}

func isUnknownSystemVariableErr(err error) bool {
	code, ok := GetSQLErrCode(err)
	if !ok {
		return strings.Contains(err.Error(), "Unknown system variable")
	}

	return code == tmysql.ErrUnknownSystemVariable
}

func createDBWitSessions(dsn string, params map[string]string) (db *sql.DB, err error) {
	// Try set this sessions if it's supported.
	defaultParams := map[string]string{
		// After https://github.com/pingcap/tidb/pull/17102
		// default is false, must enable for insert value explicit, or can't replicate.
		"allow_auto_random_explicit_insert": "1",
		"tidb_txn_mode":                     defaultTiDBTxnMode,
	}
	var tryDB *sql.DB
	tryDB, err = sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer tryDB.Close()

	support := make(map[string]string)
	for k, v := range defaultParams {
		s := fmt.Sprintf("SET SESSION %s = ?", k)
		_, err := tryDB.Exec(s, v)
		if err != nil {
			if isUnknownSystemVariableErr(err) {
				continue
			}
			return nil, errors.Trace(err)
		}

		support[k] = v
	}
	for k, v := range params {
		s := fmt.Sprintf("SET SESSION %s = ?", k)
		_, err := tryDB.Exec(s, v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		support[k] = v
	}

	for k, v := range support {
		dsn += fmt.Sprintf("&%s=%s", k, url.QueryEscape(v))
	}

	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return
}

// OpenDBWithSQLMode creates an instance of sql.DB.
func OpenDBWithSQLMode(proto string, host string, port int, username string, password string, sqlMode *string, params map[string]string) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4,utf8&interpolateParams=true&multiStatements=true", username, password, host, port)
	if sqlMode != nil {
		// same as "set sql_mode = '<sqlMode>'"
		dbDSN += "&sql_mode='" + url.QueryEscape(*sqlMode) + "'"
	}
	return createDBWitSessions(dbDSN, params)
}

// OpenDB creates an instance of sql.DB.
func OpenDB(proto string, host string, port int, username string, password string) (*sql.DB, error) {
	return OpenDBWithSQLMode(proto, host, port, username, password, nil, nil)
}

// IgnoreDDLError checks the error can be ignored or not.
func IgnoreDDLError(err error) bool {
	errCode, ok := GetSQLErrCode(err)
	if !ok {
		return false
	}
	// we can get error code from:
	// infoschema's error definition: https://github.com/pingcap/tidb/blob/master/infoschema/infoschema.go
	// DDL's error definition: https://github.com/pingcap/tidb/blob/master/ddl/ddl.go
	// tidb/mysql error code definition: https://github.com/pingcap/tidb/blob/master/mysql/errcode.go
	switch errCode {
	case infoschema.ErrDatabaseExists.Code(), infoschema.ErrDatabaseNotExists.Code(), infoschema.ErrDatabaseDropExists.Code(),
		infoschema.ErrTableExists.Code(), infoschema.ErrTableNotExists.Code(), infoschema.ErrTableDropExists.Code(),
		infoschema.ErrColumnExists.Code(), infoschema.ErrColumnNotExists.Code(), infoschema.ErrIndexExists.Code(),
		infoschema.ErrKeyNotExists.Code(), tddl.ErrCantDropFieldOrKey.Code(), tmysql.ErrDupKeyName:
		return true
	default:
		return false
	}
}

// GetSQLErrCode returns error code if err is a mysql error
func GetSQLErrCode(err error) (terror.ErrCode, bool) {
	mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
	if !ok {
		return -1, false
	}

	return terror.ErrCode(mysqlErr.Number), true
}

// GetTidbPosition gets tidb's position.
func GetTidbPosition(db *sql.DB) (int64, error) {
	/*
		example in tidb:
		mysql> SHOW MASTER STATUS;
		+-------------+--------------------+--------------+------------------+-------------------+
		| File        | Position           | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set |
		+-------------+--------------------+--------------+------------------+-------------------+
		| tidb-binlog | 400718757701615617 |              |                  |                   |
		+-------------+--------------------+--------------+------------------+-------------------+
	*/
	rows, err := db.Query("SHOW MASTER STATUS")
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, errors.New("get slave cluster's ts failed")
	}

	fields, err1 := ScanRow(rows)
	if err1 != nil {
		return 0, errors.Trace(err1)
	}

	ts, err1 := strconv.ParseInt(string(fields["Position"]), 10, 64)
	if err1 != nil {
		return 0, errors.Trace(err1)
	}
	return ts, nil
}

// ScanRow scans rows into a map.
func ScanRow(rows *sql.Rows) (map[string][]byte, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	colVals := make([][]byte, len(cols))
	colValsI := make([]interface{}, len(colVals))
	for i := range colValsI {
		colValsI[i] = &colVals[i]
	}

	err = rows.Scan(colValsI...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make(map[string][]byte)
	for i := range colVals {
		result[cols[i]] = colVals[i]
	}

	return result, nil
}

// CHHostAndPort is a CH host:port pair.
type CHHostAndPort struct {
	Host string
	Port int
}

// ParseCHAddr parses an address config string to CHHostAndPort pairs.
func ParseCHAddr(addr string) ([]CHHostAndPort, error) {
	hostParts := strings.Split(addr, ",")
	result := make([]CHHostAndPort, 0, len(hostParts))
	for _, hostStr := range hostParts {
		trimedHostStr := strings.TrimSpace(hostStr)
		host, portStr, err := net.SplitHostPort(trimedHostStr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		port, err := strconv.Atoi(strings.TrimSpace(portStr))
		if err != nil {
			return nil, errors.Annotate(err, "port is not number")
		}
		result = append(result, CHHostAndPort{
			Host: host,
			Port: port,
		})
	}
	return result, nil
}

func composeCHDSN(host string, port int, username string, password string, dbName string, blockSize int) string {
	dbDSN := fmt.Sprintf("tcp://%s:%d?", host, port)
	if len(username) > 0 {
		dbDSN = fmt.Sprintf("%susername=%s&", dbDSN, username)
	}
	if len(password) > 0 {
		dbDSN = fmt.Sprintf("%spassword=%s&", dbDSN, password)
	}
	if len(dbName) > 0 {
		dbDSN = fmt.Sprintf("%sdatabase=%s&", dbDSN, dbName)
	}
	if blockSize > 0 {
		dbDSN = fmt.Sprintf("%sblock_size=%d&", dbDSN, blockSize)
	}
	return dbDSN
}

// OpenCH opens a connection to CH and returns the standard SQL driver's DB interface.
func OpenCH(host string, port int, username string, password string, dbName string, blockSize int) (*sql.DB, error) {
	dbDSN := composeCHDSN(host, port, username, password, dbName, blockSize)
	log.Infof("Connecting to %s", dbDSN)
	db, err := sql.Open("clickhouse", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, nil
}

// QuoteSchema quote like `dbname`.`table` name
func QuoteSchema(schema string, table string) string {
	return fmt.Sprintf("`%s`.`%s`", escapeName(schema), escapeName(table))
}

// QuoteName quote name like `name`
func QuoteName(name string) string {
	return "`" + escapeName(name) + "`"
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}
