package checkpoint

import (
	"database/sql"
	"fmt"

	"github.com/juju/errors"
	// mysql driver
	_ "github.com/go-sql-driver/mysql"
)

// DBConfig is the DB configuration.
type DBConfig struct {
	Host     string `toml:"host" json:"host"`
	User     string `toml:"user" json:"user"`
	Password string `toml:"password" json:"password"`
	Port     int    `toml:"port" json:"port"`
}

// Config is the savepoint configuration
type Config struct {
	Db     *DBConfig
	Schema string
	Table  string
	Name   string

	ClusterID     uint64
	BinlogFileDir string `toml:"dir" json:"dir"`
}

func openDB(proto string, host string, port int, username string, password string) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", username, password, host, port)
	db, err := sql.Open(proto, dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, nil
}

func checkConfig(cfg *Config) error {
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
		cfg.Db.Port = 3306
	}
	if cfg.Db.User == "" {
		cfg.Db.User = "root"
	}
	if cfg.Name == "" {
		cfg.Name = "checkpoint"
	}
	if cfg.Schema == "" {
		cfg.Schema = "tidb_binlog"
	}
	if cfg.Table == "" {
		cfg.Table = "checkpoint"
	}

	return nil
}

func execSQL(db *sql.DB, sql string) (sql.Result, error) {
	return db.Exec(sql)
}

func querySQL(db *sql.DB, sql string) (*sql.Rows, error) {
	return db.Query(sql)
}

func genCreateSchema(sp *MysqlCheckPoint) string {
	return fmt.Sprintf("create schema if not exists %s", sp.schema)
}

func genCreateTable(sp *MysqlCheckPoint) string {
	return fmt.Sprintf("create table if not exists %s.%s(clusterID bigint unsigned primary key, checkPoint varchar(255))", sp.schema, sp.table)
}

func genInsertSQL(sp *MysqlCheckPoint, str string) string {
	return fmt.Sprintf("insert into %s.%s values(%d, '%s')", sp.schema, sp.table, sp.clusterID, str)
}

func genSelectSQL(sp *MysqlCheckPoint) string {
	return fmt.Sprintf("select checkPoint from %s.%s where clusterID = %d", sp.schema, sp.table, sp.clusterID)
}

func genDropSchema(cp CheckPoint) (sql.Result, error) {
	sp := cp.(*MysqlCheckPoint)
	sql := fmt.Sprintf("drop schema if exists %s", sp.schema)
	return sp.db.Exec(sql)
}
