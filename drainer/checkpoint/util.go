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
	Host          string `toml:"host" json:"host"`
	User          string `toml:"user" json:"user"`
	Password      string `toml:"password" json:"password"`
	Port          int    `toml:"port" json:"port"`
	Name          string
	Schema        string
	Table         string
	ClusterID     string
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

func execSQL(db *sql.DB, sql string) (sql.Result, error) {
	return db.Exec(sql)
}

func querySQL(db *sql.DB, sql string) (*sql.Rows, error) {
	return db.Query(sql)
}

func genCreateSchema(sp *MysqlSavePoint) string {
	return fmt.Sprintf("create schema if not exists %s", sp.schema)
}

func genCreateTable(sp *MysqlSavePoint) string {
	return fmt.Sprintf("create table if not exists %s.%s(clusterID varchar(255) primary key, savePoint varchar(255))", sp.schema, sp.table)
}

func genInsertSQL(sp *MysqlSavePoint, str string) string {
	return fmt.Sprintf("insert into %s.%s values(%s, '%s')", sp.schema, sp.table, sp.clusterID, str)
}

func genSelectSQL(sp *MysqlSavePoint) string {
	return fmt.Sprintf("select savePoint from %s.%s where clusterID = %s", sp.schema, sp.table, sp.clusterID)
}
