package util

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/diff"
)

// DBConfig is the DB configuration.
type DBConfig struct {
	Host string `toml:"host" json:"host"`

	User string `toml:"user" json:"user"`

	Password string `toml:"password" json:"password"`

	Name string `toml:"name" json:"name"`

	Port int `toml:"port" json:"port"`
}

func (c *DBConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("DBConfig(%+v)", *c)
}

// CreateDB create a mysql fd
func CreateDB(cfg DBConfig) (*sql.DB, error) {
	// just set to the same timezone so the timestamp field of mysql will return the same value
	// timestamp field will be display as the time zone of the Local time of drainer when write to kafka, so we set it to local time to pass CI now
	zone, offset := time.Now().Zone()
	zone = fmt.Sprintf("'+%02d:00'", offset/3600)

	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&interpolateParams=true&multiStatements=true&time_zone=%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name, url.QueryEscape(zone))
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, nil
}

// CloseDB close the mysql fd
func CloseDB(db *sql.DB) error {
	return errors.Trace(db.Close())
}

// CheckSyncState check if srouceDB and targetDB has the same table and data
func CheckSyncState(sourceDB, targetDB *sql.DB, schema string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tables, err := dbutil.GetTables(ctx, sourceDB, schema)
	if err != nil {
		log.Print(err)
		return false
	}

	for _, table := range tables {
		sourceTableInstance := &diff.TableInstance{
			Conn:   sourceDB,
			Schema: schema,
			Table:  table,
		}

		targetTableInstance := &diff.TableInstance{
			Conn:   targetDB,
			Schema: schema,
			Table:  table,
		}
		tableDiff := &diff.TableDiff{
			SourceTables: []*diff.TableInstance{sourceTableInstance},
			TargetTable:  targetTableInstance,
			UseChecksum:  true,
		}
		structEqual, dataEqual, err := tableDiff.Equal(context.Background(), func(sql string) error {
			log.Print(sql)
			return nil
		})

		if err != nil {
			log.Print(err)
			return false
		}
		if !structEqual || !dataEqual {
			return false
		}
	}
	return true
}

// CreateSourceDB return source sql.DB for test
func CreateSourceDB() (db *sql.DB, err error) {
	cfg := DBConfig{
		Host:     "127.0.0.1",
		User:     "root",
		Password: "",
		Name:     "test",
		Port:     4000,
	}

	return CreateDB(cfg)
}

// CreateSinkDB return sink sql.DB for test
func CreateSinkDB() (db *sql.DB, err error) {
	cfg := DBConfig{
		Host:     "127.0.0.1",
		User:     "root",
		Password: "",
		Name:     "test",
		Port:     3306,
	}

	return CreateDB(cfg)
}
