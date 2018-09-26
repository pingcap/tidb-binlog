package util

import (
	"database/sql"
	"fmt"
	"log"
	"net/url"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/diff"
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
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&time_zone=%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name, url.QueryEscape("'+08:00'"))
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
func CheckSyncState(cfg *diff.Config, sourceDB, targetDB *sql.DB) bool {
	d := diff.New(cfg, sourceDB, targetDB)
	ok, err := d.Equal()
	if err != nil {
		log.Fatal(err)
	}

	return ok
}
