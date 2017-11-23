package checkpoint

import (
	"fmt"
	"database/sql"
	"github.com/juju/errors"
	_ "github.com/go-sql-driver/mysql"
)
type DBConfig struct {
    Host          string `toml:"host" json:"host"`
    User          string `toml:"user" json:"user"`
    Password      string `toml:"password" json:"password"`
    Port          int    `toml:"port" json:"port"`
    Name          string
    Schema        string
    Table         string
    ClassId       string
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
