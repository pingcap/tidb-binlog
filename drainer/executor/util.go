package executor

import (
	// mysql driver
	_ "github.com/go-sql-driver/mysql"
)

// DBConfig is the DB configuration.
type DBConfig struct {
	Host          string `toml:"host" json:"host"`
	User          string `toml:"user" json:"user"`
	Password      string `toml:"password" json:"password"`
	Port          int    `toml:"port" json:"port"`
	BinlogFileDir string `toml:"dir" json:"dir"` // TODO: rename it to a better name
	Compression   string `toml:"compression" json:"compression"`
}
