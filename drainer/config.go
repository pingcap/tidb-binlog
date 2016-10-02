package drainer

import (
	"flag"
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("drainer", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.configFile, "config", "", "Config file")
	fs.IntVar(&cfg.ServerID, "server-id", 101, "drianer server ID")
	fs.IntVar(&cfg.Batch, "b", 1, "batch commit count")
	fs.IntVar(&cfg.RequestCount, "request-count", 1, "batch count once request")
	fs.StringVar(&cfg.PprofAddr, "pprof-addr", ":10081", "pprof addr")
	fs.StringVar(&cfg.Meta, "meta", "syncer.meta", "syncer meta info")
	fs.Int64Var(&cfg.InitTs, "init-ts", 0, "syncer meta info")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogRotate, "log-rotate", "", "log file rotate type, hour/day")
	fs.StringVar(&cfg.StorePath, "store-path", "", "tikv store path")
	fs.StringVar(&cfg.DBType, "db-type", "mysql", "to db type: Mysql, PostgreSQL")

	return cfg
}

// DBConfig is the DB configuration.
type DBConfig struct {
	Host string `toml:"host" json:"host"`

	User string `toml:"user" json:"user"`

	Password string `toml:"password" json:"password"`

	Port int `toml:"port" json:"port"`
}

func (c *DBConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("DBConfig(%+v)", *c)
}

// BinlogClientConfig is the binlog client configuration.
type BinlogClientConfig struct {
	Host string `toml:"host" json:"host"`

	Port int `toml:"port" json:"port"`
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	LogLevel string `toml:"log-level" json:"log-level"`

	LogFile string `toml:"log-file" json:"log-file"`

	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	PprofAddr string `toml:"pprof-addr" json:"pprof-addr"`

	ServerID int `toml:"server-id" json:"server-id"`

	Batch int `toml:"batch" json:"batch"`

	RequestCount int `toml:"request-count" json:"request-count"`

	InitTs int64 `toml:"init-ts" json:"init-ts"`

	Meta string `toml:"meta" json:"meta"`

	To DBConfig `toml:"to" json:"to"`

	BinlogClient BinlogClientConfig `toml:"client" json:"client"`

	StorePath string `toml:"store-path" json:"store-path"`

	DBType string `toml:"db-type" json:"db-type"`

	configFile string
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	// Load config file if specified.
	if c.configFile != "" {
		err = c.configFromFile(c.configFile)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	if c.StorePath == "" {
		return errors.New("must have store path")
	}

	return nil
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Config(%+v)", *c)
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}
