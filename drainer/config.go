package drainer

import (
	"flag"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("drainer", flag.ContinueOnError)
	fs := cfg.FlagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of drainer:")
		fs.PrintDefaults()
	}

	fs.StringVar(&cfg.configFile, "config", "", "Config file")
	fs.IntVar(&cfg.TxnBatch, "txn-batch", 1, "number of binlog events in a transaction batch")
	fs.StringVar(&cfg.PprofAddr, "pprof-addr", ":10081", "pprof addr")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", "", "prometheus pushgateway address, leaves it empty will disable prometheus push.")
	fs.IntVar(&cfg.MetricsInterval, "metrics-interval", 30, "prometheus client push interval in second, set \"0\" to disable prometheus push.")
	fs.StringVar(&cfg.DataDir, "data-dir", "data.drainer", "drainer data directory path")
	fs.Int64Var(&cfg.InitCommitTS, "init-commit-ts", 0, "the init point for sync")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogRotate, "log-rotate", "", "log file rotate type, hour/day")
	fs.StringVar(&cfg.DestDBType, "dest-db-type", "mysql", "to db type: Mysql, PostgreSQL")

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

// CisternClientConfig is the cistern client configuration.
type CisternClientConfig struct {
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

	MetricsAddr string `toml:"metrics-addr" json:"metrics-addr"`

	MetricsInterval int `toml:"metrics-interval" json:"metrics-interval"`

	TxnBatch int `toml:"txn-batch" json:"txn-batch"`

	InitCommitTS int64 `toml:"init-commit-ts" json:"init-commit-ts"`

	DataDir string `toml:"data-dir" json:"data-dir"`

	To DBConfig `toml:"to" json:"to"`

	CisternClient CisternClientConfig `toml:"client" json:"client"`

	DestDBType string `toml:"db-type" json:"db-type"`

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
