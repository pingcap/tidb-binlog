package restore

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/version"
	"github.com/pingcap/tidb-binlog/restore/executor"
)

// TableName stores the table and schema name
type TableName struct {
	Schema string `toml:"db-name" json:"db-name"`
	Name   string `toml:"tbl-name" json:"tbl-name"`
}

// Savepoint defines a toml config for savepoint.
type Savepoint struct {
	Type string `toml:"type" json:"type"`
	Name string `toml:"name" json:"name"`
}

// Config is the main configuration for the retore tool.
type Config struct {
	*flag.FlagSet
	Dir         string `toml:"data-dir" json:"data-dir"`
	Compression string `toml:"compression" json:"compression"`
	StartTS     int64  `toml:"start-ts" json:"start-ts"`
	EndTS       int64  `toml:"end-ts" json:"end-ts"`

	DestType string             `toml:"dest-type" json:"dest-type"`
	DestDB   *executor.DBConfig `toml:"dest-db" json:"dest-db"`

	Savepoint *Savepoint `toml:"savepoint" json:"savepoint"`

	DoTables []TableName `toml:"replicate-do-table" json:"replicate-do-table"`
	DoDBs    []string    `toml:"replicate-do-db" json:"replicate-do-db"`

	LogFile   string `toml:"log-file" json:"log-file"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`
	LogLevel  string `toml:"log-level" json:"log-level"`

	configFile   string
	printVersion bool
}

// NewConfig creates a Config object.
func NewConfig() *Config {
	c := &Config{}
	c.FlagSet = flag.NewFlagSet("restore", flag.ContinueOnError)
	fs := c.FlagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of restore:")
		fs.PrintDefaults()
	}
	fs.StringVar(&c.Dir, "data-dir", "", "drainer data directory path (default data.drainer)")
	fs.Int64Var(&c.StartTS, "start-ts", 0, "restore from start-ts")
	fs.Int64Var(&c.EndTS, "end-ts", 0, "restore end in end-ts, 0 means never end.")
	fs.StringVar(&c.LogFile, "log-file", "", "log file path")
	fs.StringVar(&c.LogRotate, "log-rotate", "", "log file rotate type, hour/day")
	fs.StringVar(&c.DestType, "dest-type", "print", "dest type, values can be [print,mysql,tidb]")
	fs.StringVar(&c.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&c.configFile, "config", "", "path to configuration file")
	fs.BoolVar(&c.printVersion, "V", false, "print restore version info")
	return c
}

// Parse parses keys/values from command line flags and toml configuration file.
func (c *Config) Parse(args []string) error {
	// Parse first to get config file
	perr := c.FlagSet.Parse(args)
	switch perr {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		os.Exit(2)
	}

	if c.printVersion {
		fmt.Printf("Git Commit Hash: %s\n", version.GitHash)
		fmt.Printf("Build TS: %s\n", version.BuildTS)
		fmt.Printf("Go Version: %s\n", runtime.Version())
		fmt.Printf("Go OS/Arch: %s%s\n", runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	}

	// Load config file if specified
	if c.configFile != "" {
		if err := c.configFromFile(c.configFile); err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options
	c.FlagSet.Parse(args)
	if len(c.FlagSet.Args()) > 0 {
		return errors.Errorf("'%s' is not a valid flag", c.FlagSet.Arg(0))
	}
	c.adjustDoDBAndTable()

	// replace with environment vars
	if err := flags.SetFlagsFromEnv("RESTORE", c.FlagSet); err != nil {
		return errors.Trace(err)
	}

	//TODO more

	return errors.Trace(c.validate())
}

func (c *Config) adjustDoDBAndTable() {
	for i := 0; i < len(c.DoTables); i++ {
		c.DoTables[i].Name = strings.ToLower(c.DoTables[i].Name)
		c.DoTables[i].Schema = strings.ToLower(c.DoTables[i].Schema)
	}
	for i := 0; i < len(c.DoDBs); i++ {
		c.DoDBs[i] = strings.ToLower(c.DoDBs[i])
	}
}

func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

func (c *Config) validate() error {
	if (c.DestType == "mysql" || c.DestType == "tidb") && (c.DestDB == nil) {
		return errors.New("dest-db config must not be emtpy")
	}
	return nil
}

// InitLogger initalizes Pump's logger.
func InitLogger(c *Config) {
	log.SetLevelByString(c.LogLevel)

	if len(c.LogFile) > 0 {
		log.SetOutputByName(c.LogFile)
		if c.LogRotate == "hour" {
			log.SetRotateByHour()
		} else {
			log.SetRotateByDay()
		}
	}
}
