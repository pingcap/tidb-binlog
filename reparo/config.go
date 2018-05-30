package reparo

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/version"
	"github.com/pingcap/tidb-binlog/reparo/common"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

const (
	timeFormat = "2006-01-02 15:04:05"
)

// Config is the main configuration for the retore tool.
type Config struct {
	*flag.FlagSet
	Dir           string `toml:"data-dir" json:"data-dir"`
	StartDatetime string `toml:"start-datetime" json:"start-datetime"`
	StopDatetime  string `toml:"stop-datetime" json:"stop-datetime"`
	StartTSO      int64  `toml:"start-tso" json:"start-tso"`
	StopTSO       int64  `toml:"stop-tso" json:"stop-tso"`

	TxnBatch         int  `toml:"txn-batch" json:"txn-batch"`
	WorkerCount      int  `toml:"worker-count" json:"worker-count"`
	DisableCausality bool `toml:"disable-causality" json:"disable-causality"`

	DestType string           `toml:"dest-type" json:"dest-type"`
	DestDB   *common.DBConfig `toml:"dest-db" json:"dest-db"`

	DoTables []common.Table `toml:"replicate-do-table" json:"replicate-do-table"`
	DoDBs    []string       `toml:"replicate-do-db" json:"replicate-do-db"`

	IgnoreTables []common.Table `toml:"replicate-ignore-table" json:"replicate-ignore-table"`
	IgnoreDBs    []string       `toml:"replicate-ignore-db" json:"replicate-ignore-db"`

	LogFile   string `toml:"log-file" json:"log-file"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`
	LogLevel  string `toml:"log-level" json:"log-level"`

	StatusAddr     string `toml:"status-addr" json:"status-addr"`
	JobChannelSize int    `toml:"job-channel-size" json:"job-channel-size"`

	configFile   string
	printVersion bool
}

var defaultConf = &Config{
	TxnBatch:         100,
	WorkerCount:      8,
	DisableCausality: false,
	LogFile:          "reparo.log",
	LogLevel:         "info",
	LogRotate:        "day",
	StatusAddr:       ":10085",
}

// NewConfig creates a Config object.
func NewConfig() *Config {
	c := defaultConf
	c.FlagSet = flag.NewFlagSet("reparo", flag.ContinueOnError)
	fs := c.FlagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of reparo:")
		fs.PrintDefaults()
	}

	fs.StringVar(&c.Dir, "data-dir", "", "drainer data directory path")
	fs.StringVar(&c.StartDatetime, "start-datetime", "", "recovery from start-datetime, empty string means starting from the beginning of the first file")
	fs.StringVar(&c.StopDatetime, "stop-datetime", "", "recovery end in stop-datetime, empty string means never end.")
	fs.Int64Var(&c.StartTSO, "start-tso", 0, "similar to start-datetime but in pd-server tso format")
	fs.Int64Var(&c.StopTSO, "stop-tso", 0, "similar to stop-datetime, but in pd-server tso format")
	fs.StringVar(&c.LogFile, "log-file", "", "log file path")
	fs.StringVar(&c.LogRotate, "log-rotate", "", "log file rotate type, hour/day")
	fs.StringVar(&c.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&c.DestType, "dest-type", "print", "dest type, values can be [print,mysql]")
	fs.IntVar(&c.TxnBatch, "txn-batch", 100, "number of binlog events in a transaction")
	fs.IntVar(&c.WorkerCount, "worker-count", 8, "number of workers to execute sqls")
	fs.BoolVar(&c.DisableCausality, "disable-causality", false, "disbale causality checking")
	fs.StringVar(&c.StatusAddr, "status-addr", ":10085", "status address for prometheus metrics and go pprof debugging")
	fs.IntVar(&c.JobChannelSize, "job-channel-size", 1000, "the channel size(in number,not bytes) of reparo buffer jobs")
	fs.StringVar(&c.configFile, "config", "", "[REQUIRED] path to configuration file")
	fs.BoolVar(&c.printVersion, "V", false, "print reparo version info")
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
		version.PrintVersionInfo()
		os.Exit(0)
	}

	// the mysql configuration should be in the file.
	if c.DestType == "mysql" && c.configFile == "" {
		return errors.Errorf("please specify config file")
	}

	if c.configFile != "" {
		// Load config file if specified
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
	if err := flags.SetFlagsFromEnv("Reparo", c.FlagSet); err != nil {
		return errors.Trace(err)
	}

	if c.StartDatetime != "" {
		startTime, err := time.ParseInLocation(timeFormat, c.StartDatetime, time.Local)
		if err != nil {
			return errors.Trace(err)
		}

		c.StartTSO = int64(oracle.ComposeTS(startTime.Unix()*1000, 0))
		log.Infof("start tso %d", c.StartTSO)
	}
	if c.StopDatetime != "" {
		stopTime, err := time.ParseInLocation(timeFormat, c.StopDatetime, time.Local)
		if err != nil {
			return errors.Trace(err)
		}
		c.StopTSO = int64(oracle.ComposeTS(stopTime.Unix()*1000, 0))
		log.Infof("stop tso %d", c.StopTSO)
	}

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
	if c.Dir == "" {
		return errors.New("data-dir is empty")
	}

	switch c.DestType {
	case "mysql":
		if c.DestDB == nil {
			return errors.New("dest-db config must not be emtpy")
		}
		return nil
	case "print":
		return nil
	default:
		return errors.Errorf("dest type %s is not supported", c.DestType)
	}
}

// InitLogger initalizes reparo's logger.
func InitLogger(c *Config) {
	log.SetLevelByString(c.LogLevel)

	if len(c.LogFile) > 0 {
		log.SetOutputByName(c.LogFile)
		log.SetHighlighting(false)
		if c.LogRotate == "hour" {
			log.SetRotateByHour()
		} else {
			log.SetRotateByDay()
		}
	}
}
