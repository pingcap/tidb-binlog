package arbiter

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/security"
	"github.com/pingcap/tidb-binlog/pkg/version"
)

const (
	defaultKafkaAddrs   = "127.0.0.1:9092"
	defaultKafkaVersion = "0.8.2.0"
)

// Config is the configuration of Server
type Config struct {
	*flag.FlagSet `json:"-"`
	LogLevel      string          `toml:"log-level" json:"log-level"`
	ListenAddr    string          `toml:"addr" json:"addr"`
	LogFile       string          `toml:"log-file" json:"log-file"`
	LogRotate     string          `toml:"log-rotate" json:"log-rotate"`
	Security      security.Config `toml:"security" json:"security"`

	Up   UpConfig   `toml:"up" json:"up"`
	Down DownConfig `toml:"down" json:"down"`

	MetricsAddr     string
	MetricsInterval int
	configFile      string
	printVersion    bool
	tls             *tls.Config
}

// UpConfig is configuration of upstream
type UpConfig struct {
	KafkaAddrs   string `toml:"kafka-addrs" json:"kafka-addrs"`
	KafkaVersion string `toml:"kafka-version" json:"kafka-version"`

	InitialCommitTS int64  `toml:"initial-commit-ts" json:"initial-commit-ts"`
	Topic           string `toml:"topic" json:"topic"`
}

// DownConfig is configuration of downstream
type DownConfig struct {
	Host     string `toml:"host" json:"host"`
	Port     int    `toml:"port" json:"port"`
	User     string `toml:"User" json:"User"`
	Password string `toml:"password" json:"password"`

	WorkerCount int `toml:"worker-count" json:"worker-count"`
	BatchSize   int `toml:"batch-size" json:"batch-size"`
}

// NewConfig return an instance of configuration
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("arbiter", flag.ContinueOnError)
	fs := cfg.FlagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of arbiter:")
		fs.PrintDefaults()
	}

	fs.StringVar(&cfg.ListenAddr, "addr", "127.0.0.1:8251", "addr (i.e. 'host:port') to listen on for arbiter connections")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.configFile, "config", "", "path to the configuration file")
	fs.BoolVar(&cfg.printVersion, "V", false, "print version info")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", "", "prometheus pushgateway address, leaves it empty will disable prometheus push")
	fs.IntVar(&cfg.MetricsInterval, "metrics-interval", 15, "prometheus client push interval in second, set \"0\" to disable prometheus push")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogRotate, "log-rotate", "", "log file rotate type, hour/day")

	fs.Int64Var(&cfg.Up.InitialCommitTS, "up.initial-commit-ts", 0, "if arbiter donesn't have checkpoint, use initial commitTS to initial checkpoint")
	fs.StringVar(&cfg.Up.Topic, "up.topic", "", "topic name of kafka")

	fs.IntVar(&cfg.Down.WorkerCount, "down.worker-count", 16, "concurrency write to downstream")
	fs.IntVar(&cfg.Down.BatchSize, "down.batch-size", 64, "batch size write to downstream")

	return cfg
}

func (cfg *Config) String() string {
	data, err := json.MarshalIndent(cfg, "\t", "\t")
	if err != nil {
		log.Error(err)
	}

	return string(data)
}

// Parse parses all config from command-line flags, environment vars or the configuration file
func (cfg *Config) Parse(args []string) error {
	// parse first to get config file
	perr := cfg.FlagSet.Parse(args)
	switch perr {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		os.Exit(2)
	}
	if cfg.printVersion {
		version.PrintVersionInfo()
		os.Exit(0)
	}

	// load config file if specified
	if cfg.configFile != "" {
		if err := cfg.configFromFile(cfg.configFile); err != nil {
			return errors.Trace(err)
		}
	}
	// parse again to replace with command line options
	cfg.FlagSet.Parse(args)
	if len(cfg.FlagSet.Args()) > 0 {
		return errors.Errorf("'%s' is not a valid flag", cfg.FlagSet.Arg(0))
	}
	// replace with environment vars
	err := flags.SetFlagsFromEnv("BINLOG_SERVER", cfg.FlagSet)
	if err != nil {
		return errors.Trace(err)
	}

	cfg.tls, err = cfg.Security.ToTLSConfig()
	if err != nil {
		return errors.Errorf("tls config %+v error %v", cfg.Security, err)
	}

	if err = cfg.adjustConfig(); err != nil {
		return errors.Trace(err)
	}

	return cfg.validate()
}

// validate checks whether the configuration is valid
func (cfg *Config) validate() error {
	if len(cfg.Up.Topic) == 0 {
		return errors.Errorf("up.topic not config, please config the topic name")
	}

	return nil
}

func (cfg *Config) adjustConfig() error {
	// cfg.Up
	if len(cfg.Up.KafkaAddrs) == 0 {
		cfg.Up.KafkaAddrs = defaultKafkaAddrs
	}
	if len(cfg.Up.KafkaVersion) == 0 {
		cfg.Up.KafkaVersion = defaultKafkaVersion
	}

	// cfg.Down
	if len(cfg.Down.Host) == 0 {
		cfg.Down.Host = "localhost"
	}
	if cfg.Down.Port == 0 {
		cfg.Down.Port = 3306
	}
	if len(cfg.Down.User) == 0 {
		cfg.Down.User = "root"
	}

	return nil
}

func (cfg *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, cfg)
	return errors.Trace(err)
}
