package drainer

import (
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/drainer/executor"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/zk"
)

const (
	defaultListenAddr     = "127.0.0.1:8249"
	defaultDataDir        = "data.drainer"
	defaultDetectInterval = 10
	defaultEtcdURLs       = "http://127.0.0.1:2379"
	defaultKafkaAddrs     = "127.0.0.1:9092"
	// defaultEtcdTimeout defines the timeout of dialing or sending request to etcd.
	defaultEtcdTimeout = 5 * time.Second
	defaultPumpTimeout = 5 * time.Second
)

// SyncerConfig is the Syncer's configuration.
type SyncerConfig struct {
	IgnoreSchemas    string             `toml:"ignore-schemas" json:"ignore-schemas"`
	TxnBatch         int                `toml:"txn-batch" json:"txn-batch"`
	WorkerCount      int                `toml:"worker-count" json:"worker-count"`
	To               *executor.DBConfig `toml:"to" json:"to"`
	DoTables         []TableName        `toml:"replicate-do-table" json:"replicate-do-table"`
	DoDBs            []string           `toml:"replicate-do-db" json:"replicate-do-db"`
	DestDBType       string             `toml:"db-type" json:"db-type"`
	DisableDispatch  bool               `toml:"disable-dispatch" json:"disable-dispatch"`
	SafeMode         bool               `toml:"safe-mode" json:"safe-mode"`
	DisableCausality bool               `toml:"disable-detect" json:"disable-detect"`
}

// Config holds the configuration of drainer
type Config struct {
	*flag.FlagSet
	LogLevel        string        `toml:"log-level" json:"log-level"`
	ListenAddr      string        `toml:"addr" json:"addr"`
	DataDir         string        `toml:"data-dir" json:"data-dir"`
	DetectInterval  int           `toml:"detect-interval" json:"detect-interval"`
	EtcdURLs        string        `toml:"pd-urls" json:"pd-urls"`
	KafkaAddrs      string        `toml:"kafka-addrs" json:"kafka-addrs"`
	ZkAddrs         string        `toml:"zookeeper-addrs" json:"zookeeper-addrs"`
	LogFile         string        `toml:"log-file" json:"log-file"`
	LogRotate       string        `toml:"log-rotate" json:"log-rotate"`
	SyncerCfg       *SyncerConfig `toml:"syncer" json:"sycner"`
	InitialCommitTS int64         `toml:"initial-commit-ts" json:"initial-commit-ts"`
	EtcdTimeout     time.Duration
	PumpTimeout     time.Duration
	MetricsAddr     string
	MetricsInterval int
	configFile      string
	printVersion    bool
}

// NewConfig return an instance of configuration
func NewConfig() *Config {
	cfg := &Config{
		EtcdTimeout: defaultEtcdTimeout,
		PumpTimeout: defaultPumpTimeout,
		SyncerCfg:   new(SyncerConfig),
	}
	cfg.FlagSet = flag.NewFlagSet("drainer", flag.ContinueOnError)
	fs := cfg.FlagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of drainer:")
		fs.PrintDefaults()
	}
	fs.StringVar(&cfg.ListenAddr, "addr", defaultListenAddr, "addr (i.e. 'host:port') to listen on for drainer connections")
	fs.StringVar(&cfg.DataDir, "data-dir", defaultDataDir, "drainer data directory path (default data.drainer)")
	fs.IntVar(&cfg.DetectInterval, "detect-interval", defaultDetectInterval, "the interval time (in seconds) of detect pumps' status")
	fs.StringVar(&cfg.EtcdURLs, "pd-urls", defaultEtcdURLs, "a comma separated list of PD endpoints")
	fs.StringVar(&cfg.KafkaAddrs, "kafka-addrs", defaultKafkaAddrs, "a comma separated list of the kafka broker endpoints")
	fs.StringVar(&cfg.ZkAddrs, "zookeeper-addrs", "", "a comma separated list of the zookeeper endpoints")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.configFile, "config", "", "path to the configuration file")
	fs.BoolVar(&cfg.printVersion, "V", false, "print version info")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", "", "prometheus pushgateway address, leaves it empty will disable prometheus push")
	fs.IntVar(&cfg.MetricsInterval, "metrics-interval", 15, "prometheus client push interval in second, set \"0\" to disable prometheus push")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogRotate, "log-rotate", "", "log file rotate type, hour/day")
	fs.Int64Var(&cfg.InitialCommitTS, "initial-commit-ts", 0, "if drainer donesn't have checkpoint, use initial commitTS to initial checkpoint")
	fs.IntVar(&cfg.SyncerCfg.TxnBatch, "txn-batch", 1, "number of binlog events in a transaction batch")
	fs.StringVar(&cfg.SyncerCfg.IgnoreSchemas, "ignore-schemas", "INFORMATION_SCHEMA,PERFORMANCE_SCHEMA,mysql", "disable sync those schemas")
	fs.IntVar(&cfg.SyncerCfg.WorkerCount, "c", 1, "parallel worker count")
	fs.StringVar(&cfg.SyncerCfg.DestDBType, "dest-db-type", "mysql", "target db type: mysql or pb; see syncer section in conf/drainer.toml")
	fs.BoolVar(&cfg.SyncerCfg.DisableDispatch, "disable-dispatch", false, "disable dispatching sqls that in one same binlog; if set true, work-count and txn-batch would be useless")
	fs.BoolVar(&cfg.SyncerCfg.SafeMode, "safe-mode", false, "enable safe mode to make syncer reentrant")
	fs.BoolVar(&cfg.SyncerCfg.DisableCausality, "disable-detect", false, "disbale detect causality")
	return cfg
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
		fmt.Printf("drainer Version: %s\n", Version)
		fmt.Printf("Git Commit Hash: %s\n", GitHash)
		fmt.Printf("Build TS: %s\n", BuildTS)
		fmt.Printf("Go Version: %s\n", runtime.Version())
		fmt.Printf("Go OS/Arch: %s%s\n", runtime.GOOS, runtime.GOARCH)
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
	if err := flags.SetFlagsFromEnv("BINLOG_SERVER", cfg.FlagSet); err != nil {
		return errors.Trace(err)
	}
	// adjust configuration
	adjustString(&cfg.ListenAddr, defaultListenAddr)
	cfg.ListenAddr = "http://" + cfg.ListenAddr // add 'http:' scheme to facilitate parsing
	adjustString(&cfg.DataDir, defaultDataDir)
	adjustInt(&cfg.DetectInterval, defaultDetectInterval)
	cfg.SyncerCfg.adjustWorkCount()
	cfg.SyncerCfg.adjustDoDBAndTable()

	// adjust initial commit ts
	cfg.InitialCommitTS = getSafeTS(cfg.InitialCommitTS)

	// add default syncer.to configuration if need
	if cfg.SyncerCfg.To == nil {
		cfg.SyncerCfg.To = new(executor.DBConfig)
		if cfg.SyncerCfg.DestDBType == "mysql" {
			cfg.SyncerCfg.To.Host = "localhost"
			cfg.SyncerCfg.To.Port = 3306
			cfg.SyncerCfg.To.User = "root"
			cfg.SyncerCfg.To.Password = ""
			log.Infof("use default downstream mysql config: %s@%s:%d", "root", "localhost", 3306)
		} else {
			cfg.SyncerCfg.To.BinlogFileDir = cfg.DataDir
			log.Infof("use default downstream pb directory: %s", cfg.DataDir)
		}
	}

	return cfg.validate()
}

func (c *SyncerConfig) adjustWorkCount() {
	if c.DisableDispatch {
		c.WorkerCount = 1
	}
}

func (c *SyncerConfig) adjustDoDBAndTable() {
	for i := 0; i < len(c.DoTables); i++ {
		c.DoTables[i].Table = strings.ToLower(c.DoTables[i].Table)
		c.DoTables[i].Schema = strings.ToLower(c.DoTables[i].Schema)
	}
	for i := 0; i < len(c.DoDBs); i++ {
		c.DoDBs[i] = strings.ToLower(c.DoDBs[i])
	}
}

func (cfg *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, cfg)
	return errors.Trace(err)
}

func adjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

func adjustInt(v *int, defValue int) {
	if *v == 0 {
		*v = defValue
	}
}

// validate checks whether the configuration is valid
func (cfg *Config) validate() error {
	// check ListenAddr
	urllis, err := url.Parse(cfg.ListenAddr)
	if err != nil {
		return errors.Errorf("parse ListenAddr error: %s, %v", cfg.ListenAddr, err)
	}
	if _, _, err = net.SplitHostPort(urllis.Host); err != nil {
		return errors.Errorf("bad ListenAddr host format: %s, %v", urllis.Host, err)
	}
	// check EtcdEndpoints
	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return errors.Errorf("parse EtcdURLs error: %s, %v", cfg.EtcdURLs, err)
	}
	for _, u := range urlv.URLSlice() {
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			return errors.Errorf("bad EtcdURL host format: %s, %v", u.Host, err)
		}
	}

	// check zookeeper
	if cfg.ZkAddrs != "" {
		zkClient, err := zk.NewFromConnectionString(cfg.ZkAddrs, time.Second*5, time.Second*60)
		defer zkClient.Close()
		if err != nil {
			return errors.Trace(err)
		}

		kafkaUrls, err := zkClient.KafkaUrls()
		if err != nil {
			return errors.Trace(err)
		}

		// use kafka address get from zookeeper to reset the config
		log.Infof("get kafka addrs from zookeeper: %v", kafkaUrls)
		cfg.KafkaAddrs = kafkaUrls
	}

	return nil
}
