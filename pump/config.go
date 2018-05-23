package pump

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/security"
	"github.com/pingcap/tidb-binlog/pkg/version"
	"github.com/pingcap/tidb-binlog/pkg/zk"
)

const (
	defaultEtcdDialTimeout   = 5 * time.Second
	defaultEtcdURLs          = "http://127.0.0.1:2379"
	defaultKafkaAddrs        = "127.0.0.1:9092"
	defaultListenAddr        = "127.0.0.1:8250"
	defaultSocket            = "unix:///tmp/pump.sock"
	defautMaxKafkaSize       = 1024 * 1024 * 1024
	defaultHeartbeatInterval = 2
	defaultGC                = 7
	defaultDataDir           = "data.pump"
	defaultKafkaVersion      = "1.0.0"
	defaultBinlogSlice             = 10 * 1024 * 1024
	defaultSegmentSizeBytes  int64 = 512 * 1024 * 1024
	defaultSendKafKaRetryNum int   = 10

	// default interval time to generate fake binlog, the unit is second
	defaultGenFakeBinlogInterval = 3

	kafkaWriteMode = "kafka"
	// mixedWriteMode will write binlog to local file and then send to kafka
	mixedWriteMode = "mixed"
)

// globalConfig is global config of pump to be used in any where
type globalConfig struct {
	// enable online debug log output
	enableDebug bool
	// max binlog message size limit
	maxMsgSize int

	// enable binlogger to split large binlog into small binlog slices
	enableBinlogSlice bool
	// size of one binlog slice
	slicesSize int
	// retry to send to kafka
	sendKafKaRetryNum int

	// segmentSizeBytes is the max threshold of binlog segment file size
	segmentSizeBytes int64
}

// Config holds the configuration of pump
type Config struct {
	*flag.FlagSet
	LogLevel          string `toml:"log-level" json:"log-level"`
	NodeID            string `toml:"node-id" json:"node-id"`
	ListenAddr        string `toml:"addr" json:"addr"`
	AdvertiseAddr     string `toml:"advertise-addr" json:"advertise-addr"`
	Socket            string `toml:"socket" json:"socket"`
	EtcdURLs          string `toml:"pd-urls" json:"pd-urls"`
	KafkaAddrs        string `toml:"kafka-addrs" json:"kafka-addrs"`
	KafkaVersion      string `toml:"kafka-version" json:"kafka-version"`
	ZkAddrs           string `toml:"zookeeper-addrs" json:"zookeeper-addrs"`
	EtcdDialTimeout   time.Duration
	DataDir           string          `toml:"data-dir" json:"data-dir"`
	HeartbeatInterval int             `toml:"heartbeat-interval" json:"heartbeat-interval"`
	GC                int             `toml:"gc" json:"gc"`
	LogFile           string          `toml:"log-file" json:"log-file"`
	LogRotate         string          `toml:"log-rotate" json:"log-rotate"`
	Security          security.Config `toml:"security" json:"security"`
	EnableTolerant    bool            `toml:"enable-tolerant" json:"enable-tolerant"`
	WriteMode         string          `toml:"write-mode" json:"write-mode"`

	GenFakeBinlogInterval int `toml:"gen-binlog-interval" json:"gen-binlog-interval"`

	MetricsAddr     string
	MetricsInterval int
	configFile      string
	printVersion    bool
	tls             *tls.Config
}

// NewConfig return an instance of configuration
func NewConfig() *Config {
	cfg := &Config{
		EtcdDialTimeout: defaultEtcdDialTimeout,
	}

	cfg.FlagSet = flag.NewFlagSet("pump", flag.ContinueOnError)
	fs := cfg.FlagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of pump:")
		fs.PrintDefaults()
	}

	fs.StringVar(&cfg.NodeID, "node-id", "", "the ID of pump node; if not specify, we will generate one from hostname and the listening port")
	fs.StringVar(&cfg.ListenAddr, "addr", defaultListenAddr, "addr(i.e. 'host:port') to listen on for client traffic")
	fs.StringVar(&cfg.AdvertiseAddr, "advertise-addr", "", "addr(i.e. 'host:port') to advertise to the public")
	fs.StringVar(&cfg.Socket, "socket", "", "unix socket addr to listen on for client traffic")
	fs.StringVar(&cfg.EtcdURLs, "pd-urls", defaultEtcdURLs, "a comma separated list of the PD endpoints")
	fs.StringVar(&cfg.KafkaAddrs, "kafka-addrs", defaultKafkaAddrs, "a comma separated list of the kafka broker endpoints")
	fs.StringVar(&cfg.KafkaVersion, "kafka-version", defaultKafkaVersion, "kafka version, looks like \"0.8.2.0\", \"0.8.2.1\", \"0.9.0.0\", \"0.10.2.0\", \"1.0.0\", default is \"0.8.2.0\"")
	fs.StringVar(&cfg.ZkAddrs, "zookeeper-addrs", "", "a comma separated list of the zookeeper broker endpoints")
	fs.StringVar(&cfg.DataDir, "data-dir", "", "the path to store binlog data")
	fs.IntVar(&cfg.HeartbeatInterval, "heartbeat-interval", defaultHeartbeatInterval, "number of seconds between heartbeat ticks")
	fs.IntVar(&cfg.GC, "gc", defaultGC, "recycle binlog files older than gc days, zero means never recycle")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", "", "prometheus pushgateway address, leaves it empty will disable prometheus push")
	fs.IntVar(&cfg.MetricsInterval, "metrics-interval", 15, "prometheus client push interval in second, set \"0\" to disable prometheus push")
	fs.StringVar(&cfg.configFile, "config", "", "path to the pump configuration file")
	fs.BoolVar(&cfg.printVersion, "V", false, "print pump version info")
	fs.BoolVar(&cfg.EnableTolerant, "enable-tolerant", true, "after enable tolerant, pump wouldn't return error if it fails to write binlog")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogRotate, "log-rotate", "", "log file rotate type, hour/day")
	fs.StringVar(&cfg.WriteMode, "write-mode", mixedWriteMode, "support kafka and mixed mode")
	fs.IntVar(&cfg.GenFakeBinlogInterval, "fake-binlog-interval", defaultGenFakeBinlogInterval, "interval time to generate fake binlog, the unit is second")

	// global config
	fs.BoolVar(&GlobalConfig.enableDebug, "enable-debug", false, "enable print debug log")
	fs.IntVar(&GlobalConfig.maxMsgSize, "max-message-size", defautMaxKafkaSize, "max msg size producer produce into kafka")
	fs.Int64Var(&GlobalConfig.segmentSizeBytes, "binlog-file-size", defaultSegmentSizeBytes, "binlog-file-seize is the max threshold of binlog segment file size")
	fs.BoolVar(&GlobalConfig.enableBinlogSlice, "enable-binlog-slice", false, "enable pump to split large binlog into small binlog slices")
	fs.IntVar(&GlobalConfig.slicesSize, "binlog-slice-size", defaultBinlogSlice, "size of one binlog slice")

	return cfg
}

// Parse parses all config from command-line flags, environment vars or configuration file
func (cfg *Config) Parse(arguments []string) error {
	// Parse first to get config file
	perr := cfg.FlagSet.Parse(arguments)
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

	// Load config file if specified
	if cfg.configFile != "" {
		if err := cfg.configFromFile(cfg.configFile); err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options
	cfg.FlagSet.Parse(arguments)
	if len(cfg.FlagSet.Args()) > 0 {
		return errors.Errorf("'%s' is not a valid flag", cfg.FlagSet.Arg(0))
	}

	// replace with environment vars
	err := flags.SetFlagsFromEnv("PUMP", cfg.FlagSet)
	if err != nil {
		return errors.Trace(err)
	}

	cfg.tls, err = cfg.Security.ToTLSConfig()
	if err != nil {
		return errors.Errorf("tls config %+v error %v", cfg.Security, err)
	}

	adjustString(&cfg.ListenAddr, defaultListenAddr)
	adjustString(&cfg.AdvertiseAddr, cfg.ListenAddr)
	cfg.ListenAddr = "http://" + cfg.ListenAddr       // add 'http:' scheme to facilitate parsing
	cfg.AdvertiseAddr = "http://" + cfg.AdvertiseAddr // add 'http:' scheme to facilitate parsing
	adjustDuration(&cfg.EtcdDialTimeout, defaultEtcdDialTimeout)
	adjustString(&cfg.DataDir, defaultDataDir)
	adjustString(&cfg.Socket, defaultSocket)
	adjustInt(&cfg.HeartbeatInterval, defaultHeartbeatInterval)
	initializeSaramaGlobalConfig()

	return cfg.validate()
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

func adjustDuration(v *time.Duration, defValue time.Duration) {
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
	if _, _, err := net.SplitHostPort(urllis.Host); err != nil {
		return errors.Errorf("bad ListenAddr host format: %s, %v", urllis.Host, err)
	}

	// check AdvertiseAddr
	urladv, err := url.Parse(cfg.AdvertiseAddr)
	if err != nil {
		return errors.Errorf("parse AdvertiseAddr error: %s, %v", cfg.AdvertiseAddr, err)
	}
	host, _, err := net.SplitHostPort(urladv.Host)
	if err != nil {
		return errors.Errorf("bad AdvertiseAddr host format: %s, %v", urladv.Host, err)
	}
	if host == "0.0.0.0" {
		return errors.New("advertiseAddr host is not allowed to be set to 0.0.0.0")
	}

	// check socketAddr
	urlsock, err := url.Parse(cfg.Socket)
	if err != nil {
		return errors.Errorf("parse Socket error: %s, %v", cfg.Socket, err)
	}
	if len(strings.Split(urlsock.Path, "/")) < 2 {
		return errors.Errorf("bad Socket addr format: %s", urlsock.Path)
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
			log.Errorf("connect to zookeeper %s error %v", cfg.ZkAddrs, err)
			return errors.Trace(err)
		}

		kafkaUrls, err := zkClient.KafkaUrls()
		if err != nil {
			log.Errorf("get kafka urls from zookeeper error %v", err)
			return errors.Trace(err)
		}

		// use kafka address get from zookeeper to reset the config
		log.Infof("get kafka addrs %v from zookeeper", kafkaUrls)
		cfg.KafkaAddrs = kafkaUrls
	}

	switch cfg.WriteMode {
	case kafkaWriteMode, mixedWriteMode:
		// do nothing
	default:
		return errors.Errorf("unknow binlog mode %s", cfg.WriteMode)
	}

	return nil
}
