package server

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"runtime"
	"time"

	"github.com/ghodss/yaml"
	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/pkg/flags"
)

const (
	defaultListenAddr          = "127.0.0.1:18200"
	defaultDataDir             = "data.binlog-server"
	defaultCollectInterval     = 5
	defaultDepositWindowPeriod = 10
	defaultEtcdURLs            = "http://127.0.0.1:2379"
	// defaultEtcdTimeout defines default timeout of dialing or sending request to the etcd server.
	defaultEtcdTimeout = 5 * time.Second
)

// Config holds the configuration of binlog-server
type Config struct {
	*flag.FlagSet

	ListenAddr          string        `json:"addr"`
	DataDir             string        `json:"data-dir"`
	CollectInterval     int           `json:"collect-interval"`
	DepositWindowPeriod int           `json:"deposit-window-period"`
	EtcdURLs            string        `json:"pd-urls"`
	EtcdTimeout         time.Duration
	Debug               bool
	configFile          string
	printVersion        bool
}

// NewConfig return an instance of configuration
func NewConfig() *Config {
	cfg := &Config{
		EtcdTimeout: defaultEtcdTimeout,
	}

	cfg.FlagSet = flag.NewFlagSet("binlog-server", flag.ContinueOnError)
	fs := cfg.FlagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, usageline)
	}
	fs.StringVar(&cfg.ListenAddr, "addr", defaultListenAddr, "addr (i.e. 'host:port') to listen on for connection of drainer")
	fs.StringVar(&cfg.DataDir, "data-dir", "", "path to RocksDB data files")
	fs.IntVar(&cfg.CollectInterval, "collect-interval", defaultCollectInterval, "the interval in second of collection loop pulling binlog from pumps")
	fs.IntVar(&cfg.DepositWindowPeriod, "deposit-window-period", defaultDepositWindowPeriod, "after the period of time (in minutes) binlogs stored in RocksDB will become public state")
	fs.StringVar(&cfg.EtcdURLs, "pd-urls", defaultEtcdURLs, "a comma separated list of the endpoints of PD")
	fs.BoolVar(&cfg.Debug, "debug", false, "whether to enable debug-level logging")
	fs.StringVar(&cfg.configFile, "config-file", "", "path of configuration file")
	fs.BoolVar(&cfg.printVersion, "version", false, "print version info")

	return cfg
}

func (cfg *Config) Parse(args []string) error {
	// Parse first to get config file
	perr := cfg.FlagSet.Parse(args)
	switch perr {
	case nil:
	case flag.ErrHelp:
		fmt.Fprintln(os.Stderr, flagsline)
		os.Exit(1)
	default:
		os.Exit(2)
	}

	if cfg.printVersion {
		fmt.Printf("binlog-server Version: %s\n", Version)
		fmt.Printf("Git SHA: %s\n", GitSHA)
		fmt.Printf("Build TS: %s\n", BuildTS)
		fmt.Printf("Go Version: %s\n", runtime.Version())
		fmt.Printf("Go OS/Arch: %s%s\n", runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	}

	// Load config file if specified
	if cfg.configFile != "" {
		if err := cfg.configFromFile(cfg.configFile); err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options
	cfg.FlagSet.Parse(args)
	if len(cfg.FlagSet.Args()) > 0 {
		return errors.Errorf("'%s' is not a valid flag", cfg.FlagSet.Arg(0))
	}

	// replace with environment vars
	if err := flags.SetFlagsFromEnv("PUMP", cfg.FlagSet); err != nil {
		return errors.Trace(err)
	}

	// adjust configuration
	adjustString(&cfg.ListenAddr, defaultListenAddr)
	adjustString(&cfg.DataDir, defaultDataDir)
	adjustInt(&cfg.CollectInterval, defaultCollectInterval)
	adjustInt(&cfg.DepositWindowPeriod, defaultDepositWindowPeriod)
	adjustDuration(&cfg.EtcdTimeout, defaultEtcdTimeout)

	return nil
}

func (cfg *Config) configFromFile(path string) error {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return errors.Trace(err)
	}
	err = yaml.Unmarshal(b, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
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

	return nil
}
