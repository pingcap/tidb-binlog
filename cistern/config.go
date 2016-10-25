package cistern

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
	defaultListenAddr          = "127.0.0.1:8249"
	defaultDataDir             = "data.cistern"
	defaultCollectInterval     = 10
	defaultCollectBatch        = 5000
	defaultDepositWindowPeriod = 10
	defaultEtcdURLs            = "http://127.0.0.1:2379"
	// defaultEtcdTimeout defines the timeout of dialing or sending request to etcd.
	defaultEtcdTimeout = 5 * time.Second
	defaultPumpTimeout = 5 * time.Second
)

// Config holds the configuration of cistern
type Config struct {
	*flag.FlagSet
	ClusterID           uint64 `json:"cluster-id"`
	ListenAddr          string `json:"addr"`
	DataDir             string `json:"data-dir"`
	CollectInterval     int    `json:"collect-interval"`
	CollectBatch        int    `json:"collect-batch"`
	DepositWindowPeriod int    `json:"deposit-window-period"`
	EtcdURLs            string `json:"pd-urls"`
	GC                  int    `json:gc`
	EtcdTimeout         time.Duration
	PumpTimeout         time.Duration
	MetricsAddr         string
	MetricsInterval     int
	Debug               bool
	configFile          string
	printVersion        bool
}

// NewConfig return an instance of configuration
func NewConfig() *Config {
	cfg := &Config{
		EtcdTimeout: defaultEtcdTimeout,
		PumpTimeout: defaultPumpTimeout,
	}
	cfg.FlagSet = flag.NewFlagSet("cistern", flag.ContinueOnError)
	fs := cfg.FlagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of cistern:")
		fs.PrintDefaults()
	}
	fs.Uint64Var(&cfg.ClusterID, "cluster-id", 0, "specifies the ID of TiDB cluster that cistern in charge of")
	fs.StringVar(&cfg.ListenAddr, "addr", defaultListenAddr, "addr (i.e. 'host:port') to listen on for drainer connections")
	fs.StringVar(&cfg.DataDir, "data-dir", defaultDataDir, "path to the data directory of RocksDB")
	fs.IntVar(&cfg.CollectInterval, "collect-interval", defaultCollectInterval, "the interval time (in seconds) of binlog collection loop")
	fs.IntVar(&cfg.CollectBatch, "collect-batch", defaultCollectBatch, "the max number of binlog items in a pulling batch")
	fs.IntVar(&cfg.DepositWindowPeriod, "deposit-window-period", defaultDepositWindowPeriod, "a period of time (in minutes) after that the binlog items stored in RocksDB will become to public state")
	fs.StringVar(&cfg.EtcdURLs, "pd-urls", defaultEtcdURLs, "a comma separated list of PD endpoints")
	fs.BoolVar(&cfg.Debug, "debug", false, "whether to enable debug-level logging")
	fs.StringVar(&cfg.configFile, "config-file", "", "path to the configuration file")
	fs.BoolVar(&cfg.printVersion, "version", false, "print version info")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", "", "prometheus pushgateway address, leaves it empty will disable prometheus push.")
	fs.IntVar(&cfg.MetricsInterval, "metrics-interval", 15, "prometheus client push interval in second, set \"0\" to disable prometheus push.")
	fs.IntVar(&cfg.GC, "gc", 0, "recycle binlog older than gc days.")
	return cfg
}

// Parse parses all config from command-line flags, environment vars or the configuration file
func (cfg *Config) Parse(args []string) error {
	// parse first to get config file
	perr := cfg.FlagSet.Parse(args)
	switch perr {
	case nil:
	case flag.ErrHelp:
		os.Exit(1)
	default:
		os.Exit(2)
	}
	if cfg.printVersion {
		fmt.Printf("cistern Version: %s\n", Version)
		fmt.Printf("Git SHA: %s\n", GitSHA)
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
	adjustInt(&cfg.CollectInterval, defaultCollectInterval)
	adjustInt(&cfg.CollectBatch, defaultCollectBatch)
	adjustInt(&cfg.DepositWindowPeriod, defaultDepositWindowPeriod)
	return cfg.validate()
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
	// clusterId must be configured
	if cfg.ClusterID == 0 {
		return errors.Errorf("cluster-id must be configured")
	}
	return nil
}
