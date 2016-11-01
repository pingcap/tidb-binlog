package pump

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
	"github.com/pingcap/tidb-binlog/pkg/flags"
)

const (
	defaultEtcdDialTimeout   = 5 * time.Second
	defaultEtcdURLs          = "http://127.0.0.1:2379"
	defaultListenAddr        = "127.0.0.1:8250"
	defaultSocket            = "unix:///tmp/pump.sock"
	defaultHeartbeatInterval = 2
	defaultGC                = 7
	defaultDataDir           = "data.pump"
)

// Config holds the configuration of pump
type Config struct {
	*flag.FlagSet

	NodeID            string `toml:"node-id" json:"node-id"`
	ListenAddr        string `toml:"addr" json:"addr"`
	AdvertiseAddr     string `toml:"advertise-addr" json:"advertise-addr"`
	Socket            string `toml:"socket" json:"socket"`
	EtcdURLs          string `toml:"pd-urls" json:"pd-urls"`
	EtcdDialTimeout   time.Duration
	DataDir           string `toml:"data-dir" json:"data-dir"`
	HeartbeatInterval uint   `toml:"heartbeat-interval" json:"heartbeat-interval"`
	GC                uint   `toml:"gc" json:"gc"`
	Debug             bool
	configFile        string
	printVersion      bool
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
	fs.StringVar(&cfg.DataDir, "data-dir", "", "the path to store binlog data")
	fs.UintVar(&cfg.HeartbeatInterval, "heartbeat-interval", defaultHeartbeatInterval, "number of seconds between heartbeat ticks")
	fs.UintVar(&cfg.GC, "gc", defaultGC, "recycle binlog files older than gc days, zero means never recycle")
	fs.BoolVar(&cfg.Debug, "debug", false, "whether to enable debug-level logging")
	fs.StringVar(&cfg.configFile, "config-file", "", "path to the pump configuration file")
	fs.BoolVar(&cfg.printVersion, "version", false, "print pump version info")

	return cfg
}

// Parse parses all config from command-line flags, environment vars or configuration file
func (cfg *Config) Parse(arguments []string) error {
	// Parse first to get config file
	perr := cfg.FlagSet.Parse(arguments)
	switch perr {
	case nil:
	case flag.ErrHelp:
		fmt.Fprintln(os.Stderr, flagsline)
		os.Exit(1)
	default:
		os.Exit(2)
	}

	if cfg.printVersion {
		fmt.Printf("pump Version: %s\n", Version)
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
	cfg.FlagSet.Parse(arguments)
	if len(cfg.FlagSet.Args()) > 0 {
		return errors.Errorf("'%s' is not a valid flag", cfg.FlagSet.Arg(0))
	}

	// replace with environment vars
	if err := flags.SetFlagsFromEnv("PUMP", cfg.FlagSet); err != nil {
		return errors.Trace(err)
	}

	adjustString(&cfg.ListenAddr, defaultListenAddr)
	adjustString(&cfg.AdvertiseAddr, cfg.ListenAddr)
	cfg.ListenAddr = "http://" + cfg.ListenAddr       // add 'http:' scheme to facilitate parsing
	cfg.AdvertiseAddr = "http://" + cfg.AdvertiseAddr // add 'http:' scheme to facilitate parsing
	adjustDuration(&cfg.EtcdDialTimeout, defaultEtcdDialTimeout)
	adjustString(&cfg.DataDir, defaultDataDir)
	adjustString(&cfg.Socket, defaultSocket)
	adjustUint(&cfg.HeartbeatInterval, defaultHeartbeatInterval)

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

func adjustUint(v *uint, defValue uint) {
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

	return nil
}
