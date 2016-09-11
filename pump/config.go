package pump

import (
	"flag"
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/types"
	"io/ioutil"
	"os"
	"runtime"
	"time"
)

const (
	defaultEtcdDialTimeout   = 5 * time.Second
	defaultEtcdURLs          = "http://localhost:2379"
	defaultPort              = 8250
	defaultHeartbeatInterval = 1000
	defaultBinlogDir         = "/var/pump/binlog"
)

type Config struct {
	*flag.FlagSet

	Host            string   `json:"host"`
	Port            uint     `json:"port"`
	EtcdEndpoints   []string `json:"etcd"`
	EtcdDialTimeout time.Duration
	BinlogDir       string `json:"binlog-dir"`
	HeartbeatMS     uint   `json:"heartbeat"`
	Debug           bool   `json:"debug"`

	configFile   string
	printVersion bool
}

func NewConfig() *Config {
	cfg := &Config{
		EtcdDialTimeout: defaultEtcdDialTimeout,
	}

	cfg.FlagSet = flag.NewFlagSet("pump", flag.ContinueOnError)
	fs := cfg.FlagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, usageline)
	}

	fs.StringVar(&cfg.Host, "host", "", "This pump's hostname or IP address to advertise to the public")
	fs.UintVar(&cfg.Port, "port", defaultPort, "Port to listen on for gRPC")
	fs.Var(flags.NewURLsValue(defaultEtcdURLs), "etcd", "A comma separated list of etcd endpoints")
	fs.UintVar(&cfg.HeartbeatMS, "heartbeat", defaultHeartbeatInterval, "Number of milliseconds between heartbeat ticks")
	fs.StringVar(&cfg.BinlogDir, "binlog-dir", defaultBinlogDir, "The path of binlog files")
	fs.BoolVar(&cfg.Debug, "debug", false, "Enable debug-level logging")
	fs.StringVar(&cfg.configFile, "config-file", "", "Path to the pump configuration file")
	fs.BoolVar(&cfg.printVersion, "version", false, "Print pump version info")

	return cfg
}

func (cfg *Config) Parse(arguments []string) error {
	perr := cfg.FlagSet.Parse(arguments)
	switch perr {
	case nil:
	case flag.ErrHelp:
		fmt.Fprintln(os.Stderr, flagsline)
		os.Exit(1)
	default:
		os.Exit(2)
	}
	if len(cfg.FlagSet.Args()) > 0 {
		return errors.Errorf("'%s' is not a valid flag", cfg.FlagSet.Arg(0))
	}

	if cfg.printVersion {
		fmt.Printf("pump Version: %s\n", Version)
		fmt.Printf("Git SHA: %s\n", GitSHA)
		fmt.Printf("Build TS: %s\n", BuildTS)
		fmt.Printf("Go Version: %s\n", runtime.Version())
		fmt.Printf("Go OS/Arch: %s%s\n", runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	}

	if cfg.configFile != "" {
		return cfg.configFromFile(cfg.configFile)
	} else {
		return cfg.configFromCmdLine()
	}
}

func (cfg *Config) configFromCmdLine() error {
	if err := flags.SetFlagsFromEnv("PUMP", cfg.FlagSet); err != nil {
		return errors.Trace(err)
	}
	cfg.EtcdEndpoints = flags.URLStrsFromFlag(cfg.FlagSet, "etcd")
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
	return cfg.validate()
}

// validate checks whether the flag values are valid
func (cfg *Config) validate() error {
	if cfg.Host == "" {
		return errors.New("flag 'host' must be set")
	}
	if len(cfg.EtcdEndpoints) == 0 {
		return errors.New("no etcd endpoint given")
	}
	if _, err := types.NewURLs(cfg.EtcdEndpoints); err != nil {
		return errors.Trace(err)
	}
	return nil
}
