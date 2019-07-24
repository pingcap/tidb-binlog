// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/security"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pkg/version"
	"github.com/pingcap/tidb-binlog/pump/storage"
)

const (
	defaultEtcdDialTimeout   = 5 * time.Second
	defaultEtcdURLs          = "http://127.0.0.1:2379"
	defaultListenAddr        = "127.0.0.1:8250"
	defautMaxKafkaSize       = 1024 * 1024 * 1024
	defaultHeartbeatInterval = 2
	defaultGC                = 7
	defaultDataDir           = "data.pump"

	// default interval time to generate fake binlog, the unit is second
	defaultGenFakeBinlogInterval = 3
)

// globalConfig is global config of pump to be used in any where
type globalConfig struct {
	// enable online debug log output
	enableDebug bool
	// max binlog message size limit
	maxMsgSize int
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
	EtcdDialTimeout   time.Duration
	DataDir           string `toml:"data-dir" json:"data-dir"`
	HeartbeatInterval int    `toml:"heartbeat-interval" json:"heartbeat-interval"`
	// pump only stores binlog events whose ts >= current time - GC(day)
	GC       int             `toml:"gc" json:"gc"`
	LogFile  string          `toml:"log-file" json:"log-file"`
	Security security.Config `toml:"security" json:"security"`

	GenFakeBinlogInterval int `toml:"gen-binlog-interval" json:"gen-binlog-interval"`

	MetricsAddr     string
	MetricsInterval int
	configFile      string
	printVersion    bool
	tls             *tls.Config
	Storage         storage.Config `toml:"storage" json:"storage"`
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

	fs.StringVar(&cfg.NodeID, "node-id", "", "the ID of pump node; if not specified, we will generate one from hostname and the listening port")
	fs.StringVar(&cfg.ListenAddr, "addr", util.DefaultListenAddr(8250), "addr(i.e. 'host:port') to listen on for client traffic")
	fs.StringVar(&cfg.AdvertiseAddr, "advertise-addr", "", "addr(i.e. 'host:port') to advertise to the public")
	fs.StringVar(&cfg.Socket, "socket", "", "unix socket addr to listen on for client traffic")
	fs.StringVar(&cfg.EtcdURLs, "pd-urls", defaultEtcdURLs, "a comma separated list of the PD endpoints")
	fs.StringVar(&cfg.DataDir, "data-dir", "", "the path to store binlog data")
	fs.IntVar(&cfg.HeartbeatInterval, "heartbeat-interval", defaultHeartbeatInterval, "number of seconds between heartbeat ticks")
	fs.IntVar(&cfg.GC, "gc", defaultGC, "recycle binlog files older than gc days")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", "", "prometheus pushgateway address, leaves it empty will disable prometheus push")
	fs.IntVar(&cfg.MetricsInterval, "metrics-interval", 15, "prometheus client push interval in second, set \"0\" to disable prometheus push")
	fs.StringVar(&cfg.configFile, "config", "", "path to the pump configuration file")
	fs.BoolVar(&cfg.printVersion, "V", false, "print version information and exit")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.IntVar(&cfg.GenFakeBinlogInterval, "fake-binlog-interval", defaultGenFakeBinlogInterval, "interval time to generate fake binlog, the unit is second")

	// global config
	fs.BoolVar(&GlobalConfig.enableDebug, "enable-debug", false, "enable print debug log")
	fs.IntVar(&GlobalConfig.maxMsgSize, "max-message-size", defautMaxKafkaSize, "max msg size producer produce into kafka")
	fs.Int64Var(new(int64), "binlog-file-size", 0, "DEPRECATED")
	fs.BoolVar(new(bool), "enable-binlog-slice", false, "DEPRECATED")
	fs.IntVar(new(int), "binlog-slice-size", 0, "DEPRECATED")
	fs.StringVar(new(string), "log-rotate", "", "DEPRECATED")

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
		fmt.Println(version.GetRawVersionInfo())
		os.Exit(0)
	}

	// Load config file if specified
	if cfg.configFile != "" {
		if err := cfg.configFromFile(cfg.configFile); err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options
	if err := cfg.FlagSet.Parse(arguments); err != nil {
		return errors.Trace(err)
	}
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

	util.AdjustString(&cfg.ListenAddr, defaultListenAddr)
	util.AdjustString(&cfg.AdvertiseAddr, cfg.ListenAddr)
	cfg.ListenAddr = "http://" + cfg.ListenAddr       // add 'http:' scheme to facilitate parsing
	cfg.AdvertiseAddr = "http://" + cfg.AdvertiseAddr // add 'http:' scheme to facilitate parsing
	util.AdjustDuration(&cfg.EtcdDialTimeout, defaultEtcdDialTimeout)
	util.AdjustString(&cfg.DataDir, defaultDataDir)
	util.AdjustInt(&cfg.HeartbeatInterval, defaultHeartbeatInterval)

	return cfg.validate()
}

func (cfg *Config) configFromFile(path string) error {
	return util.StrictDecodeFile(path, "pump", cfg)
}

// validate checks whether the configuration is valid
func (cfg *Config) validate() error {
	// check GC
	if cfg.GC <= 0 {
		return errors.Errorf("GC is %d, must bigger than 0", cfg.GC)
	}

	// check ListenAddr
	urllis, err := url.Parse(cfg.ListenAddr)
	if err != nil {
		return errors.Errorf("parse ListenAddr error: %s, %v", cfg.ListenAddr, err)
	}

	var host string
	if _, _, err = net.SplitHostPort(urllis.Host); err != nil {
		return errors.Errorf("bad ListenAddr host format: %s, %v", urllis.Host, err)
	}

	// check AdvertiseAddr
	urladv, err := url.Parse(cfg.AdvertiseAddr)
	if err != nil {
		return errors.Errorf("parse AdvertiseAddr error: %s, %v", cfg.AdvertiseAddr, err)
	}
	host, _, err = net.SplitHostPort(urladv.Host)
	if err != nil {
		return errors.Errorf("bad AdvertiseAddr host format: %s, %v", urladv.Host, err)
	}
	if len(host) == 0 || host == "0.0.0.0" {
		return errors.Errorf("invalid advertiseAddr host: %v", host)
	}

	// check socketAddr
	if len(cfg.Socket) > 0 {
		urlsock, err := url.Parse(cfg.Socket)
		if err != nil {
			return errors.Errorf("parse Socket error: %s, %v", cfg.Socket, err)
		}
		if len(strings.Split(urlsock.Path, "/")) < 2 {
			return errors.Errorf("bad Socket addr format: %s", urlsock.Path)
		}
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
