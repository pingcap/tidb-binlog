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

package binlogctl

import (
	"crypto/tls"
	"flag"
	"fmt"
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/security"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pkg/version"
)

const (
	defaultEtcdURLs = "http://127.0.0.1:2379"
	defaultDataDir  = "binlog_position"
)

const (
	// GenerateMeta is command used for generate meta info for drainer's first run.
	GenerateMeta = "generate_meta"

	// QueryPumps is command used for query all pump's status.
	QueryPumps = "pumps"

	// QueryDrainers is command used for query all drainer's status.
	QueryDrainers = "drainers"

	// UpdatePump is command used for update pump's status.
	UpdatePump = "update-pump"

	// UpdateDrainer is command used for update drainer's status.
	UpdateDrainer = "update-drainer"

	// PausePump is command used for pause pump.
	PausePump = "pause-pump"

	// OfflinePump is command used for offline pump.
	OfflinePump = "offline-pump"

	// PauseDrainer is comamnd used for pause drainer.
	PauseDrainer = "pause-drainer"

	// OfflineDrainer is comamnd used for offlien drainer.
	OfflineDrainer = "offline-drainer"
)

// Config holds the configuration of drainer
type Config struct {
	*flag.FlagSet

	Command      string `toml:"cmd" json:"cmd"`
	NodeID       string `toml:"node-id" json:"node-id"`
	DataDir      string `toml:"data-dir" json:"data-dir"`
	TimeZone     string `toml:"time-zone" json:"time-zone"`
	EtcdURLs     string `toml:"pd-urls" json:"pd-urls"`
	SSLCA        string `toml:"ssl-ca" json:"ssl-ca"`
	SSLCert      string `toml:"ssl-cert" json:"ssl-cert"`
	SSLKey       string `toml:"ssl-key" json:"ssl-key"`
	State        string `toml:"state" json:"state"`
	tls          *tls.Config
	printVersion bool
}

// NewConfig returns an instance of configuration
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("binlogctl", flag.ContinueOnError)

	cfg.FlagSet.StringVar(&cfg.Command, "cmd", "pumps", "operator: \"generate_meta\", \"pumps\", \"drainers\", \"update-pump\", \"update-drainer\", \"pause-pump\", \"pause-drainer\", \"offline-pump\", \"offline-drainer\"")
	cfg.FlagSet.StringVar(&cfg.NodeID, "node-id", "", "id of node, use to update some node with operation update-pump, update-drainer, pause-pump, pause-drainer, offline-pump and offline-drainer")
	cfg.FlagSet.StringVar(&cfg.DataDir, "data-dir", defaultDataDir, "meta directory path")
	cfg.FlagSet.StringVar(&cfg.EtcdURLs, "pd-urls", defaultEtcdURLs, "a comma separated list of PD endpoints")
	cfg.FlagSet.StringVar(&cfg.SSLCA, "ssl-ca", "", "Path of file that contains list of trusted SSL CAs for connection with cluster components.")
	cfg.FlagSet.StringVar(&cfg.SSLCert, "ssl-cert", "", "Path of file that contains X509 certificate in PEM format for connection with cluster components.")
	cfg.FlagSet.StringVar(&cfg.SSLKey, "ssl-key", "", "Path of file that contains X509 key in PEM format for connection with cluster components.")
	cfg.FlagSet.StringVar(&cfg.TimeZone, "time-zone", "", "set time zone if you want save time info in savepoint file, for example `Asia/Shanghai` for CST time, `Local` for local time")
	cfg.FlagSet.StringVar(&cfg.State, "state", "", "set node's state, can set to online, pausing, paused, closing or offline.")
	cfg.FlagSet.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")

	return cfg
}

// Parse parses all config from command-line flags, environment vars or the configuration file
func (cfg *Config) Parse(args []string) error {
	// parse first to get config file
	err := cfg.FlagSet.Parse(args)
	switch err {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		os.Exit(2)
	}

	// parse command line options
	if len(cfg.FlagSet.Args()) > 0 {
		return errors.Errorf("'%s' is not a valid flag", cfg.FlagSet.Arg(0))
	}

	if cfg.printVersion {
		fmt.Println(version.GetRawVersionInfo())
		return flag.ErrHelp
	}

	// adjust configuration
	util.AdjustString(&cfg.DataDir, defaultDataDir)

	// transfore tls config
	sCfg := &security.Config{
		SSLCA:   cfg.SSLCA,
		SSLCert: cfg.SSLCert,
		SSLKey:  cfg.SSLKey,
	}
	cfg.tls, err = sCfg.ToTLSConfig()
	if err != nil {
		return errors.Errorf("tls config error %v", err)
	}

	return cfg.validate()
}

// validate checks whether the configuration is valid
func (cfg *Config) validate() error {
	// check EtcdEndpoints
	_, err := flags.ParseHostPortAddr(cfg.EtcdURLs)
	if err != nil {
		return errors.Errorf("parse EtcdURLs error: %s, %v", cfg.EtcdURLs, err)
	}
	return nil
}
