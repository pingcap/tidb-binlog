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

package reparo

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/filter"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pkg/version"
	"github.com/pingcap/tidb-binlog/reparo/syncer"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	timeFormat = "2006-01-02 15:04:05"
)

// Config is the main configuration for the retore tool.
type Config struct {
	*flag.FlagSet `toml:"-" json:"-"`
	Dir           string `toml:"data-dir" json:"data-dir"`
	StartDatetime string `toml:"start-datetime" json:"start-datetime"`
	StopDatetime  string `toml:"stop-datetime" json:"stop-datetime"`
	StartTSO      int64  `toml:"start-tso" json:"start-tso"`
	StopTSO       int64  `toml:"stop-tso" json:"stop-tso"`
	TxnBatch      int    `toml:"txn-batch" json:"txn-batch"`
	WorkerCount   int    `toml:"worker-count" json:"worker-count"`

	DestType string           `toml:"dest-type" json:"dest-type"`
	DestDB   *syncer.DBConfig `toml:"dest-db" json:"dest-db"`

	DoTables []filter.TableName `toml:"replicate-do-table" json:"replicate-do-table"`
	DoDBs    []string           `toml:"replicate-do-db" json:"replicate-do-db"`

	IgnoreTables []filter.TableName `toml:"replicate-ignore-table" json:"replicate-ignore-table"`
	IgnoreDBs    []string           `toml:"replicate-ignore-db" json:"replicate-ignore-db"`

	LogFile  string `toml:"log-file" json:"log-file"`
	LogLevel string `toml:"log-level" json:"log-level"`

	SafeMode bool `toml:"safe-mode" json:"safe-mode"`

	configFile   string
	printVersion bool
}

// NewConfig creates a Config object.
func NewConfig() *Config {
	c := &Config{}
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
	fs.IntVar(&c.TxnBatch, "txn-batch", 20, "number of binlog events in a transaction batch")
	fs.IntVar(&c.WorkerCount, "c", 16, "parallel worker count")
	fs.StringVar(&c.LogFile, "log-file", "", "log file path")
	fs.StringVar(&c.DestType, "dest-type", "print", "dest type, values can be [print,mysql]")
	fs.StringVar(&c.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&c.configFile, "config", "", "[REQUIRED] path to configuration file")
	fs.BoolVar(&c.printVersion, "V", false, "print reparo version info")
	fs.BoolVar(&c.SafeMode, "safe-mode", false, "enable safe mode to make syncer reentrant")
	return c
}

func (c *Config) String() string {
	// reparo/config.go:94:31: SA1026: trying to marshal chan or func value, field *github.com/pingcap/tidb-binlog/reparo.Config.FlagSet.Usage (staticcheck)
	// but we omit the field `*flag.FlagSet`, it should be ok.
	cfgBytes, err := json.Marshal(c) //nolint:staticcheck
	if err != nil {
		log.Error("marshal config failed", zap.Error(err))
	}

	return string(cfgBytes)
}

// Parse parses keys/values from command line flags and toml configuration file.
func (c *Config) Parse(args []string) (err error) {
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
		fmt.Println(version.GetRawVersionInfo())
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
	if err := c.FlagSet.Parse(args); err != nil {
		return errors.Trace(err)
	}
	if len(c.FlagSet.Args()) > 0 {
		return errors.Errorf("'%s' is not a valid flag", c.FlagSet.Arg(0))
	}
	c.adjustDoDBAndTable()

	// replace with environment vars
	if err := flags.SetFlagsFromEnv("Reparo", c.FlagSet); err != nil {
		return errors.Trace(err)
	}

	if c.StartDatetime != "" {
		c.StartTSO, err = dateTimeToTSO(c.StartDatetime)
		if err != nil {
			return errors.Trace(err)
		}

		log.Info("Parsed start TSO", zap.Int64("ts", c.StartTSO))
	}
	if c.StopDatetime != "" {
		c.StopTSO, err = dateTimeToTSO(c.StopDatetime)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("Parsed stop TSO", zap.Int64("ts", c.StopTSO))
	}

	return errors.Trace(c.validate())
}

func (c *Config) adjustDoDBAndTable() {
	for i := 0; i < len(c.DoTables); i++ {
		c.DoTables[i].Table = strings.ToLower(c.DoTables[i].Table)
		c.DoTables[i].Schema = strings.ToLower(c.DoTables[i].Schema)
	}
	for i := 0; i < len(c.DoDBs); i++ {
		c.DoDBs[i] = strings.ToLower(c.DoDBs[i])
	}
}

func (c *Config) configFromFile(path string) error {
	return util.StrictDecodeFile(path, "reparo", c)
}

func (c *Config) validate() error {
	if c.Dir == "" {
		return errors.New("data-dir is empty")
	}

	switch c.DestType {
	case "mysql":
		if c.DestDB == nil {
			return errors.New("dest-db config must not be empty")
		}
		return nil
	case "print":
		return nil
	case "memory":
		return nil
	default:
		return errors.Errorf("dest type %s is not supported", c.DestType)
	}
}

func dateTimeToTSO(dateTimeStr string) (int64, error) {
	t, err := time.ParseInLocation(timeFormat, dateTimeStr, time.Local)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return int64(oracle.ComposeTS(t.Unix()*1000, 0)), nil
}
