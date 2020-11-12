// Copyright 2020 PingCAP, Inc.
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

package relayprinter

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/filter"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pkg/version"
	"go.uber.org/zap"
)

// Config is the main configuration for the relay printer tool.
type Config struct {
	*flag.FlagSet `toml:"-" json:"-"`
	Dir           string `toml:"data-dir" json:"data-dir"` // directory to relay log files
	File          string `toml:"file" json:"file"`         // specific file to handle, empty for all files

	DoTables []filter.TableName `toml:"do-table" json:"do-table"`
	DoDBs    []string           `toml:"do-db" json:"do-db"`

	IgnoreTables []filter.TableName `toml:"ignore-table" json:"ignore-table"`
	IgnoreDBs    []string           `toml:"ignore-db" json:"ignore-db"`

	configFile   string
	printVersion bool
}

// NewConfig creates a Config instance.
func NewConfig() *Config {
	c := &Config{}
	c.FlagSet = flag.NewFlagSet("relay-printer", flag.ContinueOnError)
	fs := c.FlagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of relay-printer:")
		fs.PrintDefaults()
	}

	fs.StringVar(&c.Dir, "data-dir", "", "directory to relay log files")
	fs.StringVar(&c.File, "file", "", "specific file to handle, empty for all files")
	fs.StringVar(&c.configFile, "config", "", "path to the configuration file")
	fs.BoolVar(&c.printVersion, "V", false, "print relay-printer version info")

	return c
}

func (c *Config) String() string {
	cfgBytes, err := json.Marshal(c)
	if err != nil {
		log.Error("marshal config failed", zap.Error(err))
	}

	return string(cfgBytes)
}

// Parse parses keys/values from command line flags and toml configuration file.
func (c *Config) Parse(args []string) error {
	// Parse first to get the config file
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

	return errors.Trace(c.validate())
}

func (c *Config) configFromFile(path string) error {
	return util.StrictDecodeFile(path, "relay-printer", c)
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

func (c *Config) validate() error {
	if c.Dir == "" {
		return errors.New("data-dir is empty")
	}
	return nil
}
