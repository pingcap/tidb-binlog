package restore

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/version"
)

type Config struct {
	*flag.FlagSet
	Dir     string `toml:"data-dir" json:"data-dir"`
	StartTS int64  `toml:"start_ts" json:"start_ts"`
	EndTS   int64  `toml:"end_ts" json:"end_ts"`

	configFile   string
	printVersion bool
}

func NewConfig() *Config {
	c := &Config{}
	c.FlagSet = flag.NewFlagSet("drainer", flag.ContinueOnError)
	fs := c.FlagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of drainer:")
		fs.PrintDefaults()
	}
	fs.StringVar(&c.Dir, "data-dir", "", "drainer data directory path (default data.drainer)")
	fs.Int64Var(&c.StartTS, "start-ts", 0, "restore from start-ts")
	fs.Int64Var(&c.EndTS, "end-ts", 0, "restore end in end-ts, 0 means never end.")

	return c
}

func (c *Config) Parse(args []string) error {
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
		fmt.Printf("Git Commit Hash: %s\n", version.GitHash)
		fmt.Printf("Build TS: %s\n", version.BuildTS)
		fmt.Printf("Go Version: %s\n", runtime.Version())
		fmt.Printf("Go OS/Arch: %s%s\n", runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	}

	// Load config file if specified
	if c.configFile != "" {
		if err := c.configFromFile(c.configFile); err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options
	c.FlagSet.Parse(args)
	if len(c.FlagSet.Args()) > 0 {
		return errors.Errorf("'%s' is not a valid flag", c.FlagSet.Arg(0))
	}

	// replace with environment vars
	if err := flags.SetFlagsFromEnv("PUMP", c.FlagSet); err != nil {
		return errors.Trace(err)
	}

	//TODO more

	return errors.Trace(c.validate())
}

func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

func (c *Config) validate() error {
	// TODO
	return nil
}
