package main

import (
	//"math/rand"
	"os"
	//"os/signal"
	//"runtime"
	//"syscall"
	//"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	//"github.com/pingcap/tidb-binlog/drainer"
)

func main() {
	cfg := NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Infof("verifying flags error, See 'drainer --help'. %s", errors.ErrorStack(err))
	}

	if err := GenSavepointInfo(cfg); err != nil {
		log.Infof("fail to generate savepoint error %v", err)
	}
	os.Exit(0)
}
