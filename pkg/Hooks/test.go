package main

import (
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

type PluginDemo struct {}
func (pd PluginDemo)UpdateMarkTable(tx *loader.Tx, info *loopbacksync.LoopBackSync) *loader.Tx {
	return nil
}

func NewPlugin() loader.LoopBack {
	return PluginDemo{}
}


