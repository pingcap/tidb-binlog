package main

import (
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

type PluginDemo struct{}

func (pd PluginDemo) ExtendTxn(tx *loader.Tx, info *loopbacksync.LoopBackSync) *loader.Tx {
	//do sth
	return nil
}

func (pd PluginDemo) FilterTxn(txn *loader.Txn, info *loopbacksync.LoopBackSync) bool {
	//do sth
	return true
}

func NewPlugin() loader.LoopBack {
	return PluginDemo{}
}
