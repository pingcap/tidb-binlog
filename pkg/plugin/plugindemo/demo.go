package main

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

//PluginDemo is a demo struct
type PluginDemo struct{}

//ExtendTxn is one of the Hook
func (pd PluginDemo) ExtendTxn(tx *loader.Tx, dmls []*loader.DML, info *loopbacksync.LoopBackSync) (*loader.Tx, []*loader.DML) {
	//do sth
	log.Info("i am ExtendTxn")
	return nil, nil
}

//FilterTxn is one of the Hook
func (pd PluginDemo) FilterTxn(txn *loader.Txn, info *loopbacksync.LoopBackSync) (bool, error) {
	//do sth
	log.Info("i am FilterTxn")
	return true, nil
}

//NewPlugin is the Factory function of plugin
func NewPlugin() interface{} {
	return PluginDemo{}
}

var _ PluginDemo
var _ = NewPlugin()
