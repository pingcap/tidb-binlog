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

package main

import (
	"flag"
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	ctl "github.com/pingcap/tidb-binlog/binlogctl"
	"github.com/pingcap/tidb-binlog/pkg/node"
	"go.uber.org/zap"
)

const (
	pause = "pause"
	close = "close"
)

func main() {
	cfg := ctl.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch err {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Error("parse cmd flags", zap.Error(err))
		os.Exit(2)
	}

	switch cfg.Command {
	case ctl.GenerateMeta:
		err = ctl.GenerateMetaInfo(cfg)
	case ctl.QueryPumps:
		err = ctl.QueryNodesByKind(cfg.EtcdURLs, node.PumpNode, cfg.ShowOfflineNodes)
	case ctl.QueryDrainers:
		err = ctl.QueryNodesByKind(cfg.EtcdURLs, node.DrainerNode, cfg.ShowOfflineNodes)
	case ctl.UpdatePump:
		err = ctl.UpdateNodeState(cfg.EtcdURLs, node.PumpNode, cfg.NodeID, cfg.State)
	case ctl.UpdateDrainer:
		err = ctl.UpdateNodeState(cfg.EtcdURLs, node.DrainerNode, cfg.NodeID, cfg.State)
	case ctl.PausePump:
		err = ctl.ApplyAction(cfg.EtcdURLs, node.PumpNode, cfg.NodeID, pause)
	case ctl.PauseDrainer:
		err = ctl.ApplyAction(cfg.EtcdURLs, node.DrainerNode, cfg.NodeID, pause)
	case ctl.OfflinePump:
		err = ctl.ApplyAction(cfg.EtcdURLs, node.PumpNode, cfg.NodeID, close)
	case ctl.OfflineDrainer:
		err = ctl.ApplyAction(cfg.EtcdURLs, node.DrainerNode, cfg.NodeID, close)
	default:
		err = errors.NotSupportedf("cmd %s", cfg.Command)
	}

	if err != nil {
		log.Fatal("fail to execute command", zap.String("command", cfg.Command), zap.Error(err))
	}
}
