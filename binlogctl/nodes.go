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
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/node"
	"go.uber.org/zap"
)

var (
	etcdDialTimeout          = 5 * time.Second
	createRegistryFuc        = createRegistry
	newEtcdClientFromCfgFunc = etcd.NewClientFromCfg
)

// QueryNodesByKind returns specified nodes, like pumps/drainers
func QueryNodesByKind(urls string, kind string, showOffline bool) error {
	registry, err := createRegistryFuc(urls)
	if err != nil {
		return errors.Trace(err)
	}

	nodes, err := registry.Nodes(context.Background(), node.NodePrefix[kind])
	if err != nil {
		return errors.Trace(err)
	}

	for _, n := range nodes {
		if n.State == node.Offline && !showOffline {
			continue
		}
		log.Info("query node", zap.String("type", kind), zap.Stringer("node", n))
	}

	return nil
}

// UpdateNodeState update pump or drainer's state.
func UpdateNodeState(urls, kind, nodeID, state string) error {
	/*
		node's state can be online, pausing, paused, closing and offline.
		if the state is one of them, will update the node's state saved in etcd directly.
	*/
	registry, err := createRegistryFuc(urls)
	if err != nil {
		return errors.Trace(err)
	}

	n, err := registry.Node(context.Background(), node.NodePrefix[kind], nodeID)
	if err != nil {
		return errors.Trace(err)
	}

	switch state {
	case node.Online, node.Pausing, node.Paused, node.Closing, node.Offline:
		n.State = state
		return registry.UpdateNode(context.Background(), node.NodePrefix[kind], n)
	default:
		return errors.Errorf("state %s is illegal", state)
	}
}

// createRegistry returns an ectd registry
func createRegistry(urls string) (*node.EtcdRegistry, error) {
	ectdEndpoints, err := flags.ParseHostPortAddr(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cli, err := newEtcdClientFromCfgFunc(ectdEndpoints, etcdDialTimeout, node.DefaultRootPath, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return node.NewEtcdRegistry(cli, etcdDialTimeout), nil
}

// ApplyAction applies action on pump or drainer
func ApplyAction(urls, kind, nodeID string, action string) error {
	registry, err := createRegistryFuc(urls)
	if err != nil {
		return errors.Trace(err)
	}

	n, err := registry.Node(context.Background(), node.NodePrefix[kind], nodeID)
	if err != nil {
		return errors.Trace(err)
	}

	var client http.Client
	url := fmt.Sprintf("http://%s/state/%s/%s", n.Addr, n.NodeID, action)
	log.Debug("send put http request", zap.String("url", url))
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = client.Do(req)
	if err == nil {
		log.Info("Apply action on node success", zap.String("action", action), zap.String("NodeID", n.NodeID))
		return nil
	}

	return errors.Trace(err)
}
