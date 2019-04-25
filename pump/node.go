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

package pump

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/node"
	"github.com/pingcap/tidb-binlog/pkg/util"
	pb "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	shortIDLen = 8
	nodeIDFile = ".node"
	lockFile   = ".lock"
)

var nodePrefix = "pumps"

type pumpNode struct {
	sync.RWMutex
	*node.EtcdRegistry
	status            *node.Status
	heartbeatInterval time.Duration

	// latestTS and latestTime is used for get approach ts
	latestTS   int64
	latestTime time.Time

	// use this function to update max commit ts
	getMaxCommitTs func() int64
}

var _ node.Node = &pumpNode{}

// NewPumpNode returns a pumpNode obj that initialized by server config
func NewPumpNode(cfg *Config, getMaxCommitTs func() int64) (node.Node, error) {
	if err := checkExclusive(cfg.DataDir); err != nil {
		return nil, errors.Trace(err)
	}

	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cli, err := etcd.NewClientFromCfg(urlv.StringSlice(), cfg.EtcdDialTimeout, node.DefaultRootPath, cfg.tls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	nodeID, err := readLocalNodeID(cfg.DataDir)
	if err != nil {
		if cfg.NodeID != "" {
			nodeID = cfg.NodeID
		} else if errors.IsNotFound(err) {
			nodeID, err = generateLocalNodeID(cfg.DataDir, cfg.ListenAddr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			return nil, errors.Trace(err)
		}
	} else if cfg.NodeID != "" {
		log.Warning("you had a node ID in local file.[if you want to change the node ID, you should delete the file data-dir/.node file]")
	}

	advURL, err := url.Parse(cfg.AdvertiseAddr)
	if err != nil {
		return nil, errors.Annotatef(err, "invalid configuration of advertise addr(%s)", cfg.AdvertiseAddr)
	}

	status := &node.Status{
		NodeID:  nodeID,
		Addr:    advURL.Host,
		State:   node.Online,
		IsAlive: true,
	}

	node := &pumpNode{
		EtcdRegistry:      node.NewEtcdRegistry(cli, cfg.EtcdDialTimeout),
		status:            status,
		heartbeatInterval: time.Duration(cfg.HeartbeatInterval) * time.Second,
		getMaxCommitTs:    getMaxCommitTs,
	}
	return node, nil
}

func (p *pumpNode) ID() string {
	return p.status.NodeID
}

func (p *pumpNode) Close() error {
	return errors.Trace(p.EtcdRegistry.Close())
}

func (p *pumpNode) ShortID() string {
	if len(p.status.NodeID) <= shortIDLen {
		return p.status.NodeID
	}
	return p.status.NodeID[0:shortIDLen]
}

func (p *pumpNode) RefreshStatus(ctx context.Context, status *node.Status) error {
	p.Lock()
	defer p.Unlock()

	p.status = status
	if p.status.UpdateTS != 0 {
		p.latestTS = p.status.UpdateTS
		p.latestTime = time.Now()
	} else {
		p.updateStatus()
	}

	err := p.UpdateNode(ctx, nodePrefix, status)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (p *pumpNode) Notify(ctx context.Context) error {
	drainers, err := p.Nodes(ctx, "drainers")
	if err != nil {
		return errors.Trace(err)
	}
	dialerOpt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("tcp", addr, timeout)
	})

	for _, c := range drainers {
		if c.State == node.Online {
			log.Info("start try to notify drainer: ", c.Addr)
			clientConn, err := grpc.Dial(c.Addr, dialerOpt, grpc.WithInsecure())
			if err != nil {
				return errors.Errorf("notify drainer(%s); but return error(%v)", c.Addr, err)
			}
			drainer := pb.NewCisternClient(clientConn)
			_, err = drainer.Notify(ctx, nil)
			clientConn.Close()
			if err != nil {
				return errors.Errorf("notify drainer(%s); but return error(%v)", c.Addr, err)
			}
		}
	}

	return nil
}

func (p *pumpNode) NodeStatus() *node.Status {
	return p.status
}

func (p *pumpNode) NodesStatus(ctx context.Context) ([]*node.Status, error) {
	return p.Nodes(ctx, nodePrefix)
}

func (p *pumpNode) Heartbeat(ctx context.Context) <-chan error {
	errc := make(chan error, 1)
	go func() {
		defer func() {
			close(errc)
			log.Info("Heartbeat goroutine exited")
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(p.heartbeatInterval):
				p.Lock()
				p.updateStatus()
				err := p.UpdateNode(ctx, nodePrefix, p.status)
				if err != nil {
					errc <- errors.Trace(err)
				}
				p.Unlock()
			}
		}
	}()
	return errc
}

func (p *pumpNode) updateStatus() {
	p.status.UpdateTS = util.GetApproachTS(p.latestTS, p.latestTime)
	p.status.MaxCommitTS = p.getMaxCommitTs()
}

func (p *pumpNode) Quit() error {
	return errors.Trace(p.Close())
}

// readLocalNodeID reads nodeID from a local file
// returns a NotFound error if the nodeID file not exist
// in this case, the caller should invoke generateLocalNodeID()
func readLocalNodeID(dataDir string) (string, error) {
	nodeIDPath := filepath.Join(dataDir, nodeIDFile)
	if fi, err := os.Stat(nodeIDPath); err != nil {
		if os.IsNotExist(err) {
			return "", errors.NotFoundf("Local nodeID file not exist: %v", err)
		}
		return "", err
	} else if fi.IsDir() {
		return "", errors.Errorf("Local nodeID path is a directory: %s", dataDir)
	}
	data, err := ioutil.ReadFile(nodeIDPath)
	if err != nil {
		return "", errors.Annotate(err, "local nodeID file is collapsed")
	}

	nodeID := FormatNodeID(string(data))

	return nodeID, nil
}

func generateLocalNodeID(dataDir string, listenAddr string) (string, error) {
	if err := os.MkdirAll(dataDir, file.PrivateDirMode); err != nil {
		return "", errors.Trace(err)
	}

	urllis, err := url.Parse(listenAddr)
	if err != nil {
		return "", errors.Trace(err)
	}

	_, port, err := net.SplitHostPort(urllis.Host)
	if err != nil {
		return "", errors.Trace(err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "", errors.Trace(err)
	}

	id := fmt.Sprintf("%s:%s", hostname, port)
	nodeID := FormatNodeID(id)

	nodeIDPath := filepath.Join(dataDir, nodeIDFile)
	if err := ioutil.WriteFile(nodeIDPath, []byte(nodeID), file.PrivateFileMode); err != nil {
		return "", errors.Trace(err)
	}
	return id, nil
}

// checkExclusive tries to get filelock of dataDir in exclusive mode
// if get lock fails, maybe some other pump is running
func checkExclusive(dataDir string) error {
	err := os.MkdirAll(dataDir, file.PrivateDirMode)
	if err != nil {
		return errors.Trace(err)
	}
	lockPath := filepath.Join(dataDir, lockFile)
	// when the process exits, the lockfile will be closed by system
	// and automatically release the lock
	_, err = file.TryLockFile(lockPath, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	return errors.Trace(err)
}

// FormatNodeID formats the nodeID, the nodeID should looks like "host:port"
func FormatNodeID(nodeID string) string {
	newNodeID := strings.TrimSpace(nodeID)

	return newNodeID
}
