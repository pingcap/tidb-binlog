package pump

import (
	"encoding/json"
	"path"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"golang.org/x/net/context"
)

const nodePrefix = "nodes"

// EtcdRegistry wraps the reaction with etcd
type EtcdRegistry struct {
	client     *etcd.Client
	reqTimeout time.Duration
}

// NewEtcdRegistry returns an EtcdRegistry client
func NewEtcdRegistry(client *etcd.Client, reqTimeout time.Duration) *EtcdRegistry {
	return &EtcdRegistry{
		client:     client,
		reqTimeout: reqTimeout,
	}
}

func (r *EtcdRegistry) prefixed(p ...string) string {
	return path.Join(p...)
}

// Node returns the nodeStatus that matchs nodeID in the etcd
func (r *EtcdRegistry) Node(pctx context.Context, nodeID string) (*NodeStatus, error) {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	resp, err := r.client.List(ctx, r.prefixed(nodePrefix, nodeID))
	if err != nil {
		return nil, errors.Trace(err)
	}
	status, err := nodeStatusFromEtcdNode(nodeID, resp)
	if err != nil {
		return nil, errors.Errorf("Invalid node, nodeID[%s], error[%v]", nodeID, err)
	}
	return status, nil
}

// RegisterNode register the node in the etcd
func (r *EtcdRegistry) RegisterNode(pctx context.Context, nodeID, host string) error {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	if exists, err := r.checkNodeExists(ctx, nodeID); err != nil {
		return errors.Trace(err)
	} else if !exists {
		// not found then create a new  node
		return r.createNode(ctx, nodeID, host)
	} else {
		// found it, update host infomation of the node
		return r.updateNode(ctx, nodeID, host)
	}

}

func (r *EtcdRegistry) checkNodeExists(ctx context.Context, nodeID string) (bool, error) {
	_, err := r.client.Get(ctx, r.prefixed(nodePrefix, nodeID))
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// UpdateNodeStatus updates the node
func (r *EtcdRegistry) UpdateNode(pctx context.Context, nodeID, host string) error {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	return r.updateNode(ctx, nodeID, host)
}

func (r *EtcdRegistry) updateNode(ctx context.Context, nodeID, host string) error {
	obj := &NodeStatus{
		NodeID: nodeID,
		Host:   host,
	}
	objstr, err := json.Marshal(obj)
	if err != nil {
		return errors.Annotatef(err, "error marshal NodeStatus(%v)", obj)
	}
	key := r.prefixed(nodePrefix, nodeID, "object")
	if err := r.client.Update(ctx, key, string(objstr), 0); err != nil {
		return errors.Annotatef(err, "fail to update node with NodeStatus(%v)", obj)
	}
	return nil
}

func (r *EtcdRegistry) createNode(ctx context.Context, nodeID, host string) error {
	obj := &NodeStatus{
		NodeID: nodeID,
		Host:   host,
	}
	objstr, err := json.Marshal(obj)
	if err != nil {
		return errors.Annotatef(err, "error marshal NodeStatus(%v)", obj)
	}
	key := r.prefixed(nodePrefix, nodeID, "object")
	if err := r.client.Create(ctx, key, string(objstr), nil); err != nil {
		return errors.Annotatef(err, "fail to create node with NodeStatus(%v)", obj)
	}
	return nil
}

// RefreshNode keeps the heartbeats with etcd
func (r *EtcdRegistry) RefreshNode(pctx context.Context, nodeID string, ttl int64) error {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	aliveKey := r.prefixed(nodePrefix, nodeID, "alive")
	// try to touch alive state of node, update ttl
	if err := r.client.UpdateOrCreate(ctx, aliveKey, "", ttl); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func nodeStatusFromEtcdNode(nodeID string, node *etcd.Node) (*NodeStatus, error) {
	status := &NodeStatus{}
	var isAlive bool
	for key, n := range node.Childs {
		switch key {
		case "object":
			if err := json.Unmarshal(n.Value, status); err != nil {
				return nil, errors.Annotatef(err, "error unmarshal NodeStatus with nodeID(%s)", nodeID)
			}
		case "alive":
			isAlive = true
		}
	}

	status.IsAlive = isAlive
	return status, nil
}
