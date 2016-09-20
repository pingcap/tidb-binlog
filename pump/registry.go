package pump

import (
	"encoding/json"
	"path"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"golang.org/x/net/context"
)

const nodePrefix = "node"

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
func (r *EtcdRegistry) Node(ctx context.Context, nodeID string) (*NodeStatus, error) {
	resp, err := r.client.List(ctx, r.prefixed(nodePrefix, nodeID))
	if err != nil {
		return nil, err
	}

	status, err := nodeStatusFromEtcdNode(nodeID, resp)
	if err != nil {
		return nil, errors.Errorf("Invalid node, nodeID[%s], error[%v]", nodeID, err)
	}

	return status, nil
}

// RegisterNode register the node in the etcd
func (r *EtcdRegistry) RegisterNode(ctx context.Context, nodeID, host string) error {
	if exists, err := r.checkNodeExists(ctx, nodeID); err != nil {
		return errors.Trace(err)
	} else if !exists {
		// not found then create a new  node
		return r.createNode(ctx, nodeID, host)
	}

	// found it, update host infomation of the node
	nodeStatus := &NodeStatus{
		NodeID: nodeID,
		Host:   host,
	}
	return r.UpdateNodeStatus(ctx, nodeID, nodeStatus)
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
func (r *EtcdRegistry) UpdateNodeStatus(ctx context.Context, nodeID string, nodeStatus *NodeStatus) error {
	object, err := json.Marshal(nodeStatus)
	if err != nil {
		return errors.Errorf("Error marshaling NodeSattus, %v, %v", object, err)
	}

	key := r.prefixed(nodePrefix, nodeID, "object")
	if err := r.client.Update(ctx, key, string(object), 0); err != nil {
		return errors.Errorf("Failed to update NodeStatus in etcd, %s, %v, %v", nodeID, object, err)
	}
	return nil
}

func (r *EtcdRegistry) createNode(ctx context.Context, nodeID string, host string) error {
	object := &NodeStatus{
		NodeID: nodeID,
		Host:   host,
	}

	objstr, err := json.Marshal(object)
	if err != nil {
		return errors.Errorf("Error marshaling NodeStatus, %v, %v", object, err)
	}

	if err = r.client.Create(ctx, r.prefixed(nodePrefix, nodeID, "object"), string(objstr), nil); err != nil {
		return errors.Errorf("Failed to create NodeStatus of node, %s, %v, %v", nodeID, object, err)
	}

	return nil
}

// RefreshNode keeps the heartbeats with etcd
func (r *EtcdRegistry) RefreshNode(ctx context.Context, nodeID string, ttl int64) error {
	aliveKey := r.prefixed(nodePrefix, nodeID, "alive")

	// try to touch alive state of node, update ttl
	if err := r.client.Update(ctx, aliveKey, "", ttl); err != nil {
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
			if err := json.Unmarshal(n.Value, &status); err != nil {
				log.Errorf("Error unmarshaling NodeStatus, nodeID: %s, %v", nodeID, err)
				return nil, err
			}
		case "alive":
			isAlive = true
		}
	}

	status.IsAlive = isAlive
	return status, nil
}
