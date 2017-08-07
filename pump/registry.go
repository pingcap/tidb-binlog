package pump

import (
	"encoding/json"
	"path"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	pb "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
)

// EtcdRegistry wraps the reactions with etcd
type EtcdRegistry struct {
	client     *etcd.Client
	reqTimeout time.Duration
}

// NewEtcdRegistry returns an EtcdRegistry client
func NewEtcdRegistry(cli *etcd.Client, reqTimeout time.Duration) *EtcdRegistry {
	return &EtcdRegistry{
		client:     cli,
		reqTimeout: reqTimeout,
	}
}

// Close closes the etcd client
func (r *EtcdRegistry) Close() error {
	err := r.client.Close()
	return errors.Trace(err)
}

func (r *EtcdRegistry) prefixed(p ...string) string {
	return path.Join(p...)
}

// Node returns the nodeStatus that matchs nodeID in the etcd
func (r *EtcdRegistry) Node(pctx context.Context, prefix, nodeID string) (*NodeStatus, error) {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	resp, err := r.client.List(ctx, r.prefixed(prefix, nodeID))
	if err != nil {
		return nil, errors.Trace(err)
	}
	status, err := nodeStatusFromEtcdNode(nodeID, resp)
	if err != nil {
		return nil, errors.Annotatef(err, "Invalid nodeID(%s)", nodeID)
	}
	return status, nil
}

// Nodes retruns all the nodeStatuses in the etcd
func (r *EtcdRegistry) Nodes(pctx context.Context, prefix string) ([]*NodeStatus, error) {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	resp, err := r.client.List(ctx, r.prefixed(prefix))
	if err != nil {
		return nil, errors.Trace(err)
	}
	status, err := nodesStatusFromEtcdNode(resp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return status, nil
}

// RegisterNode registers the node in the etcd
func (r *EtcdRegistry) RegisterNode(pctx context.Context, prefix, nodeID, host string) error {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	if exists, err := r.checkNodeExists(ctx, prefix, nodeID); err != nil {
		return errors.Trace(err)
	} else if !exists {
		// not found then create a new  node
		return r.createNode(ctx, prefix, nodeID, host)
	} else {
		// found it, update host infomation of the node
		return r.updateNode(ctx, prefix, nodeID, host)
	}

}

// MarkOfflineSign marks pump offline sign
func (r *EtcdRegistry) MarkOfflineSign(pctx context.Context, prefix, nodeID string) error {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	offlineKey := r.prefixed(prefix, "offline", nodeID)
	latestPosBytes, err := latestPos.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	// try to touch offline sign of node
	err = r.client.UpdateOrCreate(ctx, offlineKey, string(latestPosBytes), 0)
	return errors.Trace(err)
}

// GetOfflineSign queries pump offline sign
func (r *EtcdRegistry) GetOfflineSign(pctx context.Context, prefix, nodeID string) (pb.Pos, error) {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	offlineKey := r.prefixed(prefix, "offline", nodeID)
	pos := pb.Pos{}

	posBytes, err := r.client.Get(ctx, offlineKey)
	if err != nil {
		return pos, errors.Trace(err)
	}
	err = pos.Unmarshal(posBytes)
	return pos, errors.Trace(err)
}

// UnregisterNode unregisters the node in the etcd
func (r *EtcdRegistry) UnregisterNode(pctx context.Context, prefix, nodeID string) error {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	key := r.prefixed(prefix, nodeID)
	err := r.client.Delete(ctx, key, true)
	return errors.Trace(err)
}

func (r *EtcdRegistry) checkNodeExists(ctx context.Context, prefix, nodeID string) (bool, error) {
	_, err := r.client.Get(ctx, r.prefixed(prefix, nodeID, "object"))
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// UpdateNode updates the node infomation
func (r *EtcdRegistry) UpdateNode(pctx context.Context, prefix, nodeID, host string) error {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	return r.updateNode(ctx, prefix, nodeID, host)
}

func (r *EtcdRegistry) updateNode(ctx context.Context, prefix, nodeID, host string) error {
	obj := &NodeStatus{
		NodeID: nodeID,
		Host:   host,
	}
	objstr, err := json.Marshal(obj)
	if err != nil {
		return errors.Annotatef(err, "error marshal NodeStatus(%v)", obj)
	}
	key := r.prefixed(prefix, nodeID, "object")
	err = r.client.Update(ctx, key, string(objstr), 0)
	return errors.Trace(err)
}

func (r *EtcdRegistry) createNode(ctx context.Context, prefix, nodeID, host string) error {
	obj := &NodeStatus{
		NodeID: nodeID,
		Host:   host,
	}
	objstr, err := json.Marshal(obj)
	if err != nil {
		return errors.Annotatef(err, "error marshal NodeStatus(%v)", obj)
	}
	key := r.prefixed(prefix, nodeID, "object")
	err = r.client.Create(ctx, key, string(objstr), nil)
	return errors.Trace(err)
}

// RefreshNode keeps the heartbeats with etcd
func (r *EtcdRegistry) RefreshNode(pctx context.Context, prefix, nodeID string, ttl int64) error {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	aliveKey := r.prefixed(prefix, nodeID, "alive")
	latestPosBytes, err := latestPos.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	// try to touch alive state of node, update ttl
	err = r.client.UpdateOrCreate(ctx, aliveKey, string(latestPosBytes), ttl)
	return errors.Trace(err)
}

func nodeStatusFromEtcdNode(id string, node *etcd.Node) (*NodeStatus, error) {
	status := &NodeStatus{}
	latestPos := pb.Pos{}
	var isAlive bool
	for key, n := range node.Childs {
		switch key {
		case "object":
			if err := json.Unmarshal(n.Value, &status); err != nil {
				return nil, errors.Annotatef(err, "error unmarshal NodeStatus with nodeID(%s)", id)
			}
		case "alive":
			isAlive = true
			if err := latestPos.Unmarshal(n.Value); err != nil {
				return nil, errors.Annotatef(err, "error unmarshal NodeStatus with nodeID(%s)", id)
			}
		}
	}
	status.IsAlive = isAlive
	status.LatestPos = latestPos
	return status, nil
}

func nodesStatusFromEtcdNode(root *etcd.Node) ([]*NodeStatus, error) {
	var statuses []*NodeStatus
	for id, n := range root.Childs {
		status, err := nodeStatusFromEtcdNode(id, n)
		if err != nil {
			return nil, err
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}
