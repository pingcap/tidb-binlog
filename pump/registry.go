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

// MarkOfflineNode marks offline node in the etcd
func (r *EtcdRegistry) MarkOfflineNode(pctx context.Context, prefix, nodeID, host string) error {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	obj := &NodeStatus{
		NodeID: nodeID,
		Host:   host,
		//IsOffline:      true,
		//LatestKafkaPos: latestKafkaPos,
		//LatestFilePos:  latestFilePos,
		//OfflineTS:      latestTS,
	}

	log.Infof("[pump] %s mark offline information %+v", nodeID, obj)
	objstr, err := json.Marshal(obj)
	if err != nil {
		return errors.Annotatef(err, "error marshal NodeStatus(%v)", obj)
	}

	key := r.prefixed(prefix, nodeID, "object")
	err = r.client.Update(ctx, key, string(objstr), 0)
	return errors.Trace(err)
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
func (r *EtcdRegistry) RefreshNode(pctx context.Context, prefix, nodeID string) error {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	key := r.prefixed(prefix, nodeID)

	//latestPos := &LatestPos{
	//	FilePos:  latestFilePos,
	//	KafkaPos: latestKafkaPos,
	//}
	//latestPosBytes, err := json.Marshal(latestPos)
	//if err != nil {
	//	return errors.Trace(err)
	//}

	// try to touch alive state of node, update ttl
	//err = r.client.UpdateOrCreate(ctx, aliveKey, string(latestPosBytes), ttl)
	err := r.client.UpdateOrCreate(ctx, key, "", 0)
	return errors.Trace(err)
}

func nodeStatusFromEtcdNode(id string, node *etcd.Node) (*NodeStatus, error) {
	status := &NodeStatus{}

	if err := json.Unmarshal(node.Value, &status); err != nil {
		return nil, errors.Annotatef(err, "error unmarshal NodeStatus with nodeID(%s)", id)
	}

	return status, nil
}

func nodesStatusFromEtcdNode(root *etcd.Node) ([]*NodeStatus, error) {
	var statuses []*NodeStatus
	for id, n := range root.Childs {
		status, err := nodeStatusFromEtcdNode(id, n)
		if err != nil {
			return nil, err
		}
		if status == nil {
			continue
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}
