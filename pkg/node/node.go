package node

import (
	"golang.org/x/net/context"
)

// Node defines pump node
type Node interface {
	// ID returns the uuid representing of this pump node
	ID() string
	// a short ID as 8 bytes length
	ShortID() string
	// RefreshStatus refresh node's status.
	RefreshStatus(ctx context.Context, status *Status) error
	// Heartbeat refreshes the state of this pump node in etcd periodically
	// if the pump is dead, the key 'root/nodes/<nodeID>/alive' will dissolve after a TTL time passed
	Heartbeat(ctx context.Context) <-chan error
	// Notify queries all living drainer from etcd, and notifies them
	Notify(ctx context.Context) error
	// NodeStatus returns this node's status
	NodeStatus() *Status
	// NodesStatus returns all pump nodes
	NodesStatus(ctx context.Context) ([]*Status, error)
	// Quit quits the node.
	Quit() error
}

var (
	// DefaultRootPath is the root path of the keys stored in etcd, the `v1` is the tidb-binlog's version.
	DefaultRootPath = "/tidb-binlog/v1"

	// PumpNode is the name of pump.
	PumpNode = "pump"

	// DrainerNode is the name of drainer.
	DrainerNode = "drainer"

	// NodePrefix is the map (node => it's prefix in storage)
	NodePrefix = map[string]string{
		PumpNode:    "pumps",
		DrainerNode: "drainers",
	}
)

const (
	// Online means the node can receive request.
	Online = "online"

	// Pausing means the node is pausing.
	Pausing = "pausing"

	// Paused means the node is already paused.
	Paused = "paused"

	// Closing means the node is closing, and the state will be Offline when closed.
	Closing = "closing"

	// Offline means the node is offline, and will not provide service.
	Offline = "offline"
)

// Label is key/value pairs that are attached to objects
type Label struct {
	Labels map[string]string `json:"labels"`
}

// Status describes the status information of a tidb-binlog node in etcd.
type Status struct {
	// the id of node.
	NodeID string `json:"nodeId"`

	// the host of pump or node.
	Addr string `json:"host"`

	// the state of pump.
	State string `json:"state"`

	// the node is alive or not.
	IsAlive bool `json:"isAlive"`

	// the score of node, it is report by node, calculated by node's qps, disk usage and binlog's data size.
	// if Score is less than 0, means this node is useless. Now only used for pump.
	Score int64 `json:"score"`

	// the label of this node. Now only used for pump.
	// pump client will only send to a pump which label is matched.
	Label *Label `json:"label"`

	// UpdateTS is the last update ts of node's status.
	UpdateTS int64 `json:"updateTS"`
}

// NewStatus returns a new status
func NewStatus(nodeID, addr, state string, score int64, ts int64) *Status {
	return &Status{
		NodeID:   nodeID,
		Addr:     addr,
		State:    state,
		Score:    score,
		UpdateTS: ts,
	}
}
