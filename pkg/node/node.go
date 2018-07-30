package node

import (

	"golang.org/x/net/context"
	"github.com/juju/errors"
)



// Node defines pump node
type Node interface {
	// ID returns the uuid representing of this pump node
	ID() string
	// a short ID as 8 bytes length
	ShortID() string
	// Register registers this pump node to Etcd
	// creates new one if nodeID not exist, otherwise update it
	Register(ctx context.Context) error
	// Unregister unregisters this pump node from etcd
	Unregister(ctx context.Context) error
	// Update updates the node status.
	UpdateStatus(ctx context.Context, status *Status)
	// Heartbeat refreshes the state of this pump node in etcd periodically
	// if the pump is dead, the key 'root/nodes/<nodeID>/alive' will dissolve after a TTL time passed
	Heartbeat(ctx context.Context) <-chan error
	// Notify queries all living drainer from etcd, and notifies them
	Notify(ctx context.Context) error
	// NodeStatus returns this node's status
	NodeStatus() *Status 
	// NodesStatus returns all pump nodes
	NodesStatus(ctx context.Context) ([]*Status, error)
}


var (
	// DefaultRootPath is the root path of the keys stored in etcd, the `2.1` is the tidb-binlog's version.
	DefaultRootPath = "/tidb-binlog/2.1"

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

// State is the state of node.
type State string

const (
	// Online means the node can receive request.
	Online State = "online"

	// Pausing means the node is pausing.
	Pausing State = "pausing"

	// Paused means the node is already paused.
	Paused State = "paused"

	// Closing means the node is closing, and the state will be Offline when closed.
	Closing State = "closing"

	// Offline means the node is offline, and will not provide service.
	Offline State = "offline"
)

// GetState returns a state by state name.
func GetState(state string) (State, error) {
	switch state {
	case "online":
		return Online, nil
	case "pausing":
		return Pausing, nil
	case "paused":
		return Paused, nil
	case "closing":
		return Closing, nil
	case "offline":
		return Offline, nil
	default:
		return Offline, errors.NotFoundf("state %s", state)
	}
}

// Label is key/value pairs that are attached to objects
type Label struct {
	Labels map[string]string
}

// Status describes the status information of a tidb-binlog node in etcd.
type Status struct {
	// the id of node.
	NodeID string

	// the host of pump or node.
	Host string

	// the state of pump.
	State State

	// the node is alive or not.
	IsAlive bool

	// the score of node, it is report by node, calculated by node's qps, disk usage and binlog's data size.
	// if Score is less than 0, means this node is useless. Now only used for pump.
	Score int64

	// the label of this node. Now only used for pump.
	// pump client will only send to a pump which label is matched.
	Label *Label

	// UpdateTS is the last update ts of node's status.
	UpdateTS int64
}
