package pump

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/tidb-binlog/proto"
)

const (
	shortIDLen = 8
	nodeDir    = ".node"
	nodeIDFile = "nodeID"
)

// Node is a node interface that has the node basic infomation quey method
type Node interface {
	ID() string
	ShortID() string
	NodeID(ID string) bool
	Status() *NodeStatus
}

// NodeStatus has some basic node status infomations
type NodeStatus struct {
	NodeID  string
	Host    string
	IsAlive bool
	Offsets map[string]*pb.Pos
}

type pumpNode struct {
	id     string
	status *NodeStatus
}

// NewPumpNode return a pumpNode obj that inited by server config
func NewPumpNode(cfg *Config) (Node, error) {
	nodeID, err := readLocalNodeID(cfg.DataDir)
	if err != nil {
		log.Errorf("Read local node ID error, %v", err)
		return nil, errors.Trace(err)
	}

	node := &pumpNode{
		id: nodeID,
		status: &NodeStatus{
			NodeID:  nodeID,
			Host:    cfg.AdvertiseAddr,
			IsAlive: true,
		},
	}
	return node, nil
}

func (p *pumpNode) ID() string {
	return p.id
}

func (p *pumpNode) Host() string {
	return p.status.Host
}

func (p *pumpNode) ShortID() string {
	if len(p.id) <= shortIDLen {
		return p.id
	}
	return p.id[0:shortIDLen]
}

func (p *pumpNode) NodeID(ID string) bool {
	return p.id == ID || p.ShortID() == ID
}

func (p *pumpNode) Status() *NodeStatus {
	return p.status
}
