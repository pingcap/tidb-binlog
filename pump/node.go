package pump

import (
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"context"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	pb "github.com/pingcap/tidb-binlog/proto"
	"github.com/twinj/uuid"
)

const (
	shortIDLen        = 8
	nodeIDFile        = ".node"
	lockFile          = ".lock"
	heartbeatInterval = 3 * time.Second
	heartbeatTTL      = 5
)

// Node holds the states of this pump node
type Node interface {
	// ID return the uuid representing of this pump node
	ID() string
	// a short ID as 8 bytes length
	ShortID() string
	// RegisterToEtcd register this pump node to the etcd
	// create new one if nodeID not exist, or update it
	RegisterToEtcd(ctx context.Context) error
	// Heartbeat refreshes the state of this pump node in etcd periodically
	// if the pump is dead, the key 'root/nodes/<nodeID>/alive' will dissolve after a TTL time passed
	Heartbeat(ctx context.Context, done <-chan struct{}) <-chan error
}

type pumpNode struct {
	*EtcdRegistry
	id   string
	host string
}

// NodeStatus describes the status information of a node in etcd
type NodeStatus struct {
	NodeID      string
	Host        string
	IsAlive     bool
	LastReadPos map[string]*pb.Pos
}

// NewPumpNode return a pumpNode obj that inited by server config
func NewPumpNode(cfg *Config) (Node, error) {
	if err := checkExclusive(cfg.DataDir); err != nil {
		return nil, errors.Trace(err)
	}

	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cli, err := etcd.NewClientFromCfg(urlv.StringSlice(), cfg.EtcdDialTimeout, etcd.DefaultRootPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	nodeID, err := readLocalNodeID(cfg.DataDir)
	if err != nil {
		if errors.IsNotFound(err) {
			nodeID, err = generateLocalNodeID(cfg.DataDir)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			return nil, errors.Trace(err)
		}
	}

	advURL, err := url.Parse(cfg.AdvertiseAddr)
	if err != nil {
		return nil, errors.Annotatef(err, "invalid configuration of advertise addr(%s)", cfg.AdvertiseAddr)
	}

	node := &pumpNode{
		EtcdRegistry: NewEtcdRegistry(cli, cfg.EtcdDialTimeout),
		id:           nodeID,
		host:         advURL.Host,
	}
	return node, nil
}

func (p *pumpNode) ID() string {
	return p.id
}

func (p *pumpNode) ShortID() string {
	if len(p.id) <= shortIDLen {
		return p.id
	}
	return p.id[0:shortIDLen]
}

func (p *pumpNode) RegisterToEtcd(ctx context.Context) error {
	err := p.RegisterNode(ctx, p.id, p.host)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (p *pumpNode) Heartbeat(ctx context.Context, done <-chan struct{}) <-chan error {
	errc := make(chan error, 1)
	go func() {
		var clock = clockwork.NewRealClock()
		for {
			select {
			case <-done:
				// stop heartbeat and prepare to exit
				close(errc)
				return
			case <-clock.After(heartbeatInterval):
				if err := p.RefreshNode(ctx, p.id, heartbeatTTL); err != nil {
					errc <- errors.Trace(err)
				}
			}
		}
	}()
	return errc
}

// readLocalNodeID read nodeID from a local file
// returns a NotFound error if the nodeID file not exist
// in this case, the caller should invoke generateLocalNodeID()
func readLocalNodeID(dataDir string) (string, error) {
	nodeIDPath := filepath.Join(dataDir, nodeIDFile)
	if _, err := CheckFileExist(nodeIDPath); err != nil {
		return "", errors.NewNotFound(err, "local nodeID file not exist")
	}
	data, err := ioutil.ReadFile(nodeIDPath)
	if err != nil {
		return "", errors.Annotate(err, "local nodeID file is collapsed")
	}
	if len(data) < 16 {
		return "", errors.Errorf("local nodeID file(%s) is collapsed", nodeIDPath)
	}
	id := uuid.New(data)
	return id.String(), nil
}

// generate a new nodeID, and store it to local filesystem
func generateLocalNodeID(dataDir string) (string, error) {
	if err := os.MkdirAll(dataDir, file.PrivateDirMode); err != nil {
		return "", errors.Trace(err)
	}

	id := uuid.NewV1()
	nodeIDPath := filepath.Join(dataDir, nodeIDFile)
	if err := ioutil.WriteFile(nodeIDPath, id.Bytes(), file.PrivateFileMode); err != nil {
		return "", errors.Trace(err)
	}
	return id.String(), nil
}

// checkExclusive try to get filelock of dataDir in exclusive mode
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
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
