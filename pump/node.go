package pump

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tidb-binlog/pkg/flags"
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
	// Heartbeat refreshes the state of this pump node in etcd periodically
	// if the pump is dead, the key 'root/nodes/<nodeID>/alive' will dissolve after a TTL time passed
	Heartbeat(ctx context.Context) <-chan error
	// Notify queries all living drainer from etcd, and notifies them
	Notify(ctx context.Context) error
	// Nodes returns all pump nodes
	NodesStatus(ctx context.Context) ([]*NodeStatus, error)
}

type pumpNode struct {
	*EtcdRegistry
	id                string
	host              string
	heartbeatTTL      int64
	heartbeatInterval time.Duration
}

// NodeStatus describes the status information of a node in etcd
type NodeStatus struct {
	NodeID         string
	Host           string
	IsAlive        bool
	IsOffline      bool
	LatestFilePos  pb.Pos
	LatestKafkaPos pb.Pos
	OfflineTS      int64
}

// NewPumpNode returns a pumpNode obj that initialized by server config
func NewPumpNode(cfg *Config) (Node, error) {
	if err := checkExclusive(cfg.DataDir); err != nil {
		return nil, errors.Trace(err)
	}

	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cli, err := etcd.NewClientFromCfg(urlv.StringSlice(), cfg.EtcdDialTimeout, etcd.DefaultRootPath, cfg.tls)
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

	node := &pumpNode{
		EtcdRegistry:      NewEtcdRegistry(cli, cfg.EtcdDialTimeout),
		id:                nodeID,
		host:              advURL.Host,
		heartbeatInterval: time.Duration(cfg.HeartbeatInterval) * time.Second,
		heartbeatTTL:      int64(cfg.HeartbeatInterval) * 3 / 2,
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

func (p *pumpNode) Register(ctx context.Context) error {
	err := p.RegisterNode(ctx, nodePrefix, p.id, p.host)
	if err != nil {
		return errors.Trace(err)
	}
	return p.RefreshNode(ctx, nodePrefix, p.id, p.heartbeatTTL)
}

func (p *pumpNode) Unregister(ctx context.Context) error {
	err := p.MarkOfflineNode(ctx, nodePrefix, p.id, p.host)
	return errors.Trace(err)
}

func (p *pumpNode) Notify(ctx context.Context) error {
	cisterns, err := p.Nodes(ctx, "cisterns")
	if err != nil {
		return errors.Trace(err)
	}
	dialerOpt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("tcp", addr, timeout)
	})

	for _, c := range cisterns {
		if c.IsAlive {
			clientConn, err := grpc.Dial(c.Host, dialerOpt, grpc.WithInsecure())
			if err != nil {
				return errors.Errorf("notify drainer(%s); but return error(%v)", c.Host, err)
			}
			drainer := pb.NewCisternClient(clientConn)
			_, err = drainer.Notify(ctx, nil)
			clientConn.Close()
			if err != nil {
				return errors.Errorf("notify drainer(%s); but return error(%v)", c.Host, err)
			}
		}
	}

	return nil
}

func (p *pumpNode) NodesStatus(ctx context.Context) ([]*NodeStatus, error) {
	return p.Nodes(ctx, nodePrefix)
}

func (p *pumpNode) Heartbeat(ctx context.Context) <-chan error {
	errc := make(chan error, 1)
	go func() {
		defer func() {
			if err := p.Close(); err != nil {
				errc <- errors.Trace(err)
			}
			close(errc)
			log.Info("Heartbeat goroutine exited")
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(p.heartbeatInterval):
				// RefreshNode would carry lastBinlogFile infomation
				if err := p.RefreshNode(ctx, nodePrefix, p.id, p.heartbeatTTL); err != nil {
					errc <- errors.Trace(err)
				}
			}
		}
	}()
	return errc
}

// readLocalNodeID reads nodeID from a local file
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

	nodeID, err := FormatNodeID(string(data))
	if err != nil {
		return "", errors.Trace(err)
	}

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
	nodeID, err := FormatNodeID(id)
	if err != nil {
		return "", errors.Trace(err)
	}

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

// checkNodeID check NodeID's format is legal or not.
func checkNodeID(nodeID string) bool {
	hostPort := strings.Split(nodeID, ":")
	if len(hostPort) != 2 {
		log.Errorf("node id %s is illegal", nodeID)
		return false
	}

	port, err := strconv.Atoi(hostPort[1])
	if err != nil {
		log.Errorf("node id %s is illegal", nodeID)
		return false
	}

	return true
}

// FormatNodeID formats the nodeID
func FormatNodeID(nodeID string) (string, error) {
	legal := checkNodeID(nodeID)
	if legal {
		return nodeID, nil
	}

	newNodeID := strings.TrimSpace(nodeID)
	legal = checkNodeID(newNodeID)
	if !legal {
		log.Errorf("node id %s is illegal, and format failed", nodeID)
		return "", errors.Errorf("node id %s is illegal, and format failed", nodeID)
	}

	return newNodeID, nil
}
