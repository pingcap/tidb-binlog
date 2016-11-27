package cistern

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"
	grpc "google.golang.org/grpc"

	"github.com/coreos/etcd/integration"
	"github.com/juju/errors"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb-binlog/pump"
)

func TestServer(t *testing.T) {
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	tmpDir, err := ioutil.TempDir(os.TempDir(), "pump.server")
	if err != nil {
		t.Fatalf("mkdir tmp dir err:%v", err)
	}
	defer os.RemoveAll(tmpDir)

	listenAddr := "http://127.0.0.1:8250"
	cfg := &Config{
		DataDir:         tmpDir,
		EtcdURLs:        strings.Join(cluster.RandClient().Endpoints(), ","),
		EtcdTimeout:     defaultEtcdTimeout,
		ListenAddr:      listenAddr,
		MetricsAddr:     "127.0.0.1:9091",
		MetricsInterval: 1,
		GC:              7,
		LogFile:         fmt.Sprintf("%s/pump.log", tmpDir),
	}

	s, err := testInitServer(cfg)
	if err != nil {
		t.Fatalf("test init server error: %v", err)
	}

	nodeID := "testNode"
	err = s.collector.reg.RegisterNode(context.Background(), nodeID, "127.0.0.1:8250")
	if err != nil {
		t.Fatalf("register node err: %v", err)
	}

	err = s.collector.reg.RefreshNode(context.Background(), nodeID, 1)
	if err != nil {
		t.Fatalf("refresh node err: %v", err)
	}

	go func() {
		err = s.Start()
		if err == nil {
			t.Fatal("should return use closed socket")
		}
	}()

	time.Sleep(2 * time.Second)
	s.Close()
}

// because NewServer need to inititial pd client and tikv store, we have to inititial them by self
func testInitServer(cfg *Config) (*Server, error) {
	cid := uint64(1)
	windowNamespace = []byte(fmt.Sprintf("window_%d", cid))
	binlogNamespace = []byte(fmt.Sprintf("binlog_%d", cid))
	savepointNamespace = []byte(fmt.Sprintf("savepoint_%d", cid))
	ddlJobNamespace = []byte(fmt.Sprintf("ddljob_%d", cid))
	s, err := store.NewBoltStore(path.Join(cfg.DataDir, "data.bolt"), [][]byte{
		windowNamespace,
		binlogNamespace,
		savepointNamespace,
		ddlJobNamespace,
	})
	if err != nil {
		return nil, err
	}

	win, err := NewDepositWindow(s)
	if err != nil {
		return nil, err
	}

	p := NewPublisher(cfg, s, win)
	ctx, cancel := context.WithCancel(context.Background())

	var metrics *metricClient
	if cfg.MetricsAddr != "" && cfg.MetricsInterval != 0 {
		metrics = &metricClient{
			addr:     cfg.MetricsAddr,
			interval: cfg.MetricsInterval,
		}
	}

	var gc time.Duration
	if cfg.GC > 0 {
		gc = time.Duration(cfg.GC) * 24 * time.Hour
	}

	//  init collector
	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cli, err := etcd.NewClientFromCfg(urlv.StringSlice(), cfg.EtcdTimeout, etcd.DefaultRootPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tiStore, err := tidb.NewStore(fmt.Sprintf("memory://testkv"))
	if err != nil {
		return nil, err
	}

	col := &Collector{
		clusterID: cid,
		batch:     1,
		interval:  time.Second,
		reg:       pump.NewEtcdRegistry(cli, cfg.EtcdTimeout),
		pumps:     make(map[string]*Pump),
		timeout:   defaultPumpTimeout,
		window:    win,
		boltdb:    s,
		tiStore:   tiStore,
	}

	return &Server{
		boltdb:    s,
		window:    win,
		collector: col,
		publisher: p,
		metrics:   metrics,
		tcpAddr:   cfg.ListenAddr,
		gs:        grpc.NewServer(),
		ctx:       ctx,
		cancel:    cancel,
		gc:        gc,
	}, nil
}
