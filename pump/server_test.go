package pump

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"github.com/pingcap/tipb/go-binlog"
)

func TestServer(t *testing.T) {
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	tmpDir, err := ioutil.TempDir(os.TempDir(), "pump.server")
	if err != nil {
		t.Fatalf("mkdir tmp dir err:%v", err)
	}
	defer os.RemoveAll(tmpDir)

	socketFile := fmt.Sprintf("unix://%s/pump.socket", tmpDir)
	listenAddr := "http://127.0.0.1:8250"
	cfg := &Config{
		DataDir:           tmpDir,
		EtcdURLs:          strings.Join(cluster.RandClient().Endpoints(), ","),
		EtcdDialTimeout:   defaultEtcdDialTimeout,
		HeartbeatInterval: 1,
		ListenAddr:        listenAddr,
		AdvertiseAddr:     listenAddr,
		MetricsAddr:       "127.0.0.1:9091",
		MetricsInterval:   1,
		GC:                7,
		NodeID:            "testID",
		Socket:            socketFile,
		LogFile:           fmt.Sprintf("%s/pump.log", tmpDir),
	}

	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("new server err: %v", err)
	}
	InitLogger(cfg)
	PrintVersionInfo()

	go func() {
		s.Start()
	}()
	time.Sleep(100 * time.Millisecond)

	// test Concurrent start
	err = s.Start()
	if err == nil {
		t.Fatal("should return register err")
	}
	testPullBinlog(t, s, true)
	testWriteBinlog(t, s)
	testPullBinlog(t, s, false)
	for _, b := range s.dispatcher {
		CloseBinlogger(b)
	}

	s.init()
	s.Close()
}

func TestKrand(t *testing.T) {
	data := KRand(4, 3)
	if len(data) != 4 {
		t.Fatalf("krand gen data err: %s", data)
	}
}

func TestCheckBinlogNames(t *testing.T) {
	names := []string{"binlog-0000000000000001", "test", "binlog-0000000000000002"}
	excepted := []string{"binlog-0000000000000001", "binlog-0000000000000002"}
	res := checkBinlogNames(names)

	if len(res) != len(excepted) {
		t.Fatalf("get values %v, but excepted %v", res, excepted)
	}

	for i := range res {
		if res[i] != excepted[i] {
			t.Fatalf("get values %v, but excepted %v", res, excepted)
		}
	}
}

func testWriteBinlog(t *testing.T, s *Server) {
	data := []byte("test")
	in := &binlog.WriteBinlogReq{
		ClusterID: 1,
		Payload:   data,
	}

	_, err := s.WriteBinlog(s.ctx, in)
	if err != nil {
		t.Fatalf("write binlog err:%v", err)
	}
}

func testPullBinlog(t *testing.T, s *Server, isEmpty bool) {
	data := "test"
	in := &binlog.PullBinlogReq{
		ClusterID: 1,
		Batch:     1,
	}

	resp, err := s.PullBinlogs(s.ctx, in)
	if err != nil {
		t.Fatalf("pull binlog err:%v", err)
	}

	if isEmpty {
		if len(resp.Entities) != 0 {
			t.Fatal("data is corruption")
		}
		return
	}

	if len(resp.Entities) == 0 || string(resp.Entities[0].Payload) != data {
		t.Fatal("data is corruption")
	}
}
