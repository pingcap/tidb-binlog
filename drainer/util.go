package drainer

import (
	"fmt"
	"hash/crc32"
	"net"
	"net/url"
	"os"
	"path"
	"sort"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/drainer/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
)

const (
<<<<<<< HEAD
	maxMsgSize = 1024 * 1024 * 1024
=======
	maxKafkaMsgSize = 1 << 30
	// max grpc message size, leave 4MB as buffer. Because when grpc decompresses messages, it will leave a few buffer
	// for this, which overflows the int64: https://github.com/grpc/grpc-go/blob/v1.44.0/rpc_util.go#L742
	maxGrpcMsgSize = int(^uint(0)>>1) - 4*1024*1024
>>>>>>> 9992784c (*: fix drainer can't send request correctly to pump when set compressor=gzip (#1186))
)

// GenCheckPointCfg returns an CheckPoint config instance
func GenCheckPointCfg(cfg *Config, id uint64) *checkpoint.Config {
	dbCfg := checkpoint.DBConfig{
		Host:     cfg.SyncerCfg.To.Host,
		User:     cfg.SyncerCfg.To.User,
		Password: cfg.SyncerCfg.To.Password,
		Port:     cfg.SyncerCfg.To.Port,
	}
	checkpointCfg := &checkpoint.Config{
		Db:              &dbCfg,
		ClusterID:       id,
		InitialCommitTS: cfg.InitialCommitTS,
		CheckPointFile:  path.Join(cfg.DataDir, "savepoint"),
	}

	if cfg.SyncerCfg.To.Checkpoint.Schema != "" {
		checkpointCfg.Schema = cfg.SyncerCfg.To.Checkpoint.Schema
	}

	return checkpointCfg
}

func initializeSaramaGlobalConfig() {
	sarama.MaxResponseSize = int32(maxMsgSize)
	// add 1 to avoid confused log: Producer.MaxMessageBytes must be smaller than MaxRequestSize; it will be ignored
	sarama.MaxRequestSize = int32(maxMsgSize) + 1
}

func getDDLJob(tiStore kv.Storage, id int64) (*model.Job, error) {
	version, err := tiStore.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshot, err := tiStore.GetSnapshot(version)
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapMeta := meta.NewSnapshotMeta(snapshot)
	job, err := snapMeta.GetHistoryDDLJob(id)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return job, nil
}

// loadHistoryDDLJobs loads all history DDL jobs from TiDB
func loadHistoryDDLJobs(tiStore kv.Storage) ([]*model.Job, error) {
	version, err := tiStore.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshot, err := tiStore.GetSnapshot(version)
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapMeta := meta.NewSnapshotMeta(snapshot)
	jobs, err := snapMeta.GetAllHistoryDDLJobs()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// jobs from GetAllHistoryDDLJobs are sorted by job id, need sorted by schema version
	sorter := &jobsSorter{jobs: jobs}
	sort.Sort(sorter)

	return jobs, nil
}

func genDrainerID(listenAddr string) (string, error) {
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

	return fmt.Sprintf("%s:%s", hostname, port), nil
}

func execute(executor executor.Executor, sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	if len(sqls) == 0 {
		return nil
	}

	beginTime := time.Now()
	defer func() {
		executeHistogram.Observe(time.Since(beginTime).Seconds())
	}()

	return executor.Execute(sqls, args, commitTSs, isDDL)
}

func closeExecutors(executors ...executor.Executor) {
	for _, e := range executors {
		err := e.Close()
		if err != nil {
			log.Errorf("close db failed - %v", err)
		}
	}
}

func createExecutors(destDBType string, cfg *executor.DBConfig, count int, sqlMODE *string) ([]executor.Executor, error) {
	executors := make([]executor.Executor, 0, count)
	for i := 0; i < count; i++ {
		executor, err := executor.New(destDBType, cfg, sqlMODE)
		if err != nil {
			return nil, errors.Trace(err)
		}

		executors = append(executors, executor)
	}

	return executors, nil
}

func genHashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// jobsSorter implements the sort.Interface interface.
type jobsSorter struct {
	jobs []*model.Job
}

func (s *jobsSorter) Swap(i, j int) {
	s.jobs[i], s.jobs[j] = s.jobs[j], s.jobs[i]
}

func (s *jobsSorter) Len() int {
	return len(s.jobs)
}

func (s *jobsSorter) Less(i, j int) bool {
	return s.jobs[i].BinlogInfo.SchemaVersion < s.jobs[j].BinlogInfo.SchemaVersion
}
