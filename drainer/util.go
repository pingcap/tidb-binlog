package drainer

import (
	"fmt"
	"hash/crc32"
	"net"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/drainer/executor"
	"github.com/pingcap/tidb-binlog/pkg/dml"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	pb "github.com/pingcap/tipb/go-binlog"
)

const (
	maxMsgSize = 1024 * 1024 * 1024
)

// InitLogger initalizes Pump's logger.
func InitLogger(cfg *Config) {
	log.SetLevelByString(cfg.LogLevel)

	if len(cfg.LogFile) > 0 {
		log.SetOutputByName(cfg.LogFile)

		if cfg.LogRotate == "hour" {
			log.SetRotateByHour()
		} else {
			log.SetRotateByDay()
		}
	}

	sarama.Logger = util.NewStdLogger("[sarama] ")
}

// GenCheckPointCfg returns an CheckPoint config instance
func GenCheckPointCfg(cfg *Config, id uint64) *checkpoint.Config {
	dbCfg := checkpoint.DBConfig{
		Host:     cfg.SyncerCfg.To.Host,
		User:     cfg.SyncerCfg.To.User,
		Password: cfg.SyncerCfg.To.Password,
		Port:     cfg.SyncerCfg.To.Port,
	}
	return &checkpoint.Config{
		Db:              &dbCfg,
		ClusterID:       id,
		InitialCommitTS: cfg.InitialCommitTS,
		CheckPointFile:  path.Join(cfg.DataDir, "savepoint"),
	}
}

func initializeSaramaGlobalConfig() {
	sarama.MaxResponseSize = int32(maxMsgSize)
	sarama.MaxRequestSize = int32(maxMsgSize)
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

func execute(executor executor.Executor, sqls []string, args [][]interface{}, commitTSs []int64,
	tableIDs []int64, sqlTypes []pb.MutationType, isDDL bool, combineSQL bool) error {
	if len(sqls) == 0 {
		return nil
	}

	combineSQLs := make([]string, 0, len(sqls))
	combineArgs := make([][]interface{}, 0, len(args))

	if !isDDL && combineSQL {
		var (
			latestTableID int64
			latestSQLType pb.MutationType
			currentSql    string
			currentArgNum int
			currentArgs   []interface{}
			combineNum    int
			startCombine  bool
		)

		for i := 0; i < len(sqls); i++ {
			if i == 0 || !startCombine {
				currentSql = sqls[i]
				currentArgs = args[i]
				currentArgNum = len(args[i])
				combineNum = 1
				startCombine = true
			} else {
				// TODO: now only support combine insert sql, we can combine delete sql later.
				if sqlTypes[i] == pb.MutationType_Insert {
					// only when table id and sql type is same can we combine sql
					if tableIDs[i] == latestTableID && sqlTypes[i] == latestSQLType {
						currentArgs = append(currentArgs, args[i]...)
						combineNum++
						startCombine = true
					} else {
						combineSQLs = append(combineSQLs, fmt.Sprintf("%s,%s", currentSql, generatePlaceholders(currentArgNum, combineNum-1)))
						combineArgs = append(combineArgs, currentArgs)
						log.Infof("combineSQL: %s, combineArg: %v", combineSQLs[len(combineSQLs)-1], currentArgs)

						startCombine = false
					}
				} else {
					combineSQLs = append(combineSQLs, sqls[i])
					combineArgs = append(combineArgs, args[i])

					startCombine = false
				}
			}

			latestTableID = tableIDs[i]
			latestSQLType = sqlTypes[i]
		}
		if len(currentArgs) != 0 {
			if combineNum-1 == 0 {
				combineSQLs = append(combineSQLs, currentSql)
			} else {
				combineSQLs = append(combineSQLs, fmt.Sprintf("%s,%s", currentSql, generatePlaceholders(currentArgNum, combineNum-1)))
			}
			combineArgs = append(combineArgs, currentArgs)
		}
		log.Debugf("total %d sqls, after combine have %s sqls", len(sqls), len(combineSQLs))
	}

	beginTime := time.Now()
	defer func() {
		executeHistogram.Observe(time.Since(beginTime).Seconds())
	}()

	if len(combineSQLs) != 0 {
		return executor.Execute(combineSQLs, combineArgs, commitTSs, isDDL)
	}

	return executor.Execute(sqls, args, commitTSs, isDDL)
}

// generatePlaceholders returns placeholders like (?, ?, ?),(?, ?, ?)
func generatePlaceholders(i, j int) string {
	placeholders := make([]string, 0, j)
	for k := 0; k < j; k++ {
		placeholders = append(placeholders, fmt.Sprintf("(%s)", dml.GenColumnPlaceholders(i)))
	}

	return strings.Join(placeholders, ",")
}

func closeExecutors(executors ...executor.Executor) {
	for _, e := range executors {
		err := e.Close()
		if err != nil {
			log.Errorf("close db failed - %v", err)
		}
	}
}

func createExecutors(destDBType string, cfg *executor.DBConfig, count int) ([]executor.Executor, error) {
	executors := make([]executor.Executor, 0, count)
	for i := 0; i < count; i++ {
		executor, err := executor.New(destDBType, cfg)
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

func formatIgnoreSchemas(ignoreSchemas string) map[string]struct{} {
	ignoreSchemas = strings.ToLower(ignoreSchemas)
	schemas := strings.Split(ignoreSchemas, ",")

	ignoreSchemaNames := make(map[string]struct{})
	for _, schema := range schemas {
		ignoreSchemaNames[schema] = struct{}{}
	}

	return ignoreSchemaNames
}

func filterIgnoreSchema(schema *model.DBInfo, ignoreSchemaNames map[string]struct{}) bool {
	_, ok := ignoreSchemaNames[schema.Name.L]
	return ok
}
