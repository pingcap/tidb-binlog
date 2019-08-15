// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package arbiter

import (
	"context"
	"database/sql"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-tools/tidb-binlog/driver/reader"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
)

var (
	initSafeModeDuration = time.Minute * 5

	// Make it possible to mock the following functions
	createDB  = loader.CreateDB
	newReader = reader.NewReader
	newLoader = loader.NewLoader
)

// Server is the server to load data to mysql
type Server struct {
	cfg  *Config
	port int

	load loader.Loader

	checkpoint  Checkpoint
	kafkaReader *reader.Reader
	downDB      *sql.DB

	// all txn commitTS <= finishTS has loaded to downstream
	finishTS int64

	metrics *util.MetricClient

	closed bool
	mu     sync.Mutex
}

// NewServer creates a Server
func NewServer(cfg *Config) (srv *Server, err error) {
	srv = new(Server)
	srv.cfg = cfg

	_, port, err := net.SplitHostPort(cfg.ListenAddr)
	if err != nil {
		return nil, errors.Annotatef(err, "wrong ListenAddr: %s", cfg.ListenAddr)
	}

	srv.port, err = strconv.Atoi(port)
	if err != nil {
		return nil, errors.Annotatef(err, "ListenAddr: %s", cfg.ListenAddr)
	}

	up := cfg.Up
	down := cfg.Down

	srv.downDB, err = createDB(down.User, down.Password, down.Host, down.Port)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// set checkpoint
	srv.checkpoint, err = NewCheckpoint(srv.downDB, up.Topic)
	if err != nil {
		return nil, errors.Trace(err)
	}

	srv.finishTS = up.InitialCommitTS

	status, err := srv.loadStatus()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// set reader to read binlog from kafka
	readerCfg := &reader.Config{
		KafkaAddr: strings.Split(up.KafkaAddrs, ","),
		CommitTS:  srv.finishTS,
		Topic:     up.Topic,
	}

	log.Info("use kafka binlog reader", zap.Reflect("cfg", readerCfg))

	srv.kafkaReader, err = newReader(readerCfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// set loader
	srv.load, err = newLoader(srv.downDB,
		loader.WorkerCount(cfg.Down.WorkerCount),
		loader.BatchSize(cfg.Down.BatchSize),
		loader.Metrics(&loader.MetricsGroup{
			EventCounterVec:   eventCounter,
			QueryHistogramVec: queryHistogramVec,
		}))
	if err != nil {
		return nil, errors.Trace(err)
	}

	// set safe mode in first 5 min if abnormal quit last time
	if status == StatusRunning {
		log.Info("set safe mode to be true")
		srv.load.SetSafeMode(true)
		go func() {
			time.Sleep(initSafeModeDuration)
			srv.load.SetSafeMode(false)
			log.Info("set safe mode to be false")
		}()
	}

	// set metrics
	if cfg.Metrics.Addr != "" && cfg.Metrics.Interval != 0 {
		srv.metrics = util.NewMetricClient(
			cfg.Metrics.Addr,
			time.Duration(cfg.Metrics.Interval)*time.Second,
			Registry,
		)
	}

	return
}

// Close closes the Server
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.kafkaReader.Close()

	s.closed = true
	return nil
}

// Run runs the Server, will quit once encounter error or Server is closed
func (s *Server) Run() error {
	defer func() {
		if s.downDB != nil {
			s.downDB.Close()
		} else {
			log.Error("Invalid downDB address, please check the config")
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// push metrics if need
	if s.metrics != nil {
		go s.metrics.Start(ctx, map[string]string{"instance": instanceName(s.port)})
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		s.trackTS(ctx, time.Second)
		wg.Done()
	}()

	var syncErr error

	syncCtx, syncCancel := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		syncErr = syncBinlogs(syncCtx, s.kafkaReader.Messages(), s.load)
		if syncErr != nil {
			s.Close()
		}
	}()

	err := s.load.Run()
	if err != nil {
		syncCancel()
		s.Close()
	}

	wg.Wait()

	// to pass go check
	syncCancel()
	if err != nil {
		return errors.Trace(err)
	}

	if syncErr != nil {
		return errors.Trace(syncErr)
	}

	if err = s.saveFinishTS(StatusNormal); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (s *Server) updateFinishTS(msg *reader.Message) {
	s.finishTS = msg.Binlog.CommitTs

	ms := time.Now().UnixNano()/1000000 - oracle.ExtractPhysical(uint64(s.finishTS))
	txnLatencySecondsHistogram.Observe(float64(ms) / 1000.0)
}

func (s *Server) saveFinishTS(status int) error {
	err := s.checkpoint.Save(s.finishTS, status)
	if err != nil {
		return err
	}
	checkpointTSOGauge.Set(float64(oracle.ExtractPhysical(uint64(s.finishTS))))
	return nil
}

func (s *Server) trackTS(ctx context.Context, saveInterval time.Duration) {
	saveTick := time.NewTicker(saveInterval)
	defer saveTick.Stop()

L:
	for {
		select {
		case txn, ok := <-s.load.Successes():
			if !ok {
				log.Info("load successes channel closed")
				break L
			}
			msg := txn.Metadata.(*reader.Message)
			log.Debug("get success binlog", zap.Int64("ts", msg.Binlog.CommitTs), zap.Int64("offset", msg.Offset))
			s.updateFinishTS(msg)
		case <-saveTick.C:
			if err := s.saveFinishTS(StatusRunning); err != nil {
				log.Error("save finish ts failed", zap.Error(err))
			}
		case <-ctx.Done():
			break L
		}
	}

	if err := s.saveFinishTS(StatusRunning); err != nil {
		log.Error("save finish ts failed", zap.Error(err))
	}
}

func (s *Server) loadStatus() (int, error) {
	ts, status, err := s.checkpoint.Load()
	if err != nil {
		if !errors.IsNotFound(err) {
			return 0, errors.Trace(err)
		}
		err = nil
	} else {
		s.finishTS = ts
	}
	return status, errors.Trace(err)
}

func syncBinlogs(ctx context.Context, source <-chan *reader.Message, ld loader.Loader) (err error) {
	dest := ld.Input()
	defer ld.Close()
	for msg := range source {
		log.Debug("recv msg from kafka reader", zap.Int64("ts", msg.Binlog.CommitTs), zap.Int64("offset", msg.Offset))
		txn, err := loader.SlaveBinlogToTxn(msg.Binlog)
		if err != nil {
			log.Error("transfer binlog failed, program will stop handling data from loader", zap.Error(err))
			return err
		}
		txn.Metadata = msg
		// avoid block when no process is handling ld.input
		select {
		case dest <- txn:
		case <-ctx.Done():
			return nil
		}

		queueSizeGauge.WithLabelValues("kafka_reader").Set(float64(len(source)))
		queueSizeGauge.WithLabelValues("loader_input").Set(float64(len(dest)))
	}
	return nil
}
