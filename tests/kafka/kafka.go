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

package main

import (
	"context"
	"database/sql"
	"flag"
	"strings"
	"sync"

	"github.com/pingcap/tidb-binlog/tests/util"

	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	"github.com/pingcap/tidb-binlog/tests/dailytest"
	pb "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
)

// drainer -> kafka, syn data from kafka to downstream TiDB, and run the dailytest

var (
	kafkaAddr = flag.String("kafkaAddr", "127.0.0.1:9092", "kafkaAddr like 127.0.0.1:9092,127.0.0.1:9093")
	topic     = flag.String("topic", "", "topic name to consume binlog")
)

func main() {
	flag.Parse()

	sourceDBs, err := util.CreateSourceDBs()
	if err != nil {
		panic(err)
	}

	sinkDB, err := util.CreateSinkDB()
	if err != nil {
		panic(err)
	}

	sinkDBForDiff, err := util.CreateSinkDB()
	if err != nil {
		panic(err)
	}

	log.S().Info("Testing case: partition by table")
	testPartitionByTable(sourceDBs, sinkDB, sinkDBForDiff)
}

func testPartitionByTable(sourceDBs []*sql.DB, sinkDB *sql.DB, sinkDBForDiff *sql.DB) {
	kafkaAddr := strings.Split(*kafkaAddr, ",")
	client, err := sarama.NewClient(kafkaAddr, nil)
	if err != nil {
		panic(err)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	partitions, err := client.Partitions(*topic)
	if err != nil {
		panic(err)
	}
	log.S().Infof("Partitions: %d", partitions)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start sync to mysql from kafka
	ld, err := loader.NewLoader(sinkDB, loader.WorkerCount(16), loader.BatchSize(128))
	if err != nil {
		panic(err)
	}
	defer ld.Close()

	go func() {
		err := ld.Run()
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		for {
			select {
			case txn := <-ld.Successes():
				log.S().Debug("succ: ", txn)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Global states for all the partition consumers
	var ddlLock sync.RWMutex
	ddlCond := sync.NewCond(&ddlLock)
	ddlCounter := make(map[int64]int)
	for _, p := range partitions {
		partitionConsumer, err := consumer.ConsumePartition(*topic, p, 0)
		if err != nil {
			panic(err)
		}
		go func() {
			defer partitionConsumer.Close()
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-partitionConsumer.Messages():
					binlog := new(pb.Binlog)
					err := binlog.Unmarshal(msg.Value)
					if err != nil {
						panic(err)
					}
					txn, err := loader.SlaveBinlogToTxn(binlog)
					if err != nil {
						log.S().Fatal(err)
					}

					if binlog.Type == pb.BinlogType_DDL {
						log.S().Infof("[P-%d] Get DDL: %d", msg.Partition, binlog.CommitTs)

						var isLastReceiver bool
						ddlCond.L.Lock()
						ddlCounter[binlog.CommitTs] += 1
						isLastReceiver = ddlCounter[binlog.CommitTs] == len(partitions)
						ddlCond.L.Unlock()

						if !isLastReceiver {
							ddlCond.L.Lock()
							for ddlCounter[binlog.CommitTs] != 0 {
								log.S().Infof("[P-%d] Waiting for DDL: %d", msg.Partition, binlog.CommitTs)
								ddlCond.Wait()
							}
							ddlCond.L.Unlock()
						} else {
							log.S().Infof("[P-%d] Finishing DDL: %d", msg.Partition, binlog.CommitTs)
							ld.Input() <- txn
							ddlCond.L.Lock()
							delete(ddlCounter, binlog.CommitTs)
							ddlCond.L.Unlock()
							ddlCond.Broadcast()
							log.S().Infof("[P-%d] Finished DDL: %d", msg.Partition, binlog.CommitTs)
						}
					} else {
						log.S().Infof("[P-%d] Get DML: %d", msg.Partition, binlog.CommitTs)
						ld.Input() <- txn
					}
				}
			}
		}()
	}
	dailytest.RunMultiSource(sourceDBs, sinkDBForDiff, "test")
	dailytest.Run(sourceDBs[0], sinkDBForDiff, "test", 10, 1000, 10)
}
