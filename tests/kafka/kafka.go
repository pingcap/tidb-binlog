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
	"flag"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	"github.com/pingcap/tidb-binlog/tests/dailytest"
	"github.com/pingcap/tidb-binlog/tests/util"
	"github.com/pingcap/tidb-tools/tidb-binlog/driver/reader"
)

// drainer -> kafka, syn data from kafka to downstream TiDB, and run the dailytest

var (
	kafkaAddr = flag.String("kafkaAddr", "127.0.0.1:9092", "kafkaAddr like 127.0.0.1:9092,127.0.0.1:9093")
	topic     = flag.String("topic", "", "topic name to consume binlog")
	offset    = flag.Int64("offset", sarama.OffsetNewest, "offset")
	commitTS  = flag.Int64("commitTS", 0, "commitTS")
)

func main() {
	flag.Parse()
	log.S().Debug("start run kafka test...")

	cfg := &reader.Config{
		KafkaAddr: strings.Split(*kafkaAddr, ","),
		Offset:    *offset,
		CommitTS:  *commitTS,
		Topic:     *topic,
	}

	breader, err := reader.NewReader(cfg)
	if err != nil {
		panic(err)
	}

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

	// start sync to mysql from kafka
	ld, err := loader.NewLoader(sinkDB, loader.WorkerCount(16), loader.BatchSize(128))
	if err != nil {
		panic(err)
	}

	go func() {
		err := ld.Run()
		if err != nil {
			log.S().Error(errors.ErrorStack(err))
			log.S().Fatal(err)
		}
	}()

	go func() {
		defer ld.Close()
		for {
			select {
			case msg := <-breader.Messages():
				str := msg.Binlog.String()
				log.S().Debugf("recv: %.2000s", str)
				ld.Input() <- loader.SlaveBinlogToTxn(msg.Binlog)
			case txn := <-ld.Successes():
				log.S().Debug("succ: ", txn)
			}
		}
	}()

	time.Sleep(5 * time.Second)

	dailytest.RunMultiSource(sourceDBs, sinkDBForDiff, "test")
	dailytest.Run(sourceDBs[0], sinkDBForDiff, "test", 10, 1000, 10)
}
