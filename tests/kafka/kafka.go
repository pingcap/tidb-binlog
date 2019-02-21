package main

import (
	"flag"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
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
	log.Debug("start run kafka test...")

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

	sourceDB, err := util.CreateSourceDB()
	if err != nil {
		panic(err)
	}

	sinkDB, err := util.CreateSinkDB()
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
			log.Error(errors.ErrorStack(err))
			log.Fatal(err)
		}
	}()

	go func() {
		defer ld.Close()
		for {
			select {
			case msg := <-breader.Messages():
				str := msg.Binlog.String()
				if len(str) > 2000 {
					str = str[:2000] + "..."
				}
				log.Debug("recv: ", str)
				binlog := msg.Binlog
				ld.Input() <- loader.SlaveBinlogToTxn(binlog)
			case txn := <-ld.Successes():
				log.Debug("succ: ", txn)
			}
		}
	}()

	time.Sleep(5 * time.Second)

	dailytest.Run(sourceDB, sinkDB, "test", 10, 1000, 10)
}
