package main

import (
	"flag"
	"math"
	"os"
	"strings"

	"github.com/ngaut/log"

	"github.com/Shopify/sarama"
	"github.com/pingcap/tidb-binlog/pkg/util"
	pb "github.com/pingcap/tipb/go-binlog"
)

var (
	kafkaAddr = flag.String("kafkaAddr", "127.0.0.1:9092", "kafkaAddr like 127.0.0.1:9092,127.0.0.1:9093")
	version   = flag.String("version", "0.8.2.0", "kafka version")
	offset    = flag.Int64("offset", sarama.OffsetNewest, "offset")
	endOffset = flag.Int64("end-offset", 1892458687, "offset")
	limit     = flag.Int64("limit", 20000000, "kafka message number limit")

	topic = flag.String("topic", "6502327210476397260_10-30-1-9_8250", "topic name")
)

func main() {
	sarama.MaxResponseSize = math.MaxInt32

	flag.Parse()

	con, err := util.CreateKafkaConsumer(strings.Split(*kafkaAddr, ","), *version)
	if err != nil {
		log.Fatal(err)
	}

	pc, err := con.ConsumePartition(*topic, 0, *offset)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for e := range pc.Errors() {
			log.Debugf("err %+v", e)
		}
	}()

	var count int64 = 0
	for {
		for msg := range pc.Messages() {
			log.Debugf("offset %+v", msg.Offset)
			b := new(pb.Binlog)
			err := b.Unmarshal(msg.Value)
			if err != nil {
				log.Fatalf("unmarshal message  %+v error %v", msg.Value, err)
			}

			if msg.Offset >= *endOffset {
				log.Infof("finished at offset %d", msg.Offset)
				os.Exit(0)
			}
			count++
			if count >= *limit {
				log.Infof("finished at offset %d", msg.Offset)
				os.Exit(0)
			}
		}
	}

}
