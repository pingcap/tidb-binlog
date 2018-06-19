package util

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"
)

const (
	maxRetry      = 12
	retryInterval = 5 * time.Second
)

// CreateKafkaProducer create a sync producer
func CreateKafkaProducer(config *sarama.Config, addr []string, kafkaVersion string, maxMsgSize int, metricsPrefix string) (sarama.SyncProducer, error) {
	var (
		client sarama.SyncProducer
		err    error
	)

	// initial kafka client to use manual partitioner
	if config == nil {
		config = sarama.NewConfig()
	}
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.MaxMessageBytes = maxMsgSize
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	version, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		return nil, errors.Trace(err)
	}
	config.Version = version
	config.MetricRegistry = metrics.NewPrefixedRegistry(metricsPrefix)
	exp.Exp(config.MetricRegistry)

	log.Infof("kafka producer version %v", version)
	for i := 0; i < maxRetry; i++ {
		client, err = sarama.NewSyncProducer(addr, config)
		if err != nil {
			log.Errorf("create kafka client error %v", err)
			time.Sleep(retryInterval)
			continue
		}
		return client, nil
	}

	return nil, errors.Trace(err)
}
