package util

import (
	"sync"
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

// don't use directly, call GetParentMetricsRegistry to get it
var metricRegistry metrics.Registry
var metricRegistryOnce sync.Once

func initMetrics() {
	metricRegistry = metrics.NewRegistry()
	// can't call Exp multi time
	exp.Exp(metricRegistry)
}

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
	config.MetricRegistry = metrics.NewPrefixedChildRegistry(GetParentMetricsRegistry(), metricsPrefix)

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

// GetParentMetricsRegistry get the metrics registry and expose the metrics while /debug/metrics
func GetParentMetricsRegistry() metrics.Registry {
	metricRegistryOnce.Do(initMetrics)
	return metricRegistry
}

// NewSaramaConfig return the default config and set the according version and metrics
func NewSaramaConfig(kafkaVersion string, metricsPrefix string) (*sarama.Config, error) {
	config := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		return nil, errors.Trace(err)
	}
	config.Version = version
	config.MetricRegistry = metrics.NewPrefixedChildRegistry(GetParentMetricsRegistry(), metricsPrefix)

	return config, nil
}

// CreateKafkaConsumer creates a kafka consumer
func CreateKafkaConsumer(kafkaAddrs []string, kafkaVersion string) (sarama.Consumer, error) {
	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Consumer.Return.Errors = true

	version, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		return nil, errors.Trace(err)
	}
	kafkaCfg.Version = version
	log.Infof("kafka consumer version %v", version)

	registry := GetParentMetricsRegistry()
	kafkaCfg.MetricRegistry = metrics.NewPrefixedChildRegistry(registry, "drainer.")

	return sarama.NewConsumer(kafkaAddrs, kafkaCfg)
}

// NewNeverFailAsyncProducer will set the retry number  to a pretty high number, and you should treat fail
// when Producer.Successes don't return the msg after a reasonable time
func NewNeverFailAsyncProducer(addrs []string, config *sarama.Config) (sarama.AsyncProducer, error) {
	// maintain minimal set that has been necessary so far
	// this also avoid take too much time in NewAsyncProducer if kafka is down
	// because it will fetch metadata right away if setting Full = true, and we set
	// config.Metadata.Retry.Max to be a pretty high value
	// maybe when this issue if fixed: https://github.com/Shopify/sarama/issues/1145
	// we can avoid setting Metadata.Retry to be a pretty hight value too
	config.Metadata.Full = false
	config.Metadata.Retry.Max = 10000
	config.Metadata.Retry.Backoff = 500 * time.Millisecond

	// just set to a pretty high retry num, so we will not drop some msg and
	// continue to push the laster msg, server need to quit after a reasonable time
	// aim to avoid not continuity msg sent to kafka.. see: https://github.com/Shopify/sarama/issues/838
	config.Producer.Retry.Max = 10000
	config.Producer.Retry.Backoff = 500 * time.Millisecond

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(addrs, config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	go func() {
		for e := range producer.Errors() {
			log.Error(e.Err, e.Msg)
			log.Error("fail to push data to kafka after long time please check kafka is working")
			// quit ASAP, or we may miss some msg and have had sent the later msg successfully
			panic(e)
		}
	}()

	return producer, err
}
