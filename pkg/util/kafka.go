package util

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
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
