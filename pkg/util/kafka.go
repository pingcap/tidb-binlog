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

	config.ClientID = "tidb_binlog"
	config.Version = version
	log.Debugf("kafka consumer version %v", version)
	config.MetricRegistry = metrics.NewPrefixedChildRegistry(GetParentMetricsRegistry(), metricsPrefix)

	return config, nil
}

// CreateKafkaConsumer creates a kafka consumer
func CreateKafkaConsumer(kafkaAddrs []string, kafkaVersion string) (sarama.Consumer, error) {
	kafkaCfg, err := NewSaramaConfig(kafkaVersion, "drainer.")
	if err != nil {
		return nil, errors.Trace(err)
	}

	kafkaCfg.Consumer.Return.Errors = true

	return sarama.NewConsumer(kafkaAddrs, kafkaCfg)
}
