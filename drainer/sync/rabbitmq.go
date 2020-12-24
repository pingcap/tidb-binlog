package sync

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/proto/go-binlog"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

// RabbitMQSyncer sync data to rabbitmq
type RabbitMQSyncer struct {
	channel  *amqp.Channel
	client   *amqp.Connection
	exchange string
	*baseSyncer
}

// NewRabbitmqSyncer new RabbitMQSyncer driver
func NewRabbitmqSyncer(cfg *DBConfig, tableInfoGetter translator.TableInfoGetter) (*RabbitMQSyncer, error) {
	client, err := amqp.Dial(cfg.RabbitMQAddr)
	if err != nil {
		return nil, err
	}
	channel, err := client.Channel()
	if err != nil {
		return nil, err
	}
	err = channel.ExchangeDeclare(
		cfg.RabbitMQExchange,
		cfg.RabbitMQExchangeType,
		cfg.RabbitMQDurable,
		cfg.RabbitMQAutoDelete,
		cfg.RabbitMQInternal,
		cfg.RabbitMQNoWait,
		nil,
	)
	if err != nil {
		return nil, err
	}
	return &RabbitMQSyncer{
		channel:    channel,
		client:     client,
		exchange:   cfg.RabbitMQExchange,
		baseSyncer: newBaseSyncer(tableInfoGetter),
	}, nil
}

// SetSafeMode should be ignore by RabbitMQSyncer
func (p *RabbitMQSyncer) SetSafeMode(mode bool) bool {
	return false
}

// Sync implements Syncer interface
func (p *RabbitMQSyncer) Sync(item *Item) error {
	secondaryBinlog, err := translator.TiBinlogToSecondaryBinlog(p.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue)
	if err != nil {
		return errors.Trace(err)
	}

	err = p.saveBinlog(secondaryBinlog, item)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (p *RabbitMQSyncer) saveBinlog(binlog *obinlog.Binlog, item *Item) error {
	binlogBuf, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("publishing msg to rabbitmq.")
	err = p.channel.Publish(p.exchange, "", false, false, amqp.Publishing{
		Headers:      amqp.Table{},
		Body:         binlogBuf,
		DeliveryMode: amqp.Persistent,
		Priority:     0,
	})
	if err != nil {
		p.setErr(err)
		return errors.Trace(err)
	}
	log.Info("publishing msg to rabbitmq finished.", zap.Int64("commit-ts", binlog.GetCommitTs()))
	p.success <- item
	return nil
}

// Close implements Syncer interface
func (p *RabbitMQSyncer) Close() error {
	// close rabbitmq channel && client connection
	p.channel.Close()
	p.client.Close()
	err := <-p.Error()

	return err
}
