package pump

import (
	"time"

	"github.com/juju/errors"
)

type kafkaToCache struct {
	master Binlogger
	slave  Binlogger
}

func newKafkaToCache() *kafkaToCache {
	return &kafkaToCache{}
}

func (kc *kafkaToCache) init(cid string, node string, addrs []string) error {
	var err error
	kc.master, err = createKafkaBinlogger(cid, node, addrs)
	if err != nil {
		return errors.Trace(err)
	}

	kc.slave, err = createCacheBinlogger()
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (kc *kafkaToCache) writeTail(binlogger Binlogger, payload []byte) error {
	if !binlogger.WriteAvailable() {
		return kc.switchSlave(payload)
	}

	if err := binlogger.WriteTail(payload); err != nil {
		if isOpenConvert {
			return kc.switchSlave(payload)
		}
	}

	return errors.Trace(err)
}

func (kc *kafkaToCache) switchSlave(payload []byte) error {
	if err := kc.slave.WriteTail(payload); err != nil {
		return errors.Trace(err)
	}

	select {
	case <-time.After(genBinlogInterval):
		go kc.writeTailSeq()
	}

	return nil
}

func (kc *kafkaToCache) writeTailSeq() {
	slave := kc.slave.(*cacheBinloger)
	if slave.getLen() == 0 {
		return
	}

	payload := slave.getPeek()

	err := kc.master.WriteTail(payload.([]byte))
	if err == nil {
		slave.deQueue()

		for slave.getLen() > 0 {
			payload = slave.deQueue()
			kc.master.WriteTail(payload.([]byte))
		}
		kc.master.(*kafkaBinloger).closed = false
	}
}
