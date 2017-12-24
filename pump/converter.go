package pump

import (
    "time"

    "github.com/juju/errors"
)


type kafkaToCache struct {
    kMaster  Binlogger
    cSlave   Binlogger
}

func newKafkaToCache()*kafkaToCache {
    return &kafkaToCache{}
}

func (kc *kafkaToCache)init(cid string, node string, addrs []string) error {
    var err error
    kc.kMaster, err = createKafkaBinlogger(cid, node, addrs)
    if err != nil {
        return errors.Trace(err)
    }
    
    kc.cSlave, err = createCacheBinlogger()
    if err != nil {
        return errors.Trace(err)
    }

    return nil
}

func (kc *kafkaToCache) switchSlave(payload []byte) error {
    if err := kc.cSlave.WriteTail(payload); err != nil {
        return errors.Trace(err)
    }
    
    select {
    case <-time.After(genBinlogInterval):
        go kc.writeTailSeq()
    }

    return nil
}

func (kc *kafkaToCache) writeTailSeq() {
    slave := kc.cSlave.(*cacheBinloger)
    if slave.getLen() == 0 {
        return
    }

    payload := slave.getPeek()

    err := kc.kMaster.WriteTail(payload.([]byte))
    if err == nil {
        slave.deQueue()

        for slave.getLen() > 0 {
            payload = slave.deQueue()
            kc.kMaster.WriteTail(payload.([]byte))
        }
        kc.kMaster.(*kafkaBinloger).closed = false
    }
}
