package cistern

import (
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/codec"
)

// GCHistoryBinlog recycle old binlog data in the store.
// duration indicates for how long binlog will be preserve.
func GCHistoryBinlog(s *BinlogStorage, duration time.Duration) error {
	endkey, err := s.EndKey()
	if err != nil {
		return errors.Trace(err)
	}
	if endkey == nil {
		// skip gc if no data exist.
		return nil
	}

	_, ts, err := codec.DecodeInt(endkey)
	if err != nil {
		return errors.Trace(err)
	}

	physical := oracle.ExtractPhysical(uint64(ts))
	prevPhysical := physical - int64(duration/1e6)
	gcToTS := int64(oracle.ComposeTS(prevPhysical, 0))

	log.Infof("GC binlog older than timestamp: %v, until date: %v", gcToTS, time.Unix(prevPhysical/1000, (prevPhysical%1000)*1e6))
	if gcToTS <= 0 {
		gcToTS = 0
	} else {
		err = s.Purge(gcToTS)
	}
	log.Infof("FINISHED! GC binlog older than timestamp: %v, until date: %v", gcToTS, time.Unix(prevPhysical/1000, (prevPhysical%1000)*1e6))
	return errors.Trace(err)
}
