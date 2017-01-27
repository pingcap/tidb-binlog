package cistern

import (
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/codec"
)

// GCHistoryBinlog recycle old binlog data in the store.
// days indicates for how long binlog will be preserve.
func GCHistoryBinlog(store store.Store, ns []byte, duration time.Duration) error {
	endkey, err := store.EndKey(ns)
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
	gcToTS := oracle.ComposeTS(prevPhysical, 0)
	if gcToTS < 0 {
		gcToTS = 0
	}

	log.Infof("GC binlog older than timestamp: %v, until date: %v", gcToTS, time.Unix(prevPhysical/1000, (prevPhysical%1000)*1e6))

	var done bool
	if !done {
		batch := store.NewBatch()
		num := 0
		err = store.Scan(ns, nil, func(key []byte, val []byte) (bool, error) {
			_, cts, err1 := codec.DecodeInt(key)
			if err1 != nil {
				return false, errors.Trace(err1)
			}
			if uint64(cts) > gcToTS {
				done = true
				return false, nil
			}
			if uint64(cts) == gcToTS {
				return true, nil
			}
			batch.Delete(key)
			num += 1
			if num > 10000 {
				return false, nil
			}
			return true, nil
		})
		if err != nil {
			return errors.Trace(err)
		}

		err = store.Commit(ns, batch)
		if err != nil {
			return errors.Trace(err)
		}

		time.Sleep(50 * time.Millisecond)
	}

	log.Infof("FINISHED! GC binlog older than timestamp: %v, until date: %v", gcToTS, time.Unix(prevPhysical/1000, (prevPhysical%1000)*1e6))
	return nil
}

