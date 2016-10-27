package cistern

import (
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/codec"
)

// GColdBinLog recycle old binlog data in the store.
// days indicates for how long binlog will be preserve.
func GColdBinLog(store store.Store, ns []byte, days time.Duration) error {
	endkey, err := store.EndKey(ns)
	if err != nil {
		return errors.Trace(err)
	}

	v, err := store.Get(ns, endkey)
	if err != nil {
		return errors.Trace(err)
	}

	_, ts, err := codec.DecodeInt(v)
	if err != nil {
		return errors.Trace(err)
	}

	physical := oracle.ExtractPhysical(uint64(ts))
	prevPhysical := physical - int64(days*24*time.Hour/time.Millisecond)
	gcToTS := oracle.ComposeTS(prevPhysical, 0)
	if gcToTS < 0 {
		gcToTS = 0
	}

	batch := store.NewBatch()
	err = store.Scan(ns, nil, func(key []byte, val []byte) (bool, error) {
		_, cts, err := codec.DecodeInt(key)
		if err != nil {
			return false, errors.Trace(err)
		}
		if uint64(cts) > gcToTS {
			return false, nil
		}
		if uint64(cts) == gcToTS {
			return true, nil
		}
		batch.Delete(key)
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	err = store.Commit(ns, batch)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
