package cistern

import (
	"fmt"
	"os"
	"path"
	"sort"
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/util/codec"
)

// BinlogStorage stores binlogs
type BinlogStorage struct {
	metaStore store.Store
	maskShift uint
	dataDir   string
	nosync    bool
	mu        struct {
		sync.Mutex
		segments   map[int64]store.Store
		segmentTSs segmentRange
	}
}

// DS is the BinlogStorage instance
var DS *BinlogStorage

// InitTest initials test data
func InitTest(bn, sn []byte, metaStore store.Store, dataDir string, maskShift uint, nosync bool) error {
	binlogNamespace = bn
	segmentNamespace = sn
	return InitBinlogStorage(metaStore, dataDir, maskShift, nosync)
}

// InitBinlogStorage initials the BinlogStorage
func InitBinlogStorage(metaStore store.Store, dataDir string, maskShift uint, nosync bool) error {
	ds := &BinlogStorage{
		metaStore: metaStore,
		maskShift: maskShift,
		dataDir:   dataDir,
		nosync:    nosync,
	}
	ds.mu.segments = make(map[int64]store.Store)

	// initial segment infomation
	err := metaStore.Scan(segmentNamespace, nil, func(key []byte, val []byte) (bool, error) {
		_, segmentTS, err := codec.DecodeInt(key)
		if err != nil {
			return false, errors.Trace(err)
		}

		storagePath := string(val)
		s, err := store.NewBoltStore(storagePath, [][]byte{binlogNamespace}, nosync)
		if err != nil {
			return false, errors.Annotatef(err, "failed to open BoltDB store at %s", path.Join(dataDir, storagePath))
		}

		ds.mu.segments[segmentTS] = s
		ds.mu.segmentTSs = append(ds.mu.segmentTSs, segmentTS)
		return true, nil
	})
	if err != nil && !errors.IsNotFound(err) {
		return errors.Trace(err)
	}

	sort.Sort(ds.mu.segmentTSs)
	DS = ds
	return nil
}

// Get return the value by the given key
func (ds *BinlogStorage) Get(ts int64) ([]byte, error) {
	segment, ok := ds.getSegment(ts)
	if !ok {
		return nil, errors.NotFoundf("binlog has key %d", ts)
	}

	return segment.Get(binlogNamespace, codec.EncodeInt([]byte{}, ts))
}

// Put puts the key/value in the store
func (ds *BinlogStorage) Put(ts int64, payload []byte) error {
	var err error
	segment, ok := ds.getSegment(ts)
	if !ok {
		segment, err = ds.createSegment(ts)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return segment.Put(binlogNamespace, codec.EncodeInt([]byte{}, ts), payload)
}

// Scan scans the key by given function
func (ds *BinlogStorage) Scan(startTS int64, f func([]byte, []byte) (bool, error)) error {
	startSegmentTS := ds.segmentTS(startTS)
	var isEnd bool
	scanFunc := func(key, val []byte) (bool, error) {
		valid, err := f(key, val)
		if err != nil {
			return false, errors.Trace(err)
		}
		if !valid {
			isEnd = true
		}
		return valid, err
	}

	var segmentTSs segmentRange
	ds.mu.Lock()
	copy(segmentTSs, ds.mu.segmentTSs)
	ds.mu.Unlock()
	for _, segmentTS := range segmentTSs {
		if segmentTS < startSegmentTS {
			continue
		} else {
			var key []byte
			if segmentTS == startSegmentTS {
				key = codec.EncodeInt([]byte{}, startTS)
			}
			segment, ok := ds.getSegment(segmentTS)
			if !ok {
				return errors.NotFoundf("segment %d is corruption", segmentTS)
			}
			err := segment.Scan(binlogNamespace, key, scanFunc)
			if err != nil || isEnd {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// EndKey get the latest key
func (ds *BinlogStorage) EndKey() ([]byte, error) {
	ds.mu.Lock()
	if len(ds.mu.segmentTSs) == 0 {
		ds.mu.Unlock()
		return nil, nil
	}
	segmentTS := ds.mu.segmentTSs[len(ds.mu.segmentTSs)-1]
	segment, ok := ds.mu.segments[segmentTS]
	if !ok {
		return nil, errors.NotFoundf("segment %d is corruption", segmentTS)
	}
	ds.mu.Unlock()
	return segment.EndKey(binlogNamespace)
}

// Commit commit the batch operators
func (ds *BinlogStorage) Commit(b *Batch) error {
	var err error
	batchs := make(map[int64]store.Batch)
	segments := make(map[int64]store.Store)
	for _, w := range b.writes {
		segment, ok := ds.getSegment(w.key)
		if !ok {
			segment, err = ds.createSegment(w.key)
			if err != nil {
				return errors.Trace(err)
			}
		}
		ts := ds.segmentTS(w.key)
		bt, ok := batchs[ts]
		if !ok {
			bt = segment.NewBatch()
		}
		batchs[ts] = bt
		segments[ts] = segment
		bt.Put(codec.EncodeInt([]byte{}, w.key), w.value)
	}

	for ts := range batchs {
		err = segments[ts].Commit(binlogNamespace, batchs[ts])
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// Purge gc the binlogs
func (ds *BinlogStorage) Purge(ts int64) error {
	startSegmentTS := ds.segmentTS(ts)
	b := ds.metaStore.NewBatch()
	ds.mu.Lock()
	var index int
	for i, ts := range ds.mu.segmentTSs {
		if ts >= startSegmentTS {
			break
		}

		segament, ok := ds.mu.segments[ts]
		if !ok {
			return errors.NotFoundf("sgement %d, but it should be found", ts)
		}
		segament.Close()
		segamentPath := path.Join(ds.dataDir, fmt.Sprintf("binlog-%d.data", ts))
		err := os.Remove(segamentPath)
		if err != nil {
			return errors.Trace(err)
		}
		b.Delete(codec.EncodeInt([]byte{}, ts))
		index = i
	}

	for i := 0; i <= index; i++ {
		delete(ds.mu.segments, ds.mu.segmentTSs[i])
	}
	ds.mu.segmentTSs = ds.mu.segmentTSs[index:]
	ds.mu.Unlock()

	err := ds.metaStore.Commit(segmentNamespace, b)
	return errors.Trace(err)
}

// Close closes all boltdbs
func (ds *BinlogStorage) Close() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	for _, segement := range ds.mu.segments {
		err := segement.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// NewBatch implements the NewBatch() interface of Store
func (ds *BinlogStorage) NewBatch() *Batch {
	return &Batch{}
}

type write struct {
	key      int64
	value    []byte
	isDelete bool
}

// Batch wraps the operations
type Batch struct {
	writes []write
}

// Put puts one opration in the batch
func (b *Batch) Put(key int64, value []byte) {
	w := write{
		key:   key,
		value: append([]byte(nil), value...),
	}
	b.writes = append(b.writes, w)
}

func (ds *BinlogStorage) getSegment(ts int64) (store.Store, bool) {
	ds.mu.Lock()
	segment, ok := ds.mu.segments[ds.segmentTS(ts)]
	ds.mu.Unlock()
	return segment, ok
}

func (ds *BinlogStorage) createSegment(ts int64) (store.Store, error) {
	segmentTS := ds.segmentTS(ts)
	segmentPath := path.Join(ds.dataDir, fmt.Sprintf("binlog-%d.data", segmentTS))
	segment, err := store.NewBoltStore(segmentPath, [][]byte{binlogNamespace}, ds.nosync)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to open BoltDB store at %s", segmentPath)
	}
	ds.mu.Lock()
	ds.mu.segments[segmentTS] = segment
	ds.mu.segmentTSs = append(ds.mu.segmentTSs, segmentTS)
	sort.Sort(ds.mu.segmentTSs)
	ds.metaStore.Put(segmentNamespace, codec.EncodeInt([]byte{}, segmentTS), []byte(segmentPath))
	ds.mu.Unlock()
	return segment, nil
}

func (ds *BinlogStorage) segmentTS(ts int64) int64 {
	return ts >> ds.maskShift
}

type segmentRange []int64

func (a segmentRange) Len() int           { return len(a) }
func (a segmentRange) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a segmentRange) Less(i, j int) bool { return a[i] < a[j] }
