package cistern

import (
	"fmt"
	"os"
	"path"
	"sort"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/util/codec"
)

var codecEncodeZero = codec.EncodeInt([]byte{}, 0)
var binlogNamespace = []byte("binlog")

// BinlogStorage used to store binlogs
type BinlogStorage struct {
	metaStore store.Store
	maskShift uint
	dataDir   string
	nosync    bool
	mu        struct {
		sync.Mutex
		// segmentTS -> boltdb
		segments map[int64]store.Store
		// segmentTSs that sorted
		segmentTSs segmentRange
	}
}

// InitBinlogStorage initials the BinlogStorage
func NewBinlogStorage(metaStore store.Store, dataDir string, maskShift uint, nosync bool) (*BinlogStorage, error) {
	ds := &BinlogStorage{
		metaStore: metaStore,
		maskShift: maskShift,
		dataDir:   dataDir,
		nosync:    nosync,
	}
	ds.mu.segments = make(map[int64]store.Store)

	// initial segments' infomation
	err := metaStore.Scan(segmentNamespace, nil, func(key []byte, val []byte) (bool, error) {
		_, segmentTS, err := codec.DecodeInt(key)
		if err != nil {
			return false, errors.Trace(err)
		}

		// open the boltdb by give storagePath
		storagePath := string(val)
		s, err := store.NewBoltStore(storagePath, [][]byte{binlogNamespace}, nosync)
		if err != nil {
			return false, errors.Annotatef(err, "failed to open BoltDB store at %s", path.Join(dataDir, storagePath))
		}

		// add into ds.mu.segments and du.mu.segmentTSs
		ds.mu.segments[segmentTS] = s
		ds.mu.segmentTSs = append(ds.mu.segmentTSs, segmentTS)
		return true, nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	sort.Sort(ds.mu.segmentTSs)
	return ds, nil
}

// Get returns the value by the given key
func (ds *BinlogStorage) Get(ts int64) ([]byte, error) {
	// get the corresponding segment
	segment, ok := ds.getSegment(ts)
	if !ok {
		return nil, errors.NotFoundf("binlog has key %d", ts)
	}

	return segment.Get(binlogNamespace, codec.EncodeInt([]byte{}, ts))
}

// Put puts the key/value in the store
func (ds *BinlogStorage) Put(ts int64, payload []byte) error {
	var err error
	// get the corresponding segment
	// if not found, open it
	segment, ok := ds.getSegment(ts)
	if !ok {
		segment, err = ds.createSegment(ts)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return segment.Put(binlogNamespace, codec.EncodeInt([]byte{}, ts), payload)
}

// Scan scans the key and exec given function
func (ds *BinlogStorage) Scan(startTS int64, f func([]byte, []byte) (bool, error)) error {
	startSegmentTS := ds.segmentTS(startTS)
	var isEnd bool
	// wrap the operation in the scan to make it can cross multiple boltdbs
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

	var segments = make(map[int64]store.Store)
	ds.mu.Lock()
	var segmentTSs = make(segmentRange, 0, len(ds.mu.segmentTSs))
	for _, ts := range ds.mu.segmentTSs {
		segmentTSs = append(segmentTSs, ts)
		segments[ts] = ds.mu.segments[ts]
	}
	ds.mu.Unlock()
	for _, segmentTS := range segmentTSs {
		if segmentTS < startSegmentTS {
			continue
		} else {
			var key []byte
			if segmentTS == startSegmentTS {
				key = codec.EncodeInt([]byte{}, startTS)
			}
			segment, ok := segments[segmentTS]
			if !ok {
				return errors.NotFoundf("segment %d is corruption", segmentTS)
			}
			// we can view the isEnd flag to determind to scan next boltdb
			err := segment.Scan(binlogNamespace, key, scanFunc)
			if err != nil || isEnd {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// EndKey gets the latest key
func (ds *BinlogStorage) EndKey() ([]byte, error) {
	ds.mu.Lock()
	if len(ds.mu.segmentTSs) == 0 {
		ds.mu.Unlock()
		return codecEncodeZero, nil
	}
	segmentTS := ds.mu.segmentTSs[len(ds.mu.segmentTSs)-1]
	segment, ok := ds.mu.segments[segmentTS]
	if !ok {
		ds.mu.Unlock()
		return nil, errors.NotFoundf("segment %d is corruption", segmentTS)
	}
	ds.mu.Unlock()
	return segment.EndKey(binlogNamespace)
}

// Commit commits the batch operators
func (ds *BinlogStorage) Commit(b *Batch) error {
	var err error
	batchs := make(map[int64]store.Batch)
	segments := make(map[int64]store.Store)
	// firstly, dispatch into different segment
	for _, w := range b.writes {
		ts := ds.segmentTS(w.key)
		segment, ok := ds.getSegment(w.key)
		if !ok {
			segment, err = ds.createSegment(w.key)
			if err != nil {
				return errors.Trace(err)
			}
			segments[ts] = segment
		}
		bt, ok := batchs[ts]
		if !ok {
			bt = segment.NewBatch()
			batchs[ts] = bt
		}
		bt.Put(codec.EncodeInt([]byte{}, w.key), w.value)
	}

	// secondly, commit it one by one
	for ts := range batchs {
		err = segments[ts].Commit(binlogNamespace, batchs[ts])
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// Purge gcs the binlogs
func (ds *BinlogStorage) Purge(ts int64) error {
	startSegmentTS := ds.segmentTS(ts)
	log.Infof("purge boltdb files that before %d", ts)
	b := ds.metaStore.NewBatch()
	ds.mu.Lock()
	var index int
	for i, ts := range ds.mu.segmentTSs {
		// gc until to the biger or equal ts
		if ts >= startSegmentTS {
			break
		}

		segment, ok := ds.mu.segments[ts]
		if !ok {
			return errors.NotFoundf("sgement %d, but it should be found", ts)
		}
		err := segment.Close()
		if err != nil {
			return errors.Errorf("can't close segement %d", ts)
		}
		segmentPath := path.Join(ds.dataDir, fmt.Sprintf("%d", clusterID), fmt.Sprintf("binlog-%d.data", ts))
		err = os.Remove(segmentPath)
		if err != nil {
			return errors.Trace(err)
		}
		b.Delete(codec.EncodeInt([]byte{}, ts))
		index = i
	}

	for i := 0; i <= index; i++ {
		delete(ds.mu.segments, ds.mu.segmentTSs[i])
	}
	ds.mu.segmentTSs = ds.mu.segmentTSs[index+1:]
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

// NewBatch return a Batch object
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

// Put puts one operation in the batch
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
	segmentPath := path.Join(ds.dataDir, fmt.Sprintf("%d", clusterID), fmt.Sprintf("binlog-%d.data", segmentTS))
	segment, err := store.NewBoltStore(segmentPath, [][]byte{binlogNamespace}, ds.nosync)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to open BoltDB store, path %s", segmentPath)
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
