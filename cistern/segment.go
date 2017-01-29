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
	bs := &BinlogStorage{
		metaStore: metaStore,
		maskShift: maskShift,
		dataDir:   dataDir,
		nosync:    nosync,
	}
	bs.mu.segments = make(map[int64]store.Store)

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
		bs.mu.segments[segmentTS] = s
		bs.mu.segmentTSs = append(bs.mu.segmentTSs, segmentTS)
		return true, nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	sort.Sort(bs.mu.segmentTSs)
	return bs, nil
}

func (bs *BinlogStorage) Meta() store.Store {
	return bs.metaStore
}

// Get returns the value by the given key
func (bs *BinlogStorage) Get(ts int64) ([]byte, error) {
	// get the corresponding segment
	segment, ok := bs.getSegment(ts)
	if !ok {
		return nil, errors.NotFoundf("binlog has key %d", ts)
	}

	return segment.Get(binlogNamespace, codec.EncodeInt([]byte{}, ts))
}

// Put puts the key/value in the store
func (bs *BinlogStorage) Put(ts int64, payload []byte) error {
	var err error
	// get the corresponding segment
	// if not found, open it
	segment, ok := bs.getSegment(ts)
	if !ok {
		segment, err = bs.createSegment(ts)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return segment.Put(binlogNamespace, codec.EncodeInt([]byte{}, ts), payload)
}

// Scan scans the key and exec given function
func (bs *BinlogStorage) Scan(startTS int64, f func([]byte, []byte) (bool, error)) error {
	startSegmentTS := bs.segmentTS(startTS)
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
	bs.mu.Lock()
	var segmentTSs = make(segmentRange, 0, len(bs.mu.segmentTSs))
	for _, ts := range bs.mu.segmentTSs {
		segmentTSs = append(segmentTSs, ts)
		segments[ts] = bs.mu.segments[ts]
	}
	bs.mu.Unlock()
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
func (bs *BinlogStorage) EndKey() ([]byte, error) {
	bs.mu.Lock()
	if len(bs.mu.segmentTSs) == 0 {
		bs.mu.Unlock()
		return codecEncodeZero, nil
	}
	segmentTS := bs.mu.segmentTSs[len(bs.mu.segmentTSs)-1]
	segment, ok := bs.mu.segments[segmentTS]
	if !ok {
		bs.mu.Unlock()
		return nil, errors.NotFoundf("segment %d is corruption", segmentTS)
	}
	bs.mu.Unlock()
	return segment.EndKey(binlogNamespace)
}

// Commit commits the batch operators
func (bs *BinlogStorage) Commit(b *Batch) error {
	var err error
	batches := make(map[int64]store.Batch)
	segments := make(map[int64]store.Store)
	// firstly, dispatch into different segment
	for _, w := range b.writes {
		ts := bs.segmentTS(w.key)
		segment, ok := segments[ts]
		if !ok {
			segment, ok = bs.getSegment(w.key)
			if !ok {
				segment, err = bs.createSegment(w.key)
				if err != nil {
					return errors.Trace(err)
				}
			}
			segments[ts] = segment
		}

		bt, ok := batches[ts]
		if !ok {
			bt = segment.NewBatch()
			batches[ts] = bt
		}
		bt.Put(codec.EncodeInt([]byte{}, w.key), w.value)
	}

	// secondly, commit it one by one
	for ts := range batches {
		err = segments[ts].Commit(binlogNamespace, batches[ts])
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// Purge gcs the binlogs
func (bs *BinlogStorage) Purge(ts int64) error {
	startSegmentTS := bs.segmentTS(ts)
	log.Infof("purge boltdb files that before %d", ts)
	b := bs.metaStore.NewBatch()
	bs.mu.Lock()
	var index int
	for i, ts := range bs.mu.segmentTSs {
		// gc until to the biger or equal ts
		if ts >= startSegmentTS {
			break
		}

		segment, ok := bs.mu.segments[ts]
		if !ok {
			return errors.NotFoundf("sgement %d, but it should be found", ts)
		}
		err := segment.Close()
		if err != nil {
			return errors.Errorf("can't close segement %d", ts)
		}
		segmentPath := path.Join(bs.dataDir, fmt.Sprintf("%d", clusterID), fmt.Sprintf("binlog-%d.data", ts))
		err = os.Remove(segmentPath)
		if err != nil {
			return errors.Trace(err)
		}
		b.Delete(codec.EncodeInt([]byte{}, ts))
		index = i
	}

	for i := 0; i <= index; i++ {
		delete(bs.mu.segments, bs.mu.segmentTSs[i])
	}
	bs.mu.segmentTSs = bs.mu.segmentTSs[index+1:]
	bs.mu.Unlock()

	err := bs.metaStore.Commit(segmentNamespace, b)
	return errors.Trace(err)
}

// Close closes all boltdbs
func (bs *BinlogStorage) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	for _, segement := range bs.mu.segments {
		err := segement.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// NewBatch return a Batch object
func (bs *BinlogStorage) NewBatch() *Batch {
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

func (bs *BinlogStorage) getSegment(ts int64) (store.Store, bool) {
	bs.mu.Lock()
	segment, ok := bs.mu.segments[bs.segmentTS(ts)]
	bs.mu.Unlock()
	return segment, ok
}

func (bs *BinlogStorage) createSegment(ts int64) (store.Store, error) {
	segmentTS := bs.segmentTS(ts)
	segmentPath := path.Join(bs.dataDir, fmt.Sprintf("%d", clusterID), fmt.Sprintf("binlog-%d.data", segmentTS))
	segment, err := store.NewBoltStore(segmentPath, [][]byte{binlogNamespace}, bs.nosync)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to open BoltDB store, path %s", segmentPath)
	}
	bs.mu.Lock()
	bs.mu.segments[segmentTS] = segment
	bs.mu.segmentTSs = append(bs.mu.segmentTSs, segmentTS)
	sort.Sort(bs.mu.segmentTSs)
	bs.metaStore.Put(segmentNamespace, codec.EncodeInt([]byte{}, segmentTS), []byte(segmentPath))
	bs.mu.Unlock()
	return segment, nil
}

func (bs *BinlogStorage) segmentTS(ts int64) int64 {
	return ts >> bs.maskShift
}

type segmentRange []int64

func (a segmentRange) Len() int           { return len(a) }
func (a segmentRange) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a segmentRange) Less(i, j int) bool { return a[i] < a[j] }
