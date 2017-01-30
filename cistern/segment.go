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
var shift uint = 37

// BinlogStorage used to store binlogs
type BinlogStorage struct {
	metaStore store.Store
	dataDir   string
	nosync    bool
	mu        struct {
		sync.Mutex
		// segmentKey -> boltdb
		segments map[int64]store.Store
		// segmentKeys that sorted
		segmentKeys segmentRange
	}
}

// NewBinlogStorage initials the BinlogStorage
func NewBinlogStorage(metaStore store.Store, dataDir string, nosync bool) (*BinlogStorage, error) {
	bs := &BinlogStorage{
		metaStore: metaStore,
		dataDir:   dataDir,
		nosync:    nosync,
	}
	bs.mu.segments = make(map[int64]store.Store)

	// initial segments' infomation
	err := metaStore.Scan(segmentNamespace, nil, func(key []byte, val []byte) (bool, error) {
		_, segmentKey, err := codec.DecodeInt(key)
		if err != nil {
			return false, errors.Trace(err)
		}

		// open the boltdb by give storagePath
		storagePath := string(val)
		s, err := store.NewBoltStore(storagePath, [][]byte{binlogNamespace}, nosync)
		if err != nil {
			return false, errors.Annotatef(err, "failed to open BoltDB store at %s", path.Join(dataDir, storagePath))
		}

		// add into bs.mu.segments and bs.mu.segmentTSs
		bs.mu.segments[segmentKey] = s
		bs.mu.segmentKeys = append(bs.mu.segmentKeys, segmentKey)
		return true, nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Infof("open segement stores successfully, total num: [%d], loaded keys: [%v]", len(bs.mu.segments), bs.mu.segmentKeys)
	sort.Sort(bs.mu.segmentKeys)
	return bs, nil
}

// Meta returns the BinlogStore's meta fd
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
	segment, err := bs.getOrCreateSegment(ts)
	if err != nil {
		return errors.Trace(err)
	}
	return segment.Put(binlogNamespace, codec.EncodeInt([]byte{}, ts), payload)
}

// Scan scans the key and exec given function
func (bs *BinlogStorage) Scan(startTS int64, f func([]byte, []byte) (bool, error)) error {
	startSegmentKey := bs.segmentKey(startTS)
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
	var segmentKeys = make(segmentRange, 0, len(bs.mu.segmentKeys))
	for _, key := range bs.mu.segmentKeys {
		segmentKeys = append(segmentKeys, key)
		segments[key] = bs.mu.segments[key]
	}
	bs.mu.Unlock()
	for _, segmentKey := range segmentKeys {
		if segmentKey < startSegmentKey {
			continue
		} else {
			var key []byte
			if segmentKey == startSegmentKey {
				key = codec.EncodeInt([]byte{}, startTS)
			}
			segment, ok := segments[segmentKey]
			if !ok {
				return errors.NotFoundf("segment %d is corruption", segmentKey)
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
	if len(bs.mu.segmentKeys) == 0 {
		bs.mu.Unlock()
		return codecEncodeZero, nil
	}
	segmentKey := bs.mu.segmentKeys[len(bs.mu.segmentKeys)-1]
	segment, ok := bs.mu.segments[segmentKey]
	if !ok {
		bs.mu.Unlock()
		return nil, errors.NotFoundf("segment %d is corruption", segmentKey)
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
		key := bs.segmentKey(w.key)
		segment, ok := segments[key]
		if !ok {
			segment, err = bs.getOrCreateSegment(w.key)
			if err != nil {
				return errors.Trace(err)
			}
			segments[key] = segment
		}

		bt, ok := batches[key]
		if !ok {
			bt = segment.NewBatch()
			batches[key] = bt
		}
		bt.Put(codec.EncodeInt([]byte{}, w.key), w.value)
	}

	// secondly, commit it one by one
	for key := range batches {
		err = segments[key].Commit(binlogNamespace, batches[key])
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// Purge gcs the binlogs
func (bs *BinlogStorage) Purge(ts int64) error {
	startSegmentKey := bs.segmentKey(ts)
	log.Infof("purge boltdb files that before [%d]", startSegmentKey)
	b := bs.metaStore.NewBatch()
	bs.mu.Lock()
	defer bs.mu.Unlock()
	var index = -1
	for i, key := range bs.mu.segmentKeys {
		// gc until to the biger or equal ts
		if key >= startSegmentKey {
			break
		}

		segment, ok := bs.mu.segments[key]
		if !ok {
			return errors.NotFoundf("sgement %d, but it should be found", key)
		}
		err := segment.Close()
		if err != nil {
			return errors.Errorf("can't close segement %d", key)
		}
		b.Delete(codec.EncodeInt([]byte{}, key))
		index = i
	}
	// delete segment meta infomation firstly
	err := bs.metaStore.Commit(segmentNamespace, b)
	if err != nil {
		return errors.Trace(err)
	}
	// delete files and update local infomations
	for i := 0; i <= index; i++ {
		key := bs.mu.segmentKeys[i]
		segmentPath := path.Join(bs.dataDir, fmt.Sprintf("%d", clusterID), fmt.Sprintf("binlog-%d.data", key))
		err = os.Remove(segmentPath)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("removed file: ", segmentPath)

		delete(bs.mu.segments, bs.mu.segmentKeys[i])
	}
	bs.mu.segmentKeys = bs.mu.segmentKeys[index+1:]
	return nil
}

// Close closes all boltdbs
func (bs *BinlogStorage) Close() {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	for key, segement := range bs.mu.segments {
		err := segement.Close()
		if err != nil {
			log.Errorf("fail to close segment, key %d, error %v", key, err)
		}
	}
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

func (bs *BinlogStorage) getOrCreateSegment(ts int64) (store.Store, error) {
	var err error
	bs.mu.Lock()
	segmentKey := bs.segmentKey(ts)
	segment, ok := bs.mu.segments[segmentKey]
	if !ok {
		segmentPath := path.Join(bs.dataDir, fmt.Sprintf("%d", clusterID), fmt.Sprintf("binlog-%d.data", segmentKey))
		segment, err = store.NewBoltStore(segmentPath, [][]byte{binlogNamespace}, bs.nosync)
		if err != nil {
			bs.mu.Unlock()
			return nil, errors.Annotatef(err, "failed to open BoltDB store, path %s", segmentPath)
		}
		bs.mu.segments[segmentKey] = segment
		bs.mu.segmentKeys = append(bs.mu.segmentKeys, segmentKey)
		sort.Sort(bs.mu.segmentKeys)
		bs.metaStore.Put(segmentNamespace, codec.EncodeInt([]byte{}, segmentKey), []byte(segmentPath))
	}
	bs.mu.Unlock()
	return segment, nil
}

func (bs *BinlogStorage) getSegment(ts int64) (store.Store, bool) {
	bs.mu.Lock()
	segment, ok := bs.mu.segments[bs.segmentKey(ts)]
	bs.mu.Unlock()
	return segment, ok
}

func (bs *BinlogStorage) segmentKey(ts int64) int64 {
	return ts >> shift
}

type segmentRange []int64

func (a segmentRange) Len() int           { return len(a) }
func (a segmentRange) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a segmentRange) Less(i, j int) bool { return a[i] < a[j] }
