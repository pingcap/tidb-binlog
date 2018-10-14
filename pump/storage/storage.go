package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	pkgutil "github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"golang.org/x/sys/unix"
)

const (
	maxTxnTimeoutSecond int64 = 600
)

var (
	// save gcTS, the max TS we have gc, for binlog not greater than gcTS, we can delete it from storage
	gcTSKey = []byte("!binlog!gcts")
	// save maxCommitTS, we can get binlog in range [gcTS, maxCommitTS]  from PullCommitBinlog
	maxCommitTSKey = []byte("!binlog!maxCommitTS")
	// save the valuePointer we should start push binlog item to sorter when restart
	handlePointerKey = []byte("!binlog!handlePointer")
	// save valuePointer headPointer, for binlog in vlog not after headPointer, we have save it in metadata db
	// at start up, we can scan the vlog from headPointer and save the ts -> valuePointer to metadata db
	headPointerKey = []byte("!binlog!headPointer")
)

// Storage is the interface to handle binlog storage
type Storage interface {
	WriteBinlog(binlog *pb.Binlog) error

	// delete <= ts
	GCTS(ts int64)

	MaxCommitTS() int64

	// PullCommitBinlog return the chan to consume the binlog
	PullCommitBinlog(ctx context.Context, beginTs, endTs int64, useStructInfo bool) <-chan []byte

	SelectCommitBinlog(ctx context.Context, from, to int64) []string

	Close() error
}

var _ Storage = &Append{}

// Append implement the Storage interface
type Append struct {
	dir  string
	vlog *valueLog

	metadata       *leveldb.DB
	sorter         *sorter
	tiStore        kv.Storage
	tiLockResolver *tikv.LockResolver
	latestTS       int64

	gcTS          int64
	maxCommitTS   int64
	headPointer   valuePointer
	handlePointer valuePointer

	sortItems          chan sortItem
	handleSortItemQuit chan struct{}

	writeCh chan *request

	options *Options

	close chan struct{}
	wg    sync.WaitGroup
}

// NewAppend returns a instance of Append
func NewAppend(dir string, options *Options) (append *Append, err error) {
	return NewAppendWithResolver(dir, options, nil, nil)
}

// NewAppendWithResolver returns a instance of Append
// if tiStore and tiLockResolver is not nil, we will try to query tikv to know weather a txn is committed
func NewAppendWithResolver(dir string, options *Options, tiStore kv.Storage, tiLockResolver *tikv.LockResolver) (append *Append, err error) {
	if options == nil {
		options = DefaultOptions()
	}

	kvDir := path.Join(dir, "kv")
	valueDir := path.Join(dir, "value")
	err = os.MkdirAll(valueDir, 0755)
	if err != nil {
		return nil, errors.Trace(err)
	}

	vlog := new(valueLog)
	err = vlog.open(valueDir, options)
	if err != nil {
		return nil, errors.Trace(err)
	}

	metadata, err := leveldb.OpenFile(kvDir, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	writeCh := make(chan *request, 1<<10)
	append = &Append{
		dir:            dir,
		vlog:           vlog,
		metadata:       metadata,
		writeCh:        writeCh,
		options:        options,
		tiStore:        tiStore,
		tiLockResolver: tiLockResolver,

		close:     make(chan struct{}),
		sortItems: make(chan sortItem, 1024),
	}

	append.gcTS, err = append.readGCTSFromDB()
	if err != nil {
		return nil, errors.Trace(err)
	}
	gcTSGauge.Set(float64(oracle.ExtractPhysical(uint64(append.gcTS))))

	append.maxCommitTS, err = append.readInt64(maxCommitTSKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	maxCommitTSGauge.Set(float64(oracle.ExtractPhysical(uint64(append.maxCommitTS))))

	append.headPointer, err = append.readPointer(headPointerKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	append.handlePointer, err = append.readPointer(handlePointerKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	append.handleSortItemQuit = append.handleSortItem(append.sortItems)
	sorter := newSorter(func(item sortItem) {
		log.Debugf("sorter get item: %+v", item)
		append.sortItems <- item
	})

	if tiStore != nil {
		sorter.setResolver(append.resolve)
	}

	append.sorter = sorter

	minPointer := append.headPointer
	if append.handlePointer.less(minPointer) {
		minPointer = append.handlePointer
	}

	log.Infof("gcTS: %v, maxCommitTS: %v, headPointer: %+v, handlePointer: %+v", append.gcTS, append.maxCommitTS, append.headPointer, append.handlePointer)

	toKV := append.writeToValueLog(writeCh)

	append.wg.Add(1)
	go append.writeToSorter(append.writeToKV(toKV))

	// for handlePointer and headPointer, it's safe to start at a forward point, so we just chose a min point between handlePointer and headPointer, and wirte to KV, will push to sorter after write to KV too
	err = append.vlog.scan(minPointer, func(vp valuePointer, record *Record) error {
		binlog := new(pb.Binlog)
		err := binlog.Unmarshal(record.payload)
		if err != nil {
			return errors.Trace(err)
		}
		request := &request{
			startTS:      binlog.StartTs,
			commitTS:     binlog.CommitTs,
			tp:           binlog.Tp,
			payload:      record.payload,
			valuePointer: vp,
		}
		toKV <- request
		return nil
	})

	if err != nil {
		return nil, errors.Trace(err)
	}

	append.wg.Add(1)
	go append.updateStatus()

	return
}

func (a *Append) persistHandlePointer(item sortItem) error {
	log.Debug("persist item: ", item)
	tsKey := encodeTSKey(item.commit)
	pointerData, err := a.metadata.Get(tsKey, nil)
	if err != nil {
		return errors.Trace(err)
	}
	var pointer valuePointer
	err = pointer.UnmarshalBinary(pointerData)
	if err != nil {
		return errors.Trace(err)
	}

	var batch leveldb.Batch
	maxCommitTS := make([]byte, 8)
	binary.LittleEndian.PutUint64(maxCommitTS, uint64(item.commit))
	batch.Put(maxCommitTSKey, maxCommitTS)

	batch.Put(handlePointerKey, pointerData)
	err = a.metadata.Write(&batch, nil)
	if err != nil {
		return errors.Trace(err)
	}
	a.handlePointer = pointer

	return nil
}

func (a *Append) handleSortItem(items <-chan sortItem) (quit chan struct{}) {
	quit = make(chan struct{})

	go func() {
		defer close(quit)

		// we save the handlePointer and maxCommitTS to metadata at most once in persistAtLeastTime
		persistAtLeastTime := time.Second
		var toSaveItem sortItem
		var toSave <-chan time.Time
		for {
			select {
			case item, ok := <-items:
				if !ok {
					if toSave != nil {
						err := a.persistHandlePointer(toSaveItem)
						if err != nil {
							log.Error(errors.ErrorStack(err))
						}
					}
					return
				}
				// the commitTS we get from sorter is monotonic increasing, unless we forward the handlePointer at start up
				// or this should never happen
				if item.commit < atomic.LoadInt64(&a.maxCommitTS) {
					log.Warnf("sortItem's commit ts(%d) less than append.maxCommitTS(%d)", item.commit, a.maxCommitTS)
					continue
				}
				atomic.StoreInt64(&a.maxCommitTS, item.commit)
				maxCommitTSGauge.Set(float64(oracle.ExtractPhysical(uint64(item.commit))))
				toSaveItem = item
				if toSave == nil {
					toSave = time.After(persistAtLeastTime)
				}
				log.Debug("get sort item: ", item)
			case <-toSave:
				err := a.persistHandlePointer(toSaveItem)
				if err != nil {
					log.Error(errors.ErrorStack(err))
				}
				toSave = nil
			}

		}
	}()

	return quit
}

func (a *Append) updateStatus() {
	defer a.wg.Done()

	var updateLatest <-chan time.Time
	if a.tiStore != nil {
		updateLatestTicker := time.NewTicker(time.Second)
		defer updateLatestTicker.Stop()
		updateLatest = updateLatestTicker.C
	}

	updateSizeTicker := time.NewTicker(time.Second * 3)
	defer updateSizeTicker.Stop()
	updateSize := updateSizeTicker.C

	for {
		select {
		case <-a.close:
			return
		case <-updateLatest:
			ts, err := pkgutil.QueryLatestTsFromPD(a.tiStore)
			if err != nil {
				log.Errorf("QueryLatestTSFromPD err: %v", err)
			} else {
				atomic.StoreInt64(&a.latestTS, ts)
			}
		case <-updateSize:
			size, err := getStorageSize(a.dir)
			if err != nil {
				log.Error("update sotrage size err: ", err)
			} else {
				storageSizeGauge.WithLabelValues("capacity").Set(float64(size.capacity))
				storageSizeGauge.WithLabelValues("available").Set(float64(size.available))
			}
		}
	}
}

func (a *Append) resolve(startTS int64) bool {
	latestTS := atomic.LoadInt64(&a.latestTS)
	if latestTS <= 0 {
		return false
	}

	startSecond := oracle.ExtractPhysical(uint64(startTS)) / int64(time.Second/time.Millisecond)
	maxSecond := oracle.ExtractPhysical(uint64(latestTS)) / int64(time.Second/time.Millisecond)

	if maxSecond-startSecond <= maxTxnTimeoutSecond {
		return false
	}

	pbinlog, err := a.readBinlogByTS(startTS)
	if err != nil {
		log.Error(err)
		return false
	}

	log.Warnf("unknown commit stats start ts: %d", startTS)

	if pbinlog.GetDdlJobId() == 0 {
		tikvQueryCount.Add(1.0)
		primaryKey := pbinlog.GetPrewriteKey()
		status, err := a.tiLockResolver.GetTxnStatus(uint64(pbinlog.StartTs), primaryKey)
		if err != nil {
			log.Error(err)
			return false
		}

		// Write a commit binlog myself if the status is committed,
		// otherwise we can just ignore it, we will not get the commit binlog while iterator the kv by ts
		if status.IsCommitted() {
			// write the commit binlog myself
			cbinlog := new(pb.Binlog)
			cbinlog.Tp = pb.BinlogType_Commit
			cbinlog.StartTs = pbinlog.StartTs
			cbinlog.CommitTs = int64(status.CommitTS())

			req := a.writeBinlog(cbinlog)
			if req.err != nil {
				log.Error(req.err)
				return false
			}

			// when writeBinlog return success, the pointer will be write to kv async,
			// but we need to make sure it has been write to kv when we return true in the func, then we can get this commit binlog when
			// we update maxCommitTS
			// write the ts -> pointer to KV here to make sure it.
			pointer, err := req.valuePointer.MarshalBinary()
			if err != nil {
				panic(err)
			}

			err = a.metadata.Put(encodeTSKey(req.ts()), pointer, nil)
			if err != nil {
				log.Error(err)
				return false
			}
		}

		log.Infof("known txn is committed from tikv, start ts: %d, commit ts: %d", startTS, status.CommitTS())
		return true
	}

	log.Errorf("some prewrite DDL items remain single after waiting for a long time, startTs: %d", startTS)
	return false
}

func (a *Append) readBinlogByTS(ts int64) (*pb.Binlog, error) {
	var vp valuePointer

	vpData, err := a.metadata.Get(encodeTSKey(ts), nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = vp.UnmarshalBinary(vpData)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pvalue, err := a.vlog.readValue(vp)
	if err != nil {
		return nil, errors.Trace(err)
	}

	binlog := new(pb.Binlog)
	err = binlog.Unmarshal(pvalue)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return binlog, nil
}

// if the key not exist, return 0, nil
func (a *Append) readInt64(key []byte) (int64, error) {
	value, err := a.metadata.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return 0, nil
		}
		return 0, errors.Trace(err)
	}

	return int64(binary.LittleEndian.Uint64(value)), nil
}

func (a *Append) readGCTSFromDB() (int64, error) {
	return a.readInt64(gcTSKey)
}

func (a *Append) saveGCTSToDB(ts int64) error {
	var value = make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(ts))

	return a.metadata.Put(gcTSKey, value, nil)
}

func (a *Append) isClosed() bool {
	select {
	case <-a.close:
		return true
	default:
		return false
	}
}

// Close release resource of Append
func (a *Append) Close() error {
	log.Debug("close Append")

	close(a.close)
	close(a.writeCh)

	// wait for all binlog write to vlog -> KV -> sorter
	// after this, writeToValueLog, writeToKV, writeToSorter has quit sequently
	a.wg.Wait()
	log.Debug("wait group done")

	// note the call back func will use a.metadata, so we should close sorter before a.metadata
	a.sorter.close()
	log.Debug("sorter is closed")

	close(a.sortItems)
	<-a.handleSortItemQuit
	log.Debug("handle sort item quit")

	err := a.metadata.Close()
	if err != nil {
		log.Error(err)
	}

	err = a.vlog.close()
	if err != nil {
		log.Error(err)
	}

	return err
}

// GCTS implement Storage.GCTS
func (a *Append) GCTS(ts int64) {
	lastTS := atomic.LoadInt64(&a.gcTS)
	if ts <= lastTS {
		log.Infof("ignore gc ts: %d, last gc ts: %d", ts, lastTS)
		return
	}

	atomic.StoreInt64(&a.gcTS, ts)
	a.saveGCTSToDB(ts)
	gcTSGauge.Set(float64(oracle.ExtractPhysical(uint64(ts))))
	// for commit binlog TS ts_c, we may need to get the according P binlog ts_p(ts_p < ts_c
	// so we forward a little bit to make sure we can get the according P binlog
	a.doGCTS(ts - int64(oracle.EncodeTSO(maxTxnTimeoutSecond*1000)))
}

func (a *Append) doGCTS(ts int64) {
	irange := &util.Range{
		Start: encodeTSKey(0),
		Limit: encodeTSKey(ts + 1),
	}
	iter := a.metadata.NewIterator(irange, nil)
	batch := new(leveldb.Batch)
	for iter.Next() {
		batch.Delete(iter.Key())
		if batch.Len() == 1024 {
			err := a.metadata.Write(batch, nil)
			if err != nil {
				log.Error(err)
			}
			batch.Reset()
		}
	}

	if batch.Len() > 0 {
		err := a.metadata.Write(batch, nil)
		if err != nil {
			log.Error(err)
		}
	}

	a.vlog.gcTS(ts)
}

// MaxCommitTS implement Storage.MaxCommitTS
func (a *Append) MaxCommitTS() int64 {
	return atomic.LoadInt64(&a.maxCommitTS)
}

// WriteBinlog implement Storage.WriteBinlog
func (a *Append) WriteBinlog(binlog *pb.Binlog) error {
	return errors.Trace(a.writeBinlog(binlog).err)
}

func (a *Append) writeBinlog(binlog *pb.Binlog) *request {
	beginTime := time.Now()
	request := new(request)

	defer func() {
		writeBinlogTimeHistogram.WithLabelValues("single").Observe(time.Since(beginTime).Seconds())
		if request.err != nil {
			errorCount.WithLabelValues("write_binlog").Add(1.0)
		}
	}()

	payload, err := binlog.Marshal()
	if err != nil {
		request.err = errors.Trace(err)
		return request
	}

	writeBinlogSizeHistogram.WithLabelValues("single").Observe(float64(len(payload)))

	request.payload = payload
	request.startTS = binlog.StartTs
	request.commitTS = binlog.CommitTs
	request.tp = binlog.Tp
	request.wg.Add(1)

	a.writeCh <- request

	request.wg.Wait()

	return request
}

func (a *Append) writeToSorter(reqs chan *request) {
	defer a.wg.Done()

	for req := range reqs {
		log.Debugf("write request to sorter: %s", req)
		var item sortItem
		item.start = req.startTS
		item.commit = req.commitTS
		item.tp = req.tp

		a.sorter.pushTSItem(item)
	}
}

func (a *Append) writeToKV(reqs chan *request) chan *request {
	done := make(chan *request, 1024)

	batchReqs := a.batchRequest(reqs, 128)

	go func() {
		defer close(done)

		var batch leveldb.Batch
		for bufReqs := range batchReqs {
			batch.Reset()
			var lastPointer []byte
			for _, req := range bufReqs {
				log.Debugf("write request to kv: %s", req)
				var err error

				pointer, err := req.valuePointer.MarshalBinary()
				if err != nil {
					panic(err)
				}

				lastPointer = pointer
				batch.Put(encodeTSKey(req.ts()), pointer)
			}
			batch.Put(headPointerKey, lastPointer)

			for {
				err := a.metadata.Write(&batch, nil)

				// when write to vlog success, but the disk is full when write to KV here, it will cause write err
				// we just retry of quit when Append is closed
				if err != nil {
					log.Error(err)
					if a.isClosed() {
						return
					}
					time.Sleep(time.Second)
					continue
				}
				a.headPointer = bufReqs[len(bufReqs)-1].valuePointer

				for _, req := range bufReqs {
					done <- req
				}
				break
			}
		}
	}()

	return done
}

func (a *Append) batchRequest(reqs chan *request, maxBatchNum int) chan []*request {
	done := make(chan []*request, 1024)
	var bufReqs []*request

	go func() {
		defer close(done)

		for {
			if len(bufReqs) >= maxBatchNum {
				done <- bufReqs
				bufReqs = make([]*request, 0, maxBatchNum)
			}

			select {
			case req, ok := <-reqs:
				if !ok {
					if len(bufReqs) > 0 {
						done <- bufReqs
					}
					return
				}
				bufReqs = append(bufReqs, req)
			default:
				if len(bufReqs) > 0 {
					done <- bufReqs
					bufReqs = make([]*request, 0, maxBatchNum)
				} else { // get first req
					req, ok := <-reqs
					if !ok {
						return
					}
					bufReqs = append(bufReqs, req)
				}
			}
		}
	}()

	return done
}

func (a *Append) writeToValueLog(reqs chan *request) chan *request {
	done := make(chan *request, 1024)

	go func() {
		defer close(done)

		var bufReqs []*request
		var size int

		write := func(reqs []*request) {
			if len(reqs) == 0 {
				return
			}
			log.Debugf("write requests to value log: %v", reqs)
			beginTime := time.Now()
			writeBinlogSizeHistogram.WithLabelValues("batch").Observe(float64(size))

			err := a.vlog.write(reqs)
			writeBinlogTimeHistogram.WithLabelValues("batch").Observe(time.Since(beginTime).Seconds())
			if err != nil {
				for _, req := range reqs {
					req.err = err
					req.wg.Done()
				}
				errorCount.WithLabelValues("write_binlog_batch").Add(1.0)
				return
			}

			for _, req := range reqs {
				log.Debug(req.startTS, req.commitTS, " done")
				req.wg.Done()
				// payload is useless anymore, let it GC ASAP
				req.payload = nil
				done <- req
			}
		}

		for {
			select {
			case req, ok := <-reqs:
				if !ok {
					write(bufReqs)
					return
				}
				bufReqs = append(bufReqs, req)
				size += len(req.payload)

				// Allow the group to grow up to a maximum size, but if the
				// original write is small, limit the growth so we do not slow
				// down the small write too much.
				maxSize := 1 << 20
				firstSize := len(bufReqs[0].payload)
				if firstSize <= (128 << 10) {
					maxSize = firstSize + (128 << 10)
				}

				if size >= maxSize {
					write(bufReqs)
					size = 0
					bufReqs = bufReqs[:0]
				}
			default:
				if len(bufReqs) > 0 {
					write(bufReqs)
					size = 0
					bufReqs = bufReqs[:0]
					continue
				}

				// get first
				req, ok := <-reqs
				if !ok {
					return
				}
				bufReqs = append(bufReqs, req)
				size += len(req.payload)
			}
		}
	}()

	return done
}

func (a *Append) readPointer(key []byte) (valuePointer, error) {
	var vp valuePointer
	value, err := a.metadata.Get(key, nil)
	if err != nil {
		// return zero value when not found
		if err == leveldb.ErrNotFound {
			return vp, nil
		}

		return vp, errors.Trace(err)
	}
	err = vp.UnmarshalBinary(value)
	if err != nil {
		return vp, errors.Trace(err)
	}

	return vp, nil
}

func (a *Append) savePointer(key []byte, vp valuePointer) error {
	value, err := vp.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}
	err = a.metadata.Put(key, value, nil)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (a *Append) feedPreWriteValue(cbinlog *pb.Binlog) error {
	var vp valuePointer

	vpData, err := a.metadata.Get(encodeTSKey(cbinlog.StartTs), nil)
	if err != nil {
		errors.Trace(err)
	}

	err = vp.UnmarshalBinary(vpData)
	if err != nil {
		errors.Trace(err)
	}

	pvalue, err := a.vlog.readValue(vp)
	if err != nil {
		errors.Trace(err)
	}

	pbinlog := new(pb.Binlog)
	err = pbinlog.Unmarshal(pvalue)
	if err != nil {
		errors.Trace(err)
	}

	cbinlog.StartTs = pbinlog.StartTs
	cbinlog.PrewriteValue = pbinlog.PrewriteValue

	return nil
}

// PullCommitBinlog return commit binlog  > last
func (a *Append) PullCommitBinlog(ctx context.Context, beginTs, endTs int64, useStructInfo bool) <-chan []byte {
	log.Debugf("new PullCommitBinlog beginTs: %d, endTs: %d", beginTs, endTs)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-a.close:
			cancel()
		case <-ctx.Done():
		}
	}()

	gcTS := atomic.LoadInt64(&a.gcTS)
	if beginTs < gcTS {
		log.Warnf("last ts %d less than gcTS %d", beginTs, gcTS)
		beginTs = gcTS
	}

	values := make(chan []byte, 5)

	irange := &util.Range{
		Start: encodeTSKey(0),
		Limit: encodeTSKey(math.MaxInt64),
	}

	go func() {
		defer close(values)

		for {
			irange.Start = encodeTSKey(beginTs)
			irange.Limit = encodeTSKey(atomic.LoadInt64(&a.maxCommitTS) + 1)
			iter := a.metadata.NewIterator(irange, nil)

			// log.Debugf("try to get range [%d,%d)", startTS, atomic.LoadInt64(&a.maxCommitTS)+1)

			for ok := iter.Seek(encodeTSKey(beginTs)); ok; ok = iter.Next() {
				if endTs >= 0 && beginTs > endTs {
					values <- []byte("end")
					iter.Release()
				}

				var vp valuePointer
				err := vp.UnmarshalBinary(iter.Value())
				// should never happen
				if err != nil {
					panic(err)
				}

				log.Debugf("get ts: %d, pointer: %v", decodeTSKey(iter.Key()), vp)

				value, err := a.vlog.readValue(vp)
				if err != nil {
					log.Error(err)
					iter.Release()
					errorCount.WithLabelValues("read_value").Add(1.0)
					return
				}

				binlog := new(pb.Binlog)
				err = binlog.Unmarshal(value)
				if err != nil {
					log.Error(err)
					iter.Release()
					return
				}

				if binlog.Tp == pb.BinlogType_Prewrite {
					continue
				} else {
					if binlog.CommitTs == binlog.StartTs {
						// this should be a fake binlog, drainer should ignore this when push binlog to the downstream
						log.Debug("get fake c binlog: ", binlog.CommitTs)
					} else {
						err = a.feedPreWriteValue(binlog)
						if err != nil {
							errorCount.WithLabelValues("feed_pre_write_value").Add(1.0)
							log.Error(err)
							iter.Release()
							return
						}
					}

					if !useStructInfo {
						value, err = binlog.Marshal()
						if err != nil {
							log.Error(err)
							iter.Release()
							return
						}
					} else {
						value = []byte(fmt.Sprintf("%v", binlog))
					}
				}

				select {
				case values <- value:
					log.Debug("send value success")
				case <-ctx.Done():
					iter.Release()
					return
				}

				beginTs = decodeTSKey(iter.Key()) + 1
			}
			iter.Release()

			select {
			case <-ctx.Done():
				return
				// TODO signal to wait up, don't sleep
			case <-time.After(time.Millisecond * 100):
			}
		}
	}()

	return values
}

// SelectCommitBinlog return commit binlog  >= min && <= max
func (a *Append) SelectCommitBinlog(ctx context.Context, min, max int64) []string {
	log.Debug("new SelectCommitBinlog from %d to %d ", min, max)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-a.close:
			cancel()
		case <-ctx.Done():
		}
	}()

	gcTS := atomic.LoadInt64(&a.gcTS)
	if min < gcTS {
		log.Warnf("min ts %d less than gcTS %d", min, gcTS)
		min = gcTS
	}

	values := make([]string, 0, 5)

	irange := &util.Range{
		Start: encodeTSKey(0),
		Limit: encodeTSKey(math.MaxInt64),
	}

	startTS := min

	irange.Start = encodeTSKey(startTS)
	irange.Limit = encodeTSKey(atomic.LoadInt64(&a.maxCommitTS) + 1)
	iter := a.metadata.NewIterator(irange, nil)

	// log.Debugf("try to get range [%d,%d)", startTS, atomic.LoadInt64(&a.maxCommitTS)+1)

	for ok := iter.Seek(encodeTSKey(startTS)); ok; ok = iter.Next() {
		var vp valuePointer
		err := vp.UnmarshalBinary(iter.Value())
		// should never happen
		if err != nil {
			panic(err)
		}

		log.Debugf("get ts: %d, pointer: %v", decodeTSKey(iter.Key()), vp)

		value, err := a.vlog.readValue(vp)
		if err != nil {
			log.Error(err)
			iter.Release()
			errorCount.WithLabelValues("read_value").Add(1.0)
			return values
		}

		binlog := new(pb.Binlog)
		err = binlog.Unmarshal(value)
		if err != nil {
			log.Error(err)
			iter.Release()
			return values
		}

		if binlog.CommitTs > max {
			//iter.Release()
			break
		}

		if binlog.Tp == pb.BinlogType_Prewrite {
			continue
		} else {
			if binlog.CommitTs == binlog.StartTs {
				// this should be a fake binlog, drainer should ignore this when push binlog to the downstream
				log.Debug("get fake c binlog: ", binlog.CommitTs)
			} else {
				err = a.feedPreWriteValue(binlog)
				if err != nil {
					errorCount.WithLabelValues("feed_pre_write_value").Add(1.0)
					log.Error(err)
					iter.Release()
					return values
				}
			}

			value, err = binlog.Marshal()
			if err != nil {
				log.Error(err)
				iter.Release()
				return values
			}

		}

		values = append(values, fmt.Sprintf("type: %s, startTS: %d, commitTS: %d, length: %d", binlog.Tp, binlog.StartTs, binlog.CommitTs, len(binlog.PrewriteValue)))
		min = decodeTSKey(iter.Key())
	}
	iter.Release()

	return values
}

type storageSize struct {
	capacity  int
	available int
}

func getStorageSize(dir string) (size storageSize, err error) {
	var stat unix.Statfs_t

	err = unix.Statfs(dir, &stat)
	if err != nil {
		return size, errors.Trace(err)
	}

	// When container is run in MacOS, `bsize` obtained by `statfs` syscall is not the fundamental block size,
	// but the `iosize` (optimal transfer block size) instead, it's usually 1024 times larger than the `bsize`.
	// for example `4096 * 1024`. To get the correct block size, we should use `frsize`. But `frsize` isn't
	// guaranteed to be supported everywhere, so we need to check whether it's supported before use it.
	// For more details, please refer to: https://github.com/docker/for-mac/issues/2136
	bSize := uint64(stat.Bsize)
	field := reflect.ValueOf(&stat).Elem().FieldByName("Frsize")
	if field.IsValid() {
		if field.Kind() == reflect.Uint64 {
			bSize = field.Uint()
		} else {
			bSize = uint64(field.Int())
		}
	}

	// Available blocks * size per block = available space in bytes
	size.available = int(stat.Bavail) * int(bSize)
	size.capacity = int(stat.Blocks) * int(bSize)

	return
}
