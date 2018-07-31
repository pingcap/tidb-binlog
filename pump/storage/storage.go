package storage

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	gcTSKey          = []byte("!binlog!gcts")
	maxCommitTSKey   = []byte("!binlog!maxCommitTS")
	handlePointerKey = []byte("!binlog!handlePointer")
	headPointerKey   = []byte("!binlog!headPointer")
)

// Storage is the interface to handle binlog storage
type Storage interface {
	WriteBinlog(binlog *pb.Binlog) error

	// delete <= ts
	GCTS(ts int64)

	// PullCommitBinlog return the chan to consume the binlog
	PullCommitBinlog(ctx context.Context, last int64) <-chan []byte

	Close() error
}

var _ Storage = &Append{}

// Append inplement the Storage interface
type Append struct {
	dir  string
	vlog *valueLog

	db     *leveldb.DB
	sorter *sorter

	gcTS          int64
	maxCommitTS   int64
	headPointer   valuePointer
	handlePointer valuePointer

	writeCh chan *request

	options *Options
	wg      sync.WaitGroup
}

// NewAppend return a instance of Append
func NewAppend(dir string, options *Options) (append *Append, err error) {
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

	db, err := leveldb.OpenFile(kvDir, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	writeCh := make(chan *request, 1<<10)
	append = &Append{
		dir:     dir,
		vlog:    vlog,
		db:      db,
		writeCh: writeCh,
		options: options,
	}

	append.gcTS = append.readGCTSFromDB()
	append.maxCommitTS = append.readInt64OrPanic(maxCommitTSKey)
	append.headPointer = append.readPointerOrPanic(headPointerKey)
	append.handlePointer = append.readPointerOrPanic(handlePointerKey)

	sorter := newSorter(func(item sortItem) {
		log.Debugf("sorter get item: %+v", item)

		tsKey := encodeTs(item.commit)
		pointer, err := db.Get(tsKey, nil)
		if err != nil {
			panic(err)
		}

		var batch leveldb.Batch
		maxCommitTS := make([]byte, 8)
		binary.LittleEndian.PutUint64(maxCommitTS, uint64(item.commit))
		batch.Put(maxCommitTSKey, maxCommitTS)

		batch.Put(handlePointerKey, pointer)
		err = db.Write(&batch, nil)
		if err != nil {
			panic(err)
		}

		atomic.StoreInt64(&append.maxCommitTS, item.commit)
	})

	append.sorter = sorter

	minPointer := append.headPointer
	if append.handlePointer.less(minPointer) {
		minPointer = append.handlePointer
	}

	toKV := append.writeToValueLog(writeCh)

	go append.writeToSorter(append.writeToKV(toKV))

	err = append.vlog.scan(minPointer, func(vp valuePointer, record *Record) error {
		binlog := new(pb.Binlog)
		err := binlog.Unmarshal(record.payload)
		if err != nil {
			return errors.Trace(err)
		}
		request := &request{
			startTS:  binlog.StartTs,
			commitTS: binlog.CommitTs,
			tp:       binlog.Tp,
			payload:  record.payload,
		}
		toKV <- request
		return nil
	})

	if err != nil {
		return nil, errors.Trace(err)
	}

	return
}

// if the key not exist, return 0, or panic on err
func (a *Append) readInt64OrPanic(key []byte) int64 {
	value, err := a.db.Get(gcTSKey, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return 0
		}
		panic("unreachable err: " + err.Error())
	}

	return int64(binary.LittleEndian.Uint64(value))
}

func (a *Append) readGCTSFromDB() int64 {
	return a.readInt64OrPanic(gcTSKey)
}

func (a *Append) saveGCTSToDB(ts int64) error {
	var value = make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(ts))

	return a.db.Put(gcTSKey, value, nil)
}

// Close release resource of Append
func (a *Append) Close() error {
	close(a.writeCh)

	a.sorter.close()

	a.wg.Wait()

	err := a.db.Close()
	if err != nil {
		log.Error(err)
	}
	return err
}

// GCTS inplement Storage.GCTS
func (a *Append) GCTS(ts int64) {
	a.gcTS = ts
	a.saveGCTSToDB(ts)
	a.doGCTS(ts)
}

func (a *Append) doGCTS(ts int64) {
	irange := &util.Range{
		Start: encodeTs(0),
		Limit: encodeTs(ts + 1),
	}
	iter := a.db.NewIterator(irange, nil)
	batch := new(leveldb.Batch)
	for iter.Next() {
		batch.Delete(iter.Key())
		if batch.Len() == 1024 {
			err := a.db.Write(batch, nil)
			if err != nil {
				log.Error(err)
			}
			batch.Reset()
		}
	}

	if batch.Len() > 0 {
		a.db.Write(batch, nil)
		err := a.db.Write(batch, nil)
		if err != nil {
			log.Error(err)
		}
	}

	a.vlog.gcTS(ts)
}

// WriteBinlog inplement Storage.WriteBinlog
func (a *Append) WriteBinlog(binlog *pb.Binlog) error {
	payload, err := binlog.Marshal()
	if err != nil {
		return err
	}

	request := new(request)
	request.payload = payload
	request.startTS = binlog.StartTs
	request.commitTS = binlog.CommitTs
	request.tp = binlog.Tp
	request.wg.Add(1)

	a.writeCh <- request

	request.wg.Wait()

	return errors.Trace(request.err)
}

func (a *Append) writeToSorter(reqs chan *request) {
	a.wg.Add(1)
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

	go func() {
		defer close(done)

		var batch leveldb.Batch
		for req := range reqs {
			log.Debugf("write request to kv: %s", req)
			var err error

			pointer, err := req.valuePointer.MarshalBinary()
			if err != nil {
				panic(err)
			}

			batch.Put(encodeTs(req.ts()), pointer)
			batch.Put(headPointerKey, pointer)

			err = a.db.Write(&batch, nil)
			batch.Reset()

			if err != nil {
				panic(err)
			}

			done <- req
		}
	}()

	return done
}

func (a *Append) writeToValueLog(reqs chan *request) chan *request {
	done := make(chan *request, 1024)

	go func() {
		defer close(done)

		write := func(reqs []*request) {
			if len(reqs) == 0 {
				return
			}
			log.Debugf("write requests to value log: %v", reqs)

			err := a.vlog.write(reqs)
			if err != nil {
				for _, req := range reqs {
					req.err = err
				}
			}

			for _, req := range reqs {
				log.Debug(req.commitTS, req.startTS, " done")
				req.wg.Done()
				done <- req
			}
		}

		var bufReqs []*request
		var size int
		for {
			select {
			case req, ok := <-reqs:
				if !ok {
					write(bufReqs)
					return
				}
				bufReqs = append(bufReqs, req)
				size += len(req.payload)
				if size >= 4*(1<<20) || len(bufReqs) > 128 {
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
				select {
				case req, ok := <-reqs:
					if !ok {
						return
					}
					bufReqs = append(bufReqs, req)
					size += len(req.payload)
				}
			}
		}
	}()

	return done
}

func (a *Append) readPointerOrPanic(key []byte) valuePointer {
	var vp valuePointer
	value, err := a.db.Get(key, nil)
	if err != nil {
		// return zero value when not found
		if err == leveldb.ErrNotFound {
			return vp
		}
		panic("unreachable err: " + err.Error())
	}
	err = vp.UnmarshalBinary(value)
	if err != nil {
		panic("unreachable err: " + err.Error())
	}

	return vp
}

func (a *Append) savePointer(key []byte, vp valuePointer) error {
	value, err := vp.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}
	err = a.db.Put(key, value, nil)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (a *Append) feedPreWriteValue(cbinlog *pb.Binlog) error {
	var vp valuePointer

	vpData, err := a.db.Get(encodeTs(cbinlog.StartTs), nil)
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
func (a *Append) PullCommitBinlog(ctx context.Context, last int64) <-chan []byte {
	log.Debug("new PullCommitBinlog last: ", last)

	gcTS := atomic.LoadInt64(&a.gcTS)
	if last < gcTS {
		log.Warnf("last ts %d less than gcTS %d", last, gcTS)
		last = gcTS
	}

	values := make(chan []byte, 5)

	irange := &util.Range{
		Start: encodeTs(0),
		Limit: encodeTs(math.MaxInt64),
	}

	go func() {
		defer close(values)

		for {
			startTS := last + 1

			irange.Start = encodeTs(startTS)
			irange.Limit = encodeTs(atomic.LoadInt64(&a.maxCommitTS) + 1)
			iter := a.db.NewIterator(irange, nil)

			// log.Debugf("try to get range [%d,%d)", startTS, atomic.LoadInt64(&a.maxCommitTS)+1)

			for ok := iter.Seek(encodeTs(startTS)); ok; ok = iter.Next() {
				var vp valuePointer
				err := vp.UnmarshalBinary(iter.Value())
				// should never happen
				if err != nil {
					panic(err)
				}

				log.Debugf("get ts: %d, pointer: %v", decodeTs(iter.Key()), vp)

				value, err := a.vlog.readValue(vp)
				if err != nil {
					log.Error(err)
					iter.Release()
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
							log.Error(err)
							iter.Release()
							return
						}
					}

					value, err = binlog.Marshal()
					if err != nil {
						log.Error(err)
						iter.Release()
						return
					}

				}

				select {
				case values <- value:
					log.Debug("send value success")
				case <-ctx.Done():
					iter.Release()
					return
				}

				last = decodeTs(iter.Key())
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
