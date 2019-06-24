// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	pb "github.com/pingcap/tipb/go-binlog"
)

const (
	// we finalize the curFile when size >= finalizeFileSizeAtClose when closing the vlog, we don't need to scan the too big curFile to recover the maxTS of the file when reopen the vlog
	// TODO we can always finalize the curFile when close Storage, and truncate the footer when open if we want to continue writing this file, so no need to scan the file to get the info in footer
	finalizeFileSizeAtClose = 50 * 1024 // 50K
	fileExt                 = ".vlog"
)

// Options is the config options of Append and vlog
type Options struct {
	ValueLogFileSize   int64
	Sync               bool
	KVChanCapacity     int
	SlowWriteThreshold float64

	KVConfig *KVConfig
}

// DefaultOptions return the default options
func DefaultOptions() *Options {
	return &Options{
		ValueLogFileSize:   500 * (1 << 20),
		Sync:               true,
		KVChanCapacity:     chanCapacity,
		SlowWriteThreshold: slowWriteThreshold,
	}
}

// WithKVConfig set the Config
func (o *Options) WithKVConfig(kvConfig *KVConfig) *Options {
	o.KVConfig = kvConfig
	return o
}

// WithSlowWriteThreshold set the Config
func (o *Options) WithSlowWriteThreshold(threshold float64) *Options {
	o.SlowWriteThreshold = threshold
	return o
}

// WithValueLogFileSize set the ValueLogFileSize
func (o *Options) WithValueLogFileSize(size int64) *Options {
	o.ValueLogFileSize = size
	return o
}

// WithKVChanCapacity set the ChanCapacity
func (o *Options) WithKVChanCapacity(capacity int) *Options {
	o.KVChanCapacity = capacity
	return o
}

// WithSync set the Sync
func (o *Options) WithSync(sync bool) *Options {
	o.Sync = sync
	return o
}

type request struct {
	startTS  int64
	commitTS int64
	tp       pb.BinlogType

	payload      []byte
	valuePointer valuePointer
	wg           sync.WaitGroup
	err          error
}

func (r *request) ts() int64 {
	if r.tp == pb.BinlogType_Prewrite {
		return r.startTS
	}

	return r.commitTS
}

func (r *request) String() string {
	return fmt.Sprintf("{ts: %d, payload len: %d, valuePointer: %+v}", r.ts(), len(r.payload), r.valuePointer)
}

type valuePointer struct {
	Fid    uint32
	Offset int64
}

func (vp valuePointer) less(other valuePointer) bool {
	if vp.Fid != other.Fid {
		return vp.Fid < other.Fid
	}

	return vp.Offset < other.Offset
}

// MarshalBinary never return not nil err now
func (vp *valuePointer) MarshalBinary() (data []byte, err error) {
	data = make([]byte, 12)
	binary.LittleEndian.PutUint32(data, vp.Fid)
	binary.LittleEndian.PutUint64(data[4:], uint64(vp.Offset))

	return
}

// UnmarshalBinary implement encoding.BinaryMarshal
func (vp *valuePointer) UnmarshalBinary(data []byte) error {
	if len(data) < 12 {
		return errors.New("not enough data")
	}
	vp.Fid = binary.LittleEndian.Uint32(data)
	vp.Offset = int64(binary.LittleEndian.Uint64(data[4:]))

	return nil
}

type valueLog struct {
	buf *bytes.Buffer // buf to write to the current log file

	dirPath   string
	sync      bool
	maxFid    uint32
	filesLock sync.RWMutex
	filesMap  map[uint32]*logFile

	opt *Options
}

func (vlog *valueLog) filePath(fid uint32) string {
	return filepath.Join(vlog.dirPath, fmt.Sprintf("%06d%s", fid, fileExt))
}

func (vlog *valueLog) getFileRLocked(fid uint32) (*logFile, error) {
	vlog.filesLock.RLock()
	defer vlog.filesLock.RUnlock()
	ret, ok := vlog.filesMap[fid]
	if !ok {
		return nil, errors.NotFoundf("not found fid: %d", fid)
	}
	ret.lock.RLock()
	return ret, nil
}

func (vlog *valueLog) open(path string, opt *Options) error {
	if opt == nil {
		opt = DefaultOptions()
	}

	vlog.dirPath = path
	vlog.sync = opt.Sync
	vlog.opt = opt

	vlog.buf = new(bytes.Buffer)

	vlog.filesMap = make(map[uint32]*logFile)
	if err := vlog.openOrCreateFiles(); err != nil {
		return errors.Annotatef(err, "unable to open value log")
	}

	return nil
}

func (vlog *valueLog) openOrCreateFiles() error {
	files, err := ioutil.ReadDir(vlog.dirPath)
	if err != nil {
		return errors.Annotatef(err, "error while read dir: %s", vlog.dirPath)
	}

	// open all files at start, or we can lazily open it to quick start time
	// the vlog file name <fid>.vlog will be like "000001.vlog"
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fName := file.Name()
		if !strings.HasSuffix(fName, fileExt) {
			continue
		}

		fid64, err := strconv.ParseUint(strings.TrimSuffix(fName, fileExt), 10, 32)
		if err != nil {
			return errors.Annotatef(err, "parse file %s err", fName)
		}
		fid := uint32(fid64)

		logFile, err := newLogFile(fid, vlog.filePath(fid))
		if err != nil {
			return errors.Annotatef(err, "error open file %s", fName)
		}

		vlog.filesMap[fid] = logFile

		if fid > vlog.maxFid {
			vlog.maxFid = fid
		}
	}

	// no files, then create the first file with fid = 0
	if len(vlog.filesMap) == 0 {
		_, err := vlog.createLogFile(0)
		if err != nil {
			return errors.Annotatef(err, "error create first file")
		}
	} else {
		// maxFid is the file we will append record to, check if we need to create a new one
		curFile := vlog.filesMap[vlog.maxFid]
		if curFile.end {
			_, err := vlog.createLogFile(atomic.AddUint32(&vlog.maxFid, 1))
			if err != nil {
				return errors.Annotatef(err, "error create new file")
			}
		}
	}

	return nil
}

func (vlog *valueLog) createLogFile(fid uint32) (*logFile, error) {
	path := vlog.filePath(fid)
	logFile, err := newLogFile(fid, path)
	if err != nil {
		return nil, errors.Annotate(err, "unable to create log file")
	}

	vlog.filesLock.Lock()
	vlog.filesMap[fid] = logFile
	vlog.filesLock.Unlock()

	return logFile, nil
}

func (vlog *valueLog) close() error {
	vlog.filesLock.Lock()
	defer vlog.filesLock.Unlock()

	var err error
	curFile := vlog.filesMap[vlog.maxFid]

	// finalize the curFile when it's tool big, so when restart, we don't need to scan the too big curFile to recover the maxTS of the file
	if curFile.GetWriteOffset() >= finalizeFileSizeAtClose {
		err = curFile.finalize()
		if err != nil {
			return errors.Annotatef(err, "finalize file %s failed", curFile.path)
		}
	}

	for _, logFile := range vlog.filesMap {
		err = logFile.close()
		if err != nil {
			return errors.Annotatef(err, "close %s failed", logFile.path)
		}
	}

	return nil
}

func (vlog *valueLog) readValue(vp valuePointer) ([]byte, error) {
	logFile, err := vlog.getFileRLocked(vp.Fid)
	if err != nil {
		return nil, errors.Annotatef(err, "get file(id: %d) failed", vp.Fid)
	}

	defer logFile.lock.RUnlock()

	record, err := logFile.readRecord(vp.Offset)
	if err != nil {
		return nil, errors.Annotatef(err, "read record at %+v failed", vp)
	}

	return record.payload, nil
}

// write is thread-unsafe by design and should not be called concurrently.
func (vlog *valueLog) write(reqs []*request) error {
	vlog.filesLock.RLock()
	curFile := vlog.filesMap[vlog.maxFid]
	vlog.filesLock.RUnlock()

	var bufReqs []*request

	toDisk := func() error {
		err := curFile.Write(vlog.buf.Bytes(), vlog.sync)
		if err != nil {
			return errors.Trace(err)
		}

		for _, req := range bufReqs {
			curFile.updateMaxTS(req.ts())
		}
		vlog.buf.Reset()
		bufReqs = bufReqs[:0]

		// rotate file
		if curFile.GetWriteOffset() > vlog.opt.ValueLogFileSize {
			err := curFile.finalize()
			if err != nil {
				return errors.Annotatef(err, "finalize file %s failed", curFile.path)
			}

			id := atomic.AddUint32(&vlog.maxFid, 1)
			curFile, err = vlog.createLogFile(id)
			if err != nil {
				return errors.Annotatef(err, "create file id %d failed", id)
			}
		}
		return nil
	}

	for _, req := range reqs {
		req.valuePointer.Fid = curFile.fid
		req.valuePointer.Offset = curFile.GetWriteOffset() + int64(vlog.buf.Len())
		_, err := encodeRecord(vlog.buf, req.payload)
		if err != nil {
			return errors.Trace(err)
		}

		bufReqs = append(bufReqs, req)

		writeNow := curFile.GetWriteOffset()+int64(vlog.buf.Len()) > vlog.opt.ValueLogFileSize

		if writeNow {
			if err := toDisk(); err != nil {
				return errors.Annotate(err, "write to disk failed")
			}
		}
	}

	return toDisk()
}

// sortedFids returns the file id sorted
func (vlog *valueLog) sortedFids() []uint32 {
	ret := make([]uint32, 0, len(vlog.filesMap))
	for fid := range vlog.filesMap {
		ret = append(ret, fid)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i] < ret[j]
	})

	return ret
}

func (vlog *valueLog) scanRequests(start valuePointer, fn func(*request) error) error {
	return vlog.scan(start, func(vp valuePointer, record *Record) error {
		binlog := new(pb.Binlog)
		err := binlog.Unmarshal(record.payload)
		if err != nil {
			return errors.Trace(err)
		}

		// skip the wrongly write binlog by pump client previous
		if binlog.StartTs == 0 && binlog.CommitTs == 0 {
			log.Info("skip empty binlog")
			return nil
		}

		rq := request{
			startTS:      binlog.StartTs,
			commitTS:     binlog.CommitTs,
			tp:           binlog.Tp,
			valuePointer: vp,
		}
		return fn(&rq)
	})
}

// currently we only use this in NewAppend** to scan the record which not write to KV but in the value log, so it's OK to hold the vlog.filesLock lock
func (vlog *valueLog) scan(start valuePointer, fn func(vp valuePointer, record *Record) error) error {
	vlog.filesLock.Lock()
	defer vlog.filesLock.Unlock()

	fids := vlog.sortedFids()

	for _, fid := range fids {
		if fid < start.Fid {
			continue
		}
		lf := vlog.filesMap[fid]
		var startOffset int64
		if fid == start.Fid {
			startOffset = start.Offset
		}
		err := lf.scan(startOffset, fn)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// delete data <= gcTS
func (vlog *valueLog) gcTS(gcTS int64) {
	vlog.filesLock.Lock()
	var toDeleteFiles []*logFile

	for _, logFile := range vlog.filesMap {
		if logFile.fid == vlog.maxFid {
			continue
		}

		if logFile.maxTS <= gcTS {
			toDeleteFiles = append(toDeleteFiles, logFile)
		}
	}

	for _, logFile := range toDeleteFiles {
		delete(vlog.filesMap, logFile.fid)
	}
	vlog.filesLock.Unlock()

	for _, logFile := range toDeleteFiles {
		logFile.lock.Lock()
		err := logFile.close()
		if err != nil {
			log.Error("close file failed", zap.String("path", logFile.path), zap.Error(err))
		}
		err = os.Remove(logFile.path)
		if err != nil {
			log.Error("remove file failed", zap.String("path", logFile.path), zap.Error(err))
		}
		logFile.lock.Unlock()
	}
}
