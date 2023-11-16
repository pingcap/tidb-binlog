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

package binlogfile

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

var (
	// ErrFileContentCorruption represents file or directory's content is curruption for some season
	ErrFileContentCorruption = errors.NewNoStackError("binlogger: content is corruption")

	// ErrCRCMismatch is the error represents crc don't match
	ErrCRCMismatch = errors.NewNoStackError("binlogger: crc mismatch")

	// ErrMagicMismatch is the error represents magic don't match
	ErrMagicMismatch = errors.NewNoStackError("binlogger: magic mismatch")

	crcTable = crc32.MakeTable(crc32.Castagnoli)

	// SegmentSizeBytes is the max file size of file
	SegmentSizeBytes int64 = 512 * 1024 * 1024
)

// Binlogger is the interface that for append and read binlog
type Binlogger interface {
	// read nums binlog events from the "from" position
	ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error)

	// ReadAll reads all binlog in the directory.
	ReadAll(ctx context.Context) (<-chan *binlog.Entity, <-chan error)

	// batch write binlog event, and returns current offset(if have).
	WriteTail(entity *binlog.Entity) (binlog.Pos, error)

	// Walk reads binlog from the "from" position and sends binlogs in the streaming way
	Walk(ctx context.Context, from binlog.Pos, sendBinlog func(entity *binlog.Entity) error) error

	// close the binlogger
	Close() error

	// GGCByTime delete all files that's older than the specified duration, the latest file is always kept
	GCByTime(retentionTime time.Duration)

	// GCByPos delete all files that's before the specified position, the latest file is always kept
	GCByPos(pos binlog.Pos)
}

// binlogger is a logical representation of the log storage
// it is either in read mode or append mode.
type binlogger struct {
	dir         string
	maxFileSize int64

	// encoder encodes binlog payload into bytes, and write to file
	encoder Encoder

	lastSuffix uint64
	lastOffset int64

	// file is the lastest file in the dir
	file    *file.LockedFile
	dirLock *file.LockedFile
	mutex   sync.Mutex
}

// OpenBinlogger returns a binlogger for write, then it can be appended
func OpenBinlogger(dirpath string, maxFileSize int64) (Binlogger, error) {
	log.Info("open binlogger", zap.String("directory", dirpath))
	var (
		err            error
		lastFileName   string
		lastFileSuffix uint64
		dirLock        *file.LockedFile
		fileLock       *file.LockedFile
	)
	err = os.MkdirAll(dirpath, file.PrivateDirMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// lock directory firstly
	dirLockFile := path.Join(dirpath, ".lock")
	dirLock, err = file.LockFile(dirLockFile, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err != nil && dirLock != nil {
			if err1 := dirLock.Close(); err1 != nil {
				log.Error("failed to unlock", zap.String("directory", dirpath), zap.Error(err1))
			}
		}
	}()

	// ignore file not found error
	names, _ := ReadBinlogNames(dirpath)
	// if no binlog files, we create from index 0, the file name like binlog-0000000000000000-20190101010101
	if len(names) == 0 {
		lastFileName = path.Join(dirpath, BinlogName(0))
		lastFileSuffix = 0
	} else {
		// check binlog files and find last binlog file
		if !IsValidBinlog(names) {
			err = ErrFileContentCorruption
			return nil, errors.Trace(err)
		}

		lastFileName = path.Join(dirpath, names[len(names)-1])
		lastFileSuffix, _, err = ParseBinlogName(names[len(names)-1])
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	log.Info("open and lock binlog file", zap.String("name", lastFileName))
	fileLock, err = file.TryLockFile(lastFileName, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	offset, err := fileLock.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, errors.Trace(err)
	}

	binlog := &binlogger{
		dir:         dirpath,
		maxFileSize: maxFileSize,
		file:        fileLock,
		encoder:     NewEncoder(fileLock, offset),
		dirLock:     dirLock,
		lastSuffix:  lastFileSuffix,
		lastOffset:  offset,
	}

	return binlog, nil
}

// CloseBinlogger closes the binlogger
func CloseBinlogger(binlogger Binlogger) error {
	return binlogger.Close()
}

// ReadFrom reads `nums` binlogs from the given binlog position
// read all binlogs from one file then close it and open the following file
func (b *binlogger) ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error) {
	if nums < 0 {
		return nil, errors.Errorf("read number must be positive")
	}

	inums := int(nums)
	var ents []binlog.Entity

	ctx, cancel := context.WithCancel(context.Background())

	err := b.Walk(ctx, from, func(entity *binlog.Entity) error {
		if len(ents) < inums {
			ents = append(ents, *entity)
		}

		if len(ents) == inums {
			cancel()
		}

		return nil
	})

	return ents, err
}

// ReadAll reads all binlog in the directory.
// `result` contains the returned binlogs, errChan contains any error that occurs in reading.
func (b *binlogger) ReadAll(ctx context.Context) (<-chan *binlog.Entity, <-chan error) {
	errChan := make(chan error, 1)
	result := make(chan *binlog.Entity)

	var minFileIndex uint64
	names, err := ReadBinlogNames(b.dir)
	if err != nil {
		log.Error("read binlog files failed", zap.Error(err))
	} else if len(names) > 0 {
		minFileIndex, _, err = ParseBinlogName(names[0])
	}
	if err != nil {
		errChan <- err
	}
	if err != nil || len(names) == 0 {
		close(errChan)
		close(result)
		return result, errChan
	}

	from := binlog.Pos{Suffix: minFileIndex, Offset: 0}
	go func() {
		err := b.Walk(ctx, from, func(entity *binlog.Entity) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case result <- entity:
				return nil
			}
		})
		if err != nil {
			errChan <- err
		}
		close(errChan)
		close(result)
	}()
	return result, errChan
}

// Walk reads binlog from the "from" position and sends binlogs in the streaming way
func (b *binlogger) Walk(ctx context.Context, from binlog.Pos, sendBinlog func(entity *binlog.Entity) error) error {
	log.Info("receive walk request", zap.Reflect("position", from))
	var (
		decoder Decoder
		first   = true
		dirpath = b.dir
	)

	names, err := ReadBinlogNames(dirpath)
	if err != nil {
		return errors.Trace(err)
	}

	nameIndex, ok := SearchIndex(names, from.Suffix)
	if !ok {
		return ErrFileNotFound
	}

	var isLastFile bool
	for idx, name := range names[nameIndex:] {
		if idx == len(names[nameIndex:])-1 {
			isLastFile = true
		}

		select {
		case <-ctx.Done():
			log.Warn("Walk Done!")
			return nil
		default:
		}

		p := path.Join(dirpath, name)
		f, err := os.OpenFile(p, os.O_RDONLY, file.PrivateFileMode)
		if err != nil {
			return errors.Trace(err)
		}
		defer f.Close()

		if first {
			first = false
			_, err := f.Seek(from.Offset, io.SeekStart)
			if err != nil {
				return errors.Trace(err)
			}
		}

		decoder = NewDecoder(io.Reader(f), from.Offset)

		for {
			select {
			case <-ctx.Done():
				log.Warn("Walk Done!")
				return nil
			default:
			}

			var payload []byte
			var offset int64
			beginTime := time.Now()
			payload, offset, err = decoder.Decode()

			if err != nil {
				errCause := errors.Cause(err)
				// if this is the current file we are writing,
				// may contain a partial write entity in the file end
				// we treat is as io.EOF and will return nil
				if errCause == io.ErrUnexpectedEOF && isLastFile {
					err = io.EOF
				}

				// seek next binlog and report metrics
				if errCause == ErrCRCMismatch || errCause == ErrMagicMismatch {
					corruptionBinlogCounter.Add(1)
					log.Error("decode binlog failed %v", zap.Reflect("position", from), zap.Error(err))
					// offset + 1 to skip magic code of current binlog
					offset, err1 := seekBinlog(f, from.Offset+1)
					if err1 == nil {
						from.Offset = offset
						decoder = NewDecoder(io.Reader(f), from.Offset)
						continue
					}
					if err1 == io.EOF || err1 == io.ErrUnexpectedEOF {
						err = io.EOF
					} else {
						err = errors.Annotatef(err1, "decode %+v binlog error %v, and fail to seek next magic", from, err)
					}
				}
				break
			}
			readBinlogHistogram.WithLabelValues("local").Observe(time.Since(beginTime).Seconds())

			from.Offset = offset

			err := sendBinlog(&binlog.Entity{
				Payload: payload,
				Pos: binlog.Pos{
					Suffix: from.Suffix,
					Offset: from.Offset,
				},
			})
			if err != nil {
				return errors.Trace(err)
			}
		}

		if err != nil && err != io.EOF {
			log.Error("read from local binlog file failed", zap.Uint64("suffix", from.Suffix), zap.Error(err))
			return errors.Trace(err)
		}

		from.Suffix++
		from.Offset = 0
	}

	return nil
}

// GCByPos delete all files that's before the specified position, the latest file is always kept
func (b *binlogger) GCByPos(pos binlog.Pos) {
	names, err := ReadBinlogNames(b.dir)
	if err != nil {
		log.Error("read binlog files failed", zap.Error(err))
		return
	}

	if len(names) == 0 {
		return
	}

	// skip the latest binlog file
	for _, name := range names[:len(names)-1] {
		curSuffix, _, err := ParseBinlogName(name)
		if err != nil {
			log.Error("parse binlog failed", zap.Error(err))
		}
		if curSuffix < pos.Suffix {
			fileName := path.Join(b.dir, name)
			if err := os.Remove(fileName); err != nil {
				log.Error("fail to remove old binlog file ", zap.Error(err), zap.String("file name", fileName))
				continue
			}
			log.Info("GC binlog file", zap.String("file name", fileName))
		}
	}
}

// GGCByTime delete all files that's older than the specified duration, the latest file is always kept
func (b *binlogger) GCByTime(retentionTime time.Duration) {
	names, err := ReadBinlogNames(b.dir)
	if err != nil {
		log.Error("read binlog files failed", zap.Error(err))
		return
	}

	if len(names) == 0 {
		return
	}

	// skip the latest binlog file
	for _, name := range names[:len(names)-1] {
		fileName := path.Join(b.dir, name)
		fi, err := os.Stat(fileName)
		if err != nil {
			log.Error("GC binlog file stat failed", zap.Error(err))
			continue
		}

		if time.Since(fi.ModTime()) > retentionTime {
			if err := os.Remove(fileName); err != nil {
				log.Error("fail to remove old binlog file", zap.Error(err), zap.String("file name", fileName))
				continue
			}
			log.Info("GC binlog file", zap.String("file name", fileName))
		}
	}
}

// Writes appends the binlog
// if size of current file is bigger than `maxFileSize`, then rotate a new file
func (b *binlogger) WriteTail(entity *binlog.Entity) (binlog.Pos, error) {
	beginTime := time.Now()
	payload := entity.Payload
	defer func() {
		writeBinlogHistogram.WithLabelValues("local").Observe(time.Since(beginTime).Seconds())
		writeBinlogSizeHistogram.WithLabelValues("local").Observe(float64(len(payload)))
	}()

	b.mutex.Lock()
	defer b.mutex.Unlock()

	if len(payload) == 0 {
		return binlog.Pos{}, nil
	}

	curOffset, err := b.encoder.Encode(payload)
	if err != nil {
		log.Error("write local binlog failed", zap.Uint64("suffix", b.lastSuffix), zap.Error(err))
		return binlog.Pos{}, errors.Trace(err)
	}

	b.lastOffset = curOffset
	pos := binlog.Pos{Suffix: b.lastSuffix, Offset: curOffset}

	if curOffset < b.maxFileSize {
		return pos, nil
	}

	err = b.rotate()
	return pos, errors.Trace(err)
}

// Close closes the binlogger
func (b *binlogger) Close() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.file != nil {
		if err := b.file.Close(); err != nil {
			log.Error("failed to unlock file during closing file", zap.String("name", b.file.Name()), zap.Error(err))
		}
	}

	if b.dirLock != nil {
		if err := b.dirLock.Close(); err != nil {
			log.Error("failed to unlock dir during closing file", zap.String("directory", b.dir), zap.Error(err))
		}
	}

	return nil
}

// rotate creates a new file for append binlog
func (b *binlogger) rotate() error {
	filename := BinlogName(b.seq() + 1)
	b.lastSuffix = b.seq() + 1
	b.lastOffset = 0

	fpath := path.Join(b.dir, filename)

	newTail, err := file.LockFile(fpath, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return errors.Trace(err)
	}

	if err = b.file.Close(); err != nil {
		log.Error("failed to unlock during closing file", zap.Error(err))
	}
	b.file = newTail

	b.encoder = NewEncoder(b.file, 0)
	log.Info("segmented binlog file is created", zap.String("path", fpath))
	return nil
}

func (b *binlogger) seq() uint64 {
	if b.file == nil {
		return 0
	}

	seq, _, err := ParseBinlogName(path.Base(b.file.Name()))
	if err != nil {
		log.Fatal("bad binlog name", zap.String("name", b.file.Name()), zap.Error(err))
	}

	return seq
}

// use magic code to find next binlog and skips corruption data
// seekBinlog seeks one binlog from current offset
func seekBinlog(f *os.File, offset int64) (int64, error) {
	var (
		batchSize    = 1024
		headerLength = 3 // length of magic code - 1
		tailLength   = batchSize - headerLength
		buff         = make([]byte, batchSize)
		header       = buff[0:headerLength]
		tail         = buff[headerLength:]
	)

	_, err := f.Seek(offset, io.SeekStart)
	if err != nil {
		return 0, errors.Trace(err)
	}

	// read header firstly
	_, err = io.ReadFull(f, header)
	if err != nil {
		return 0, err
	}

	for {
		// read tail
		n, err := io.ReadFull(f, tail)
		// maybe it meets EOF and dont read fully
		for i := 0; i < n; i++ {
			// forward one byte and compute magic
			magicNum := binary.LittleEndian.Uint32(buff[i : i+4])
			if CheckMagic(magicNum) == nil {
				offset = offset + int64(i)
				if _, err1 := f.Seek(offset, io.SeekStart); err1 != nil {
					return 0, errors.Trace(err1)
				}
				return offset, nil
			}
		}
		if err != nil {
			return 0, err
		}

		// hard code
		offset += int64(tailLength)
		header[0], header[1], header[2] = tail[tailLength-3], tail[tailLength-2], tail[tailLength-1]
	}
}
