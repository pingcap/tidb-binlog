package binlogfile

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
)

var (
	// ErrFileContentCorruption represents file or directory's content is curruption for some season
	ErrFileContentCorruption = errors.New("binlogger: content is corruption")

	// ErrCRCMismatch is the error represents crc don't match
	ErrCRCMismatch = errors.New("binlogger: crc mismatch")

	// ErrMagicMismatch is the error represents magic don't match
	ErrMagicMismatch = errors.New("binlogger: magic mismatch")

	crcTable = crc32.MakeTable(crc32.Castagnoli)

	SegmentSizeBytes int64 = 512 * 1024 * 1024
)

// Binlogger is the interface that for append and read binlog
type Binlogger interface {
	// read nums binlog events from the "from" position
	ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error)

	// batch write binlog event, and returns current offset(if have).
	WriteTail(entity *binlog.Entity) (int64, error)

	// Walk reads binlog from the "from" position and sends binlogs in the streaming way
	Walk(ctx context.Context, from binlog.Pos, sendBinlog func(entity *binlog.Entity) error) error

	// close the binlogger
	Close() error

	// GC recycles the old binlog file
	GC(days time.Duration, pos binlog.Pos)

	// CompressFile compress the old binlog file
	CompressFile() error
}

// binlogger is a logical representation of the log storage
// it is either in read mode or append mode.
type binlogger struct {
	dir string

	// encoder encodes binlog payload into bytes, and write to file
	encoder Encoder

	codec compress.CompressionCodec

	lastSuffix uint64
	lastOffset int64

	// file is the lastest file in the dir
	file    *file.LockedFile
	dirLock *file.LockedFile
	mutex   sync.Mutex

	// if compressing is 1 meaning the binlog file is in compressing
	compressing int32
}

// OpenBinlogger returns a binlogger for write, then it can be appended
func OpenBinlogger(dirpath string, codec compress.CompressionCodec) (Binlogger, error) {
	log.Infof("open binlog directory %s", dirpath)
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
				log.Errorf("failed to unlock directory %s: %v with return error %v", dirpath, err1, err)
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

	log.Infof("open and lock binlog file %s", lastFileName)
	fileLock, err = file.TryLockFile(lastFileName, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	offset, err := fileLock.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, errors.Trace(err)
	}

	binlog := &binlogger{
		dir:        dirpath,
		file:       fileLock,
		encoder:    NewEncoder(fileLock, offset),
		codec:      codec,
		dirLock:    dirLock,
		lastSuffix: lastFileSuffix,
		lastOffset: offset,
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

// Walk reads binlog from the "from" position and sends binlogs in the streaming way
func (b *binlogger) Walk(ctx context.Context, from binlog.Pos, sendBinlog func(entity *binlog.Entity) error) error {
	log.Infof("[binlogger] walk from position %+v", from)
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
			log.Warningf("Walk Done!")
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

		decoder, err = NewDecoder(f, from.Offset)
		if err != nil {
			return errors.Trace(err)
		}

		for {
			select {
			case <-ctx.Done():
				log.Warningf("Walk Done!")
				return nil
			default:
			}

			var payload []byte
			var offset int64
			beginTime := time.Now()
			payload, offset, err = decoder.Decode()

			if err != nil {
				// if this is the current file we are writing,
				// may contain a partial write entity in the file end
				// we treat is as io.EOF and will return nil
				if err == io.ErrUnexpectedEOF && isLastFile {
					err = io.EOF
				}

				// seek next binlog and report metrics
				if err == ErrCRCMismatch || err == ErrMagicMismatch {
					corruptionBinlogCounter.Add(1)
					log.Errorf("decode %+v binlog error %v", from, err)
					// offset + 1 to skip magic code of current binlog
					offset, err1 := seekBinlog(f, from.Offset+1)
					if err1 == nil {
						from.Offset = offset
						decoder, err = NewDecoder(f, from.Offset)

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
			log.Errorf("read from local binlog file %d error %v", from.Suffix, err)
			return errors.Trace(err)
		}

		from.Suffix++
		from.Offset = 0
	}

	return nil
}

// GC recycles the old binlog file
func (b *binlogger) GC(days time.Duration, pos binlog.Pos) {
	names, err := ReadBinlogNames(b.dir)
	if err != nil {
		log.Error("read binlog files error:", names)
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
			log.Error("GC binlog file stat error:", err)
			continue
		}

		curSuffix, _, err := ParseBinlogName(name)
		if err != nil {
			log.Errorf("parse binlog error %v", err)
		}

		if curSuffix < pos.Suffix {
			err := os.Remove(fileName)
			if err != nil {
				log.Error("remove old binlog file err")
				continue
			}
			log.Info("GC binlog file:", fileName)
		} else if time.Since(fi.ModTime()) > days {
			log.Warningf("binlog file %s is already reach the gc time, but data is not send to kafka, position is %v", fileName, pos)
		}
	}
}

// Writes appends the binlog
// if size of current file is bigger than SegmentSizeBytes, then rotate a new file
func (b *binlogger) WriteTail(entity *binlog.Entity) (int64, error) {
	beginTime := time.Now()
	payload := entity.Payload
	defer func() {
		writeBinlogHistogram.WithLabelValues("local").Observe(time.Since(beginTime).Seconds())
		writeBinlogSizeHistogram.WithLabelValues("local").Observe(float64(len(payload)))
	}()

	b.mutex.Lock()
	defer b.mutex.Unlock()

	if len(payload) == 0 {
		return 0, nil
	}

	curOffset, err := b.encoder.Encode(payload)
	if err != nil {
		log.Errorf("write local binlog file %d error %v", b.lastSuffix, err)
		return 0, errors.Trace(err)
	}

	b.lastOffset = curOffset

	if curOffset < SegmentSizeBytes {
		return curOffset, nil
	}

	err = b.rotate()
	return curOffset, errors.Trace(err)
}

// Close closes the binlogger
func (b *binlogger) Close() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.file != nil {
		if err := b.file.Close(); err != nil {
			log.Errorf("failed to unlock file %s during closing file: %v", b.file.Name(), err)
		}
	}

	if b.dirLock != nil {
		if err := b.dirLock.Close(); err != nil {
			log.Errorf("failed to unlock dir %s during closing file: %v", b.dir, err)
		}
	}

	return nil
}

// rotate creates a new file for append binlog
func (b *binlogger) rotate() error {
	if err := b.file.Close(); err != nil {
		log.Errorf("failed to unlock during closing file: %s", err)
	}

	filename := BinlogName(b.seq() + 1)
	b.lastSuffix = b.seq() + 1
	b.lastOffset = 0

	fpath := path.Join(b.dir, filename)

	newTail, err := file.LockFile(fpath, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return errors.Trace(err)
	}

	b.file = newTail
	b.encoder = NewEncoder(b.file, 0)
	log.Infof("segmented binlog file %v is created", fpath)

	return nil
}

func (b *binlogger) seq() uint64 {
	if b.file == nil {
		return 0
	}

	seq, _, err := ParseBinlogName(path.Base(b.file.Name()))
	if err != nil {
		log.Fatalf("bad binlog name %s (%v)", b.file.Name(), err)
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

// CompressFile compresses the old binlog files
func (b *binlogger) CompressFile() error {
	if b.codec == compress.CompressionNone {
		return nil
	}

	if atomic.LoadInt32(&b.compressing) == 1 {
		log.Debug("binlog file is in compressing")
		return nil
	}

	atomic.StoreInt32(&b.compressing, 1)
	defer atomic.StoreInt32(&b.compressing, 0)

	names, err := ReadBinlogNames(b.dir)
	if err != nil {
		log.Error("read binlog files error:", names)
		return errors.Trace(err)
	}

	if len(names) == 0 {
		return nil
	}

	// skip the latest binlog file
	for _, name := range names[:len(names)-1] {
		fileName := path.Join(b.dir, name)

		if compress.IsCompressFile(fileName) {
			continue
		}

		ts, err := GetFirstBinlogCommitTS(fileName)
		if err != nil {
			log.Errorf("get first binlog's commit ts in %s failed %v", fileName, err)
			return errors.Trace(err)
		}

		startT := time.Now()
		if err = compress.CompressFileWithTS(fileName, b.codec, ts); err != nil {
			log.Errorf("compress file %s failed %v", fileName, err)
			return errors.Trace(err)
		}
		log.Infof("compress file %s cost %v", fileName, time.Since(startT))
	}

	return nil
}
