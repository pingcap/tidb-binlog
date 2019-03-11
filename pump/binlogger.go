package pump

import (
	"hash/crc32"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	bf "github.com/pingcap/tidb-binlog/pkg/binlogfile"
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
)

// Binlogger is the interface that for append and read binlog
type Binlogger interface {
	// read nums binlog events from the "from" position
	ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error)

	// batch write binlog event, and returns current offset(if have).
	WriteTail(entity *binlog.Entity) (int64, error)

	AsyncWriteTail(entity *binlog.Entity, cb callback)

	// Walk reads binlog from the "from" position and sends binlogs in the streaming way
	Walk(ctx context.Context, from binlog.Pos, sendBinlog func(entity *binlog.Entity) error) error

	// close the binlogger
	Close() error

	// GC recycles the old binlog file
	GC(days time.Duration, pos binlog.Pos)

	// Name tells the name of underlying file
	Name() string

	// Rotate rotates binlog file
	Rotate() error
}

// binlogger is a logical representation of the log storage
// it is either in read mode or append mode.
type binlogger struct {
	dir string

	// encoder encodes binlog payload into bytes, and write to file
	encoder Encoder

	codec compress.CompressionCodec

	// file is the lastest file in the dir
	file    *file.LockedFile
	dirLock *file.LockedFile
	mutex   sync.Mutex
}

// OpenBinlogger returns a binlogger for write, then it can be appended
func OpenBinlogger(dirpath string, codec compress.CompressionCodec) (Binlogger, error) {
	log.Infof("open binlog directory %s", dirpath)
	var (
		err            error
		lastFileName   string
		lastFileSuffix uint64
		dirLock        *file.LockedFile
		f              *file.LockedFile
		offset         int64
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
	names, _ := bf.ReadBinlogNames(dirpath)
	// if no binlog files, we create from binlog-0000000000000000
	if len(names) == 0 {
		// create a binlog file with ts = 0
		lastFileName = path.Join(dirpath, bf.BinlogName(0))
		lastFileSuffix = 0
	} else {
		// check binlog files and find last binlog file
		if !bf.IsValidBinlog(names) {
			err = ErrFileContentCorruption
			return nil, errors.Trace(err)
		}

		lastFileName = path.Join(dirpath, names[len(names)-1])
		lastFileSuffix, err = bf.ParseBinlogName(names[len(names)-1])
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	log.Infof("open and lock binlog file %s", lastFileName)
	f, err = file.TryLockFile(lastFileName, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	offset, err = f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, errors.Trace(err)
	}

	latestFilePos.Suffix = lastFileSuffix
	latestFilePos.Offset = offset
	checkpointGauge.WithLabelValues("latest").Set(posToFloat(&latestFilePos))

	binlog := &binlogger{
		dir:     dirpath,
		file:    f,
		encoder: newEncoder(f, codec),
		codec:   codec,
		dirLock: dirLock,
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
		ent     = &binlog.Entity{}
		decoder Decoder
		first   = true
		dirpath = b.dir
	)

	names, err := bf.ReadBinlogNames(dirpath)
	if err != nil {
		corruptionBinlogCounter.Add(1)
		return errors.Trace(err)
	}

	nameIndex, ok := bf.SearchIndex(names, from.Suffix)
	if !ok {
		corruptionBinlogCounter.Add(1)
		return bf.ErrFileNotFound
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
			readErrorCounter.WithLabelValues("local").Add(1)
			return errors.Trace(err)
		}
		defer f.Close()

		if first {
			first = false
			_, err := f.Seek(from.Offset, io.SeekStart)
			if err != nil {
				readErrorCounter.WithLabelValues("local").Add(1)
				return errors.Trace(err)
			}
		}

		decoder = NewDecoder(from, io.Reader(f))

		for {
			select {
			case <-ctx.Done():
				log.Warningf("Walk Done!")
				return nil
			default:
			}

			buf := binlogBufferPool.Get().(*binlogBuffer)
			beginTime := time.Now()
			err = decoder.Decode(ent, buf)

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
						decoder = NewDecoder(from, io.Reader(f))
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

			from.Offset = ent.Pos.Offset

			dup := cloneEntity(ent)

			err := sendBinlog(dup)
			binlogBufferPool.Put(buf)
			if err != nil {
				return errors.Trace(err)
			}
		}

		if err != nil && err != io.EOF {
			readErrorCounter.WithLabelValues("local").Add(1)
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
	names, err := bf.ReadBinlogNames(b.dir)
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

		curSuffix, err := bf.ParseBinlogName(name)
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
		} else if time.Now().Sub(fi.ModTime()) > days {
			log.Warningf("binlog file %s is already reach the gc time, but data is not send to kafka, position is %v", fileName, pos)
		}
	}
}

// Writes appends the binlog
// if size of current file is bigger than SegmentSizeBytes, then rotate a new file
func (b *binlogger) WriteTail(entity *binlog.Entity) (int64, error) {
	beginTime := time.Now()
	defer func() {
		writeBinlogHistogram.WithLabelValues("local").Observe(time.Since(beginTime).Seconds())
		writeBinlogSizeHistogram.WithLabelValues("local").Observe(float64(len(entity.Payload)))
	}()

	b.mutex.Lock()
	defer b.mutex.Unlock()

	if len(entity.Payload) == 0 {
		return 0, nil
	}

	curOffset, err := b.encoder.Encode(entity)
	if err != nil {
		writeErrorCounter.WithLabelValues("local").Add(1)
		log.Errorf("write local binlog file %d error %v", latestFilePos.Suffix, err)
		return 0, errors.Trace(err)
	}

	latestFilePos.Offset = curOffset
	checkpointGauge.WithLabelValues("latest").Set(posToFloat(&latestFilePos))

	if curOffset < GlobalConfig.segmentSizeBytes {
		return curOffset, nil
	}

	err = b.Rotate()
	return curOffset, errors.Trace(err)
}

// AsyncWriteTail use WriteTail actually now
func (b *binlogger) AsyncWriteTail(entity *binlog.Entity, cb callback) {
	offset, err := b.WriteTail(entity)
	if cb != nil {
		cb(offset, err)
	}
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

func (b *binlogger) Name() string {
	return b.file.Name()
}

// Rotate creates a new file for append binlog
func (b *binlogger) Rotate() error {
	filename := bf.BinlogName(b.seq() + 1)
	latestFilePos.Suffix = b.seq() + 1
	latestFilePos.Offset = 0
	checkpointGauge.WithLabelValues("latest").Set(posToFloat(&latestFilePos))

	fpath := path.Join(b.dir, filename)

	newTail, err := file.LockFile(fpath, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return errors.Trace(err)
	}

	if err = b.file.Close(); err != nil {
		log.Errorf("failed to unlock during closing file: %s", err)
	}
	b.file = newTail

	b.encoder = newEncoder(b.file, b.codec)
	log.Infof("segmented binlog file %v is created", fpath)
	return nil
}

func (b *binlogger) seq() uint64 {
	if b.file == nil {
		return 0
	}

	seq, err := bf.ParseBinlogName(path.Base(b.file.Name()))
	if err != nil {
		log.Fatalf("bad binlog name %s (%v)", b.file.Name(), err)
	}

	return seq
}

func cloneEntity(ent *binlog.Entity) (dup *binlog.Entity) {
	dup = new(binlog.Entity)
	dup.Pos = ent.Pos
	dup.Payload = make([]byte, len(ent.Payload))
	copy(dup.Payload, ent.Payload)
	dup.Checksum = make([]byte, len(ent.Checksum))
	copy(dup.Checksum, ent.Checksum)

	return
}
