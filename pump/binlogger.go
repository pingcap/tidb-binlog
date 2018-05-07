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
	// SegmentSizeBytes is the max threshold of binlog segment file size
	// as an exported variable, you can define a different size
	SegmentSizeBytes int64 = 512 * 1024 * 1024

	// ErrFileContentCorruption represents file or directory's content is curruption for some season
	ErrFileContentCorruption = errors.New("binlogger: content is corruption")

	// ErrCRCMismatch is the error represents crc don't match
	ErrCRCMismatch = errors.New("binlogger: crc mismatch")
	crcTable       = crc32.MakeTable(crc32.Castagnoli)
)

// Binlogger is the interface that for append and read binlog
type Binlogger interface {
	// read nums binlog events from the "from" position
	ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error)

	// batch write binlog event, and returns current offset(if have).
	WriteTail(payload []byte) (int64, error)

	// Walk reads binlog from the "from" position and sends binlogs in the streaming way
	Walk(ctx context.Context, from binlog.Pos, sendBinlog func(entity binlog.Entity) error) error

	// close the binlogger
	Close() error

	// GC recycles the old binlog file
	GC(days time.Duration, pos binlog.Pos)

	// Name tells the name of underlying file
	Name() string
}

// binlogger is a logical representation of the log storage
// it is either in read mode or append mode.
type binlogger struct {
	dir string

	// encoder encodes binlog payload into bytes, and write to file
	encoder Encoder

	codec compress.CompressionCodec

	// file is the lastest file in the dir
	file  *file.LockedFile
	mutex sync.Mutex
}

// CreateBinlogger creates a binlog directory, then can append binlogs
func CreateBinlogger(dirpath string, codec compress.CompressionCodec) (Binlogger, error) {
	if Exist(dirpath) {
		return nil, os.ErrExist
	}

	if err := bf.CreateDirAll(dirpath); err != nil {
		return nil, errors.Trace(err)
	}

	p := path.Join(dirpath, bf.BinlogName(0))
	log.Infof("create and lock binlog file %s", p)
	f, err := file.LockFile(p, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	binlog := &binlogger{
		dir:     dirpath,
		encoder: newEncoder(f, codec),
		file:    f,
	}

	return binlog, nil
}

//OpenBinlogger returns a binlogger for write, then it can be appended
func OpenBinlogger(dirpath string, codec compress.CompressionCodec) (Binlogger, error) {
	names, err := bf.ReadBinlogNames(dirpath)
	if err != nil {
		return nil, err
	}

	if !bf.IsValidBinlog(names) {
		return nil, ErrFileContentCorruption
	}

	lastFileName := names[len(names)-1]
	lastFileSuffix, err := bf.ParseBinlogName(lastFileName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	p := path.Join(dirpath, lastFileName)
	log.Infof("open and lock binlog file %s", p)
	f, err := file.TryLockFile(p, os.O_WRONLY, file.PrivateFileMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	offset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, errors.Trace(err)
	}

	latestFilePos.Suffix = lastFileSuffix
	latestFilePos.Offset = offset

	binlog := &binlogger{
		dir:     dirpath,
		file:    f,
		encoder: newEncoder(f, codec),
		codec:   codec,
	}

	return binlog, nil
}

//CloseBinlogger closes the binlogger
func CloseBinlogger(binlogger Binlogger) error {
	return binlogger.Close()
}

// ReadFrom reads `nums` binlogs from the given binlog position
// read all binlogs from one file then close it and open the following file
func (b *binlogger) ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error) {
	var ent = &binlog.Entity{}
	var ents = []binlog.Entity{}
	var index int32
	var decoder Decoder
	var first = true

	dirpath := b.dir

	if nums < 0 {
		return nil, errors.Errorf("read number must be positive")
	}

	names, err := bf.ReadBinlogNames(b.dir)
	if err != nil {
		return nil, errors.Trace(err)
	}

	nameIndex, ok := bf.SearchIndex(names, from.Suffix)
	if !ok {
		return nil, errors.Annotatef(bf.ErrFileNotFound, "can not find index %d", from.Suffix)
	}

	for _, name := range names[nameIndex:] {
		p := path.Join(dirpath, name)
		f, err := os.OpenFile(p, os.O_RDONLY, file.PrivateFileMode)
		if err != nil {
			return ents, errors.Trace(err)
		}
		defer f.Close()

		if first {
			first = false

			size, err := f.Seek(from.Offset, io.SeekStart)
			if err != nil {
				return ents, errors.Trace(err)
			}

			if size < from.Offset {
				return ents, errors.Errorf("pos'offset is wrong")
			}
		}

		decoder = NewDecoder(from, io.Reader(f))

		for ; index < nums; index++ {
			err = decoder.Decode(ent, &binlogBuffer{})
			if err != nil {
				break
			}

			newEnt := binlog.Entity{
				Pos:     ent.Pos,
				Payload: ent.Payload,
			}
			ents = append(ents, newEnt)
		}

		if (err != nil && err != io.EOF) || index == nums {
			return ents, err
		}

		from.Suffix++
		from.Offset = 0
	}

	return ents, nil
}

// Walk reads binlog from the "from" position and sends binlogs in the streaming way
func (b *binlogger) Walk(ctx context.Context, from binlog.Pos, sendBinlog func(entity binlog.Entity) error) error {
	log.Infof("[binlogger] walk from position %+v", from)
	var ent = &binlog.Entity{}
	var decoder Decoder
	var first = true

	dirpath := b.dir
	names, err := bf.ReadBinlogNames(dirpath)
	if err != nil {
		return errors.Trace(err)
	}

	nameIndex, ok := bf.SearchIndex(names, from.Suffix)
	if !ok {
		return bf.ErrFileNotFound
	}

	for _, name := range names[nameIndex:] {
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

			size, err := f.Seek(from.Offset, io.SeekStart)
			if err != nil {
				return errors.Trace(err)
			}

			if size < from.Offset {
				return errors.Errorf("pos'offset is wrong")
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
				break
			}
			readBinlogHistogram.WithLabelValues("local").Observe(time.Since(beginTime).Seconds())

			newEnt := binlog.Entity{
				Pos:     ent.Pos,
				Payload: ent.Payload,
			}
			err := sendBinlog(newEnt)
			if err != nil {
				return errors.Trace(err)
			}

			binlogBufferPool.Put(buf)
		}

		if err != nil && err != io.EOF {
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

		if curSuffix < pos.Suffix || time.Now().Sub(fi.ModTime()) > days {
			err := os.Remove(fileName)
			if err != nil {
				log.Error("remove old binlog file err")
				continue
			}
			log.Info("GC binlog file:", fileName)
		}
	}
}

// Writes appends the binlog
// if size of current file is bigger than SegmentSizeBytes, then rotate a new file
func (b *binlogger) WriteTail(payload []byte) (int64, error) {
	beginTime := time.Now()
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
		return 0, errors.Trace(err)
	}

	latestFilePos.Offset = curOffset

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
			log.Errorf("failed to unlock during closing file: %s", err)
		}
	}

	return nil
}

func (b *binlogger) Name() string {
	return b.file.Name()
}

// rotate creates a new file for append binlog
func (b *binlogger) rotate() error {
	filename := bf.BinlogName(b.seq() + 1)
	latestFilePos.Suffix = b.seq() + 1
	latestFilePos.Offset = 0
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
