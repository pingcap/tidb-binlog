package pump

import (
	"hash/crc32"
	"io"
	"os"
	"path"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
	pb "github.com/pingcap/tidb-binlog/proto"
)

var (
	// segmentSizeBytes is size of each binlog segment file,
	// as an exported variable, you can define a different size
	SegmentSizeBytes int64 = 64 * 1024 * 1024

	ErrFileNotFound          = errors.New("pump/binlogger: file not found")
	ErrFileContentCorruption = errors.New("pump/binlogger: content is corruption")
	ErrCRCMismatch           = errors.New("pump/binlogger: crc mismatch")
	crcTable                 = crc32.MakeTable(crc32.Castagnoli)
)

type Binlogger interface {
	// read nums binlog events from the "from" position
	ReadFrom(from pb.Pos, nums int) ([]pb.Binlog, error)

	// batch write binlog events
	WriteTail(entries []pb.Binlog) error

	// close the binlogger
	Close() error
}

// filelog is a logical representation of the log storage.
// it is either in read mode or append mode.
type binlogger struct {
	dir string

	// decoder read from fd, then decode bytes to pb.Binlog
	decoder *decoder

	// encoder encode pb.Binlog to bytes, the write to fd
	encoder *encoder

	// file is the lastest file in the dir
	file  *file.LockedFile
	mutex sync.Mutex
}

// Create creates a binlog directory, then can append binlogs.
// crc is stored in the head of each binlog file for be retrieved
func Create(dirpath string) (Binlogger, error) {
	if Exist(dirpath) {
		return nil, os.ErrExist
	}

	if err := file.CreateDirAll(dirpath); err != nil {
		return nil, errors.Trace(err)
	}

	p := path.Join(dirpath, fileName(0))
	f, err := file.LockFile(p, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	binlog := &binlogger{
		dir:     dirpath,
		encoder: newEncoder(f),
		file:    f,
	}

	return binlog, nil
}

//open for write, after read the binlog last CRC , then it can be appendd
func Open(dirpath string) (Binlogger, error) {
	names, err := readBinlogNames(dirpath)
	if err != nil {
		return nil, err
	}

	lastFileName := names[len(names)-1]
	p := path.Join(dirpath, lastFileName)
	f, err := file.TryLockFile(p, os.O_RDWR, file.PrivateFileMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if _, err := f.Seek(0, os.SEEK_END); err != nil {
		return nil, errors.Trace(err)
	}

	binlog := &binlogger{
		dir:     dirpath,
		file:    f,
		encoder: newEncoder(f),
	}

	return binlog, nil
}

// Read read nums logs from the given log position.
// it read binlogs from files and append to result set util the count = num
// after read all binlog from one file  then close it and open the following file
func (b *binlogger) ReadFrom(from pb.Pos, nums int) ([]pb.Binlog, error) {
	var ent = &pb.Binlog{}
	var ents = []pb.Binlog{}
	var index int
	var first = true

	dirpath := b.dir

	if nums < 0 {
		return nil, errors.Errorf("read number must be positive")
	}

	names, err := readBinlogNames(b.dir)
	if err != nil {
		return nil, errors.Trace(err)
	}

	nameIndex, ok := searchIndex(names, from.Suffix)
	if !ok {
		return nil, ErrFileNotFound
	}

	for _, name := range names[nameIndex:] {
		p := path.Join(dirpath, name)
		f, err := os.OpenFile(p, os.O_RDONLY, file.PrivateFileMode)
		if err != nil {
			return ents, errors.Trace(err)
		}

		fileIndex, err := parseBinlogName(name)
		if err != nil {
			f.Close()
			return ents, errors.Trace(err)
		}

		if first {
			first = false
			if fileIndex != from.Suffix {
				return ents, errors.Errorf("pos's suffix is wrong")
			}

			size, err := f.Seek(from.Offset, os.SEEK_SET)
			if err != nil {
				return ents, errors.Trace(err)
			}

			if size < from.Offset {
				return ents, errors.Errorf("pos'offset is wrong")
			}
		}

		b.decoder = newDecoder(from, io.Reader(f))

		for ; index < nums; index++ {
			err = b.decoder.decode(ent)
			if err != nil {
				break
			}

			newEnt := pb.Binlog{
				Payload: ent.Payload,
				Pos:     ent.Pos,
			}
			ents = append(ents, newEnt)
		}

		f.Close()

		if (err != nil && err != io.EOF) || index == nums {
			return ents, err
		}

		from.Suffix += 1
		from.Offset = 0
	}

	return ents, nil
}

// Writes appends the binlog
// if size of current file is bigger than SegmentSizeBytes, then rotate a new file
func (b *binlogger) WriteTail(ents []pb.Binlog) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if len(ents) == 0 {
		return nil
	}

	for i := range ents {
		if err := b.encoder.encode(&ents[i]); err != nil {
			return errors.Trace(err)
		}
	}

	curOffset, err := b.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return errors.Trace(err)
	}

	if curOffset < SegmentSizeBytes {
		return nil
	}

	return b.rotate()
}

// Rotate create a new file for append binlog
// it should store the previsou CRC sum in the head of the file
func (b *binlogger) rotate() error {
	_, err := b.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return errors.Trace(err)
	}

	if err := b.sync(); err != nil {
		return errors.Trace(err)
	}

	fpath := path.Join(b.dir, fileName(b.seq()+1))

	newTail, err := file.LockFile(fpath, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return errors.Trace(err)
	}

	b.file.Close()
	b.file = newTail

	b.encoder = newEncoder(b.file)

	log.Infof("segmented binlog file %v is created", fpath)
	return nil
}

func (b *binlogger) sync() error {
	return file.Fsync(b.file.File)
}

func (b *binlogger) Close() error {
	if b.file != nil {
		if err := b.sync(); err != nil {
			return errors.Trace(err)
		}

		if err := b.file.Close(); err != nil {
			log.Errorf("failed to unlock during closing wal: %s", err)
		}
	}

	return nil
}

func (b *binlogger) seq() uint64 {
	if b.file == nil {
		return 0
	}

	seq, err := parseBinlogName(path.Base(b.file.Name()))
	if err != nil {
		log.Fatalf("bad binlog name %s (%v)", b.file.Name(), err)
	}

	return seq
}
