package binlog

import (
	"hash/crc32"
	"io"
	"os"
	"path"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/binlog/scheme"
	"github.com/pingcap/tidb-binlog/pkg/file"
)

const (
	entryType uint32 = iota
	crcType
)

var (
	// segmentSizeBytes is size of each binlog segment file,
	// as an exported variable, you can define a different size
	SegmentSizeBytes int64 = 64 * 1024 * 1024

	ErrFileNotFound    = errors.New("file: file not found")
	ErrCRCMismatch     = errors.New("binlog: crc mismatch")
	ErrFileCourruption = errors.New("binlog: content is courruption")
	crcTable           = crc32.MakeTable(crc32.Castagnoli)
)

// Binlog is a logical representation of the binlog storage.
// it is either in read mode or append mode.
type Binlog struct {
	dir     string
	decoder *decoder
	encoder *encoder
	file    *file.LockedFile
}

// Create creates a binlog directory, then can append binlogs.
// crc is stored in the head of each binlog file for be retrieved
func Create(dirpath string) (*Binlog, error) {
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

	binlog := &Binlog{
		dir:     dirpath,
		encoder: newEncoder(f, 0),
		file:    f,
	}

	if err := binlog.saveCrc(0); err != nil {
		return nil, err
	}

	return binlog, nil
}

func OpenForRead(dirpath string) (*Binlog, error) {
	_, err := readBinlogNames(dirpath)
	if err == ErrFileNotFound {
		return nil, err
	}

	binlog := &Binlog{
		dir: dirpath,
	}

	return binlog, nil
}

//open for write, after read the binlog last CRC , then it can be appendd
func Open(dirpath string) (*Binlog, error) {
	names, err := readBinlogNames(dirpath)
	if err == ErrFileNotFound {
		return Create(dirpath)
	}

	if err != nil {
		return nil, errors.Trace(err)
	}

	lastFileName := names[len(names)-1]
	p := path.Join(dirpath, lastFileName)
	f, err := file.TryLockFile(p, os.O_RDWR, file.PrivateFileMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	binlog := &Binlog{
		dir:  dirpath,
		file: f,
	}

	crc, err := binlog.lastCRC(SegmentSizeBytes, io.Reader(f))
	if err != io.EOF {
		return nil, errors.Trace(err)
	}
	binlog.encoder = newEncoder(f, crc)

	if _, err := f.Seek(0, os.SEEK_END); err != nil {
		return nil, errors.Trace(err)
	}

	return binlog, nil
}

// Read read nums binlogs from the given binlogPosition.
// it read binlogs from files and append to result set util the count = nums
// after read all binlog from one file  then close it and open the following file
func (b *Binlog) Read(nums int, from scheme.BinlogPosition) ([]scheme.Entry, error) {
	var ent = &scheme.Entry{}
	var ents = []scheme.Entry{}
	var index int

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

		if fileIndex == from.Suffix {
			crc, err := b.lastCRC(from.Offset, io.Reader(f))
			if err != nil && err != io.EOF {
				f.Close()
				return ents, errors.Errorf("unexpected position %v, get error %v", from, err)
			}

			f.Seek(from.Offset, os.SEEK_SET)
			b.decoder = newDecoder(from, io.Reader(f))
			b.decoder.updateCRC(crc)
		} else {
			b.decoder = newDecoder(from, io.Reader(f))
		}

		for ; index < nums; index++ {
			err = b.decoder.decode(ent)
			if err != nil {
				break
			}

			switch ent.Type {
			case entryType:
				newEnt := scheme.Entry{
					Payload: ent.Payload,
					Offset:  ent.Offset,
					Crc:     ent.Crc,
				}
				ents = append(ents, newEnt)
			case crcType:
				// check the each CRC in the head of each binlog file
				// it must be equal with the all binlog payload crc checksum before the file
				crc := b.decoder.getCRC()
				if crc != 0 && crc != ent.Crc {
					return ents, ErrCRCMismatch
				}
				index--

				b.decoder.updateCRC(ent.Crc)
			default:
				return ents, errors.Errorf("unexpected entry type %d", ent.Type)
			}
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
func (b *Binlog) Write(ents []scheme.Entry) error {
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
		return b.sync()
		return nil
	}

	return b.rotate()
}

// Rotate create a new file for append binlog
// it should store the previsou CRC sum in the head of the file
func (b *Binlog) rotate() error {
	_, err := b.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return errors.Trace(err)
	}

	if err := b.sync(); err != nil {
		return errors.Trace(err)
	}

	fpath := path.Join(b.dir, fileName(b.seq()+1))

	b.file.Close()

	newTail, err := file.LockFile(fpath, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return errors.Trace(err)
	}

	b.file = newTail

	b.encoder = newEncoder(b.file, b.encoder.getCRC())
	if err = b.saveCrc(b.encoder.crc); err != nil {
		return errors.Trace(err)
	}

	log.Infof("segmented binlog file %v is created", fpath)
	return nil
}

func (b *Binlog) sync() error {
	return file.Fsync(b.file.File)
}

func (b *Binlog) Close() error {
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

func (b *Binlog) seq() uint64 {
	if b.file == nil {
		return 0
	}

	seq, err := parseBinlogName(path.Base(b.file.Name()))
	if err != nil {
		log.Fatalf("bad binlog name %s (%v)", b.file.Name(), err)
	}

	return seq
}

func (b *Binlog) saveCrc(crc uint32) error {
	return b.encoder.encode(&scheme.Entry{Type: crcType, Crc: crc})
}

// lastCRC reads from head of the file util to the end(append model) or the give position(read model)
// to calculate the previous binlogs CRC sum
func (b *Binlog) lastCRC(toPosition int64, reader io.Reader) (uint32, error) {
	b.decoder = newDecoder(scheme.BinlogPosition{Offset: 0}, reader)
	ent := &scheme.Entry{}
	var prevCrc uint32

	err := b.decoder.decode(ent)
	for ; err == nil && toPosition > ent.Offset.Offset; err = b.decoder.decode(ent) {
		prevCrc = b.decoder.getCRC()

		if ent.Type == crcType {
			if prevCrc != 0 && prevCrc != ent.Crc {
				return 0, ErrCRCMismatch
			}

			b.decoder.updateCRC(ent.Crc)
		}
	}

	b.decoder = nil
	return prevCrc, err
}
