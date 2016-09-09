package binlog

import (
	"io"
	"os"
	"path"
	"sync"
	"errors"
	"hash/crc32"
	
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tidb-binlog/binlog/scheme"
)

var (
	SegmentSizeBytes int64 	= 64 * 1000 * 1000
	ErrFileNotFound        	= errors.New("file: file not found")
	crcTable            	= crc32.MakeTable(crc32.Castagnoli)
)

type Binlog struct {
	dir		string
	offset		scheme.BinlogOffset

	decoder		*decoder
	readCloser	func() error
	
	mu		sync.Mutex
	encoder		*encoder
	
	locks           []*file.LockedFile
	fp		*filePipeline
}

func Create(dirpath string) (*Binlog, error) {
	if Exist(dirpath) {
		return nil, os.ErrExist
	}

	// the temporary dir make the create dir atomic
	tmpdirpath := path.Clean(dirpath) + ".tmp"
	if file.Exist(tmpdirpath) {
		if err := os.RemoveAll(tmpdirpath); err != nil {
			return nil, err
		}
	}

	if err := file.CreateDirAll(tmpdirpath); err != nil {
		return nil, err
	}

	p := path.Join(tmpdirpath, fileName(0))
	f, err := file.LockFile(p, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return nil, err
	}

	if _, err := f.Seek(0, os.SEEK_END); err != nil {
		return nil, err
	}

	binlog := &Binlog{
		dir:      dirpath,
		encoder:  newEncoder(f),
	}
	binlog.locks = append(binlog.locks, f)
	return binlog.renameFile(tmpdirpath)
}

func Open(dirpath string, offset scheme.BinlogOffset) (*Binlog, error) {
	names, err := readBinlogNames(dirpath)
	if err != nil {
		return nil, err
	}

	nameIndex, ok := searchIndex(names, offset.Suffix)
	if !ok {
		return nil, ErrFileNotFound
	}

	first := true

	rcs := make([]io.ReadCloser, 0)
	rs  := make([]io.Reader, 0)
	for _, name := range names[nameIndex:] {
		p := path.Join(dirpath, name)
		rf, err := os.OpenFile(p, os.O_RDONLY, file.PrivateFileMode)
		if err != nil {
			closeAll(rcs...)
			return nil, err
		}

		if first {
			first = false

			index, _ := parseBinlogName(name)
			if index == offset.Suffix {
				ofs, err  := rf.Seek(offset.Offset, os.SEEK_SET)
				if err != nil {
					closeAll(rcs...)
					return nil, err
				}

				if ofs < offset.Offset {
                                	continue
                        	}
			}
		}

		rcs	= append(rcs, rf)
		rs 	= append(rs, rf)
	}

	closer := func() error {return closeAll(rcs...)}
	binlog := &Binlog{
		dir : 		dirpath,
		offset:  	offset,
		decoder:   	newDecoder(offset, rs...),
		readCloser: 	closer,
	}

	return binlog, nil
}

func OpenForWrite(dirpath string) (*Binlog, error) {
	names, err := readBinlogNames(dirpath)
        if err != nil {
                return  nil, err
        }

	lastFileName := names[len(names)-1]
	p := path.Join(dirpath, lastFileName)
        f, err := file.TryLockFile(p, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
        if err != nil {
                return nil, err
        }

        if _, err := f.Seek(0, os.SEEK_END); err != nil {
                return nil, err
        }


        binlog := &Binlog{
                dir:      dirpath,
                encoder:  newEncoder(f),
        }
        binlog.locks = append(binlog.locks, f)
	binlog.fp = newFilePipeline(binlog.dir, SegmentSizeBytes)

        return binlog, nil
}

func (b *Binlog) Read(nums int) (ents []scheme.Entry, err error)  {
	b.mu.Lock()
	defer b.mu.Unlock()

	var ent  = &scheme.Entry{}
	decoder := b.decoder

	for index := 0; index < nums; index++ {	
		err = decoder.decode(ent)
		if err != nil {
			break
		}

		newEnt := scheme.Entry {
			CommitTs:	ent.CommitTs,
			StartTs:	ent.StartTs,
			Size:		ent.Size,
			Payload:	ent.Payload,
			Offset:		ent.Offset,
		}
		ents = append(ents, newEnt)
	}

	return 
}

func (b *Binlog) Write(ents []scheme.Entry) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(ents) == 0 {
		return nil
	}

	for i := range ents {
		if err := b.encoder.encode(&ents[i]); err != nil {
			return err
		}
	}

	curOff, err := b.tail().Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}

	if curOff < SegmentSizeBytes {
		return b.sync()
		return nil
	}

	return b.rotate()
}

func (b *Binlog) rotate() error {
	off, err := b.tail().Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}

	if err := b.tail().Truncate(off); err != nil {
		return err
	}

	if err := b.sync(); err != nil {
		return err
	}

	fpath := path.Join(b.dir, fileName(b.seq()+1))

	newTail, err := b.fp.Open()
	if err != nil {
		return err
	}

	b.locks = append(b.locks, newTail)

	if err = os.Rename(newTail.Name(), fpath); err != nil {
		return err
	}
	newTail.Close()

	if newTail, err = file.LockFile(fpath, os.O_WRONLY, file.PrivateFileMode); err != nil {
		return err
	}

	b.locks[len(b.locks)-1] = newTail
	b.encoder = newEncoder(b.tail())

	log.Infof("segmented binlog file %v is created", fpath)
	return nil
}

func (b *Binlog) sync() error {
	err := file.Fsync(b.tail().File)

	return err
}


func (b *Binlog) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.fp != nil {
		b.fp.Close()
		b.fp = nil
	}

	if b.tail() != nil {
		if err := b.sync(); err != nil {
			return err
		}
	}

	for _, l := range b.locks {
		if l == nil {
			continue
		}
		if err := l.Close(); err != nil {
			log.Errorf("failed to unlock during closing binlog: %s", err)
		}
	}
	return nil
}

func (b *Binlog) renameFile(tmpdirpath string) (*Binlog, error) {

	if err := os.RemoveAll(b.dir); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpdirpath, b.dir); err != nil {
		return nil, err
	}

	b.fp = newFilePipeline(b.dir, SegmentSizeBytes)
	return b, nil
}

func (b *Binlog) tail() *file.LockedFile {
	if len(b.locks) > 0 {
		return b.locks[len(b.locks)-1]
	}
	return nil
}

func (b *Binlog) seq() uint64 {
	t := b.tail()
	if t == nil {
		return 0
	}
	seq, err := parseBinlogName(path.Base(t.Name()))
	if err != nil {
		log.Fatalf("bad binlog name %s (%v)", t.Name(), err)
	}
	return seq
}

func closeAll(rcs ...io.ReadCloser) error {
	for _, f := range rcs {
		if err := f.Close(); err != nil {
			return err
		}
	}

	return nil
}
