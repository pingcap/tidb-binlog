package binlog

import (
	"fmt"
	"os"
	"path"
	
	"github.com/pingcap/tidb-binlog/pkg/file"
)

//pipe for generate new file
type filePipeline struct {
	dir 	string
	size	int64
	count	int // just for temporary filename 

	filec 	chan *file.LockedFile
	errc	chan error
	donec	chan struct{}
}

func newFilePipeline(dir string, fileSize int64) *filePipeline {
	fp := &filePipeline {
		dir:	dir,
		size:	fileSize,
		filec:	make(chan *file.LockedFile),
		errc:	make(chan error, 1),
		donec:	make(chan struct{}),
	}

	go fp.run()
	return fp
}

func (fp *filePipeline) Open() (f *file.LockedFile, err error) {
	select {
	case f = <- fp.filec:
	case err = <- fp.errc:
	}

	return f, err
}

func (fp *filePipeline) Close() error {
	close(fp.donec)
	return <-fp.errc
}

func (fp *filePipeline) alloc() (f *file.LockedFile, err error) {
	fpath := path.Join(fp.dir, fmt.Sprintf("%d.tmp", fp.count%2))
	if f, err = file.LockFile(fpath, os.O_CREATE|os.O_WRONLY, file.PrivateFileMode); err != nil {
		return nil, err
	}

	fp.count++
	return f, nil
}

func (fp *filePipeline) run() {
	defer close(fp.errc)
	for {
		f, err := fp.alloc()
		if err != nil {
			fp.errc <- err
			return
		}

		select {
		case fp.filec <- f:
		case <- fp.donec:
			os.Remove(f.Name())
			f.Close()
			return
		}
	}
}
