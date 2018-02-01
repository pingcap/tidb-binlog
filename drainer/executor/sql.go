package executor

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
)

var (
	// ErrPayloadTooLarge defines an error about too-large payload.
	ErrPayloadTooLarge = errors.New("payload is too large to write")
)

var (
	// SegmentSizeBytes is the max threshold of sql segment file size
	// as an exported variable, you can define a different size
	SegmentSizeBytes int64 = 512 * 1024 * 1024 // 512 MB

	defaultBufSize = 16 * 1024 // 16 KB
)

type sqlExecutor struct {
	dir     string
	file    *file.LockedFile
	bw      *bufio.Writer
	size    int64
	maxSize int64
	mu      sync.Mutex
}

func newSQL(cfg *DBConfig) (Executor, error) {
	var (
		err error
		f   *file.LockedFile
	)

	dirPath := cfg.BinlogFileDir
	err = os.MkdirAll(dirPath, file.PrivateDirMode)
	if err != nil {
		return nil, errors.Trace(err)
	}
	names, err := file.ReadDir(dirPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sqlFiles := getSQLFiles(names)
	if len(sqlFiles) > 0 {
		// sqlFiles is sorted asc.
		lastFileName := sqlFiles[len(sqlFiles)-1]
		fullpath := path.Join(dirPath, lastFileName)
		f, err = file.TryLockFile(fullpath, os.O_WRONLY, file.PrivateFileMode)
	} else {
		// just create a new one.
		p := path.Join(dirPath, sqlFileName(0))
		f, err = file.LockFile(p, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &sqlExecutor{
		dir:     cfg.BinlogFileDir,
		file:    f,
		bw:      bufio.NewWriterSize(f, defaultBufSize),
		maxSize: SegmentSizeBytes,
	}, nil
}

// Execute implements the Executor interface.
func (s *sqlExecutor) Execute(sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	// data format
	// ts|sql;
	sqls, err := formatSQL(sqls, args)
	if err != nil {
		return errors.Trace(err)
	}

	for i := range sqls {
		// FIXME: does len(sqls) always equal to len(commitTSs ?)
		payload := fmt.Sprintf("%d|%s;", commitTSs[i], sqls[i])
		err = s.write([]byte(payload))
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (s *sqlExecutor) write(payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	writeLen := int64(len(payload))
	if writeLen > s.maxSize {
		return ErrPayloadTooLarge
	}

	if s.size+writeLen > s.maxSize {
		if err := s.doRotate(); err != nil {
			return errors.Trace(err)
		}
	}

	// write by line
	written, err := fmt.Fprintln(s.bw, payload)
	if err != nil {
		return errors.Trace(err)
	}
	s.size += int64(written)

	return nil
}

func (s *sqlExecutor) doRotate() error {
	newFileName := sqlFileName(s.seq() + 1)
	fpath := path.Join(s.dir, newFileName)
	newFile, err := file.LockFile(fpath, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return errors.Trace(err)
	}
	if err = s.bw.Flush(); err != nil {
		return errors.Trace(err)
	}
	if err = s.file.Close(); err != nil {
		log.Errorf("failed to unlock during closing file: %s", err)
	}
	s.file = newFile
	s.bw = bufio.NewWriterSize(s.file, defaultBufSize)

	log.Infof("rotate to new sql file %s", fpath)
	return nil
}

func (s *sqlExecutor) seq() uint64 {
	if s.file == nil {
		return 0
	}

	seq, err := parseSQLFileIndex(path.Base(s.file.Name()))
	if err != nil {
		log.Fatalf("bad binlog name %s (%v)", s.file.Name(), err)
	}

	return seq
}

// Close implements the Executor interface.
func (s *sqlExecutor) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.bw.Flush(); err != nil {
		log.Errorf("flush sql error %v", err)
	}
	return errors.Trace(s.file.Close())
}

func getSQLFiles(names []string) []string {
	files := make([]string, 0, len(names))
	for _, name := range names {
		// TODO: use regexp to parser file pattern.
		if !strings.HasPrefix(name, "sql-") {
			continue
		}
		files = append(files, name)
	}
	return files
}

// the file name format is like sql-0000000000000001
func sqlFileName(index uint64) string {
	return fmt.Sprintf("sql-%016d", index)
}

func parseSQLFileIndex(str string) (index uint64, err error) {
	if !strings.HasPrefix(str, "sql-") {
		return 0, errors.New("bad sql file name")
	}

	_, err = fmt.Sscanf(str, "sql-%016d", &index)
	return index, errors.Trace(err)
}

// select * from foo.bar where a=? and b=?   (1,2).  select * from foo.bar where a=1 and b=2
func formatSQL(sqls []string, args [][]interface{}) ([]string, error) {
	if len(sqls) != len(args) {
		return nil, errors.Errorf("mismatch length of sqls and args  %d vs %d", len(sqls), len(args))
	}

	newSQLs := make([]string, 0, len(sqls))
	for i := range sqls {
		// TODO: enhance this format. it must have BUG now.
		// maybe we should learn from database/sql and go-sql-driver/mysql.
		newSQLs = append(newSQLs, fmt.Sprintf(sqls[i], args[i]))
	}

	return newSQLs, nil
}
