package drainer

import (
	"bytes"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/pkg/sql"
)

func (t *testDrainerSuite) TestIgnoreDDLError(c *C) {
	// test non-mysqltype error
	err := errors.New("test")
	ok := sql.IgnoreDDLError(err)
	c.Assert(ok, IsFalse)
	// test ignore error
	err1 := &mysql.MySQLError{
		Number:  1054,
		Message: "test",
	}
	ok = sql.IgnoreDDLError(err1)
	c.Assert(ok, IsTrue)
	// test non-ignore error
	err2 := &mysql.MySQLError{
		Number:  1052,
		Message: "test",
	}
	ok = sql.IgnoreDDLError(err2)
	c.Assert(ok, IsFalse)
}

func fileCopy(src, dst string, modify func([]byte) []byte, c *C) {
	var (
		err   error
		dstfd *os.File

		srcContent []byte
	)

	srcContent, err = ioutil.ReadFile(src)
	c.Assert(err, IsNil)

	dstfd, err = os.Create(dst)
	c.Assert(err, IsNil)
	defer dstfd.Close()

	srcContent = modify(srcContent)
	_, err = dstfd.Write(srcContent)
	c.Assert(err, IsNil)
}

func (t *testDrainerSuite) TestGenCheckpointCfg(c *C) {
	dir := c.MkDir()
	tmpToml := path.Join(dir, "drainer.toml")
	fileCopy("../cmd/drainer/drainer.toml", tmpToml, func(v []byte) []byte {
		return bytes.Replace(v, []byte("[syncer]"), []byte(`[syncer]
checkpoint-save-interval = 6
`), 1)
	}, c)
	args := []string{
		"-config", tmpToml,
	}

	cfg := NewConfig()
	err := cfg.Parse(args)
	c.Assert(err, IsNil)
	err = cfg.adjustConfig()
	c.Assert(err, IsNil)
	c.Assert(cfg.SyncerCfg.CheckpointSaveInterval, Equals, 6)
	cpCfg := GenCheckPointCfg(cfg, 1)
	c.Assert(cpCfg.MaxSaveTime, Equals, 6*time.Second)
	cp, err := checkpoint.NewCheckPoint(cfg.SyncerCfg.DestDBType, cpCfg)
	c.Assert(err, IsNil)
	c.Assert(cp.Check(0), IsFalse)
	time.Sleep(4 * time.Second)
	c.Assert(cp.Check(0), IsFalse)
	time.Sleep(3 * time.Second)
	c.Assert(cp.Check(0), IsTrue)
}
