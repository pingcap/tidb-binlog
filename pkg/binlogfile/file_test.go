package binlogfile

import (
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	gb "github.com/pingcap/tipb/go-binlog"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testFileSuite{})

type testFileSuite struct{}

func (t *testFileSuite) TestCreate(c *C) {
	tmpdir, err := ioutil.TempDir("", "")
	defer os.RemoveAll(tmpdir)
	c.Assert(err, IsNil)

	files := []string{"tidb", "tikv", "pd", "k8s"}
	for _, f := range files {
		var fh *os.File
		fh, err = os.Create(path.Join(tmpdir, f))
		c.Assert(err, IsNil)

		err = fh.Close()
		c.Assert(err, IsNil)
	}
	fs, err := ReadDir(tmpdir)
	c.Assert(err, IsNil)

	wfs := []string{"k8s", "pd", "tidb", "tikv"}
	c.Assert(reflect.DeepEqual(fs, wfs), IsTrue)
}

func (t *testFileSuite) TestReadNorExistDir(c *C) {
	_, err := ReadDir("testNoExistDir")
	c.Assert(err, NotNil)
}

func (t *testFileSuite) TestCreateDirAll(c *C) {
	tmpdir, err := ioutil.TempDir(os.TempDir(), "foo")
	c.Assert(err, IsNil)
	defer os.RemoveAll(tmpdir)

	tmpdir2 := path.Join(tmpdir, "testdir")
	err = CreateDirAll(tmpdir2)
	c.Assert(err, IsNil)

	err = ioutil.WriteFile(path.Join(tmpdir2, "text.txt"), []byte("test text"), file.PrivateFileMode)
	c.Assert(err, IsNil)

	err = CreateDirAll(tmpdir2)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "to be empty, got"), IsTrue)
}

func (t *testFileSuite) TestExist(c *C) {
	f, err := ioutil.TempFile(os.TempDir(), "fileutil")
	c.Assert(err, IsNil)
	f.Close()

	c.Assert(Exist(f.Name()), IsTrue)

	os.Remove(f.Name())
	c.Assert(Exist(f.Name()), IsFalse)
}

func (t *testFileSuite) TestFilterBinlogNames(c *C) {
	names := []string{"binlog-0000000000000001", "test", "binlog-0000000000000002-20180315121212", "savepoint", "binlog-0000000000000003-20180315121212.tar.gz"}
	exceptedNames := []string{"binlog-0000000000000001", "binlog-0000000000000002-20180315121212", "binlog-0000000000000003-20180315121212.tar.gz"}

	fnames := FilterBinlogNames(names)
	c.Assert(fnames, HasLen, len(exceptedNames))
	c.Assert(fnames, DeepEquals, exceptedNames)
}

func (t *testFileSuite) TestParseBinlogName(c *C) {
	cases := []struct {
		name          string
		expectedIndex uint64
		expectedts    int64
		expectedError bool
	}{
		{"binlog-0000000000000001", 0000000000000001, 0, false},
		{"binlog-index-20180315121212", 0, 0, true},
		{"binlog-0000000000000003-20180315121212", 0000000000000003, 0, false},
		{"binlog-index-20180315121212", 0, 0, true},
		{"binlog-0000000000000005", 0000000000000005, 0, false},
		{"binlog-index", 0, 0, true},
		{"binlog-0000000000000003-20180315121212-000000000000000001.tar.gz", 0000000000000003, 1, false},
		{"binlog-index-20180315121212-000000000000000001.tar.gz", 0, 0, true},
	}

	for _, t := range cases {
		index, _, err := ParseBinlogName(t.name)
		c.Assert(err != nil, Equals, t.expectedError)
		c.Assert(index, Equals, t.expectedIndex)
	}

	index := uint64(1)
	name := BinlogName(index)
	gotIndex, gotTs, err := ParseBinlogName(name)
	c.Assert(err, IsNil)
	c.Assert(gotIndex, Equals, index)
	c.Assert(gotTs, Equals, int64(0))
}

func (t *testFileSuite) TestGetFirstBinlogCommitTS(c *C) {
	// test read gzip binlog file, will get ts from file name
	gzipBinlogFileName := "binlog-0000000000000001-20180315121212-000000000000000001.tar.gz"
	ts, err := GetFirstBinlogCommitTS(gzipBinlogFileName)
	c.Assert(err, IsNil)
	c.Assert(ts, Equals, int64(1))

	// test read normal binlog file, will read file to get commit ts
	dir := c.MkDir()
	bl, err := OpenBinlogger(dir, compress.CompressionNone)
	c.Assert(err, IsNil)

	b, ok := bl.(*binlogger)
	c.Assert(ok, IsTrue)

	binlog := &pb.Binlog {CommitTs: 2,}
	data, err := binlog.Marshal()
	c.Assert(err, IsNil)

	_, err = bl.WriteTail(&gb.Entity{Payload: data})
	c.Assert(err, IsNil)
	bl.Close()

	ts, err = GetFirstBinlogCommitTS(b.file.Name())
	c.Assert(err, IsNil)
	c.Assert(ts, Equals, int64(2))
}
