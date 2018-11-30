package binlogfile

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tidb/store/tikv/oracle"
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
	names := []string{"binlog-v2.1.0-0000000000000001-20180315121212", "test", "binlog-v2.1.0-0000000000000002-20180315121212"}
	excepted := []string{"binlog-v2.1.0-0000000000000001-20180315121212", "binlog-v2.1.0-0000000000000002-20180315121212"}
	res := FilterBinlogNames(names)
	c.Assert(res, HasLen, len(excepted))
	c.Assert(res, DeepEquals, excepted)
}

func (t *testFileSuite) TestParseBinlogName(c *C) {
	cases := []struct {
		name          string
		expectedIndex uint64
		expectedError bool
	}{
		{"binlog-v2.1.0-0000000000000001-20180315121212", 0000000000000001, false},
		{"binlog-0000000000000001", 0000000000000001, false},
		{"binlog-index", 0, true},
	}

	for _, t := range cases {
		index, err := ParseBinlogName(t.name)
		c.Assert(err != nil, Equals, t.expectedError)
		c.Assert(index, Equals, t.expectedIndex)
	}

	index := uint64(1)
	name := BinlogName(index, 1)
	gotIndex, err := ParseBinlogName(name)
	c.Assert(err, IsNil)
	c.Assert(gotIndex, Equals, index)
}

func (t *testFileSuite) TestBinlogNameWithDatetime(c *C) {
	datetimeStr := "20180315121212"
	index := uint64(1)
	datetime, err := time.Parse(datetimeFormat, datetimeStr)
	c.Assert(err, IsNil)

	ts := int64(oracle.ComposeTS(datetime.Unix()*1000, 0))
	binlogName := binlogNameWithDateTime(index, ts)
	c.Assert(binlogName, Equals, fmt.Sprintf("binlog-%s-0000000000000001-%s", version, datetimeStr))
}
