package file

import (
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"

	. "github.com/pingcap/check"
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

	err = ioutil.WriteFile(path.Join(tmpdir2, "text.txt"), []byte("test text"), PrivateFileMode)
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
