package file

import (
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testFileSuite{})

type testFileSuite struct{}

func (s *testFileSuite) TestCreate(c *C) {
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
	if !reflect.DeepEqual(fs, wfs) {
		c.Fatalf("ReadDir: got %v, want %v", fs, wfs)
	}
}

func (s *testFileSuite) TestCreateDirAll(c *C) {
	tmpdir, err := ioutil.TempDir(os.TempDir(), "foo")
	c.Assert(err, IsNil)
	defer os.RemoveAll(tmpdir)

	tmpdir2 := path.Join(tmpdir, "testdir")
	err = CreateDirAll(tmpdir2)
	c.Assert(err, IsNil)

	err = ioutil.WriteFile(path.Join(tmpdir2, "text.txt"), []byte("test text"), PrivateFileMode)
	c.Assert(err, IsNil)

	if err = CreateDirAll(tmpdir2); err == nil || !strings.Contains(err.Error(), "to be empty, got") {
		c.Fatalf("unexpected error %v", err)
	}
}

func (s *testFileSuite) TestExist(c *C) {
	f, err := ioutil.TempFile(os.TempDir(), "fileutil")
	c.Assert(err, IsNil)
	f.Close()

	if g := Exist(f.Name()); !g {
		c.Errorf("exist = %v, want true", g)
	}

	os.Remove(f.Name())
	if g := Exist(f.Name()); g {
		c.Errorf("exist = %v, want false", g)
	}
}

func (s *testFileSuite) TestLockAndUnlock(c *C) {
	f, err := ioutil.TempFile("", "lock")
	c.Assert(err, IsNil)
	f.Close()
	defer func() {
		err = os.Remove(f.Name())
		c.Assert(err, IsNil)
	}()

	// lock the file
	l, err := LockFile(f.Name(), os.O_WRONLY, PrivateFileMode)
	c.Assert(err, IsNil)

	// try lock a locked file
	if _, err = TryLockFile(f.Name(), os.O_WRONLY, PrivateFileMode); err != ErrLocked {
		c.Fatal(err)
	}

	// unlock the file
	err = l.Close()
	c.Assert(err, IsNil)

	// try lock the unlocked file
	dupl, err := TryLockFile(f.Name(), os.O_WRONLY, PrivateFileMode)
	c.Assert(err, IsNil)

	// blocking on locked file
	locked := make(chan struct{}, 1)
	go func() {
		bl, blerr := LockFile(f.Name(), os.O_WRONLY, PrivateFileMode)
		c.Assert(blerr, IsNil)

		locked <- struct{}{}
		blerr = bl.Close()
		c.Assert(blerr, IsNil)
	}()

	select {
	case <-locked:
		c.Error("unexpected unblocking")
	case <-time.After(100 * time.Millisecond):
	}

	// unlock
	err = dupl.Close()
	c.Assert(err, IsNil)

	// the previously blocked routine should be unblocked
	select {
	case <-locked:
	case <-time.After(1 * time.Second):
		c.Error("unexpected blocking")
	}
}
