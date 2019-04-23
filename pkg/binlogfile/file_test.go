// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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
	names := []string{"binlog-0000000000000001", "test", "binlog-0000000000000002-20180315121212", "savepoint"}
	excepted := []string{"binlog-0000000000000001", "binlog-0000000000000002-20180315121212"}

	res := FilterBinlogNames(names)
	c.Assert(res, HasLen, len(excepted))
	c.Assert(res, DeepEquals, excepted)
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
