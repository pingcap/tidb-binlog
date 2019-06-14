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
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"path"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tipb/go-binlog"
)

var _ = Suite(&testBinloggerSuite{})

type testBinloggerSuite struct{}

func (s *testBinloggerSuite) TestCreate(c *C) {
	dir := c.MkDir()
	// check create binloger with non-exist directory
	checkTest(c, dir)

	// // check create binloger with empty directory
	c.Assert(os.RemoveAll(path.Join(dir, BinlogName(0))), IsNil)
	checkTest(c, dir)
}

func checkTest(c *C, dir string) {
	bl, err := OpenBinlogger(dir)
	c.Assert(err, IsNil)
	defer func() {
		err := CloseBinlogger(bl)
		c.Assert(err, IsNil)
	}()

	b, ok := bl.(*binlogger)
	c.Assert(ok, IsTrue)
	c.Assert(path.Base(b.file.Name()), Equals, BinlogName(0))
	bl.Close()
}

func (s *testBinloggerSuite) TestOpenForWrite(c *C) {
	dir := c.MkDir()
	bl, err := OpenBinlogger(dir)
	c.Assert(err, IsNil)

	b, ok := bl.(*binlogger)
	c.Assert(ok, IsTrue)
	err = b.rotate()
	c.Assert(err, IsNil)

	_, err = bl.WriteTail(&binlog.Entity{Payload: []byte("binlogtest")})
	c.Assert(err, IsNil)
	bl.Close()

	bl, err = OpenBinlogger(dir)
	c.Assert(err, IsNil)

	b, ok = bl.(*binlogger)
	curFile := b.file
	c.Assert(ok, IsTrue)
	c.Assert(path.Base(curFile.Name()), Equals, BinlogName(1))
	latestPos := &binlog.Pos{Suffix: 1}
	c.Assert(latestPos.Suffix, Equals, uint64(1))

	curOffset, err := curFile.Seek(0, io.SeekCurrent)
	c.Assert(err, IsNil)

	_, err = bl.WriteTail(&binlog.Entity{Payload: []byte("binlogtest")})
	c.Assert(err, IsNil)

	nowOffset, err := curFile.Seek(0, io.SeekCurrent)
	c.Assert(err, IsNil)
	c.Assert(nowOffset, Equals, curOffset+26)

	bl.Close()
}

func (s *testBinloggerSuite) TestRotateFile(c *C) {
	dir := c.MkDir()
	bl, err := OpenBinlogger(dir)
	c.Assert(err, IsNil)

	payload := []byte("binlogtest")

	_, err = bl.WriteTail(&binlog.Entity{Payload: payload})
	c.Assert(err, IsNil)

	b, ok := bl.(*binlogger)
	c.Assert(ok, IsTrue)

	err = b.rotate()
	c.Assert(err, IsNil)
	c.Assert(path.Base(b.file.Name()), Equals, BinlogName(1))

	_, err = bl.WriteTail(&binlog.Entity{Payload: payload})
	c.Assert(err, IsNil)

	bl.Close()

	bl, err = OpenBinlogger(dir)
	c.Assert(err, IsNil)

	binlogs, err := bl.ReadFrom(binlog.Pos{}, 1)
	c.Assert(err, IsNil)
	c.Assert(binlogs, HasLen, 1)
	c.Assert(binlogs[0].Pos, DeepEquals, binlog.Pos{Offset: 26})
	c.Assert(binlogs[0].Payload, BytesEquals, []byte("binlogtest"))

	binlogs, err = bl.ReadFrom(binlog.Pos{Suffix: 1, Offset: 0}, 1)
	c.Assert(err, IsNil)
	c.Assert(binlogs, HasLen, 1)
	c.Assert(binlogs[0].Pos, DeepEquals, binlog.Pos{Suffix: 1, Offset: 26})
	c.Assert(binlogs[0].Payload, BytesEquals, []byte("binlogtest"))
	bl.Close()
}

func (s *testBinloggerSuite) TestRead(c *C) {
	dir := c.MkDir()
	bl, err := OpenBinlogger(dir)
	c.Assert(err, IsNil)
	defer bl.Close()

	b, ok := bl.(*binlogger)
	c.Assert(ok, IsTrue)

	for i := 0; i < 10; i++ {
		for i := 0; i < 20; i++ {
			_, err = bl.WriteTail(&binlog.Entity{Payload: []byte("binlogtest")})
			c.Assert(err, IsNil)
		}

		c.Assert(b.rotate(), IsNil)
	}

	ents, err := bl.ReadFrom(binlog.Pos{}, 11)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 11)
	c.Assert(ents[10].Pos, DeepEquals, binlog.Pos{Offset: 286})

	ents, err = bl.ReadFrom(binlog.Pos{Suffix: 0, Offset: 286}, 11)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 11)
	c.Assert(ents[10].Pos, DeepEquals, binlog.Pos{Suffix: 1, Offset: 52})

	ents, err = bl.ReadFrom(binlog.Pos{Suffix: 1, Offset: 52}, 18)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 18)
	c.Assert(ents[17].Pos, DeepEquals, binlog.Pos{Suffix: 1, Offset: 26 * 20})

	ents, err = bl.ReadFrom(binlog.Pos{Offset: 26, Suffix: 5}, 20)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 20)
	c.Assert(ents[19].Pos, Equals, binlog.Pos{Offset: 26, Suffix: 6})
}

func (s *testBinloggerSuite) TestCourruption(c *C) {
	dir := c.MkDir()
	bl, err := OpenBinlogger(dir)
	c.Assert(err, IsNil)
	defer bl.Close()

	b, ok := bl.(*binlogger)
	c.Assert(ok, IsTrue)

	for i := 0; i < 3; i++ {
		for j := 0; j < 4; j++ {
			_, err = bl.WriteTail(&binlog.Entity{Payload: []byte("binlogtest")})
			c.Assert(err, IsNil)
		}

		c.Assert(b.rotate(), IsNil)
	}

	file := path.Join(dir, BinlogName(1))
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE, 0600)
	c.Assert(err, IsNil)

	err = f.Truncate(73)
	c.Assert(err, IsNil)

	err = f.Close()
	c.Assert(err, IsNil)

	ents, err := bl.ReadFrom(binlog.Pos{Suffix: 1, Offset: 26}, 4)
	c.Assert(ents, HasLen, 1)
	c.Assert(errors.Cause(err), Equals, io.ErrUnexpectedEOF)
}

func (s *testBinloggerSuite) TestGC(c *C) {
	dir := c.MkDir()
	bl, err := OpenBinlogger(dir)
	c.Assert(err, IsNil)
	// 1. A binlog file with index 0 is created at this point
	defer func() {
		err := CloseBinlogger(bl)
		c.Assert(err, IsNil)
	}()

	b, ok := bl.(*binlogger)
	c.Assert(ok, IsTrue)
	err = b.rotate()
	// 2. rotate creates a new binlog file with index 1
	c.Assert(err, IsNil)

	// No binlog files should be collected,
	// because both of the files has an index that's >= 0
	time.Sleep(10 * time.Millisecond)
	b.GC(time.Millisecond, binlog.Pos{Suffix: 0})

	names, err := ReadBinlogNames(b.dir)
	c.Assert(err, IsNil)
	c.Assert(names, HasLen, 2)
	for i, name := range names {
		suffix, _, err := ParseBinlogName(name)
		c.Assert(err, IsNil)
		c.Assert(suffix, Equals, uint64(i))
	}

	// The one with index 0 should be garbage collected
	b.GC(time.Millisecond, binlog.Pos{Suffix: 1})
	names, err = ReadBinlogNames(b.dir)
	c.Assert(err, IsNil)
	c.Assert(names, HasLen, 1)
	suffix, _, err := ParseBinlogName(names[0])
	c.Assert(err, IsNil)
	c.Assert(suffix, Equals, uint64(1))
}

func (s *testBinloggerSuite) TestSeekBinlog(c *C) {
	f, err := ioutil.TempFile(os.TempDir(), "testOffset")
	c.Assert(err, IsNil)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	encoder := NewEncoder(f, 0)
	_, err = encoder.Encode([]byte("binlogtest"))
	c.Assert(err, IsNil)

	testCase := make([]byte, 2048)
	binary.LittleEndian.PutUint32(testCase[:4], magic)
	for i := 4; i < 2048; i++ {
		testCase[i] = 'a'
	}
	_, err = f.Write(testCase)
	c.Assert(err, IsNil)

	_, err = encoder.Encode([]byte("binlogtest"))
	c.Assert(err, IsNil)
	_, err = f.Write(testCase)
	c.Assert(err, IsNil)

	offset, err := seekBinlog(f, 10)
	c.Assert(offset, Equals, int64(26))
	c.Assert(err, IsNil)

	offset, err = seekBinlog(f, 26)
	c.Assert(offset, Equals, int64(26))
	c.Assert(err, IsNil)

	offset, err = seekBinlog(f, 27)
	c.Assert(offset, Equals, int64(2074))
	c.Assert(err, IsNil)

	offset, err = seekBinlog(f, 2080)
	c.Assert(offset, Equals, int64(2100))
	c.Assert(err, IsNil)

	offset, err = seekBinlog(f, 2100)
	c.Assert(offset, Equals, int64(2100))
	c.Assert(err, IsNil)

	_, err = seekBinlog(f, 2101)
	c.Assert(err, Equals, io.ErrUnexpectedEOF)
}

func (s *testBinloggerSuite) TestSkipCRCRead(c *C) {
	dir := c.MkDir()
	bl, err := OpenBinlogger(dir)
	c.Assert(err, IsNil)
	defer bl.Close()

	b, ok := bl.(*binlogger)
	c.Assert(ok, IsTrue)

	for i := 0; i < 10; i++ {
		for i := 0; i < 20; i++ {
			_, err = bl.WriteTail(&binlog.Entity{Payload: []byte("binlogtest")})
			c.Assert(err, IsNil)

			_, err = b.file.Write([]byte("test"))
			c.Assert(err, IsNil)
		}

		c.Assert(b.rotate(), IsNil)
	}

	ents, err := bl.ReadFrom(binlog.Pos{}, 11)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 11)
	c.Assert(ents[10].Pos, DeepEquals, binlog.Pos{Offset: 326})

	ents, err = bl.ReadFrom(binlog.Pos{Suffix: 0, Offset: 322}, 11)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 11)
	c.Assert(ents[10].Pos, DeepEquals, binlog.Pos{Suffix: 1, Offset: 56})

	ents, err = bl.ReadFrom(binlog.Pos{Suffix: 1, Offset: 56}, 18)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 18)
	c.Assert(ents[17].Pos, DeepEquals, binlog.Pos{Suffix: 1, Offset: 26*20 + 19*4})

	ents, err = bl.ReadFrom(binlog.Pos{Offset: 26, Suffix: 5}, 20)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 20)
	c.Assert(ents[19].Pos, Equals, binlog.Pos{Offset: 26, Suffix: 6})
}
