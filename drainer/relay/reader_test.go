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

package relay

import (
	"os"
	"path"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

var _ = Suite(&testReaderSuite{})

type testReaderSuite struct {
	translator.BinlogGenerator
}

func (r *testReaderSuite) TestCreate(c *C) {
	_, err := NewReader("", 8)
	c.Assert(err, NotNil)

	_, err = NewReader("/", 8)
	c.Assert(err, NotNil)

	dir := c.MkDir()
	relayReader, err := NewReader(dir, 8)
	c.Assert(err, IsNil)
	err = relayReader.Close()
	c.Assert(err, IsNil)
}

func (r *testReaderSuite) TestReadBinlog(c *C) {
	dir := c.MkDir()
	r.SetDDL()
	r.writeBinlog(c, dir)
	txn := r.readBinlogAndCheck(c, dir, 1)
	c.Assert(r.Table, Equals, txn.DDL.Table)
	c.Assert(r.Schema, Equals, txn.DDL.Database)

	r.SetInsert(c)
	r.writeBinlog(c, dir)
	txn = r.readBinlogAndCheck(c, dir, 2)
	c.Assert(txn.DMLs[0].Tp, Equals, loader.InsertDMLType)

	r.SetUpdate(c)
	r.writeBinlog(c, dir)
	txn = r.readBinlogAndCheck(c, dir, 3)
	c.Assert(txn.DMLs[0].Tp, Equals, loader.UpdateDMLType)

	r.SetDelete(c)
	r.writeBinlog(c, dir)
	txn = r.readBinlogAndCheck(c, dir, 4)
	c.Assert(txn.DMLs[0].Tp, Equals, loader.DeleteDMLType)
}

func (r *testReaderSuite) writeBinlog(c *C, dir string) {
	relayer, err := NewRelayer(dir, binlogfile.SegmentSizeBytes, r)
	c.Assert(relayer, NotNil)
	c.Assert(err, IsNil)
	defer func() { c.Assert(relayer.Close(), IsNil) }()

	_, err = relayer.WriteBinlog(r.Schema, r.Table, r.TiBinlog, r.PV)
	c.Assert(err, IsNil)
}

func (r *testReaderSuite) readBinlogAndCheck(c *C, dir string, expectedNumber int) *loader.Txn {
	relayReader, err := NewReader(dir, 8)
	c.Assert(relayReader, NotNil)
	c.Assert(err, IsNil)
	defer func() { c.Assert(relayReader.Close(), IsNil) }()

	relayReader.Run()

	var lastTxn *loader.Txn
	number := 0
	for txn := range relayReader.Txns() {
		number++
		lastTxn = txn
	}
	c.Assert(<-relayReader.Error(), IsNil)
	c.Assert(number, Equals, expectedNumber)
	return lastTxn
}

func (r *testReaderSuite) TestReadBinlogError(c *C) {
	dir := c.MkDir()
	r.SetDDL()
	r.writeBinlog(c, dir)

	// Set the file mode to 0100
	names, err := binlogfile.ReadBinlogNames(dir)
	c.Assert(err, IsNil)
	c.Assert(names, HasLen, 1)
	fpath := path.Join(dir, names[0])
	f, err := os.OpenFile(fpath, os.O_WRONLY, file.PrivateFileMode)
	c.Assert(err, IsNil)
	c.Assert(f.Chmod(0222), IsNil)
	c.Assert(f.Close(), IsNil)

	relayReader, err := NewReader(dir, 8)
	c.Assert(err, IsNil)
	relayReader.Run()
	c.Assert(<-relayReader.Error(), ErrorMatches, "*permission denied*")
	c.Assert(relayReader.Close(), IsNil)

	// Append some invalid data to the file.
	f, err = os.OpenFile(fpath, os.O_WRONLY, 0222)
	c.Assert(err, IsNil)
	c.Assert(f.Chmod(file.PrivateFileMode), IsNil)
	_, err = f.WriteString("test")
	c.Assert(err, IsNil)
	c.Assert(f.Close(), IsNil)

	relayReader, err = NewReader(dir, 8)
	c.Assert(err, IsNil)
	relayReader.Run()
	// It's recovered by binlogger.
	c.Assert(<-relayReader.Error(), IsNil)
	c.Assert(relayReader.Close(), IsNil)
}

func (r *testReaderSuite) TestCancelRead(c *C) {
	dir := c.MkDir()

	r.SetInsert(c)
	for i := 0; i < 1000; i++ {
		r.writeBinlog(c, dir)
	}

	relayReader, err := NewReader(dir, 8)
	defer func() { c.Assert(relayReader.Close(), IsNil) }()
	c.Assert(err, IsNil)
	cancelFunc := relayReader.Run()
	cancelFunc()
	c.Assert(<-relayReader.Error(), ErrorMatches, "context canceled")
}
