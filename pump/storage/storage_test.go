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

package storage

import (
	"context"
	"math/rand"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fuzz "github.com/google/gofuzz"
	"github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/syndtr/goleveldb/leveldb"
)

type Log interface {
	Fatal(args ...interface{})
	Log(args ...interface{})
}

var _ Log = &testing.B{}
var _ Log = &testing.T{}
var _ Log = &check.C{}

type AppendSuit struct{}

var _ = check.Suite(&AppendSuit{})

func newAppend(t Log) *Append {
	return newAppendWithOptions(t, nil)
}

func newAppendWithOptions(t Log, options *Options) *Append {
	dir := path.Join(os.TempDir(), strconv.Itoa(rand.Int()))
	// t.Log("use dir: ", dir)
	err := os.Mkdir(dir, 0777)
	if err != nil {
		t.Fatal(err)
	}

	append, err := NewAppend(dir, options)
	if err != nil {
		t.Fatal(err)
	}

	return append
}

func cleanAppend(append *Append) {
	select {
	case <-append.close:
	default:
		append.Close()
	}

	os.RemoveAll(append.dir)
}

func (as *AppendSuit) TestNewAppend(c *check.C) {
	append := newAppend(c)
	defer cleanAppend(append)

	append.Close()
}

func (as *AppendSuit) TestBlockedWriteKVShouldNotStopWritingVlogs(c *check.C) {
	// Set KVChanCapacity to be extremely small so that we can feed it up
	store := newAppendWithOptions(c, DefaultOptions().WithKVChanCapacity(10))
	incoming := make(chan *request, 100)
	// We are not calling WriteBinlog here, instead, we set up an isolated
	// instance of writeToValueLog so that we can control the input and output
	// channels.
	written := store.writeToValueLog(incoming)

	// Send many requests to simulate WriteBinlog calls
	finished := make(chan struct{})
	go func() {
		reqs := createDummyReqs(4000)
		for _, r := range reqs {
			r.wg.Add(1)
			incoming <- r
		}
		for _, r := range reqs {
			r.wg.Wait()
		}
		close(finished)
	}()

	// The tiny `written` channel is never consumed, once it's full,
	// no new requests can be sent to it.
	// The slowChaser should detect this and make sure writes are not blocked.
	select {
	case <-finished:
		c.Assert(
			len(written),
			check.Equals,
			store.options.KVChanCapacity,
			check.Commentf("No consumer of the written channel is set up, it should be full at this point"),
		)
	case <-time.After(1 * time.Second):
		c.Fatal("Takes too long to finish writing binlogs, writing may have been blocked.")
	}
}

func (as *AppendSuit) TestVlogsShouldBeInSyncWhenDownStreamRecovers(c *check.C) {
	store := newAppendWithOptions(c, DefaultOptions().WithKVChanCapacity(10))
	incoming := make(chan *request, 100)
	written := store.writeToValueLog(incoming)

	finished := make(chan struct{})
	go func() {
		reqs := createDummyReqs(2000)
		for _, r := range reqs {
			r.wg.Add(1)
			incoming <- r
		}
		for _, r := range reqs {
			r.wg.Wait()
		}
		close(finished)
	}()
	<-finished

	for i := 0; i < 10; i++ {
		r := <-written
		c.Assert(r.startTS, check.Equals, int64(i)+1)
	}
	time.Sleep(300 * time.Millisecond)
	for i := 11; i < 500; i++ {
		r := <-written
		c.Assert(r.startTS, check.Equals, int64(i))
	}
	time.Sleep(300 * time.Millisecond)
	for i := 500; i <= 2000; i++ {
		r := <-written
		c.Assert(r.startTS, check.Equals, int64(i))
	}
}

func (as *AppendSuit) TestCloseAndOpenAgain(c *check.C) {
	append := newAppend(c)
	defer cleanAppend(append)

	err := append.Close()
	c.Assert(err, check.IsNil)

	append, err = NewAppend(append.dir, append.options)
	c.Assert(err, check.IsNil)

	// populate some data and close open back to check the status
	populateBinlog(c, append, 128, 1)
	time.Sleep(time.Second * 3)

	gcTS := append.gcTS
	maxCommitTS := append.maxCommitTS
	headPointer := append.headPointer
	handlePointer := append.handlePointer

	c.Log(gcTS, maxCommitTS, headPointer, handlePointer)

	err = append.Close()
	c.Assert(err, check.IsNil)

	append, err = NewAppend(append.dir, append.options)
	c.Assert(err, check.IsNil)

	c.Assert(gcTS, check.Equals, append.gcTS)
	c.Assert(maxCommitTS, check.Equals, append.maxCommitTS)
	c.Assert(headPointer, check.Equals, append.headPointer)
	c.Assert(handlePointer, check.Equals, append.handlePointer)
	append.Close()
}

func (as *AppendSuit) TestWriteBinlogAndPullBack(c *check.C) {
	as.testWriteBinlogAndPullBack(c, -1, 1024)
}

func (as *AppendSuit) testWriteBinlogAndPullBack(c *check.C, prewriteValueSize int, binlogNum int32) {
	appendStorage := newAppend(c)
	defer cleanAppend(appendStorage)

	populateBinlog(c, appendStorage, prewriteValueSize, binlogNum)

	for i := 0; i < 2; i++ {
		// close and open the check again
		if i == 1 {
			err := appendStorage.Close()
			c.Assert(err, check.IsNil)

			appendStorage, err = NewAppend(appendStorage.dir, appendStorage.options)
			c.Assert(err, check.IsNil)
		}

		ctx, cancel := context.WithCancel(context.Background())
		values := appendStorage.PullCommitBinlog(ctx, 0)

		// pull the binlogs back and check sorted
		var binlogs []*pb.Binlog
	PullLoop:
		for {
			select {
			case value := <-values:
				getBinlog := new(pb.Binlog)
				err := getBinlog.Unmarshal(value)
				c.Assert(err, check.IsNil)
				binlogs = append(binlogs, getBinlog)
				if len(binlogs) == int(binlogNum) {
					break PullLoop
				}
			case <-time.After(time.Second * 5):
				c.Fatal("get value timeout")
			}
		}

		// check commitTS increasing
		for i := 1; i < len(binlogs); i++ {
			c.Assert(binlogs[i].CommitTs, check.Greater, binlogs[i-1].CommitTs)
		}

		cancel()
	}
	appendStorage.Close()
}

func (as *AppendSuit) TestDoGCTS(c *check.C) {
	var value = make([]byte, 10)
	append := newAppend(c)
	defer cleanAppend(append)

	var i int64
	var n int64 = 1 << 11
	var batch leveldb.Batch
	for i = 1; i < n; i++ {
		batch.Put(encodeTSKey(i), value)
	}
	err := append.metadata.Write(&batch, nil)
	c.Assert(err, check.IsNil)

	var gcTS int64 = 10
	append.doGCTS(gcTS)

	for i = 1; i < n; i++ {
		_, err := append.metadata.Get(encodeTSKey(i), nil)
		if i <= gcTS {
			c.Assert(err, check.Equals, leveldb.ErrNotFound, check.Commentf("after gc still found ts: %v", i))
		} else {
			c.Assert(err, check.IsNil, check.Commentf("can't found ts: %v", i))
		}
	}
}

func (as *AppendSuit) TestReadWritePointer(c *check.C) {
	append := newAppend(c)
	defer cleanAppend(append)

	// check return zero valuePointer when the key not exist
	var readVP valuePointer
	var err error
	readVP, err = append.readPointer([]byte("no_exist_key"))
	c.Assert(err, check.IsNil)
	c.Assert(readVP, check.Equals, valuePointer{})

	// test with random key and valuePointer value
	fuzz := fuzz.New().NilChance(0)
	for i := 0; i < 100; i++ {
		var vp valuePointer
		var key []byte
		fuzz.Fuzz(&vp)
		// offset should >= 0, so just take abs(vp.Offset), when the random value is negative
		if vp.Offset < 0 {
			vp.Offset = -vp.Offset
		}
		fuzz.Fuzz(&key)

		err = append.savePointer(key, vp)
		c.Assert(err, check.IsNil)

		readVP, err = append.readPointer(key)
		c.Assert(err, check.IsNil)
		c.Assert(readVP, check.Equals, vp)
	}
}

func (as *AppendSuit) TestReadWriteGCTS(c *check.C) {
	append := newAppend(c)
	defer cleanAppend(append)

	ts, err := append.readGCTSFromDB()
	c.Assert(err, check.IsNil)
	c.Assert(ts, check.Equals, int64(0))

	err = append.saveGCTSToDB(100)
	c.Assert(err, check.IsNil)

	ts, err = append.readGCTSFromDB()
	c.Assert(err, check.IsNil)
	c.Assert(ts, check.Equals, int64(100))

	// close and open read again
	append.Close()
	append, err = NewAppend(append.dir, append.options)
	c.Assert(err, check.IsNil)

	c.Assert(append.gcTS, check.Equals, int64(100))
	append.Close()
}

// test helper to write binlogNum binlog to append
// If `prewriteValueSize` is less than or equal to 0, the size of prewriteValue for
// binlogs will be random integers between [1, 1024].
func populateBinlog(b Log, append *Append, prewriteValueSize int, binlogNum int32) {
	fixedValueSize := prewriteValueSize > 0
	var prewriteValue []byte
	if fixedValueSize {
		prewriteValue = make([]byte, prewriteValueSize)
	} else {
		prewriteValue = make([]byte, 1024)
	}
	var ts int64
	getTS := func() int64 {
		return atomic.AddInt64(&ts, 1)
	}

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				num := atomic.AddInt32(&binlogNum, -1)
				if num < 0 {
					return
				}
				startTS := getTS()

				// write P binlog
				binlog := pb.Binlog{
					Tp:      pb.BinlogType_Prewrite,
					StartTs: startTS,
				}
				if fixedValueSize {
					binlog.PrewriteValue = prewriteValue
				} else {
					size := 1 + rand.Intn(len(prewriteValue))
					binlog.PrewriteValue = prewriteValue[:size]
					b.Log("Setting prewrite value of length", size)
				}

				if err := append.WriteBinlog(&binlog); err != nil {
					b.Fatal(err)
				}

				// write C binlog
				binlog = pb.Binlog{
					Tp:       pb.BinlogType_Commit,
					StartTs:  startTS,
					CommitTs: getTS(),
				}
				if err := append.WriteBinlog(&binlog); err != nil {
					b.Fatal(err)
				}
			}
		}()
	}

	// wait finish populate data
	wg.Wait()
}

func (as *AppendSuit) TestNoSpace(c *check.C) {
	// TODO test when no space left
}

func (as *AppendSuit) TestResolve(c *check.C) {
	// TODO test the case we query tikv to know weather a txn a commit
	// is there a fake or mock kv.Storage and tikv.LockResolver to easy the test?
}

type OpenDBSuit struct {
	dir string
}

var _ = check.Suite(&OpenDBSuit{})

func (s *OpenDBSuit) SetUpTest(c *check.C) {
	s.dir = c.MkDir()
}

func (s *OpenDBSuit) TestWhenConfigIsNotProvided(c *check.C) {
	_, err := openMetadataDB(s.dir, nil)
	c.Assert(err, check.IsNil)
}

func (s *OpenDBSuit) TestProvidedConfigValsNotOverwritten(c *check.C) {
	cf := KVConfig{
		BlockRestartInterval: 32,
		WriteL0PauseTrigger:  12,
	}
	_, err := openMetadataDB(s.dir, &cf)
	c.Assert(err, check.IsNil)
	c.Assert(cf.BlockRestartInterval, check.Equals, 32)
	c.Assert(cf.WriteL0PauseTrigger, check.Equals, 12)
	c.Assert(cf.BlockCacheCapacity, check.Equals, defaultStorageKVConfig.BlockCacheCapacity)
}

func createDummyReqs(n int) []*request {
	reqs := make([]*request, 0, n)
	for i := 0; i < n; i++ {
		ts := int64(i) + 1
		binlog := pb.Binlog{
			StartTs: ts,
			Tp:      pb.BinlogType_Prewrite,
		}
		payload, err := binlog.Marshal()
		if err != nil {
			panic(err)
		}
		r := &request{
			startTS: ts,
			payload: payload,
			tp:      pb.BinlogType_Prewrite,
		}
		reqs = append(reqs, r)
	}
	return reqs
}
