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

package drainer

import (
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tipb/go-binlog"
	"google.golang.org/grpc"
)

type pumpSuite struct{}

var _ = Suite(&pumpSuite{})

func (s *pumpSuite) TestGetCompressorName(c *C) {
	ctx := context.Background()
	_, ok := getCompressorName(ctx)
	c.Assert(ok, IsFalse)

	ctx = context.WithValue(ctx, drainerKeyType("compressor"), 42)
	_, ok = getCompressorName(ctx)
	c.Assert(ok, IsFalse)

	ctx = context.WithValue(ctx, drainerKeyType("compressor"), "")
	_, ok = getCompressorName(ctx)
	c.Assert(ok, IsFalse)

	ctx = context.WithValue(ctx, drainerKeyType("compressor"), "gzip")
	cp, ok := getCompressorName(ctx)
	c.Assert(ok, IsTrue)
	c.Assert(cp, Equals, "gzip")
}

type mockPumpPullBinlogsClient struct {
	grpc.ClientStream
	binlogBytesChan chan []byte
}

func (x *mockPumpPullBinlogsClient) Recv() (*binlog.PullBinlogResp, error) {
	payload, ok := <-x.binlogBytesChan
	if !ok {
		return nil, errors.Errorf("pump test has ran out of binlog items!")
	}
	return &binlog.PullBinlogResp{Entity: binlog.Entity{Payload: payload}}, nil
}

func (s *pumpSuite) TestPullBinlog(c *C) {
	errChan := make(chan error, 10)
	p := NewPump("pump_test", "", nil, 0, 5, errChan)
	p.grpcConn = &grpc.ClientConn{}
	binlogBytesChan := make(chan []byte, 10)
	p.pullCli = &mockPumpPullBinlogsClient{binlogBytesChan: binlogBytesChan}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		p.grpcConn = nil
		p.Close()
		close(binlogBytesChan)
	}()
	ret := p.PullBinlog(ctx, 0)
	// commitTs ascending
	var commitTsArray = []int64{7, 9, 11, 13, 15, 17, 19, 21, 23, 25}
	// commitTs disordering
	var wrongCommitTsArray = []int64{27, 29, 28}

	// ascending commitTs order
	pullBinlogCommitTSChecker(commitTsArray, ret, binlogBytesChan, c)
	time.Sleep(10 * time.Microsecond)
	c.Assert(p.latestTS, Equals, commitTsArray[len(commitTsArray)-1])

	// should omit disorder binlog item, latestTs should be 29
	pullBinlogCommitTSChecker(wrongCommitTsArray, ret, binlogBytesChan, c)
	time.Sleep(10 * time.Microsecond)
	c.Assert(p.latestTS, Equals, wrongCommitTsArray[len(wrongCommitTsArray)-2])
}

func pullBinlogCommitTSChecker(commitTsArray []int64, ret chan MergeItem, binlogBytesChan chan []byte, c *C) {
	go func() {
		for _, commitTs := range commitTsArray {
			binlogVal := new(binlog.Binlog)
			binlogVal.CommitTs = commitTs
			payload, err := binlogVal.Marshal()
			c.Assert(err, IsNil)
			binlogBytesChan <- payload
		}
	}()
	for _, commitTs := range commitTsArray {
		select {
		case binlogItemEntity := <-ret:
			c.Assert(binlogItemEntity.GetCommitTs(), Equals, commitTs)
		case <-time.After(time.Second):
			c.Fatal("Haven't receive pump binlog item in 1 sec")
		}
	}
}
