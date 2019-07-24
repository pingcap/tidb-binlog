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

package util

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-binlog/pkg/security"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap/zapcore"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type utilSuite struct{}

var _ = Suite(&utilSuite{})

type dummyStore struct {
	kv.Storage
	err error
	ver kv.Version
}

func (s dummyStore) CurrentVersion() (kv.Version, error) {
	return s.ver, s.err
}

func (s *utilSuite) TestQueryLatestTsFromPD(c *C) {
	ds := dummyStore{err: errors.New("test")}
	ver, err := QueryLatestTsFromPD(ds)
	c.Assert(ver, Equals, int64(0))
	c.Assert(err, NotNil)

	ds.err = nil
	ds.ver = kv.Version{Ver: 2018}

	ver, err = QueryLatestTsFromPD(ds)
	c.Assert(ver, Equals, int64(2018))
	c.Assert(err, IsNil)
}

func (s *utilSuite) TestIsValidateListenHost(c *C) {
	c.Assert(IsValidateListenHost("192.168.3.72"), IsTrue)
	c.Assert(IsValidateListenHost("localhost"), IsFalse)
	c.Assert(IsValidateListenHost("127.0.0.1"), IsFalse)
	c.Assert(IsValidateListenHost("0.0.0.0"), IsTrue)
	c.Assert(IsValidateListenHost(""), IsFalse)
	c.Assert(IsValidateListenHost("::1"), IsFalse)
}

func (s *utilSuite) TestToColumnTypeMap(c *C) {
	cols := []*model.ColumnInfo{
		{
			ID:        10,
			FieldType: types.FieldType{Tp: mysql.TypeVarchar},
		},
		{
			ID:        1984,
			FieldType: types.FieldType{Tp: mysql.TypeLong},
		},
	}
	colTypes := ToColumnTypeMap(cols)
	c.Assert(colTypes, HasLen, 2)
	c.Assert(colTypes, HasKey, int64(10))
	c.Assert(colTypes[10].Tp, Equals, mysql.TypeVarchar)
	c.Assert(colTypes, HasKey, int64(1984))
	c.Assert(colTypes[1984].Tp, Equals, mysql.TypeLong)
}

func (s *utilSuite) TestStdLogger(c *C) {
	var logHook LogHook
	logHook.SetUp()
	oldLevel := log.GetLevel()
	log.SetLevel(zapcore.InfoLevel)
	defer log.SetLevel(oldLevel)
	defer logHook.TearDown()

	logger := NewStdLogger("hola:")
	logger.Print("Hello,")
	logger.Printf(" %d!", 42)
	logger.Println("Goodbye!")

	entrys := logHook.Entrys
	c.Assert(entrys, HasLen, 3)
	c.Assert(entrys[0].Message, Matches, ".*hola:Hello,.*")
	c.Assert(entrys[1].Message, Matches, ".*hola: 42!.*")
	c.Assert(entrys[2].Message, Matches, ".*hola:Goodbye!.*")
}

type getAddrIPSuite struct{}

var _ = Suite(&getAddrIPSuite{})

func (s *getAddrIPSuite) TestShouldRetIPV4(c *C) {
	addr := net.IPNet{
		IP: net.ParseIP("192.168.1.2"),
	}
	c.Assert(getAddrDefaultIP(&addr), Equals, "192.168.1.2")

	addr2 := net.IPAddr{
		IP: net.ParseIP("192.168.1.3"),
	}
	c.Assert(getAddrDefaultIP(&addr2), Equals, "192.168.1.3")
}

func (s *getAddrIPSuite) TestShouldIgnoreLoopback(c *C) {
	addr := net.IPNet{
		IP: net.ParseIP("127.0.0.1"),
	}
	c.Assert(getAddrDefaultIP(&addr), Equals, "")
}

func (s *getAddrIPSuite) TestShouldIgnoreIPV6(c *C) {
	addr := net.IPNet{
		IP: net.ParseIP("2001:db8::68"),
	}
	c.Assert(getAddrDefaultIP(&addr), Equals, "")
}

type retrySuite struct{}

var _ = Suite(&retrySuite{})

func (s *retrySuite) TestShouldNotRetryOnSuccess(c *C) {
	callCount := 0
	err := RetryOnError(10, time.Millisecond, "", func() error {
		callCount++
		return nil
	})
	c.Assert(err, IsNil)
	c.Assert(callCount, Equals, 1)
}

func (s *retrySuite) TestShouldRetry(c *C) {
	callCount := 0
	err := RetryOnError(10, time.Millisecond, "", func() error {
		callCount++
		if callCount < 3 {
			return errors.New("Fail")
		}
		return nil
	})
	c.Assert(err, IsNil)
	c.Assert(callCount, Equals, 3)
}

func (s *retrySuite) TestShouldReturnErr(c *C) {
	callCount := 0
	err := RetryOnError(4, time.Microsecond, "", func() error {
		callCount++
		return errors.New("Fail")
	})
	c.Assert(err, ErrorMatches, "Fail")
	c.Assert(callCount, Equals, 4)
}

type getPdClientSuite struct{}

var _ = Suite(&getPdClientSuite{})

func (s *getPdClientSuite) TestShouldRejectInvalidAddr(c *C) {
	_, err := GetPdClient("asdfasdf", security.Config{})
	c.Assert(err, NotNil)
}

func (s *getPdClientSuite) TestShouldRetPdCli(c *C) {
	expected := dummyCli{}
	origF := newPdCli
	newPdCli = func(pdAddrs []string, security pd.SecurityOption) (pd.Client, error) {
		return expected, nil
	}
	defer func() {
		newPdCli = origF
	}()
	cli, err := GetPdClient("http://192.168.101.42:7979", security.Config{})
	c.Assert(err, IsNil)
	c.Assert(cli.(dummyCli), Equals, expected)
}

type adjustValueSuite struct{}

var _ = Suite(&adjustValueSuite{})

func (s *adjustValueSuite) TestAdjustString(c *C) {
	var str string
	AdjustString(&str, "hi")
	c.Assert(str, Equals, "hi")

	AdjustString(&str, "hello")
	c.Assert(str, Equals, "hi")
}

func (s *adjustValueSuite) TestAdjustInt(c *C) {
	var i int
	AdjustInt(&i, 1)
	c.Assert(i, Equals, 1)

	AdjustInt(&i, 2)
	c.Assert(i, Equals, 1)
}

func (s *adjustValueSuite) TestAdjustDuration(c *C) {
	var d time.Duration
	AdjustDuration(&d, time.Duration(time.Second))
	c.Assert(d, Equals, time.Duration(time.Second))

	AdjustDuration(&d, time.Duration(time.Hour))
	c.Assert(d, Equals, time.Duration(time.Second))
}

type tryUntilSuccSuite struct{}

var _ = Suite(&tryUntilSuccSuite{})

func (s *tryUntilSuccSuite) TestShouldStopWhenDone(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	signal := make(chan struct{})
	go func() {
		err := TryUntilSuccess(ctx, 10*time.Millisecond, "Testing", func() error {
			return errors.New("Just Failed")
		})
		close(signal)
		c.Assert(err, ErrorMatches, "context canceled")
	}()
	cancel()
	select {
	case <-signal:
	case <-time.After(time.Second):
		c.Fatal("Doesn't stop in time after done")
	}
}

func (s *tryUntilSuccSuite) TestShouldStopOnSuccess(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var callCount int
	signal := make(chan struct{})
	go func() {
		err := TryUntilSuccess(ctx, time.Millisecond, "Testing", func() error {
			callCount++
			if callCount < 3 {
				return errors.New("Just Failed")
			}
			return nil
		})
		close(signal)
		c.Assert(err, IsNil)
	}()
	select {
	case <-signal:
	case <-time.After(time.Second):
		c.Fatal("Doesn't stop in time after done")
	}
	c.Assert(callCount, Equals, 3)
}

type retryCtxSuite struct{}

var _ = Suite(&retryCtxSuite{})

func (s *retryCtxSuite) TestOnlyRetrySpecifiedTimes(c *C) {
	ctx := context.Background()
	var callCount int
	err := RetryContext(ctx, 2, time.Microsecond, 3, func(ictx context.Context) error {
		callCount++
		return errors.New("Fail")
	})
	c.Assert(err, ErrorMatches, "Fail")
	c.Assert(callCount, Equals, 2)
}

func (s *retryCtxSuite) TestRetryUntilTimeout(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var callCount int
	err := RetryContext(ctx, 10, time.Millisecond, 2, func(ictx context.Context) error {
		callCount++
		return errors.New("Fail")
	})
	c.Assert(err, ErrorMatches, "Fail")
	c.Assert(callCount, Less, 10)

	callCount = 0
	err = RetryContext(ctx, 10, time.Millisecond, 2, func(ictx context.Context) error {
		callCount++
		<-ictx.Done()
		return errors.New("Canceled")
	})
	c.Assert(err, ErrorMatches, "Canceled")
	c.Assert(callCount, Equals, 1)
}

func (s *retryCtxSuite) TestSuccessAfterRetry(c *C) {
	ctx := context.Background()
	var callCount int
	err := RetryContext(ctx, 5, time.Microsecond, 2, func(ictx context.Context) error {
		callCount++
		if callCount == 2 {
			return nil
		}
		return errors.New("Fail")
	})
	c.Assert(err, IsNil)
	c.Assert(callCount, Equals, 2)
}
