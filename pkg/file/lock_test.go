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

package file

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testLockSuite{})

func Test(t *testing.T) { TestingT(t) }

type testLockSuite struct{}

func (t *testLockSuite) TestLockAndUnlock(c *C) {
	// lock the nonexist file that would return error
	_, err := LockFile("testNoExistFile", os.O_WRONLY, PrivateFileMode)
	c.Assert(err, NotNil)

	// lock the nonexist file that would return error
	_, err = TryLockFile("testNoExistFile", os.O_WRONLY, PrivateFileMode)
	c.Assert(err, NotNil)

	// create test file
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
