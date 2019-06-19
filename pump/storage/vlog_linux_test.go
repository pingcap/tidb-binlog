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

// +build linux

package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/docker/docker/pkg/mount"
	"github.com/pingcap/check"
)

// Need root privilege for using mount to get a specify tmpfs file system
func (vs *VlogSuit) TestNoSpace(c *check.C) {
	dir := c.MkDir()
	c.Log("use dir: ", dir)

	size := "40k"
	err := mount.ForceMount("tmpfs", dir, "tmpfs", fmt.Sprintf("size=%s", size))
	if strings.Contains(err.Error(), "operation not permitted") {
		c.Skip("operation not permitted to using mount")
	}

	c.Assert(err, check.IsNil)
	defer func() {
		err := mount.Unmount(dir)
		if err != nil {
			c.Log(err)
		}
	}()

	// occupy 20k, leaving 20k available
	occupyFile := path.Join(dir, "dummmy")
	err = ioutil.WriteFile(occupyFile, make([]byte, 20*1024), 0644)
	c.Assert(err, check.IsNil)

	vlog := new(valueLog)
	err = vlog.open(dir, DefaultOptions())
	c.Assert(err, check.IsNil)

	// 1k payload per record
	payload := make([]byte, 1<<10)
	req := &request{
		payload: payload,
	}

	// should be enough space to write 19 records
	for i := 0; i < 19; i++ {
		err = vlog.write([]*request{req})
		c.Assert(err, check.IsNil)
	}

	// failed because only 20k space available and may write a incomplete record
	err = vlog.write([]*request{req})
	c.Assert(err, check.NotNil)

	// free space and write again
	err = os.Remove(occupyFile)
	c.Assert(err, check.IsNil)

	err = vlog.write([]*request{req})
	c.Assert(err, check.IsNil)

	// read back normally
	_, err = vlog.readValue(req.valuePointer)
	c.Assert(err, check.IsNil)

	err = vlog.close()
	c.Assert(err, check.IsNil)
}
