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
	"bytes"
	"sort"
	"testing"

	"github.com/pingcap/check"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

type EncodeTSKeySuite struct{}

var _ = check.Suite(&EncodeTSKeySuite{})

func (e *EncodeTSKeySuite) TestEncodeTSKey(c *check.C) {
	var tsSlice = []int64{401603357443358721, 40160311937754726, 401605694141759490, 401605694129438725}

	sort.Slice(tsSlice, func(i int, j int) bool {
		return tsSlice[i] < tsSlice[j]
	})

	var encodes [][]byte

	for _, ts := range tsSlice {
		data := encodeTSKey(ts)
		encodes = append(encodes, data)

		decodedTS := decodeTSKey(data)
		c.Assert(ts, check.Equals, decodedTS)
	}

	// the encode way must be sorted like origin integer ts
	sorted := sort.SliceIsSorted(encodes, func(i int, j int) bool {
		return bytes.Compare(encodes[i], encodes[j]) < 0
	})

	c.Assert(sorted, check.IsTrue)
}
