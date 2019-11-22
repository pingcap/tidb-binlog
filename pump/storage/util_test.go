package storage

import (
	"bytes"
	"sort"
	"testing"

	"github.com/BurntSushi/toml"
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

type UtilSuite struct{}

var _ = check.Suite(&UtilSuite{})

func (u *UtilSuite) TestHumanizeBytes(c *check.C) {
	var s = struct {
		DiskSize HumanizeBytes `toml:"disk_size" json:"disk_size"`
	}{}

	tomlData := `
disk_size = "42 MB"

	`

	_, err := toml.Decode(tomlData, &s)
	c.Assert(err, check.IsNil)
	c.Assert(s.DiskSize.Uint64(), check.Equals, uint64(42*1000*1000))
}
