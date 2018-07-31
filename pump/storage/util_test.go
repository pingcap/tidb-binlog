package storage

import (
	"bytes"
	"sort"
	"testing"
)

func TestEncodeTs(t *testing.T) {
	var tsSlice = []int64{401603357443358721, 40160311937754726, 401605694141759490, 401605694129438725}

	sort.Slice(tsSlice, func(i int, j int) bool {
		return tsSlice[i] < tsSlice[j]
	})

	var encodes [][]byte

	for _, ts := range tsSlice {
		data := encodeTs(ts)
		encodes = append(encodes, data)

		decodedTs := decodeTs(data)
		if decodedTs != ts {
			t.Fatalf("want: %d, get: %d", ts, decodedTs)
		}
	}

	// the encode way must be sorted like origin integer ts
	sorted := sort.SliceIsSorted(encodes, func(i int, j int) bool {
		return bytes.Compare(encodes[i], encodes[j]) < 0
	})

	if !sorted {
		t.Fatal("not sorted")
	}

}
