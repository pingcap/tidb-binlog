package pump

import (
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"

	bf "github.com/pingcap/tidb-binlog/pkg/binlogfile"
)

// AtomicBool is bool type that support atomic operator
type AtomicBool int32

// Set sets the value
func (b *AtomicBool) Set(v bool) {
	if v {
		atomic.StoreInt32((*int32)(b), 1)
	} else {
		atomic.StoreInt32((*int32)(b), 0)
	}
}

// Get returns the value
func (b *AtomicBool) Get() bool {
	return atomic.LoadInt32((*int32)(b)) == 1
}

// KRand is an algorithm that compute rand nums
func KRand(size int, kind int) []byte {
	ikind, kinds, result := kind, [][]int{{10, 48}, {26, 97}, {26, 65}}, make([]byte, size)
	isAll := kind > 2 || kind < 0
	for i := 0; i < size; i++ {
		if isAll { // random ikind
			ikind = rand.Intn(3)
		}
		scope, base := kinds[ikind][0], kinds[ikind][1]
		result[i] = uint8(base + rand.Intn(scope))
	}
	return result
}

// Exist checks the dir exist, that it should have some file
func Exist(dirpath string) bool {
	names, err := bf.ReadDir(dirpath)
	if err != nil {
		return false
	}

	return len(names) != 0
}

// TopicName returns topic name
func TopicName(clusterID string, nodeID string) string {
	// ":" is not a valid kafka topic name
	topicName := fmt.Sprintf("%s_%s", clusterID, strings.Replace(nodeID, ":", "_", -1))
	return topicName
}
