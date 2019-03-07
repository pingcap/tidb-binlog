package pump

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
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

// CheckFileExist chekcs the file exist status and wether it is a file
func CheckFileExist(filepath string) (string, error) {
	fi, err := os.Stat(filepath)
	if err != nil {
		return "", errors.Trace(err)
	}
	if fi.IsDir() {
		return "", errors.Errorf("filepath: %s, is a directory, not a file", filepath)
	}
	return filepath, nil
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
	// ":" is not valide in kafka topic name
	topicName := fmt.Sprintf("%s_%s", clusterID, strings.Replace(nodeID, ":", "_", -1))
	return topicName
}
