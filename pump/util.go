package pump

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	bf "github.com/pingcap/tidb-binlog/pkg/binlogfile"
	binlog "github.com/pingcap/tipb/go-binlog"
)

const (
	physicalShiftBits = 18
	maxRetry          = 12
	retryInterval     = 5 * time.Second
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

// InitLogger initalizes Pump's logger.
func InitLogger(cfg *Config) {
	log.SetLevelByString(cfg.LogLevel)

	if len(cfg.LogFile) > 0 {
		log.SetOutputByName(cfg.LogFile)

		if cfg.LogRotate == "hour" {
			log.SetRotateByHour()
		} else {
			log.SetRotateByDay()
		}
	}
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

func composeTS(physical, logical int64) uint64 {
	return uint64((physical << physicalShiftBits) + logical)
}

// TopicName returns topic name
func TopicName(clusterID string, nodeID string) string {
	// ":" is not valide in kafka topic name
	topicName := fmt.Sprintf("%s_%s", clusterID, strings.Replace(nodeID, ":", "_", -1))
	return topicName
}

// DefaultTopicPartition returns Deault topic partition
func DefaultTopicPartition() int32 {
	return defaultPartition
}

// ComparePos compares the two positions of binlog items, return 0 when the left equal to the right,
// return -1 if the left is ahead of the right, oppositely return 1.
func ComparePos(left, right binlog.Pos) int {
	if left.Suffix < right.Suffix {
		return -1
	} else if left.Suffix > right.Suffix {
		return 1
	} else if left.Offset < right.Offset {
		return -1
	} else if left.Offset > right.Offset {
		return 1
	} else {
		return 0
	}
}

func createKafkaClient(addr []string) (sarama.SyncProducer, error) {
	var (
		client sarama.SyncProducer
		err    error
	)

	for i := 0; i < maxRetry; i++ {
		// initial kafka client to use manual partitioner
		config := sarama.NewConfig()
		config.Producer.Partitioner = sarama.NewManualPartitioner
		config.Producer.MaxMessageBytes = maxMsgSize
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll

		client, err = sarama.NewSyncProducer(addr, config)
		if err != nil {
			log.Errorf("create kafka client error %v", err)
			time.Sleep(retryInterval)
			continue
		}
		return client, nil
	}

	return nil, errors.Trace(err)
}

func seekMagic(f *os.File, offset int64) (int64, error) {
	var (
		batchSize = 1024
		buff      = make([]byte, batchSize)
		header    = buff[0:3]
		tail      = buff[3:]
	)

	err := seekOffset(f, offset)
	if err != nil {
		return 0, err
	}

	// read head firstly
	_, err = io.ReadFull(f, header)
	if err != nil {
		return 0, err
	}

	for {
		_, err = io.ReadFull(f, tail)
		for i := 0; i < batchSize-3; i++ {
			magicNum := binary.LittleEndian.Uint32(buff[i : i+4])
			err = checkMagic(magicNum)
			if err == nil {
				offset = offset + int64(i)
				if err1 := seekOffset(f, offset); err1 != nil {
					return 0, errors.Trace(err1)
				}
				return offset, nil
			}
		}
		if err != nil {
			return 0, err
		}

		// hard code
		offset += int64(batchSize - 4)
		header[0], header[1], header[2] = tail[batchSize-3], tail[batchSize-2], tail[batchSize-1]
	}
}

func seekOffset(f *os.File, offset int64) error {
	currentOffset, err := f.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	if currentOffset < offset {
		return errors.Errorf("seek to wrong offset %d is, not expected %d", currentOffset, offset)
	}

	return nil
}
