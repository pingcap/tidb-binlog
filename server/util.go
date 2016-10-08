package server

import (
	"github.com/ngaut/log"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

// InitLogger initalizes Pump's logger.
func InitLogger(isDebug bool) {
	if isDebug {
		log.SetLevelByString("debug")
	} else {
		log.SetLevelByString("info")
	}
	log.SetHighlighting(false)
}

// ComparePos compares the two positions of binlog items, return 0 when the left equal to the right,
// return -1 if the left is ahead of the right, oppositely return 1.
func ComparePos(left, right pb.Pos) int {
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

// CalculateNextPos calculates the position of binlog item next to the given one.
func CalculateNextPos(item pb.Binlog) pb.Pos {
	pos := item.Pos
	// 4 bytes(magic) + 8 bytes(size) + length of payload + 4 bytes(CRC)
	pos.Offset += int64(len(item.Payload) + 16)
	return pos
}
