package drainer

import (
	pb "github.com/pingcap/tipb/go-binlog"
)

// EstimateSize estimates the interface's size
func EstimateSize(i interface{}) uint64 {
	switch v := i.(type) {
	case *pb.Binlog:
		return uint64(len(v.PrewriteValue))
	case *pb.TableMutation:
		return estimateMutationSize(v)
	case pb.TableMutation:
		return estimateMutationSize(&v)
	case *pb.Entity:
		return uint64(len(v.Payload))
	case *pb.PrewriteValue:
		totalSize := uint64(0)
		for _, m := range v.Mutations {
			totalSize += estimateMutationSize(&m)
		}
		return totalSize
	case *binlogItem:
		return uint64(len(v.binlog.PrewriteValue))
	case []string:
		totalSize := 0
		for _, str := range v {
			totalSize += len(str)
		}
		return uint64(totalSize)
	case *job:
		return uint64(2 * len(v.sql))
	default:
		return 0
	}
}

func estimateMutationSize(m *pb.TableMutation) uint64 {
	totalSize := 0
	for _, row := range m.InsertedRows {
		totalSize += len(row)
	}
	for _, row := range m.UpdatedRows {
		totalSize += len(row)
	}
	for _, row := range m.DeletedRows {
		totalSize += len(row)
	}
	return uint64(totalSize)
}
