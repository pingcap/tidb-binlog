package syncer

// execute sql to mysql/tidb

import (
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

// MemSyncer just save the pb.Binlog in memory, for test only
type MemSyncer struct {
	binlogs []*pb.Binlog
}

var _ Syncer = &MemSyncer{}

func newMemSyncer() (*MemSyncer, error) {
	return &MemSyncer{}, nil
}

// Sync implement interface of Syncer
func (m *MemSyncer) Sync(pbBinlog *pb.Binlog, cb func(binlog *pb.Binlog)) error {
	m.binlogs = append(m.binlogs, pbBinlog)
	cb(pbBinlog)

	return nil
}

// Close implement interface of Syncer
func (m *MemSyncer) Close() error {
	return nil
}

// GetBinlogs return binlogs receive
func (m *MemSyncer) GetBinlogs() []*pb.Binlog {
	return m.binlogs
}
