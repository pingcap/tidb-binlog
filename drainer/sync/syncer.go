package sync

import (
	"github.com/pingcap/tidb-binlog/drainer/translator"
	pb "github.com/pingcap/tipb/go-binlog"
)

// Item contains information about binlog
type Item struct {
	Binlog        *pb.Binlog
	PrewriteValue *pb.PrewriteValue // only for DML
	Schema        string
	Table         string
}

// Syncer sync binlog item to downstream
type Syncer interface {
	// Sync the binlog item to downstream
	Sync(item *Item) error
	// will be close if Close normally or meet error, call Err() to check it
	Successes() <-chan *Item
	// Return not nil if fail to sync data to downstream or nil if closed normally
	Error() <-chan error
	// Close the Syncer, no more item can be added by `Sync`
	Close() error
}

type baseSyncer struct {
	*baseError
	success         chan *Item
	tableInfoGetter translator.TableInfoGetter
}

func newBaseSyncer(tableInfoGetter translator.TableInfoGetter) *baseSyncer {
	return &baseSyncer{
		baseError:       newBaseError(),
		success:         make(chan *Item, 1024),
		tableInfoGetter: tableInfoGetter,
	}
}

// Successes implements Syncer interface
func (s *baseSyncer) Successes() <-chan *Item {
	return s.success
}
