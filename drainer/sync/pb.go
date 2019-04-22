package sync

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	tb "github.com/pingcap/tipb/go-binlog"
)

var _ Syncer = &pbSyncer{}

type pbSyncer struct {
	binlogger binlogfile.Binlogger

	*baseSyncer
}

func NewPBSyncer(dir string, compression string, tableInfoGetter translator.TableInfoGetter) (*pbSyncer, error) {
	codec := compress.ToCompressionCodec(compression)
	binlogger, err := binlogfile.OpenBinlogger(dir, codec)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s := &pbSyncer{
		binlogger:  binlogger,
		baseSyncer: newBaseSyncer(tableInfoGetter),
	}

	return s, nil
}

func (p *pbSyncer) Sync(item *Item) error {
	pbBinlog, err := translator.TiBinlogToPbBinlog(p.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue)
	if err != nil {
		return errors.Trace(err)
	}

	err = p.saveBinlog(pbBinlog)
	if err != nil {
		return errors.Trace(err)
	}

	p.success <- item

	return nil
}

func (p *pbSyncer) saveBinlog(binlog *pb.Binlog) error {
	data, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	_, err = p.binlogger.WriteTail(&tb.Entity{Payload: data})
	return errors.Trace(err)
}

func (p *pbSyncer) Close() error {
	err := p.binlogger.Close()
	p.setErr(err)
	close(p.success)

	return p.err
}
