package reparo

import (
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

// Decode decodes binlog from protobuf content.
// return *pb.Binlog and how many bytes read from reader
func Decode(r io.Reader) (*pb.Binlog, int64, error) {
	payload, length, err := binlogfile.Decode(r)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	binlog := &pb.Binlog{}
	err = binlog.Unmarshal(payload)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return binlog, length, nil
}
