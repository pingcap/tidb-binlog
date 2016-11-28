package cistern

import (
	"golang.org/x/net/context"
	grpc "google.golang.org/grpc"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tipb/go-binlog"
)

var furtherPullBinlog bool

type mockPumpClient struct{}

func (p *mockPumpClient) WriteBinlog(ctx context.Context, in *binlog.WriteBinlogReq, opts ...grpc.CallOption) (*binlog.WriteBinlogResp, error) {
	return nil, nil
}

// Obtains a batch of binlog from a given location.
func (p *mockPumpClient) PullBinlogs(ctx context.Context, in *binlog.PullBinlogReq, opts ...grpc.CallOption) (*binlog.PullBinlogResp, error) {
	var ents []binlog.Entity
	if !furtherPullBinlog {
		furtherPullBinlog = true
		bls := []binlog.Binlog{
			{
				Tp:      binlog.BinlogType_Prewrite,
				StartTs: 1,
			}, {
				Tp:       binlog.BinlogType_Commit,
				StartTs:  1,
				CommitTs: 2,
			}, {
				Tp:      binlog.BinlogType_Prewrite,
				StartTs: 2,
			}, {
				Tp:       binlog.BinlogType_Rollback,
				StartTs:  2,
				CommitTs: 3,
			}, {
				Tp:      binlog.BinlogType_Prewrite,
				StartTs: 3,
			}, {
				Tp:      binlog.BinlogType_Prewrite,
				StartTs: 4,
			}, {
				Tp:      binlog.BinlogType(100),
				StartTs: 5,
			},
		}
		for _, bl := range bls {
			rawBl, err := bl.Marshal()
			if err != nil {
				return nil, errors.New("pull binlog error")
			}
			ents = append(ents, binlog.Entity{Pos: binlog.Pos{Suffix: 1, Offset: 1}, Payload: rawBl})
		}
		return &binlog.PullBinlogResp{
			Entities: ents,
		}, nil
	}

	bls := []binlog.Binlog{
		{
			Tp:       binlog.BinlogType_Commit,
			StartTs:  3,
			CommitTs: 4,
		}, {
			Tp:       binlog.BinlogType_Rollback,
			StartTs:  4,
			CommitTs: 5,
		},
	}
	for _, bl := range bls {
		rawBl, err := bl.Marshal()
		if err != nil {
			return nil, errors.New("pull binlog error")
		}
		ents = append(ents, binlog.Entity{Pos: binlog.Pos{Suffix: 1, Offset: 1}, Payload: rawBl})
	}
	return &binlog.PullBinlogResp{
		Entities: ents,
	}, nil
}

func (s *testCisternSuite) TestPump(c *C) {
	p, err := NewPump("test", 1, "127.0.0.1", 0, binlog.Pos{}, 0, 0)
	c.Assert(err, IsNil)
	p.Close()

	// test collect binlog
	p.client = &mockPumpClient{}
	res := p.Collect(context.Background(), nil)
	c.Assert(res.binlogs, HasLen, 2)
	c.Assert(res.err, NotNil)
}
