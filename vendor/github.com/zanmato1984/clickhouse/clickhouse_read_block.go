package clickhouse

import (
	"github.com/zanmato1984/clickhouse/lib/data"
)

func (ch *clickhouse) readBlock() (*data.Block, error) {
	if _, err := ch.decoder.String(); err != nil { // temporary table
		return nil, err
	}

	if ch.compress {

	}
	var block data.Block
	if err := block.Read(&ch.ServerInfo, ch.decoder); err != nil {
		return nil, err
	}
	return &block, nil
}
