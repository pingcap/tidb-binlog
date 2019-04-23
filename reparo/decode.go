// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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
