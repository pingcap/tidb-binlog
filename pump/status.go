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

package pump

import (
	"encoding/json"
	"net/http"

	"github.com/pingcap/tidb-binlog/pkg/node"
	pb "github.com/pingcap/tipb/go-binlog"
)

// HTTPStatus exposes current status of all pumps via HTTP
type HTTPStatus struct {
	StatusMap  map[string]*node.Status `json:"status"`
	CommitTS   int64                   `json:"CommitTS"`
	CheckPoint pb.Pos                  `json:"Checkpoint"`
	ErrMsg     string                  `json:"ErrMsg"`
}

// Status implements http.ServeHTTP interface
func (s *HTTPStatus) Status(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(s)
}
