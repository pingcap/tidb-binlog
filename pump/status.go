package pump

import (
	"encoding/json"
	"net/http"

	pb "github.com/pingcap/tipb/go-binlog"
)

// HTTPStatus exposes current status of all pumps via HTTP
type HTTPStatus struct {
	CommitTS   int64                 `json:"CommitTS"`
	CheckPoint pb.Pos                `json:"Checkpoint"`
	ErrMsg     string                `json:"ErrMsg"`
}

// Status implements http.ServeHTTP interface
func (s *HTTPStatus) Status(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(s)
}
