package pump

import (
	"encoding/json"
	"net/http"

	"github.com/pingcap/tipb/go-binlog"
)

// HTTPStatus exposes current status of all pumps via HTTP
type HTTPStatus struct {
	LatestBinlog map[string]binlog.Pos `json:"LatestBinlog"`
	CommitTS     int64                 `json:"CommitTS"`
	ErrMsg       string                `json:"ErrMsg"`
}

// Status implements http.ServeHTTP interface
func (s *HTTPStatus) Status(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(s)
}
