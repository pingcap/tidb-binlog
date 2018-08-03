package drainer

import (
	"encoding/json"
	"net/http"
)

// HTTPStatus exposes current status of the collector via HTTP
type HTTPStatus struct {
	PumpPos       map[string]int64 `json:"PumpPos"`
	Synced        bool                  `json:"Synced"`
	DepositWindow struct {
		Upper int64 `json:"Upper"`
		Lower int64 `json:"Lower"`
	}
	TsMap string `json:"TsMap"`
}

// Status implements http.ServeHTTP interface
func (s *HTTPStatus) Status(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(s)
}
