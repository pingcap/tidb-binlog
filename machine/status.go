package machine

import "github.com/pingcap/tidb-binlog/binlog/scheme"

type MachineStatus struct {
	MachID   string
	IsAlive  bool
	MachInfo MachineInfo
}

type MachineInfo struct {
	Host       string
	Offset	   scheme.BinlogOffset
}
