package pump

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/tidb-binlog/proto"
)

const (
	shortIDLen    = 8
	machineDir    = ".machine"
	machineIDFile = "machineID"
)

// Machine is a machine interface that has the machine basic infomation quey method
type Machine interface {
	ID() string
	ShortID() string
	MatchID(ID string) bool
	Status() *MachineStatus
}

// MachineStatus has some basic machine status infomations
type MachineStatus struct {
	MachID  string
	Host    string
	IsAlive bool
	Offsets map[string]*pb.Pos
}

type pumpMachine struct {
	id     string
	status *MachineStatus
}

// NewPumpMachine return a pumpMachine obj that inited by server config
func NewPumpMachine(cfg *Config) (Machine, error) {
	machID, err := readLocalMachineID(cfg.MachineMetaDir)
	if err != nil {
		log.Errorf("Read local machine ID error, %v", err)
		return nil, errors.Trace(err)
	}

	mach := &pumpMachine{
		id: machID,
		status: &MachineStatus{
			MachID:  machID,
			Host:    cfg.Host,
			IsAlive: true,
		},
	}
	return mach, nil
}

func (m *pumpMachine) ID() string {
	return m.id
}

func (m *pumpMachine) Host() string {
	return m.status.Host
}

func (m *pumpMachine) ShortID() string {
	if len(m.id) <= shortIDLen {
		return m.id
	}
	return m.id[0:shortIDLen]
}

func (m *pumpMachine) MatchID(ID string) bool {
	return m.id == ID || m.ShortID() == ID
}

func (m *pumpMachine) Status() *MachineStatus {
	return m.status
}
