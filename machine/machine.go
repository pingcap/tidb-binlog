package machine

import (
	"crypto/sha1"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/util"
	"github.com/pingcap/tidb-binlog/binlog/scheme"
)

const (
	shortIDLen    = 8
	machineDir    = ".machine"
	machineIDFile = "machineID"
)

type Machine interface {
	ID() string
	ShortID() string
	MatchID(ID string) bool
	Status() *MachineStatus
}

type machine struct {
	machID     string
	host       string
	offset     *scheme.BinlogOffset
	rwMutex    sync.RWMutex
}

func NewMachineFromConfig(host string) (Machine, error) {
	machID, err := readLocalMachineID()
	if err != nil {
		log.Errorf("Read local machine ID error, %v", err)
		return nil, err
	}

	if len(host) <= 0 {
		if ipaddrs, err := util.IntranetIP(); err != nil {
			return nil, err
		} else {
			log.Debugf("Get local IP addr: %v", ipaddrs)
			if len(ipaddrs) > 0 {
				host = ipaddrs[0]
			}
		}
	}

	mach := &machine{
		machID:     	machID,
		host:   	host,
	}
	return mach, nil
}

// IsLocalMachineID returns whether the given machine ID is equal to that of the local machine
func IsLocalMachineID(mID string) bool {
	m, err := readLocalMachineID()
	return err == nil && m == mID
}

func readLocalMachineID() (string, error) {
	fullPath := filepath.Join(util.GetDataDir(), machineDir, machineIDFile)
	if _, err := util.CheckFileExist(fullPath); err != nil {
		return generateLocalMachineID()
	} else {
		// read the machine ID from file
		hash, err := ioutil.ReadFile(fullPath)
		if err != nil {
			return "", err
		}
		machID := fmt.Sprintf("%X", hash)
		if len(machID) == 0 {
			return generateLocalMachineID()
		}
		return machID, nil
	}
}

// generate a new machine ID, and save it to file
func generateLocalMachineID() (string, error) {
	rand64 := string(util.KRand(64, util.KC_RAND_KIND_ALL))
	log.Debugf("Generated a randomized string with 64 runes, %s", rand64)
	t := sha1.New()
	io.WriteString(t, rand64)
	hash := t.Sum(nil)

	dir := filepath.Join(util.GetDataDir(), machineDir)
	if _, err := os.Stat(dir); err != nil {
		if !os.IsNotExist(err) {
			return "", err
		}
		// dir not exists, make it
		if err := os.Mkdir(dir, os.ModePerm); err != nil {
			return "", err
		}
	}

	file := filepath.Join(dir, machineIDFile)
	if err := ioutil.WriteFile(file, hash, os.ModePerm); err != nil {
		return "", err
	}
	machID := fmt.Sprintf("%X", hash)
	return machID, nil
}

func (m *machine) ID() string {
	return m.machID
}

func (m *machine) ShortID() string {
	if len(m.machID) <= shortIDLen {
		return m.machID
	}
	return m.machID[0:shortIDLen]
}

func (m *machine) MatchID(ID string) bool {
	return m.machID == ID || m.ShortID() == ID
}

func (m *machine) Info() *MachineInfo {
	return &MachineInfo {
		Host:		m.host,
		Offset:		*m.offset,	
		}
}

func (m *machine) Status() *MachineStatus {
	return &MachineStatus{
		MachID:  m.machID,
		IsAlive: true,
		MachInfo: MachineInfo{
			Host:   	m.host,
			Offset:	   	* m.offset,
		},
	}
}
