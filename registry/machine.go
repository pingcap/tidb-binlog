package registry

import (
	"path"
	"fmt"
	"errors"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/machine"
)

const machinePrefix = "machine"

type node struct {
	child map[string] *node
}

func (r *EtcdRegistry) Machine(machineID string) (*machine.MachineStatus, error) {
	ctx, cancel := r.ctx()
	defer cancel()
	resp, err := r.client.List(ctx, path.Join(machinePrefix, machineID))
	if err != nil {
		if isEtcdError(err, etcd.ErrCodeKeyNotFound) {
			e := fmt.Sprintf("Machine not found in etcd, machID: %s, %v", machineID, err)
			log.Error(e)
			return nil, errors.New(e)
                }
		return nil, err
	}

	status, err := machineStatusFromEtcdNode(machineID, resp)
	if err != nil || status == nil {
	e := errors.New(fmt.Sprintf("Invalid machine node, machID[%s], error[%v]", machineID, err))
		return nil, e
	}
	return status, nil
}

func (r *EtcdRegistry) Machines() (map[string]*machine.MachineStatus, error) {
	ctx, cancel := r.ctx()
	defer cancel()
	resp, err := r.client.List(ctx, machinePrefix)
	if err != nil {
		if isEtcdError(err, etcd.ErrCodeKeyNotFound) {
			e := errors.New(fmt.Sprintf("%s not found in etcd, cluster may not be properly bootstrapped", machinePrefix))
			return nil, e
                }
		return nil, err
	}
	IDToMachine := make(map[string]*machine.MachineStatus)
	for machID, node := range resp.Childs {
		status, err := machineStatusFromEtcdNode(machID, node)
		if err != nil || status == nil {
			e := errors.New(fmt.Sprintf("Invalid machine node, machID[%s], error[%v]", machID, err))
			return nil, e
		}
		IDToMachine[machID] = status
	}
	return IDToMachine, nil
}

func machineStatusFromEtcdNode(machID string, node *etcd.Node) (*machine.MachineStatus, error) {
	status := &machine.MachineStatus{
		MachID: machID,
	}
	for key, n := range node.Childs  {
		switch key {
		case "object":
			if err := unmarshal(n.Value, &status.MachInfo); err != nil {
				log.Errorf("Error unmarshaling MachInfo, machID: %s, %v", machID, err)
				return nil, err
			}
		case "alive":
			status.IsAlive = true
		}
	}
	return status, nil
}

func (r *EtcdRegistry) RegisterMachine(machID, host string) error {
	if exists, err := r.checkMachineExists(machID); err != nil {
		return err
	} else if !exists {
		// not found then create a new machine node
		return r.createMachine(machID, host)
	}

	// found it, update host infomation of the machine
	machInfo := &machine.MachineInfo{
		Host:   host,
	}
	return r.UpdateMeachineInfo(machID, machInfo)
}

func (r *EtcdRegistry) checkMachineExists(machID string) (bool, error) {
	ctx, cancel := r.ctx()
	defer cancel()
	_, err := r.client.Get(ctx, r.prefixed(machinePrefix, machID))
	if err != nil {
		if isEtcdError(err, etcd.ErrCodeKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (r *EtcdRegistry) UpdateMeachineInfo(machID string, machInfo *machine.MachineInfo) error {
	object, err := marshal(machInfo)
	if err != nil {
		e := fmt.Sprintf("Error marshaling MachineInfo, %v, %v", object, err)
		log.Errorf(e)
		return errors.New(e)
	}

	ctx, cancel := r.ctx()
	defer cancel()
	key := r.prefixed(machinePrefix, machID, "object")
	if err := r.client.Update(ctx, key, object,  0); err != nil {
		e := fmt.Sprintf("Failed to update MachInfo in etcd, %s, %v, %v", machID, object, err)
		log.Error(e)
		return errors.New(e)
	}
	return nil
}

func (r *EtcdRegistry) createMachine(machID string, host string) error {
	object := &machine.MachineInfo{
		Host:   host,
	}

	ctx, cancel := r.ctx()
        defer cancel()	
	if objstr, err := marshal(object); err == nil {
		if err := r.client.Create(ctx, r.prefixed(machinePrefix, machID, "object"), objstr, nil); err != nil {
			e := fmt.Sprintf("Failed to create MachInfo of machine node, %s, %v, %v", machID, object, err)
			log.Error(e)
			return errors.New(e)
		}
	} else {
		e := fmt.Sprintf("Error marshaling MachineInfo, %v, %v", object, err)
		log.Errorf(e)
		return errors.New(e)
	}

	return nil
}

func (r *EtcdRegistry) RefreshMachine(machID string,  ttl int64) error {
	if err := r.refreshMachineAlive(machID, ttl); err != nil {
		return nil
	}
	return nil
}

func (r *EtcdRegistry) refreshMachineAlive(machID string, ttl int64) error {
	aliveKey := r.prefixed(machinePrefix, machID, "alive")
	// try to touch alive state of machine, update ttl
	ctx, cancel := r.ctx()
	defer cancel()
	if err := r.client.Update(ctx, aliveKey, "", ttl); err != nil {
		return err
	}
	return nil
}
