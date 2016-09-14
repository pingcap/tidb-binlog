package pump

import (
	"encoding/json"
	"path"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"golang.org/x/net/context"
)

const machinePrefix = "machine"

// EtcdRegistry wraps the reaction with etcd
type EtcdRegistry struct {
	client     *etcd.Client
	reqTimeout time.Duration
}

// NewEtcdRegistry returns an EtcdRegistry client
func NewEtcdRegistry(client *etcd.Client, reqTimeout time.Duration) *EtcdRegistry {
	return &EtcdRegistry{
		client:     client,
		reqTimeout: reqTimeout,
	}
}

func (r *EtcdRegistry) ctx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), r.reqTimeout)
	return ctx, cancel
}

func (r *EtcdRegistry) prefixed(p ...string) string {
	return path.Join(p...)
}

// Machine returns the machineStatus that matchs machID in the etcd
func (r *EtcdRegistry) Machine(machID string) (*MachineStatus, error) {
	ctx, cancel := r.ctx()
	defer cancel()

	resp, err := r.client.List(ctx, r.prefixed(machinePrefix, machID))
	if err != nil {
		return nil, err
	}

	status, err := machineStatusFromEtcdNode(machID, resp)
	if err != nil {
		return nil, errors.Errorf("Invalid machine node, machID[%s], error[%v]", machID, err)
	}

	return status, nil
}

// RegisterMachine register the machine in the etcd
func (r *EtcdRegistry) RegisterMachine(machID, host string) error {
	if exists, err := r.checkMachineExists(machID); err != nil {
		return errors.Trace(err)
	} else if !exists {
		// not found then create a new machine node
		return r.createMachine(machID, host)
	}

	// found it, update host infomation of the machine
	machStatus := &MachineStatus{
		MachID: machID,
		Host:   host,
	}
	return r.UpdateMeachineStatus(machID, machStatus)
}

func (r *EtcdRegistry) checkMachineExists(machID string) (bool, error) {
	ctx, cancel := r.ctx()
	defer cancel()
	_, err := r.client.Get(ctx, r.prefixed(machinePrefix, machID))
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// UpdateMeachineStatus updates the mach
func (r *EtcdRegistry) UpdateMeachineStatus(machID string, machStatus *MachineStatus) error {
	object, err := marshal(machStatus)
	if err != nil {
		return errors.Errorf("Error marshaling MachineSattus, %v, %v", object, err)
	}

	ctx, cancel := r.ctx()
	defer cancel()
	key := r.prefixed(machinePrefix, machID, "object")
	if err := r.client.Update(ctx, key, object, 0); err != nil {
		return errors.Errorf("Failed to update MachStatus in etcd, %s, %v, %v", machID, object, err)
	}
	return nil
}

func (r *EtcdRegistry) createMachine(machID string, host string) error {
	object := &MachineStatus{
		MachID: machID,
		Host:   host,
	}

	ctx, cancel := r.ctx()
	defer cancel()
	if objstr, err := marshal(object); err == nil {
		if err := r.client.Create(ctx, r.prefixed(machinePrefix, machID, "object"), objstr, nil); err != nil {
			return errors.Errorf("Failed to create MachStatus of machine node, %s, %v, %v", machID, object, err)
		}
	} else {
		return errors.Errorf("Error marshaling MachineStatus, %v, %v", object, err)
	}

	return nil
}

// RefreshMachine keeps the heartbeats with etcd
func (r *EtcdRegistry) RefreshMachine(machID string, ttl int64) error {
	aliveKey := r.prefixed(machinePrefix, machID, "alive")

	// try to touch alive state of machine, update ttl
	ctx, cancel := r.ctx()
	defer cancel()
	if err := r.client.Update(ctx, aliveKey, "", ttl); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func machineStatusFromEtcdNode(machID string, node *etcd.Node) (*MachineStatus, error) {
	status := &MachineStatus{}

	var isAlive bool
	for key, n := range node.Childs {
		switch key {
		case "object":
			if err := unmarshal(n.Value, &status); err != nil {
				log.Errorf("Error unmarshaling MachStatus, machID: %s, %v", machID, err)
				return nil, err
			}
		case "alive":
			isAlive = true
		}
	}

	status.IsAlive = isAlive
	return status, nil
}

func marshal(obj interface{}) (string, error) {
	encoded, err := json.Marshal(obj)
	if err == nil {
		return string(encoded), nil
	}
	return "", errors.Errorf("unable to JSON-serialize object: %s", err)
}

func unmarshal(val []byte, obj interface{}) error {
	err := json.Unmarshal(val, &obj)
	if err == nil {
		return nil
	}
	return errors.Errorf("unable to JSON-deserialize object: %s", err)
}
