package server

import (
	"fmt"
        "path"
        "time"
	"errors"
        "strconv"
	"encoding/json"

        "github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/machine"
        "golang.org/x/net/context"
	"github.com/ngaut/log"
)

const (
	 WindowBoardPrefix = "windowBoard"
	 machinePrefix = "machine"
)
type EtcdRegistry struct {
        client          *etcd.Etcd
        reqTimeout      time.Duration
}

func NewEtcdRegistry(client *etcd.Etcd, reqTimeout time.Duration) *EtcdRegistry {
        return &EtcdRegistry {
                client:         client,
                reqTimeout:     reqTimeout,
        }
}

func (r *EtcdRegistry) ctx() (context.Context, context.CancelFunc) {
        ctx, cancel := context.WithTimeout(context.Background(), r.reqTimeout)
        return ctx, cancel
}

func (r *EtcdRegistry) prefixed(p ...string) string {
        return path.Join(p...)
}

func (r *EtcdRegistry) GetWindowBoard() (int64, error) {
        ctx, cancel := r.ctx()
        defer cancel()
        resp, err := r.client.Get(ctx, WindowBoardPrefix)
        if err != nil {
                if isEtcdError(err, etcd.ErrCodeKeyNotFound) {
                        // not found
                        e := fmt.Sprintf("Window Board not found in etcd, %s", err)
                        log.Error(e)
                        return -1, errors.New(e)
                }
                return -1, err
        }

        board, err := strconv.Atoi(string(resp))
        if err != nil {
                return 0, err
        }

        return int64(board), nil
}

func (r *EtcdRegistry) UpdateWindowBoard(board int64) error {
        ctx, cancel := r.ctx()
        defer cancel()
        boardStr := fmt.Sprintf("%d", board)
        if err := r.client.Update(ctx, WindowBoardPrefix, boardStr, 0); err != nil {
                e := fmt.Sprintf("Failed to update Window Board in etcd %v, %v", WindowBoardPrefix, err)
                log.Error(e)
                return errors.New(e)
        }
        return nil
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

func marshal(obj interface{}) (string, error) {
        encoded, err := json.Marshal(obj)
        if err == nil {
                return string(encoded), nil
        }
        return "", fmt.Errorf("unable to JSON-serialize object: %s", err)
}

func unmarshal(val []byte, obj interface{}) error {
        err := json.Unmarshal(val, &obj)
        if err == nil {
                return nil
        }
        return fmt.Errorf("unable to JSON-deserialize object: %s", err)
}

func isEtcdError(err error, code int) bool {
        eerr, ok := err.(*etcd.Error)
        return ok && eerr.Code == code
}
