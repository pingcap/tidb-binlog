package registry

import (
	"path"
	"time"

	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"golang.org/x/net/context"
)

type EtcdRegistry struct {
	client		*etcd.Etcd
	reqTimeout	time.Duration
}

func NewEtcdRegistry(client *etcd.Etcd, reqTimeout time.Duration) *EtcdRegistry {
	return &EtcdRegistry {
		client:		client,
		reqTimeout:	reqTimeout,
	}
}

func (r *EtcdRegistry) ctx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), r.reqTimeout)
	return ctx, cancel
}

func (r *EtcdRegistry) prefixed(p ...string) string {
	return path.Join(p...)
}

func isEtcdError(err error, code int) bool {
	eerr, ok := err.(*etcd.Error)
	return ok && eerr.Code == code
}
