package etcd

import (
	"time"
	"path"
	"strings"

	"github.com/ngaut/log"
	"golang.org/x/net/context"
	"github.com/coreos/etcd/clientv3"
)

type Node struct {
	Value  []byte
	Childs map[string] *Node	
}

type Etcd struct {
	client 		*clientv3.Client
	pathPrefix	string
	reqTimeout	time.Duration
	ttl		int64
}

func NewEtcd(client *clientv3.Client, pathPrefix string, reqTimeout time.Duration) *Etcd {
	return &Etcd {
		client:		client,
		pathPrefix:	pathPrefix,
		reqTimeout:	reqTimeout,
	}
}

func (e *Etcd) Create(ctx context.Context, key string, val string, opts []clientv3.OpOption) error {
	key = keyWithPrefix(e.pathPrefix, key)
	txnResp, err := e.client.KV.Txn(ctx).If(
		notFound(key),
	).Then(
		clientv3.OpPut(key, val, opts...),
	).Commit()
	if err != nil {
		return err
	}

	if !txnResp.Succeeded {
		return NewKeyExistsError(key)
	}

	return nil
}

func (e *Etcd) Get(ctx context.Context, key string) ([]byte, error) {
	key = keyWithPrefix(e.pathPrefix, key)
	resp, err := e.client.KV.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, NewKeyNotFoundError(key)
	} 

	return resp.Kvs[0].Value, nil
}

func (e *Etcd) Update(ctx context.Context, key string, val string, ttl int64)  error {
	key = keyWithPrefix(e.pathPrefix, key)
	
	var opts  []clientv3.OpOption
	if  ttl > 0 {
		lcr, err := e.client.Lease.Grant(ctx, ttl)
		if err != nil {
			return err
		}

		opts = []clientv3.OpOption{clientv3.WithLease(clientv3.LeaseID(lcr.ID))}
	}
	
	getResp, err := e.client.KV.Get(ctx, key)
	if err != nil {
		return nil
	}

	originRevision := int64(0)

	if len(getResp.Kvs) == 0 {
		err = e.Create(ctx, key, val, opts)
		if err != nil {
			return err
		}

		return nil
	} else {
		originRevision = getResp.Kvs[0].ModRevision
	}

	for {
		txnResp, err := e.client.KV.Txn(ctx).If(
			clientv3.Compare(clientv3.ModRevision(key) , "=", originRevision),
		).Then(
			clientv3.OpPut(key, val, opts...),
		).Else(
			clientv3.OpGet(key),
		).Commit()
		if err != nil {
			return err
		}

		if !txnResp.Succeeded {
			getResp = (*clientv3.GetResponse)(txnResp.Responses[0].GetResponseRange())
			log.Infof("Update of %s failed because of a conflict, originRevision = %d, want Revision = %d,going to retry", key, originRevision, getResp.Kvs[0].ModRevision)
			originRevision = getResp.Kvs[0].ModRevision
			continue
		}

		break
	}

	return nil
}

func (e *Etcd) List(ctx context.Context, key string) (*Node, error) {
	key = keyWithPrefix(e.pathPrefix, key)
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}

	resp, err := e.client.KV.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	root := new(Node)
	length := len(key)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if len(key) <= length {
			continue
		}
		
		keyTail := key[length:]
		tailNode := parseToDirTree(root, keyTail)
		tailNode.Value = kv.Value
	}

	return root, nil
}

func parseToDirTree(root *Node, path string) *Node  {
	pathDirs := strings.Split(path, "/")
	current := root
	var next *Node 
	var ok bool

        for _, dir :=  range pathDirs {
        	if current.Childs == nil {
                	current.Childs = make(map[string] *Node)
                }

               	next , ok = current.Childs[dir]
		if !ok {
			current.Childs[dir] = new(Node)
			next = current.Childs[dir] 
		}
		
		current = next
       	}
	
	return current
}

func notFound(key string) clientv3.Cmp {
	return clientv3.Compare(clientv3.ModRevision(key), "=", 0)
}

func keyWithPrefix(prefix, key string) string {
	if strings.HasPrefix(key, prefix) {
		return key
	}

	return path.Join(prefix, key)
}
