package cistern

import (
	"fmt"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb-binlog/pump"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
)

// Collector keeps all connections to pumps, and pulls binlog from each pump server periodically.
// If find any pump server gone away collector should halt processing until recovery.
// Each Prewrite binlog in a batch must be paired up with a Commit or Abort binlog that have same startTS.
// If there are some ones who don't have a girlfriend:), it should request for the next batch after a while,
// and finally abort the txn in TiKV for that single ones.
// After a batch processing is complete, collector will update the savepoint of each pump binlog stored in Etcd.
type Collector struct {
	clusterID uint64
	batch     int32
	interval  time.Duration
	reg       *pump.EtcdRegistry
	pumps     map[string]*Pump
	timeout   time.Duration
	window    *DepositWindow
	boltdb    store.Store
	tiClient  *tikv.LockResolver
	tiStore   kv.Storage
}

// NewCollector returns an instance of Collector
func NewCollector(cfg *Config, s store.Store, w *DepositWindow) (*Collector, error) {
	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tiClient, err := tikv.NewLockResolver(urlv.StringSlice(), cfg.ClusterID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tidb.RegisterStore("tikv", tikv.Driver{})
	tiPath := fmt.Sprintf("tikv://%s?cluster=%d&disableGC=true", urlv.HostString(), cfg.ClusterID)
	tiStore, err := tidb.NewStore(tiPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cli, err := etcd.NewClientFromCfg(urlv.StringSlice(), cfg.EtcdTimeout, etcd.DefaultRootPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Collector{
		clusterID: cfg.ClusterID,
		batch:     int32(cfg.CollectBatch),
		interval:  time.Duration(cfg.CollectInterval) * time.Second,
		reg:       pump.NewEtcdRegistry(cli, cfg.EtcdTimeout),
		pumps:     make(map[string]*Pump),
		timeout:   cfg.PumpTimeout,
		window:    w,
		boltdb:    s,
		tiClient:  tiClient,
		tiStore:   tiStore,
	}, nil
}

// Start run a loop of collecting binlog from pumps online
func (c *Collector) Start(ctx context.Context) {
	defer func() {
		for _, p := range c.pumps {
			p.Close()
		}
		if err := c.reg.Close(); err != nil {
			log.Error(err.Error())
		}
		if err := c.tiStore.Close(); err != nil {
			log.Error(err.Error())
		}
		log.Info("Collector goroutine exited")
	}()

	round := 1
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.interval):
			start := time.Now()
			if err := c.collect(ctx); err != nil {
				log.Errorf("collect error: %v", err)
			}
			elapsed := time.Now().Sub(start)
			log.Debugf("finished collecting at round[%d], elapsed time[%s]", round, elapsed)
			round++
		}
	}
}

func (c *Collector) collect(ctx context.Context) error {
	if err := c.prepare(ctx); err != nil {
		return errors.Trace(err)
	}

	// start to collect binlog from each pump
	resc := make(chan Result)
	var wg sync.WaitGroup
	for _, p := range c.pumps {
		wg.Add(1)
		go func(p *Pump) {
			select {
			case resc <- p.Collect(ctx, c.tiClient):
			case <-ctx.Done():
			}
			wg.Done()
		}(p)
	}
	go func() {
		wg.Wait()
		close(resc)
	}()

	items := make(map[int64]*binlog.Binlog)
	savepoints := make(map[string]binlog.Pos)
	for r := range resc {
		if r.err != nil {
			return errors.Annotatef(r.err, "failed to collect binlog of cluster(%d) from pump node(%s)",
				r.clusterID, r.nodeID)
		}
		for commitTS, item := range r.binlogs {
			items[commitTS] = item
		}

		if ComparePos(r.end, r.begin) > 0 {
			savepoints[r.nodeID] = r.end
		}
	}

	jobs, err := c.grabDDLJobs(ctx, items)
	if err != nil {
		return errors.Trace(err)
	}
	if err := c.storeDDLJobs(jobs); err != nil {
		return errors.Trace(err)
	}
	if err := c.store(items); err != nil {
		return errors.Trace(err)
	}
	if err := c.updateSavepoints(savepoints); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Collector) prepare(ctx context.Context) error {
	nodes, err := c.reg.Nodes(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	online := make(map[string]bool)
	for _, n := range nodes {
		// stop collect if any pump node is offline
		if !n.IsAlive {
			return errors.Errorf("pump with nodeID(%s) is offline, give up this round of processing", n.NodeID)
		}
		_, ok := c.pumps[n.NodeID]
		if !ok {
			pos, err := c.getSavePoints(n.NodeID)
			if err != nil {
				return errors.Trace(err)
			}

			log.Infof("node %s get save point %v", n.NodeID, pos)
			p, err := NewPump(n.NodeID, c.clusterID, n.Host, c.timeout, pos, c.batch, c.interval)
			if err != nil {
				return errors.Trace(err)
			}
			c.pumps[n.NodeID] = p
		}
		online[n.NodeID] = true
	}
	for id, p := range c.pumps {
		if !online[id] {
			// release invalid connection
			p.Close()
			delete(c.pumps, id)
			log.Infof("node(%s) of cluster(%d) on host(%s) has been removed and release the connection to it",
				id, p.clusterID, p.host)
		}
	}
	return nil
}

func (c *Collector) store(items map[int64]*binlog.Binlog) error {
	boundary := c.window.LoadLower()
	b := c.boltdb.NewBatch()

	for commitTS, item := range items {
		if commitTS < boundary {
			log.Errorf("FATAL ERROR: commitTs(%d) of binlog exceeds the lower boundary of window, may miss processing, ITEM(%v)",
				commitTS, item)
		}
		payload, err := item.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		key := codec.EncodeInt([]byte{}, commitTS)
		data, err := encodePayload(payload)
		if err != nil {
			return errors.Trace(err)
		}
		b.Put(key, data)
	}

	err := c.boltdb.Commit(binlogNamespace, b)
	return errors.Trace(err)
}

func (c *Collector) updateSavepoints(savePoints map[string]binlog.Pos) error {
	for id, pos := range savePoints {
		data, err := pos.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		err = c.boltdb.Put(savepointNamespace, []byte(id), data)
		if err != nil {
			return errors.Trace(err)
		}
		if p, ok := c.pumps[id]; ok {
			p.current = pos
		}
	}
	return nil
}

func (c *Collector) getSavePoints(nodeID string) (binlog.Pos, error) {
	var savePoint = binlog.Pos{}
	payload, err := c.boltdb.Get(savepointNamespace, []byte(nodeID))
	if err != nil {
		if errors.IsNotFound(err) {
			return savePoint, nil
		}

		return savePoint, errors.Trace(err)
	}
	if err := savePoint.Unmarshal(payload); err != nil {
		return savePoint, errors.Trace(err)
	}
	return savePoint, nil
}

func (c *Collector) grabDDLJobs(ctx context.Context, items map[int64]*binlog.Binlog) (map[int64]*model.Job, error) {
	res := make(map[int64]*model.Job)
	for _, item := range items {
		if item.DdlJobId > 0 {
			job, err := c.getDDLJob(item.DdlJobId)
			if err != nil {
				return nil, errors.Trace(err)
			}
			for job == nil {
				select {
				case <-ctx.Done():
					return nil, errors.Trace(ctx.Err())
				case <-time.After(c.timeout):
					job, err = c.getDDLJob(item.DdlJobId)
					if err != nil {
						return nil, errors.Trace(err)
					}
				}
			}
			res[item.DdlJobId] = job
		}
	}
	return res, nil
}

func (c *Collector) getDDLJob(id int64) (*model.Job, error) {
	version, err := c.tiStore.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshot, err := c.tiStore.GetSnapshot(version)
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapMeta := meta.NewSnapshotMeta(snapshot)
	job, err := snapMeta.GetHistoryDDLJob(id)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return job, nil
}

func (c *Collector) storeDDLJobs(jobs map[int64]*model.Job) error {
	b := c.boltdb.NewBatch()
	for id, job := range jobs {
		if err := job.DecodeArgs(); err != nil {
			return errors.Trace(err)
		}
		payload, err := job.Encode()
		if err != nil {
			return errors.Trace(err)
		}
		key := codec.EncodeInt([]byte{}, id)
		b.Put(key, payload)
	}
	err := c.boltdb.Commit(ddlJobNamespace, b)
	return errors.Trace(err)
}

// LoadHistoryDDLJobs loads all history DDL jobs from TiDB
func (c *Collector) LoadHistoryDDLJobs() error {
	version, err := c.tiStore.CurrentVersion()
	if err != nil {
		return errors.Trace(err)
	}
	snapshot, err := c.tiStore.GetSnapshot(version)
	if err != nil {
		return errors.Trace(err)
	}
	snapMeta := meta.NewSnapshotMeta(snapshot)
	jobs, err := snapMeta.GetAllHistoryDDLJobs()
	if err != nil {
		return errors.Trace(err)
	}
	for _, job := range jobs {
		key := codec.EncodeInt([]byte{}, job.ID)
		_, err = c.boltdb.Get(ddlJobNamespace, key)
		if err != nil {
			if !errors.IsNotFound(err) {
				return errors.Trace(err)
			}
			if err := job.DecodeArgs(); err != nil {
				return errors.Trace(err)
			}
			payload, err := job.Encode()
			if err != nil {
				return errors.Trace(err)
			}
			if err := c.boltdb.Put(ddlJobNamespace, key, payload); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}
