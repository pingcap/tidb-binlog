package cistern

import (
	"fmt"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb-binlog/pump"
	"github.com/pingcap/tidb/_vendor/src/github.com/ngaut/log"
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
	// TODO handle TiKV Client
	pumps   map[string]*Pump
	timeout time.Duration
	window  *DepositWindow
	rocksdb store.Store
}

// NewCollector returns an instance of Collector
func NewCollector(cfg *Config, s store.Store, w *DepositWindow) (*Collector, error) {
	// TODO start a TiKV connection
	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
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
		rocksdb:   s,
	}, nil
}

// Start run a loop of collecting binlog from pumps online
func (c *Collector) Start(ctx context.Context) {
	defer func() {
		// TODO close TiKV connection
		for _, p := range c.pumps {
			p.Close()
		}
		if err := c.reg.Close(); err != nil {
			log.Error(err.Error())
		}
		log.Info("Collect coroutine exited")
	}()

	round := 1
	var clock = clockwork.NewRealClock()
	for {
		select {
		case <-ctx.Done():
			return
		case <-clock.After(c.interval):
			log.Debugf("start to collect binlog at round[%d]", round)
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
			cid := fmt.Sprintf("%d", c.clusterID)
			pos := n.LastReadPos[cid]
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
		}
	}

	// start to collect binlog from each pump
	resc := make(chan result)
	var wg sync.WaitGroup
	for _, p := range c.pumps {
		wg.Add(1)
		go func(p *Pump) {
			p.Collect(ctx, resc)
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
			return errors.Annotatef(err, "failed to collect binlog of cluster(%d) from pump node(%s)",
				r.clusterID, r.nodeID)
		}
		for commitTS, item := range r.binlogs {
			items[commitTS] = item
		}
		if ComparePos(r.end, r.begin) > 0 {
			savepoints[r.nodeID] = r.end
		}
	}

	if err := c.store(items); err != nil {
		return errors.Trace(err)
	}

	if err := c.updateSavepoints(ctx, savepoints); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (c *Collector) store(items map[int64]*binlog.Binlog) error {
	boundary := c.window.LoadLower()
	for commitTS, item := range items {
		if commitTS < boundary {
			log.Errorf("FATAL ERROR: commitTs(%d) of binlog exceeds the lower boundary of window, may miss processing, ITEM(%v)",
				commitTS, item)
		}
		payload, err := item.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		if err := c.rocksdb.Put(commitTS, payload); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *Collector) updateSavepoints(ctx context.Context, savepoints map[string]binlog.Pos) error {
	for id, pos := range savepoints {
		err := c.reg.UpdateSavepoint(ctx, id, c.clusterID, pos)
		if err != nil {
			return errors.Trace(err)
		}
		if p, ok := c.pumps[id]; ok {
			p.current = pos
		}
	}
	return nil
}
