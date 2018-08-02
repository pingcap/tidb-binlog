package drainer

 import (
 	"github.com/juju/errors"
 	"github.com/pingcap/tidb/kv"
 	"github.com/pingcap/tidb/meta"
 	"github.com/pingcap/tidb/model"
 )

 type jobGetter struct {
 	tiStore kv.Storage
 }

 func newJobGetter(tiStore kv.Storage) *jobGetter {
 	return &jobGetter{
 		tiStore: tiStore,
 	}
 }

 func (g *jobGetter) getDDLJob(id int64) (*model.Job, error) {
 	version, err := g.tiStore.CurrentVersion()
 	if err != nil {
 		return nil, errors.Trace(err)
 	}
 	snapshot, err := g.tiStore.GetSnapshot(version)
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