package drainer

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pump"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
)

// GenMetaInfo generates drainer meta from pd
func GenMetaInfo(cfg *Config) error {
	if err1 := os.MkdirAll(cfg.DataDir, 0700); err1 != nil {
		return errors.Trace(err)
	}

	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return errors.Trace(err)
	}
	tidb.RegisterStore("tikv", tikv.Driver{})
	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	tiStore, err := tidb.NewStore(tiPath)
	if err != nil {
		return errors.Trace(err)
	}
	defer tiStore.Close()

	cli, err := etcd.NewClientFromCfg(urlv.StringSlice(), cfg.EtcdTimeout, etcd.DefaultRootPath)
	if err != nil {
		return errors.Trace(err)
	}
	defer cli.Close()

	etcdCli := pump.NewEtcdRegistry(cli, cfg.EtcdTimeout)
	status, err := etcdCli.Nodes(context.Background(), "pumps")
	if err != nil {
		return errors.Trace(err)
	}

	// get all pumps' latest binlog position
	binlogPos := make(map[string]binlog.Pos)
	for _, st := range status {
		seq, err := parseBinlogName(path.Base(st.LatestBinlogFile))
		if err != nil {
			return errors.Trace(err)
		}

		pos := binlog.Pos{}
		if seq > 2 {
			pos.Suffix = seq - 2
		}
		binlogPos[st.NodeID] = pos
	}
	// get newest ts from pd
	version, err := tiStore.CurrentVersion()
	if err != nil {
		return errors.Trace(err)
	}
	commitTS := int64(version.Ver)

	// generate meta infomation
	meta := NewLocalMeta(path.Join(cfg.DataDir, "savePoint"))
	err = meta.Save(commitTS, binlogPos)
	return errors.Trace(err)
}

func parseBinlogName(str string) (index uint64, err error) {
	if !strings.HasPrefix(str, "binlog-") {
		return 0, errors.Errorf("invalid binlog name %s", str)
	}

	_, err = fmt.Sscanf(str, "binlog-%016d", &index)
	return
}
