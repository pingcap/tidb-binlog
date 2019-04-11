package checkpoint

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"

	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCheckPointSuite{})

type testCheckPointSuite struct{}

func (t *testCheckPointSuite) TestnewMysql(c *C) {
	cfg := new(Config)
	cfg.Db = new(DBConfig)
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if port == 0 {
		port = 3306
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	pass := os.Getenv("MYSQL_PSWD")
	cfg.Db.Host = host
	cfg.Db.Port = port
	cfg.Db.User = user
	cfg.Db.Password = pass
	cfg.ClusterID = 123
	cfg.Schema = "tidb_binlog"
	cfg.Table = "checkpoint"

	// in test cases, we drop the checkpoint schema first
	t.dropMysqlSchema(c, cfg)

	// zero (initial) CommitTs
	sp, err := newMysql("mysql", cfg)
	t.skipRefusedCase(c, err)
	c.Assert(err, IsNil)
	c.Assert(sp.TS(), Equals, int64(0))

	// with InitialCommitTS
	cfg.InitialCommitTS = 123
	sp, err = newMysql("mysql", cfg)
	c.Assert(err, IsNil)
	c.Assert(sp.TS(), Equals, cfg.InitialCommitTS)

	testTs := int64(1)
	err = sp.Save(testTs)
	c.Assert(err, IsNil)

	ts := sp.TS()
	c.Assert(ts, Equals, testTs)

	sp1, ero := newMysql("mysql", cfg)
	c.Assert(ero, IsNil)
	c.Assert(sp1.TS(), Equals, ts) // checkpoint's priority is greater than initial-commit-ts
	err = sp1.Load()
	c.Assert(err, IsNil)
	sp2 := sp1.(*MysqlCheckPoint)
	c.Assert(sp2.CommitTS, Equals, ts)

	// test for Check
	c.Assert(sp1.Check(0), IsFalse)
	sp2.saveTime = sp2.saveTime.Add(-maxSaveTime - time.Second) // hack the `saveTime`
	c.Assert(sp1.Check(0), IsTrue)

	// close the checkpoint
	err = sp.Close()
	c.Assert(err, IsNil)
	c.Assert(errors.Cause(sp.Load()), Equals, ErrCheckPointClosed)
	c.Assert(errors.Cause(sp.Save(0)), Equals, ErrCheckPointClosed)
	c.Assert(sp.Check(0), IsFalse)
	c.Assert(errors.Cause(sp.Close()), Equals, ErrCheckPointClosed)

	// test for TsMap (use MySQL as TiDB)
	spTiDB, err := newMysql("tidb", cfg)
	c.Assert(err, IsNil)
	c.Assert(spTiDB.TS(), Equals, ts) // equal previous save TS
	spTiDB2, ok := spTiDB.(*MysqlCheckPoint)
	c.Assert(ok, IsTrue)
	c.Assert(spTiDB2.TsMap, HasLen, 0)
	spTiDB2.snapshot = spTiDB2.snapshot.Add(-time.Millisecond - time.Second) // hack the `snapshot`
	err = spTiDB.Save(cfg.InitialCommitTS)
	c.Assert(err, IsNil)
	c.Assert(spTiDB2.TsMap, HasLen, 2)
	c.Assert(spTiDB2.TsMap["master-ts"], Equals, spTiDB.TS())
	c.Assert(spTiDB2.TsMap["slave-ts"], Greater, int64(0))
}

func (t *testCheckPointSuite) dropMysqlSchema(c *C, cfg *Config) {
	db, err := pkgsql.OpenDB("mysql", cfg.Db.Host, cfg.Db.Port, cfg.Db.User, cfg.Db.Password)
	c.Assert(err, IsNil)
	defer db.Close()

	query := fmt.Sprintf("DROP SCHEMA IF EXISTS %s", cfg.Schema)
	_, err = db.Exec(query)
	t.skipRefusedCase(c, err)
	c.Assert(err, IsNil)
}

func (t *testCheckPointSuite) skipRefusedCase(c *C, err error) {
	if err != nil && strings.Contains(err.Error(), "connection refused") {
		c.Skip("no mysql available")
	}
}
