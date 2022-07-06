// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package drainer

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"

	. "github.com/pingcap/check"
	"github.com/stretchr/testify/require"
)

type taskGroupSuite struct{}

var _ = Suite(&taskGroupSuite{})

/* May only get one log entry
func (s *taskGroupSuite) TestShouldRecoverFromPanic(c *C) {
	var logHook util.LogHook
	logHook.SetUp()
	defer logHook.TearDown()

	var called bool
	var g taskGroup
	g.GoNoPanic("test", func() {
		called = true
		panic("Evil Smile")
	})
	g.Wait()
	c.Assert(called, IsTrue)
	c.Assert(logHook.Entrys, HasLen, 2)
	c.Assert(logHook.Entrys[0].Message, Matches, ".*Recovered.*")
	c.Assert(logHook.Entrys[1].Message, Matches, ".*Exit.*")
}
*/

func TestGetParserFromSQLModeStr(t *testing.T) {
	var (
		DDL1 = `ALTER TABLE tbl ADD COLUMN c1 INT`
		DDL2 = `ALTER TABLE tbl ADD COLUMN 'c1' INT`
		DDL3 = `ALTER TABLE tbl ADD COLUMN "c1" INT`
	)
	p, err := getParserFromSQLModeStr("hahaha")
	require.Error(t, err)
	require.Nil(t, p)

	// no `ANSI_QUOTES`
	p, err = getParserFromSQLModeStr("")
	require.NoError(t, err)
	_, err = p.ParseOneStmt(DDL1, "", "")
	require.NoError(t, err)
	_, err = p.ParseOneStmt(DDL2, "", "")
	require.Error(t, err)
	_, err = p.ParseOneStmt(DDL3, "", "")
	require.Error(t, err)

	// `ANSI_QUOTES`
	p, err = getParserFromSQLModeStr("ANSI_QUOTES")
	require.NoError(t, err)
	_, err = p.ParseOneStmt(DDL1, "", "")
	require.NoError(t, err)
	_, err = p.ParseOneStmt(DDL2, "", "")
	require.Error(t, err)
	_, err = p.ParseOneStmt(DDL3, "", "")
	require.NoError(t, err)
}

func TestCollectDirFiles(t *testing.T) {
	fileNames := []string{"schema.sql", "table.sql"}

	localDir := t.TempDir()
	for _, fileName := range fileNames {
		f, err := os.Create(path.Join(localDir, fileName))
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)
	}
	localRes, err := collectDirFiles(localDir)
	require.NoError(t, err)
	for _, fileName := range fileNames {
		_, ok := localRes[fileName]
		require.True(t, ok)
	}

	// current dir
	pwd, err := os.Getwd()
	require.NoError(t, err)
	tempDir, err := os.MkdirTemp(pwd, "TestCollectDirFiles")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)
	for _, fileName := range fileNames {
		f, err1 := os.Create(path.Join(tempDir, fileName))
		require.NoError(t, err1)
		err1 = f.Close()
		require.NoError(t, err1)
	}
	localRes, err = collectDirFiles("./" + path.Base(tempDir))
	require.NoError(t, err)
	for _, fileName := range fileNames {
		_, ok := localRes[fileName]
		require.True(t, ok)
	}
}

func TestGetDBFromDumpFilename(t *testing.T) {
	db, ok := getDBFromDumpFilename("schema.sql")
	require.False(t, ok)
	require.Len(t, db, 0)

	db, ok = getDBFromDumpFilename("db-schema-create.sql")
	require.True(t, ok)
	require.Equal(t, "db", db)

	db, ok = getDBFromDumpFilename("db.tb.0.sql")
	require.False(t, ok)
	require.Len(t, db, 0)
}

func TestGetTableFromDumpFilename(t *testing.T) {
	db, tb, ok := getTableFromDumpFilename("db-schema-create.sql")
	require.False(t, ok)
	require.Len(t, db, 0)
	require.Len(t, tb, 0)

	db, tb, ok = getTableFromDumpFilename("db.tb-schema.sql")
	require.True(t, ok)
	require.Equal(t, "db", db)
	require.Equal(t, "tb", tb)

	db, tb, ok = getTableFromDumpFilename("db.tb.0.sql")
	require.False(t, ok)
	require.Len(t, db, 0)
	require.Len(t, tb, 0)

	db, tb, ok = getTableFromDumpFilename("db.tb.wrong-schema.sql")
	require.False(t, ok)
	require.Len(t, db, 0)
	require.Len(t, tb, 0)
}

func TestGetStmtFromFile(t *testing.T) {
	var (
		localDir     = t.TempDir()
		dbFilename   = "db-schema-create.sql"
		tbFilename   = "db.tb-schema.sql"
		dbPath       = filepath.Join(localDir, dbFilename)
		tbPath       = filepath.Join(localDir, tbFilename)
		createDbStmt = `/* some comment */;
CREATE DATABASE ` + "`db`" + ` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;`
		dbStmt       = "CREATE DATABASE `db` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"
		createTbStmt = `CREATE TABLE ` + "`tb`" + ` (
	` + "`id`" + ` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`
		tbStmt = `CREATE TABLE ` + "`tb`" + ` (
	` + "`id`" + ` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`
	)

	err := ioutil.WriteFile(dbPath, []byte(createDbStmt), 0644)
	require.NoError(t, err)
	err = ioutil.WriteFile(tbPath, []byte(createTbStmt), 0644)
	require.NoError(t, err)

	stmt, err := getStmtFromFile(dbPath)
	require.NoError(t, err)
	require.Equal(t, dbStmt, stmt)

	stmt, err = getStmtFromFile(tbPath)
	require.NoError(t, err)
	require.Equal(t, tbStmt, stmt)

	stmt, err = getStmtFromFile("wrong")
	require.Error(t, err)
	require.Len(t, stmt, 0)

	err = ioutil.WriteFile(dbPath, []byte(""), 0644)
	require.NoError(t, err)
	stmt, err = getStmtFromFile(dbPath)
	require.EqualError(t, err, "no stmt found")
	require.Len(t, stmt, 0)
}

func TestLoadInfosFromDump(t *testing.T) {
	var (
		localDir     = t.TempDir()
		dbFilename   = "db-schema-create.sql"
		tbFilename   = "db.tb-schema.sql"
		dbPath       = filepath.Join(localDir, dbFilename)
		tbPath       = filepath.Join(localDir, tbFilename)
		createDbStmt = `/* some comment */;
CREATE DATABASE ` + "`db`" + ` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;`
		dbStmt       = "CREATE DATABASE `db` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"
		createTbStmt = `CREATE TABLE ` + "`tb`" + ` (
	` + "`id`" + ` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`
		tbStmt = `CREATE TABLE ` + "`tb`" + ` (
	` + "`id`" + ` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`
	)

	err := ioutil.WriteFile(dbPath, []byte(createDbStmt), 0644)
	require.NoError(t, err)
	err = ioutil.WriteFile(tbPath, []byte(createTbStmt), 0644)
	require.NoError(t, err)

	dbInfos, tbInfos, err := loadInfosFromDump(localDir)
	require.NoError(t, err)
	require.Len(t, dbInfos, 1)
	require.Len(t, tbInfos, 1)
	require.Contains(t, dbInfos, schemaKey{schemaName: "db"})
	require.Contains(t, tbInfos, schemaKey{schemaName: "db", tableName: "tb"})

	dbInfo := dbInfos[schemaKey{schemaName: "db"}]
	tbInfo := tbInfos[schemaKey{schemaName: "db", tableName: "tb"}]
	require.Equal(t, schemaInfo{stmt: dbStmt, id: 1}, dbInfo)
	require.Equal(t, schemaInfo{stmt: tbStmt, id: 1}, tbInfo)
}
