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
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

type schemaSuite struct{}

var _ = Suite(&schemaSuite{})

func (t *schemaSuite) TestSchema(c *C) {
	var jobs []*model.Job
	dbName := model.NewCIStr("Test")
	// db and ignoreDB info
	dbInfo := &model.DBInfo{
		ID:    1,
		Name:  dbName,
		State: model.StatePublic,
	}
	// `createSchema` job
	job := &model.Job{
		ID:         3,
		State:      model.JobStateSynced,
		SchemaID:   1,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{SchemaVersion: 1, DBInfo: dbInfo, FinishedTS: 123},
		Query:      "create database test",
	}
	jobDup := &model.Job{
		ID:         3,
		State:      model.JobStateSynced,
		SchemaID:   1,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{SchemaVersion: 2, DBInfo: dbInfo, FinishedTS: 123},
		Query:      "create database test",
	}
	jobs = append(jobs, job)

	// construct a rollbackdone job
	jobs = append(jobs, &model.Job{ID: 5, State: model.JobStateRollbackDone, BinlogInfo: &model.HistoryInfo{}})

	// reconstruct the local schema
	schema, err := NewSchema(jobs, nil, nil, false)
	c.Assert(err, IsNil)
	err = schema.handlePreviousDDLJobIfNeed(2)
	c.Assert(err, IsNil)

	// test drop schema
	jobs = append(
		jobs,
		&model.Job{
			ID:         6,
			State:      model.JobStateSynced,
			SchemaID:   1,
			Type:       model.ActionDropSchema,
			BinlogInfo: &model.HistoryInfo{SchemaVersion: 3, FinishedTS: 123},
			Query:      "drop database test",
		},
	)
	schema, err = NewSchema(jobs, nil, nil, false)
	c.Assert(err, IsNil)
	err = schema.handlePreviousDDLJobIfNeed(3)
	c.Assert(err, IsNil)

	// test create schema already exist error
	jobs = jobs[:0]
	jobs = append(jobs, job)
	jobs = append(jobs, jobDup)
	schema, err = NewSchema(jobs, nil, nil, false)
	c.Assert(err, IsNil)
	err = schema.handlePreviousDDLJobIfNeed(2)
	c.Log(err)
	c.Assert(errors.IsAlreadyExists(err), IsTrue)

	// test schema drop schema error
	jobs = jobs[:0]
	jobs = append(
		jobs,
		&model.Job{
			ID:         9,
			State:      model.JobStateSynced,
			SchemaID:   1,
			Type:       model.ActionDropSchema,
			BinlogInfo: &model.HistoryInfo{SchemaVersion: 1, FinishedTS: 123},
			Query:      "drop database test",
		},
	)
	schema, err = NewSchema(jobs, nil, nil, false)
	c.Assert(err, IsNil)
	err = schema.handlePreviousDDLJobIfNeed(1)
	c.Assert(errors.IsNotFound(err), IsTrue)
}

func (*schemaSuite) TestTable(c *C) {
	var jobs []*model.Job
	dbName := model.NewCIStr("Test")
	tbName := model.NewCIStr("T")
	colName := model.NewCIStr("A")
	idxName := model.NewCIStr("idx")
	// column info
	colInfo := &model.ColumnInfo{
		ID:        1,
		Name:      colName,
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     model.StatePublic,
	}
	// index info
	idxInfo := &model.IndexInfo{
		Name:  idxName,
		Table: tbName,
		Columns: []*model.IndexColumn{
			{
				Name:   colName,
				Offset: 0,
				Length: 10,
			},
		},
		Unique:  true,
		Primary: true,
		State:   model.StatePublic,
	}
	// table info
	tblInfo := &model.TableInfo{
		ID:    2,
		Name:  tbName,
		State: model.StatePublic,
	}
	// db info
	dbInfo := &model.DBInfo{
		ID:    3,
		Name:  dbName,
		State: model.StatePublic,
	}

	// `createSchema` job
	job := &model.Job{
		ID:         5,
		State:      model.JobStateSynced,
		SchemaID:   3,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{SchemaVersion: 1, DBInfo: dbInfo, FinishedTS: 123},
		Query:      "create database " + dbName.O,
	}
	jobs = append(jobs, job)

	// `createTable` job
	job = &model.Job{
		ID:         6,
		State:      model.JobStateSynced,
		SchemaID:   3,
		TableID:    2,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{SchemaVersion: 2, TableInfo: tblInfo, FinishedTS: 123},
		Query:      "create table " + tbName.O,
	}
	jobs = append(jobs, job)

	// `addColumn` job
	tblInfo.Columns = []*model.ColumnInfo{colInfo}
	job = &model.Job{
		ID:         7,
		State:      model.JobStateSynced,
		SchemaID:   3,
		TableID:    2,
		Type:       model.ActionAddColumn,
		BinlogInfo: &model.HistoryInfo{SchemaVersion: 3, TableInfo: tblInfo, FinishedTS: 123},
		Query:      "alter table " + tbName.O + " add column " + colName.O,
	}
	jobs = append(jobs, job)

	// construct a historical `addIndex` job
	tblInfo.Indices = []*model.IndexInfo{idxInfo}
	job = &model.Job{
		ID:         8,
		State:      model.JobStateSynced,
		SchemaID:   3,
		TableID:    2,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{SchemaVersion: 4, TableInfo: tblInfo, FinishedTS: 123},
		Query:      fmt.Sprintf("alter table %s add index %s(%s)", tbName, idxName, colName),
	}
	jobs = append(jobs, job)

	// reconstruct the local schema
	schema, err := NewSchema(jobs, nil, nil, false)
	c.Assert(err, IsNil)
	err = schema.handlePreviousDDLJobIfNeed(4)
	c.Assert(err, IsNil)

	// check the historical db that constructed above whether in the schema list of local schema
	_, ok := schema.SchemaByID(dbInfo.ID)
	c.Assert(ok, IsTrue)
	// check the historical table that constructed above whether in the table list of local schema
	table, ok := schema.TableByID(tblInfo.ID)
	c.Assert(ok, IsTrue)
	c.Assert(table.Columns, HasLen, 1)
	c.Assert(table.Indices, HasLen, 1)
	// check truncate table
	tblInfo1 := &model.TableInfo{
		ID:    9,
		Name:  tbName,
		State: model.StatePublic,
	}
	jobs = append(
		jobs,
		&model.Job{
			ID:         9,
			State:      model.JobStateSynced,
			SchemaID:   3,
			TableID:    2,
			Type:       model.ActionTruncateTable,
			BinlogInfo: &model.HistoryInfo{SchemaVersion: 5, TableInfo: tblInfo1, FinishedTS: 123},
			Query:      "truncate table " + tbName.O,
		},
	)
	schema1, err := NewSchema(jobs, nil, nil, false)
	c.Assert(err, IsNil)
	err = schema1.handlePreviousDDLJobIfNeed(5)
	c.Assert(err, IsNil)
	_, ok = schema1.TableByID(tblInfo1.ID)
	c.Assert(ok, IsTrue)

	_, ok = schema1.TableByID(2)
	c.Assert(ok, IsFalse)
	// check drop table
	jobs = append(
		jobs,
		&model.Job{
			ID:         9,
			State:      model.JobStateSynced,
			SchemaID:   3,
			TableID:    9,
			Type:       model.ActionDropTable,
			BinlogInfo: &model.HistoryInfo{SchemaVersion: 6, FinishedTS: 123},
			Query:      "drop table " + tbName.O,
		},
	)
	schema2, err := NewSchema(jobs, nil, nil, false)
	c.Assert(err, IsNil)
	err = schema2.handlePreviousDDLJobIfNeed(6)
	c.Assert(err, IsNil)

	_, ok = schema2.TableByID(tblInfo.ID)
	c.Assert(ok, IsFalse)
	// test schemaAndTableName
	_, _, ok = schema1.SchemaAndTableName(9)
	c.Assert(ok, IsTrue)
	// drop schema
	_, err = schema1.DropSchema(3)
	c.Assert(err, IsNil)
	// test schema version
	c.Assert(schema.SchemaMetaVersion(), Equals, int64(0))
}

func (t *schemaSuite) TestHandleDDL(c *C) {
	schema, err := NewSchema(nil, nil, nil, false)
	c.Assert(err, IsNil)
	dbName := model.NewCIStr("Test")
	colName := model.NewCIStr("A")
	tbName := model.NewCIStr("T")
	newTbName := model.NewCIStr("RT")

	// check rollback done job
	job := &model.Job{ID: 1, State: model.JobStateRollbackDone}
	_, _, sql, err := schema.handleDDL(job)
	c.Assert(err, IsNil)
	c.Assert(sql, Equals, "")

	// check job.Query is empty
	job = &model.Job{ID: 1, State: model.JobStateDone}
	_, _, sql, err = schema.handleDDL(job)
	c.Assert(sql, Equals, "")
	c.Assert(err, NotNil, Commentf("should return not found job.Query"))

	// db info
	dbInfo := &model.DBInfo{
		ID:    2,
		Name:  dbName,
		State: model.StatePublic,
	}
	// table Info
	tblInfo := &model.TableInfo{
		ID:    6,
		Name:  tbName,
		State: model.StatePublic,
	}
	// column info
	colInfo := &model.ColumnInfo{
		ID:        8,
		Name:      colName,
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     model.StatePublic,
	}
	tblInfo.Columns = []*model.ColumnInfo{colInfo}

	testCases := []struct {
		name        string
		jobID       int64
		schemaID    int64
		tableID     int64
		jobType     model.ActionType
		binlogInfo  *model.HistoryInfo
		query       string
		resultQuery string
		schemaName  string
		tableName   string
	}{
		{name: "createSchema", jobID: 3, schemaID: 2, tableID: 0, jobType: model.ActionCreateSchema, binlogInfo: &model.HistoryInfo{SchemaVersion: 1, DBInfo: dbInfo, TableInfo: nil, FinishedTS: 123}, query: "create database Test", resultQuery: "create database Test", schemaName: dbInfo.Name.O, tableName: ""},
		{name: "updateSchema", jobID: 4, schemaID: 2, tableID: 0, jobType: model.ActionModifySchemaCharsetAndCollate, binlogInfo: &model.HistoryInfo{SchemaVersion: 8, DBInfo: dbInfo, TableInfo: nil, FinishedTS: 123}, query: "ALTER DATABASE Test CHARACTER SET utf8mb4;", resultQuery: "ALTER DATABASE Test CHARACTER SET utf8mb4;", schemaName: dbInfo.Name.O},
		{name: "createTable", jobID: 7, schemaID: 2, tableID: 6, jobType: model.ActionCreateTable, binlogInfo: &model.HistoryInfo{SchemaVersion: 3, DBInfo: nil, TableInfo: tblInfo, FinishedTS: 123}, query: "create table T(id int);", resultQuery: "create table T(id int);", schemaName: dbInfo.Name.O, tableName: tblInfo.Name.O},
		{name: "addColumn", jobID: 9, schemaID: 2, tableID: 6, jobType: model.ActionAddColumn, binlogInfo: &model.HistoryInfo{SchemaVersion: 4, DBInfo: nil, TableInfo: tblInfo, FinishedTS: 123}, query: "alter table T add a varchar(45);", resultQuery: "alter table T add a varchar(45);", schemaName: dbInfo.Name.O, tableName: tblInfo.Name.O},
		{name: "truncateTable", jobID: 10, schemaID: 2, tableID: 6, jobType: model.ActionTruncateTable, binlogInfo: &model.HistoryInfo{SchemaVersion: 5, DBInfo: nil, TableInfo: tblInfo, FinishedTS: 123}, query: "truncate table T;", resultQuery: "truncate table T;", schemaName: dbInfo.Name.O, tableName: tblInfo.Name.O},
		{name: "renameTable", jobID: 11, schemaID: 2, tableID: 10, jobType: model.ActionRenameTable, binlogInfo: &model.HistoryInfo{SchemaVersion: 6, DBInfo: nil, TableInfo: tblInfo, FinishedTS: 123}, query: "rename table T to RT;", resultQuery: "rename table T to RT;", schemaName: dbInfo.Name.O, tableName: newTbName.O},
		{name: "dropTable", jobID: 12, schemaID: 2, tableID: 12, jobType: model.ActionDropTable, binlogInfo: &model.HistoryInfo{SchemaVersion: 7, DBInfo: nil, TableInfo: nil, FinishedTS: 123}, query: "drop table RT;", resultQuery: "drop table RT;", schemaName: dbInfo.Name.O, tableName: newTbName.O},
		{name: "dropSchema", jobID: 13, schemaID: 2, tableID: 0, jobType: model.ActionDropSchema, binlogInfo: &model.HistoryInfo{SchemaVersion: 8, DBInfo: nil, TableInfo: nil, FinishedTS: 123}, query: "drop database test;", resultQuery: "drop database test;", schemaName: dbInfo.Name.O, tableName: ""},
	}

	for _, testCase := range testCases {
		// prepare for ddl
		switch testCase.name {
		case "addColumn":
			tblInfo.Columns = []*model.ColumnInfo{colInfo}
		case "truncateTable":
			tblInfo.ID = 10
		case "renameTable":
			tblInfo.ID = 12
			tblInfo.Name = newTbName
		}

		job = &model.Job{
			ID:         testCase.jobID,
			State:      model.JobStateDone,
			SchemaID:   testCase.schemaID,
			TableID:    testCase.tableID,
			Type:       testCase.jobType,
			BinlogInfo: testCase.binlogInfo,
			Query:      testCase.query,
		}
		testDoDDLAndCheck(c, schema, job, false, testCase.resultQuery, testCase.schemaName, testCase.tableName)

		// custom check after ddl
		switch testCase.name {
		case "createSchema":
			_, ok := schema.SchemaByID(dbInfo.ID)
			c.Assert(ok, IsTrue)
		case "createTable":
			_, ok := schema.TableByID(tblInfo.ID)
			c.Assert(ok, IsTrue)
		case "renameTable":
			tb, ok := schema.TableByID(tblInfo.ID)
			c.Assert(ok, IsTrue)
			c.Assert(tblInfo.Name, Equals, tb.Name)
		case "addColumn", "truncateTable":
			tb, ok := schema.TableByID(tblInfo.ID)
			c.Assert(ok, IsTrue)
			c.Assert(tb.Columns, HasLen, 1)
		case "dropTable":
			_, ok := schema.TableByID(tblInfo.ID)
			c.Assert(ok, IsFalse)
		case "dropSchema":
			_, ok := schema.SchemaByID(job.SchemaID)
			c.Assert(ok, IsFalse)
		}
	}
}

func (t *schemaSuite) TestAddImplicitColumn(c *C) {
	tbl := model.TableInfo{}

	addImplicitColumn(&tbl)

	c.Assert(tbl.Columns, HasLen, 1)
	c.Assert(tbl.Columns[0].ID, Equals, int64(implicitColID))
	c.Assert(tbl.Indices, HasLen, 1)
	c.Assert(tbl.Indices[0].Primary, IsTrue)
}

func testDoDDLAndCheck(c *C, schema *Schema, job *model.Job, isErr bool, sql string, expectedSchema string, expectedTable string) {
	schemaName, tableName, resSQL, err := schema.handleDDL(job)
	c.Logf("handle: %s", job.Query)
	c.Logf("result: %s, %s, %s, %v", schemaName, tableName, resSQL, err)
	c.Assert(err != nil, Equals, isErr)
	c.Assert(sql, Equals, resSQL)
	c.Assert(schemaName, Equals, expectedSchema)
	c.Assert(tableName, Equals, expectedTable)
}

func TestGetCharsetAndCollateInDatabaseOption(t *testing.T) {
	chs, coll, err := getCharsetAndCollateInDatabaseOption(0, []*ast.DatabaseOption{
		{Tp: ast.DatabaseOptionCharset, Value: "utf8"},
		{Tp: ast.DatabaseOptionCollate, Value: "utf8_bin"},
	})
	require.NoError(t, err)
	require.Equal(t, "utf8", chs)
	require.Equal(t, "utf8_bin", coll)

	chs, coll, err = getCharsetAndCollateInDatabaseOption(0, []*ast.DatabaseOption{
		{Tp: ast.DatabaseOptionCharset, Value: "latin1"},
		{Tp: ast.DatabaseOptionCollate, Value: "latin1_bin"},
	})
	require.NoError(t, err)
	require.Equal(t, "latin1", chs)
	require.Equal(t, "latin1_bin", coll)
}

func TestCreateDBInfo(t *testing.T) {
	stmt := "CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"
	dbInfo, err := createDBInfo(1, stmt)
	require.NoError(t, err)
	require.Equal(t, &model.DBInfo{
		ID:      1,
		Name:    model.NewCIStr("test"),
		Charset: "utf8mb4",
		Collate: "utf8mb4_bin",
		State:   model.StatePublic,
	}, dbInfo)

	stmt = "CREATE DATABASE `mydatabase` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_bin */"
	dbInfo, err = createDBInfo(1, stmt)
	require.NoError(t, err)
	require.Equal(t, &model.DBInfo{
		ID:      1,
		Name:    model.NewCIStr("mydatabase"),
		Charset: "utf8",
		Collate: "utf8_bin",
		State:   model.StatePublic,
	}, dbInfo)

	stmt = "CREATE DATABASE `test` /*!40100 DEFAULT COLLATE utf8mb4_bin */"
	dbInfo, err = createDBInfo(1, stmt)
	require.NoError(t, err)
	require.Equal(t, &model.DBInfo{
		ID:      1,
		Name:    model.NewCIStr("test"),
		Charset: "utf8mb4",
		Collate: "utf8mb4_bin",
		State:   model.StatePublic,
	}, dbInfo)

	stmt = "CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET latin1 */"
	dbInfo, err = createDBInfo(1, stmt)
	require.NoError(t, err)
	require.Equal(t, &model.DBInfo{
		ID:      1,
		Name:    model.NewCIStr("test"),
		Charset: "latin1",
		Collate: "latin1_bin",
		State:   model.StatePublic,
	}, dbInfo)
}

func TestCreateTableInfo(t *testing.T) {
	stmt := `CREATE TABLE ` + "`t1`" + `(
	` + "`id`" + ` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`
	tblInfo, err := createTableInfo(1, stmt)
	require.NoError(t, err)
	require.Equal(t, int64(1), tblInfo.ID)
	require.Equal(t, model.StatePublic, tblInfo.State)
	require.Equal(t, model.NewCIStr("t1"), tblInfo.Name)

	stmt = `CREATE TABLE ` + "`tb`" + ` (
	` + "`c`" + ` int(11) NOT NULL,
	PRIMARY KEY (` + "`c`" + `) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin`
	require.NoError(t, err)
	tblInfo, err = createTableInfo(2, stmt)
	require.NoError(t, err)
	require.Equal(t, int64(2), tblInfo.ID)
	require.Equal(t, model.StatePublic, tblInfo.State)
	require.Equal(t, model.NewCIStr("tb"), tblInfo.Name)
}

func TestMockCreateDatabaseJob(t *testing.T) {
	s, err := NewSchema(nil, nil, nil, false)
	require.NoError(t, err)
	stmt := "CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"

	job, err := s.mockCreateSchemaJob(stmt, 1, 1)
	require.NoError(t, err)
	require.Equal(t, model.ActionCreateSchema, job.Type)
	require.Equal(t, model.JobStateDone, job.State)
	require.Equal(t, stmt, job.Query)
	require.Equal(t, model.NewCIStr("test"), job.BinlogInfo.DBInfo.Name)
	require.Equal(t, int64(1), job.BinlogInfo.SchemaVersion)
	require.Equal(t, int64(1), job.BinlogInfo.DBInfo.ID)
	require.Equal(t, "utf8mb4", job.BinlogInfo.DBInfo.Charset)
	require.Equal(t, "utf8mb4_bin", job.BinlogInfo.DBInfo.Collate)

	stmt = "CREATE DATABASE `db` /*!40100 DEFAULT CHARACTER SET latin1 */"
	job, err = s.mockCreateSchemaJob(stmt, 2, 3)
	require.NoError(t, err)
	require.Equal(t, model.ActionCreateSchema, job.Type)
	require.Equal(t, model.JobStateDone, job.State)
	require.Equal(t, stmt, job.Query)
	require.Equal(t, model.NewCIStr("db"), job.BinlogInfo.DBInfo.Name)
	require.Equal(t, int64(3), job.BinlogInfo.SchemaVersion)
	require.Equal(t, int64(2), job.BinlogInfo.DBInfo.ID)
	require.Equal(t, "latin1", job.BinlogInfo.DBInfo.Charset)
	require.Equal(t, "latin1_bin", job.BinlogInfo.DBInfo.Collate)

	stmt = ""
	job, err = s.mockCreateSchemaJob(stmt, 2, 3)
	require.Error(t, err)
	require.Nil(t, job)
}

func TestMockCreateTableJob(t *testing.T) {
	s, err := NewSchema(nil, nil, nil, false)
	require.NoError(t, err)
	stmt := `CREATE TABLE ` + "`t1`" + `(
		` + "`id`" + ` int(11) DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`

	job, err := s.mockCreateTableJob(stmt, 1, 2, 3)
	require.NoError(t, err)
	require.Equal(t, model.ActionCreateTable, job.Type)
	require.Equal(t, model.JobStateDone, job.State)
	require.Equal(t, stmt, job.Query)
	require.Equal(t, int64(1), job.SchemaID)
	require.Equal(t, model.NewCIStr("t1"), job.BinlogInfo.TableInfo.Name)
	require.Equal(t, int64(3), job.BinlogInfo.SchemaVersion)
	require.Equal(t, int64(2), job.BinlogInfo.TableInfo.ID)
	require.Equal(t, "utf8mb4", job.BinlogInfo.TableInfo.Charset)
	require.Equal(t, "utf8mb4_bin", job.BinlogInfo.TableInfo.Collate)

	stmt = `CREATE TABLE ` + "`tb`" + ` (
		` + "`c`" + ` int(11) NOT NULL,
		PRIMARY KEY (` + "`c`" + `) /*T![clustered_index] CLUSTERED */
	) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin`
	job, err = s.mockCreateTableJob(stmt, 3, 4, 4)
	require.NoError(t, err)
	require.Equal(t, model.ActionCreateTable, job.Type)
	require.Equal(t, model.JobStateDone, job.State)
	require.Equal(t, stmt, job.Query)
	require.Equal(t, int64(3), job.SchemaID)
	require.Equal(t, model.NewCIStr("tb"), job.BinlogInfo.TableInfo.Name)
	require.Equal(t, int64(4), job.BinlogInfo.SchemaVersion)
	require.Equal(t, int64(4), job.BinlogInfo.TableInfo.ID)
	require.Equal(t, "latin1", job.BinlogInfo.TableInfo.Charset)
	require.Equal(t, "latin1_bin", job.BinlogInfo.TableInfo.Collate)

	stmt = ""
	job, err = s.mockCreateTableJob(stmt, 3, 4, 4)
	require.Error(t, err)
	require.Nil(t, job)
}

func TestHandlePreviousSchemasIfNeed(t *testing.T) {
	dbInfos := map[schemaKey]schemaInfo{
		{schemaName: "db1"}: {stmt: "CREATE DATABASE `db1` /*!40100 DEFAULT CHARACTER SET utf8mb4 */", id: 1},
		{schemaName: "db2"}: {stmt: "CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET latin1 */", id: 2},
	}
	tbInfos := map[schemaKey]schemaInfo{
		{
			schemaName: "db1",
			tableName:  "t1",
		}: {
			stmt: `CREATE TABLE ` + "`t1`" + `(
			` + "`id`" + ` int(11) DEFAULT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`,
			id: 3,
		},
		{
			schemaName: "db1",
			tableName:  "t2",
		}: {
			stmt: `CREATE TABLE ` + "`t2`" + `(
				` + "`id`" + ` int(11) DEFAULT NULL
			) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`,
			id: 4,
		},
		{
			schemaName: "db2",
			tableName:  "t1",
		}: {
			stmt: `CREATE TABLE ` + "`t1`" + `(
			` + "`id`" + ` int(11) DEFAULT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;`,
			id: 5,
		},
		{
			schemaName: "db2",
			tableName:  "t2",
		}: {
			stmt: `CREATE TABLE ` + "`t2`" + `(
				` + "`id`" + ` int(11) DEFAULT NULL
			) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`,
			id: 6,
		},
	}

	schema, err := NewSchema(nil, dbInfos, tbInfos, false)
	require.NoError(t, err)

	err = schema.handlePreviousSchemasIfNeed(1000)
	require.NoError(t, err)
	require.Len(t, schema.schemas, 2)
	require.Len(t, schema.tables, 4)
	require.Len(t, schema.version2SchemaTable, 6)
	require.Equal(t, int64(6), schema.currentVersion)

	err = schema.handlePreviousSchemasIfNeed(2)
	require.NoError(t, err)
	require.Len(t, schema.schemas, 2)
	require.Len(t, schema.tables, 4)
	require.Len(t, schema.version2SchemaTable, 6)
	require.Equal(t, int64(6), schema.currentVersion)
}
