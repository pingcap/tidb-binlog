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
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-binlog/pkg/filter"
	"github.com/pingcap/tidb/ddl"
	_ "github.com/pingcap/tidb/planner/core" // init expression.EvalAstExpr
	"go.uber.org/zap"
)

const implicitColName = "_tidb_rowid"
const implicitColID = -1

// Schema stores the source TiDB all schema infomations
// schema infomations could be changed by drainer init and ddls appear
type Schema struct {
	tableIDToName  map[int64]TableName
	schemaNameToID map[string]int64

	schemas map[int64]*model.DBInfo
	tables  map[int64]*model.TableInfo

	truncateTableID map[int64]struct{}
	tblsDroppingCol map[int64]bool

	schemaMetaVersion int64

	hasImplicitCol bool

	jobs                []*model.Job
	dbInfos             map[schemaKey]schemaInfo
	tbInfos             map[schemaKey]schemaInfo
	version2SchemaTable map[int64]TableName
	currentVersion      int64
}

// TableName stores the table and schema name
type TableName = filter.TableName

// NewSchema returns the Schema object
func NewSchema(jobs []*model.Job, dbInfos map[schemaKey]schemaInfo, tbInfos map[schemaKey]schemaInfo, hasImplicitCol bool) (*Schema, error) {
	s := &Schema{
		hasImplicitCol:      hasImplicitCol,
		version2SchemaTable: make(map[int64]TableName),
		truncateTableID:     make(map[int64]struct{}),
		tblsDroppingCol:     make(map[int64]bool),
		jobs:                jobs,
		dbInfos:             dbInfos,
		tbInfos:             tbInfos,
	}

	s.tableIDToName = make(map[int64]TableName)
	s.schemas = make(map[int64]*model.DBInfo)
	s.schemaNameToID = make(map[string]int64)
	s.tables = make(map[int64]*model.TableInfo)

	return s, nil
}

func (s *Schema) String() string {
	mp := map[string]interface{}{
		"tableIDToName":  s.tableIDToName,
		"schemaNameToID": s.schemaNameToID,
		// "schemas":           s.schemas,
		// "tables":            s.tables,
		"schemaMetaVersion": s.schemaMetaVersion,
		"hasImplicitCol":    s.hasImplicitCol,
	}

	data, _ := json.MarshalIndent(mp, "\t", "\t")

	return string(data)
}

// SchemaMetaVersion returns the current schemaversion in drainer
func (s *Schema) SchemaMetaVersion() int64 {
	return s.schemaMetaVersion
}

// SchemaAndTableName returns the tableName by table id
func (s *Schema) SchemaAndTableName(id int64) (string, string, bool) {
	tn, ok := s.tableIDToName[id]
	if !ok {
		return "", "", false
	}

	return tn.Schema, tn.Table, true
}

// SchemaByID returns the DBInfo by schema id
func (s *Schema) SchemaByID(id int64) (val *model.DBInfo, ok bool) {
	val, ok = s.schemas[id]
	return
}

// SchemaByTableID returns the schema ID by table ID
func (s *Schema) SchemaByTableID(tableID int64) (*model.DBInfo, bool) {
	tn, ok := s.tableIDToName[tableID]
	if !ok {
		return nil, false
	}
	schemaID, ok := s.schemaNameToID[tn.Schema]
	if !ok {
		return nil, false
	}
	return s.SchemaByID(schemaID)
}

// TableByID returns the TableInfo by table id
func (s *Schema) TableByID(id int64) (val *model.TableInfo, ok bool) {
	val, ok = s.tables[id]
	return
}

// DropSchema deletes the given DBInfo
func (s *Schema) DropSchema(id int64) (string, error) {
	schema, ok := s.schemas[id]
	if !ok {
		return "", errors.NotFoundf("schema %d", id)
	}

	for _, table := range schema.Tables {
		delete(s.tables, table.ID)
		delete(s.tableIDToName, table.ID)
	}

	delete(s.schemas, id)
	delete(s.schemaNameToID, schema.Name.O)

	return schema.Name.O, nil
}

// CreateSchema adds new DBInfo
func (s *Schema) CreateSchema(db *model.DBInfo) error {
	if _, ok := s.schemas[db.ID]; ok {
		return errors.AlreadyExistsf("schema %s(%d)", db.Name, db.ID)
	}

	s.schemas[db.ID] = db
	s.schemaNameToID[db.Name.O] = db.ID

	log.Debug("create schema failed, schema id", zap.String("name", db.Name.O), zap.Int64("id", db.ID))
	return nil
}

// DropTable deletes the given TableInfo
func (s *Schema) DropTable(id int64) (string, error) {
	table, ok := s.tables[id]
	if !ok {
		return "", errors.NotFoundf("table %d", id)
	}
	err := s.removeTable(id)
	if err != nil {
		return "", errors.Trace(err)
	}

	delete(s.tables, id)
	delete(s.tableIDToName, id)

	log.Debug("drop table success", zap.String("name", table.Name.O), zap.Int64("id", id))
	return table.Name.O, nil
}

// CreateTable creates new TableInfo
func (s *Schema) CreateTable(schema *model.DBInfo, table *model.TableInfo) error {
	_, ok := s.tables[table.ID]
	if ok {
		return errors.AlreadyExistsf("table %s.%s", schema.Name, table.Name)
	}

	if s.hasImplicitCol && !table.PKIsHandle {
		addImplicitColumn(table)
	}

	schema.Tables = append(schema.Tables, table)
	s.tables[table.ID] = table
	s.tableIDToName[table.ID] = TableName{Schema: schema.Name.O, Table: table.Name.O}

	log.Debug("create table success", zap.String("name", schema.Name.O+"."+table.Name.O), zap.Int64("id", table.ID))
	return nil
}

// ReplaceTable replace the table by new tableInfo
func (s *Schema) ReplaceTable(table *model.TableInfo) error {
	_, ok := s.tables[table.ID]
	if !ok {
		return errors.NotFoundf("table %s(%d)", table.Name, table.ID)
	}

	if s.hasImplicitCol && !table.PKIsHandle {
		addImplicitColumn(table)
	}

	s.tables[table.ID] = table

	return nil
}

func (s *Schema) removeTable(tableID int64) error {
	schema, ok := s.SchemaByTableID(tableID)
	if !ok {
		return errors.NotFoundf("table(%d)'s schema", tableID)
	}

	for i := range schema.Tables {
		if schema.Tables[i].ID == tableID {
			copy(schema.Tables[i:], schema.Tables[i+1:])
			schema.Tables = schema.Tables[:len(schema.Tables)-1]
			return nil
		}
	}
	return nil
}

func (s *Schema) addJob(job *model.Job) {
	if len(s.jobs) == 0 || s.jobs[len(s.jobs)-1].BinlogInfo.SchemaVersion < job.BinlogInfo.SchemaVersion {
		s.jobs = append(s.jobs, job)
	}
}

func (s *Schema) handlePreviousDDLJobIfNeed(version int64) error {
	var i int
	for i = 0; i < len(s.jobs); i++ {
		job := s.jobs[i]

		if job.BinlogInfo.SchemaVersion > version {
			break
		}

		if job.BinlogInfo.SchemaVersion <= s.currentVersion {
			log.Warn("ddl job schema version is less than current version, skip this ddl job",
				zap.Stringer("job", job),
				zap.Int64("currentVersion", s.currentVersion))
			continue
		}

		if job.SchemaState == model.StateDeleteOnly && job.Type == model.ActionDropColumn {
			s.tblsDroppingCol[job.TableID] = true
			log.Info("Got DeleteOnly Job", zap.Stringer("job", job))
			continue
		}
		_, _, _, err := s.handleDDL(job)
		if err != nil {
			return errors.Annotatef(err, "handle ddl job %v failed, the schema info: %s", s.jobs[i], s)
		}
	}

	s.jobs = s.jobs[i:]

	return nil
}

// handleDDL has four return values,
// the first value[string]: the schema name
// the second value[string]: the table name
// the third value[string]: the sql that is corresponding to the job
// the fourth value[error]: the handleDDL execution's err
func (s *Schema) handleDDL(job *model.Job) (schemaName string, tableName string, sql string, err error) {
	if skipJob(job) {
		log.Debug("Skip job", zap.Stringer("job", job))
		return "", "", "", nil
	}

	log.Debug("Handle job", zap.Stringer("job", job))

	sql = job.Query
	if sql == "" {
		return "", "", "", errors.Errorf("[ddl job sql miss]%+v", job)
	}

	switch job.Type {
	case model.ActionCreateSchema:
		// get the DBInfo from job rawArgs
		schema := job.BinlogInfo.DBInfo

		err := s.CreateSchema(schema)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schema.Name.O, Table: ""}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = schema.Name.O

	case model.ActionModifySchemaCharsetAndCollate:
		db := job.BinlogInfo.DBInfo
		if _, ok := s.schemas[db.ID]; !ok {
			return "", "", "", errors.NotFoundf("schema %s(%d)", db.Name, db.ID)
		}

		s.schemas[db.ID] = db
		s.schemaNameToID[db.Name.O] = db.ID
		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: db.Name.O, Table: ""}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = db.Name.O

	case model.ActionDropSchema:
		schemaName, err = s.DropSchema(job.SchemaID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schemaName, Table: ""}
		s.currentVersion = job.BinlogInfo.SchemaVersion

	case model.ActionRenameTable:
		// ignore schema doesn't support reanme ddl
		_, ok := s.SchemaByTableID(job.TableID)
		if !ok {
			return "", "", "", errors.NotFoundf("table(%d) or it's schema", job.TableID)
		}
		// first drop the table
		_, err := s.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}
		// create table
		table := job.BinlogInfo.TableInfo
		schema, ok := s.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err = s.CreateTable(schema, table)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schema.Name.O, Table: table.Name.O}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = schema.Name.O
		tableName = table.Name.O

	case model.ActionCreateTable, model.ActionCreateView, model.ActionRecoverTable:
		table := job.BinlogInfo.TableInfo
		if table == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}

		schema, ok := s.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err := s.CreateTable(schema, table)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schema.Name.O, Table: table.Name.O}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = schema.Name.O
		tableName = table.Name.O

	case model.ActionDropTable, model.ActionDropView:
		schema, ok := s.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		tableName, err = s.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schema.Name.O, Table: tableName}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = schema.Name.O

	case model.ActionTruncateTable:
		schema, ok := s.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		// job.TableID is the old table id, different from table.ID
		_, err := s.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		table := job.BinlogInfo.TableInfo
		if table == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}

		err = s.CreateTable(schema, table)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schema.Name.O, Table: table.Name.O}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = schema.Name.O
		tableName = table.Name.O
		s.truncateTableID[job.TableID] = struct{}{}

	default:
		binlogInfo := job.BinlogInfo
		if binlogInfo == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}
		tbInfo := binlogInfo.TableInfo
		if tbInfo == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}

		schema, ok := s.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err := s.ReplaceTable(tbInfo)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schema.Name.O, Table: tbInfo.Name.O}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = schema.Name.O
		tableName = tbInfo.Name.O

		if job.Type == model.ActionDropColumn {
			log.Info("Finished dropping column", zap.Stringer("job", job))
			delete(s.tblsDroppingCol, job.TableID)
		}
	}

	return
}

// IsDroppingColumn returns true if the table is in the middle of dropping a column
func (s *Schema) IsDroppingColumn(id int64) bool {
	return s.tblsDroppingCol[id]
}

// IsTruncateTableID returns true if the table id have been truncated by truncate table DDL
func (s *Schema) IsTruncateTableID(id int64) bool {
	_, ok := s.truncateTableID[id]
	return ok
}

func (s *Schema) getSchemaTableAndDelete(version int64) (string, string, error) {
	schemaTable, ok := s.version2SchemaTable[version]
	if !ok {
		return "", "", errors.NotFoundf("version: %d", version)
	}
	delete(s.version2SchemaTable, version)

	return schemaTable.Schema, schemaTable.Table, nil
}

func addImplicitColumn(table *model.TableInfo) {
	newColumn := &model.ColumnInfo{
		ID:   implicitColID,
		Name: model.NewCIStr(implicitColName),
	}
	newColumn.Tp = mysql.TypeInt24
	table.Columns = append(table.Columns, newColumn)

	newIndex := &model.IndexInfo{
		Primary: true,
		Columns: []*model.IndexColumn{{Name: model.NewCIStr(implicitColName)}},
	}
	table.Indices = []*model.IndexInfo{newIndex}
}

// TiDB write DDL Binlog for every DDL Job, we must ignore jobs that are cancelled or rollback
// For older version TiDB, it write DDL Binlog in the txn that the state of job is changed to *synced*
// Now, it write DDL Binlog in the txn that the state of job is changed to *done* (before change to *synced*)
// At state *done*, it will be always and only changed to *synced*.
func skipJob(job *model.Job) bool {
	return !job.IsSynced() && !job.IsDone()
}

// learn from handlePreviousDDLJobIfNeed
func (s *Schema) handlePreviousSchemasIfNeed(version int64) error {
	// start from 1 is ok?
	v := int64(1)
	for key, info := range s.dbInfos {
		if v > version {
			return nil
		}
		if v <= s.currentVersion {
			log.Warn("schema version is less than current version, skip this schema",
				zap.String("db", key.schemaName),
				zap.String("stmt", info.stmt),
				zap.Int64("id", info.id),
				zap.Int64("currentVersion", s.currentVersion))
			continue
		}
		if job, err := s.mockCreateSchemaJob(info.stmt, info.id, v); err != nil {
			log.Error("fail to handle create schema", zap.Error(err))
			return err
		} else if _, _, _, err = s.handleDDL(job); err != nil {
			return err
		}
		v++
	}

	for key, info := range s.tbInfos {
		if v > version {
			return nil
		}
		if v <= s.currentVersion {
			log.Warn("schema version is less than current version, skip this schema",
				zap.String("db", key.schemaName),
				zap.String("tb", key.tableName),
				zap.String("stmt", info.stmt),
				zap.Int64("id", info.id),
				zap.Int64("currentVersion", s.currentVersion))
			continue
		}
		dbInfo, ok := s.dbInfos[schemaKey{schemaName: key.schemaName}]
		if !ok {
			return errors.Errorf("schema %s not found", key.schemaName)
		}
		if job, err := s.mockCreateTableJob(info.stmt, dbInfo.id, info.id, v); err != nil {
			log.Error("fail to handle create table", zap.Any("table", key), zap.Error(err))
			return err
		} else if _, _, _, err = s.handleDDL(job); err != nil {
			return err
		}
		v++
	}
	// safe for gc
	s.dbInfos = nil
	s.tbInfos = nil
	return nil
}

// learn from https://github.com/pingcap/tidb/blob/7f2b1359a4829b571b1e7714088aebc216f6455a/ddl/ddl_api.go#L1839
func getCharsetAndCollateInDatabaseOption(startIdx int, options []*ast.DatabaseOption) (chs, coll string, err error) {
	for i := startIdx; i < len(options); i++ {
		opt := options[i]
		// we set the charset to the last option. example: alter table t charset latin1 charset utf8 collate utf8_bin;
		// the charset will be utf8, collate will be utf8_bin
		switch opt.Tp {
		case ast.DatabaseOptionCharset:
			name, defaultCollation, err := charset.GetCharsetInfo(opt.Value)
			if err != nil {
				return "", "", err
			}
			if len(chs) == 0 {
				chs = name
			} else if chs != name {
				return "", "", errors.Errorf("conflicting declarations, lhs: %s, rhs %s", chs, name)
			}
			if len(coll) == 0 {
				coll = defaultCollation
			}
		case ast.DatabaseOptionCollate:
			info, err := charset.GetCollationByName(opt.Value)
			if err != nil {
				return "", "", err
			}
			if len(chs) == 0 {
				chs = info.CharsetName
			} else if chs != info.CharsetName {
				return "", "", errors.Errorf("collation charset mismatch, lhs: %s, rhs: %s", info.Name, chs)
			}
			coll = info.Name
		}
	}
	return
}

func createDBInfo(schemaID int64, stmt string) (*model.DBInfo, error) {
	p, err := getParserFromSQLModeStr(mysql.DefaultSQLMode)
	if err != nil {
		return nil, err
	}
	stmtNode, err := p.ParseOneStmt(stmt, "", "")
	if err != nil {
		return nil, err
	}
	createStmt, ok := stmtNode.(*ast.CreateDatabaseStmt)
	if !ok {
		return nil, errors.Errorf("%s is not a create database statement", stmt)
	}
	chs, col, err := getCharsetAndCollateInDatabaseOption(0, createStmt.Options)
	if err != nil {
		log.L().Error("get charset and collate error", zap.Error(err))
		return nil, err
	}
	return &model.DBInfo{
		ID:      schemaID,
		Name:    model.NewCIStr(createStmt.Name),
		State:   model.StatePublic,
		Charset: chs,
		Collate: col,
	}, nil
}

func createTableInfo(tableID int64, stmt string) (*model.TableInfo, error) {
	p, err := getParserFromSQLModeStr(mysql.DefaultSQLMode)
	if err != nil {
		return nil, err
	}
	stmtNode, err := p.ParseOneStmt(stmt, "", "")
	if err != nil {
		return nil, err
	}
	createStmt, ok := stmtNode.(*ast.CreateTableStmt)
	if !ok {
		return nil, errors.Errorf("%s is not a create table statement", stmt)
	}
	ti, err := ddl.BuildTableInfoFromAST(createStmt)
	if err != nil {
		return nil, err
	}
	ti.State = model.StatePublic
	ti.ID = tableID
	return ti, nil
}

func (s *Schema) mockCreateSchemaJob(stmt string, schemaID, schemaVersion int64) (*model.Job, error) {
	dbInfo, err := createDBInfo(schemaID, stmt)
	if err != nil {
		return nil, err
	}
	return &model.Job{
		Type:  model.ActionCreateSchema,
		State: model.JobStateDone,
		Query: stmt,
		BinlogInfo: &model.HistoryInfo{
			SchemaVersion: schemaVersion,
			DBInfo:        dbInfo,
		},
	}, nil
}

func (s *Schema) mockCreateTableJob(stmt string, schemaID, tableID, schemaVersion int64) (*model.Job, error) {
	tableInfo, err := createTableInfo(tableID, stmt)
	if err != nil {
		return nil, err
	}
	return &model.Job{
		Type:     model.ActionCreateTable,
		State:    model.JobStateDone,
		Query:    stmt,
		SchemaID: schemaID,
		BinlogInfo: &model.HistoryInfo{
			SchemaVersion: schemaVersion,
			TableInfo:     tableInfo,
		},
	}, nil
}
