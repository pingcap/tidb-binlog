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

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/pkg/filter"
	"github.com/pingcap/tidb/mysql"
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

	schemaMetaVersion int64

	hasImplicitCol bool

	jobs                []*model.Job
	version2SchemaTable map[int64]TableName
	currentVersion      int64
}

// TableName stores the table and schema name
type TableName = filter.TableName

// NewSchema returns the Schema object
func NewSchema(jobs []*model.Job, hasImplicitCol bool) (*Schema, error) {
	s := &Schema{
		hasImplicitCol:      hasImplicitCol,
		version2SchemaTable: make(map[int64]TableName),
		truncateTableID:     make(map[int64]struct{}),
		jobs:                jobs,
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

	log.Debugf("create schema %s, schema id %d", db.Name.O, db.ID)
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

	log.Debugf("drop table %s, table id %d", table.Name.O, id)
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

	log.Debugf("create table %s.%s, table id %d", schema.Name.O, table.Name.O, table.ID)
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
		if skipJob(s.jobs[i]) {
			log.Debugf("skip ddl job %v", s.jobs[i])
			continue
		}

		if s.jobs[i].BinlogInfo.SchemaVersion <= version {
			if s.jobs[i].BinlogInfo.SchemaVersion <= s.currentVersion {
				log.Warnf("ddl job %v schema version is less than current version %d, skip this ddl job", s.jobs[i], s.currentVersion)
				continue
			}

			data, err := json.Marshal(s.jobs[i])
			if err != nil {
				log.Error(err)
			} else {
				log.Debugf("handle ddl job id(%d): %s", s.jobs[i].ID, string(data))
			}

			_, _, _, err = s.handleDDL(s.jobs[i])
			if err != nil {
				return errors.Annotatef(err, "handle ddl job %v failed, the schema info: %s", s.jobs[i], s)
			}
		} else {
			break
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
		return "", "", "", nil
	}

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

	case model.ActionCreateTable, model.ActionCreateView:
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
	}

	return
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
