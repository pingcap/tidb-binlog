package drainer

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
)

// Schema stores the source TiDB all schema infomations
// schema infomations could be changed by drainer init and ddls apear
type Schema struct {
	schemaNameToID map[string]int64
	tableNameToID  map[tableName]int64

	schemas map[int64]*model.DBInfo
	tables  map[int64]*model.TableInfo

	schemaMetaVersion int64
}

type tableName struct {
	schema string
	table  string
}

// NewSchema returns the Schema object
func NewSchema(store kv.Storage, ts uint64) (*Schema, error) {
	s := &Schema{}

	err := s.SyncTiDBSchema(store, ts)
	if err != nil {
		log.Errorf("schema: sync schema at %d failed - %v", ts, err)
		return nil, errors.Trace(err)
	}

	return s, nil
}

// SyncTiDBSchema syncs the all schema infomations that at ts
func (s *Schema) SyncTiDBSchema(store kv.Storage, ts uint64) error {
	s.schemaNameToID = make(map[string]int64)
	s.tableNameToID = make(map[tableName]int64)
	s.schemas = make(map[int64]*model.DBInfo)
	s.tables = make(map[int64]*model.TableInfo)

	version := kv.NewVersion(ts)
	snapshot, err := store.GetSnapshot(version)
	if err != nil {
		return errors.Trace(err)
	}

	// sync all databases
	snapMeta := meta.NewSnapshotMeta(snapshot)
	dbs, err := snapMeta.ListDatabases()
	if err != nil {
		return errors.Trace(err)
	}

	for _, db := range dbs {
		// sync all tables under the databases
		tables, err := snapMeta.ListTables(db.ID)
		if err != nil {
			return errors.Trace(err)
		}

		db.Tables = tables
		s.schemaNameToID[db.Name.L] = db.ID
		s.schemas[db.ID] = db

		for _, table := range tables {
			s.tableNameToID[tableName{schema: db.Name.L, table: table.Name.L}] = table.ID
			s.tables[table.ID] = table
		}
	}

	schemeMetaVersion, err := snapMeta.GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	s.schemaMetaVersion = schemeMetaVersion
	return nil
}

// SchemaMetaVersion returns the current schemaversion in drainer
func (s *Schema) SchemaMetaVersion() int64 {
	return s.schemaMetaVersion
}

// SchemaByName returns the DBInfo by the schema name
func (s *Schema) SchemaByName(schema model.CIStr) (val *model.DBInfo, ok bool) {
	id, ok := s.schemaNameToID[schema.L]
	if !ok {
		return
	}

	val, ok = s.schemas[id]
	return
}

// TableByName returns the TableInfo by the table name
func (s *Schema) TableByName(schema, table model.CIStr) (t *model.TableInfo, ok bool) {
	id, ok := s.tableNameToID[tableName{schema: schema.L, table: table.L}]
	if !ok {
		return
	}

	t = s.tables[id]
	return
}

// SchemaByID returns the DBInfo by schema id
func (s *Schema) SchemaByID(id int64) (val *model.DBInfo, ok bool) {
	val, ok = s.schemas[id]
	return
}

// TableByID returns the TableInfo by table id
func (s *Schema) TableByID(id int64) (val *model.TableInfo, ok bool) {
	val, ok = s.tables[id]
	return
}

// DropSchema deletes the given DBInfo
func (s *Schema) DropSchema(schema model.CIStr) {
	id, ok := s.schemaNameToID[schema.L]
	if !ok {
		return
	}

	db, _ := s.schemas[id]
	for _, table := range db.Tables {
		tn := tableName{schema: db.Name.L, table: table.Name.L}
		id, ok = s.tableNameToID[tn]
		if !ok {
			continue
		}
		delete(s.tableNameToID, tn)
		delete(s.tables, id)
	}

	delete(s.schemaNameToID, schema.L)
	delete(s.schemas, id)

	return
}

// CreateSchema adds new DBInfo
func (s *Schema) CreateSchema(db *model.DBInfo) error {
	_, ok := s.schemaNameToID[db.Name.L]
	if ok {
		return errors.AlreadyExistsf("schema %s already exists", db.Name)
	}

	s.schemaNameToID[db.Name.L] = db.ID
	s.schemas[db.ID] = db

	return nil
}

// DropTable deletes the given TableInfo
func (s *Schema) DropTable(schema, table model.CIStr) {
	tn := tableName{schema: schema.L, table: table.L}
	id, ok := s.tableNameToID[tn]
	if !ok {
		return
	}

	delete(s.tableNameToID, tn)
	delete(s.tables, id)

	return
}

// Createtable creates new TableInfo
func (s *Schema) Createtable(schema model.CIStr, table *model.TableInfo) error {
	tn := tableName{schema: schema.L, table: table.Name.L}
	_, ok := s.tableNameToID[tn]
	if ok {
		return errors.AlreadyExistsf("table %s.%s already exists", schema, table.Name)
	}

	id, ok := s.schemaNameToID[schema.L]
	if !ok {
		return errors.NotFoundf("schema %s not found", schema)
	}

	s.schemas[id].Tables = append(s.schemas[id].Tables, table)

	s.tableNameToID[tn] = table.ID
	s.tables[table.ID] = table

	return nil
}
