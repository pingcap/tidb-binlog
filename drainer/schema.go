package drainer

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
)

// Schema stores the source TiDB all schema infomations
// schema infomations could be changed by drainer init and ddls appear
type Schema struct {
	tableIDToName map[int64]tableName

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
	s.tableIDToName = make(map[int64]tableName)
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
		s.schemas[db.ID] = db

		for _, table := range tables {
			s.tables[table.ID] = table
			s.tableIDToName[table.ID] = tableName{schema: db.Name.L, table: table.Name.L}
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

// SchemaAndTableName returns the tableName by table id
func (s *Schema) SchemaAndTableName(id int64) (string, string, bool) {
	tn, ok := s.tableIDToName[id]
	if !ok {
		return "", "", false
	}

	return tn.schema, tn.table, true
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
func (s *Schema) DropSchema(id int64) (string, error) {
	schema, ok := s.schemas[id]
	if !ok {
		return "", errors.NotFoundf("schema %s", schema.Name)
	}

	for _, table := range schema.Tables {
		delete(s.tables, table.ID)
		delete(s.tableIDToName, table.ID)
	}

	delete(s.schemas, id)

	return schema.Name.L, nil
}

// CreateSchema adds new DBInfo
func (s *Schema) CreateSchema(db *model.DBInfo) error {
	if _, ok := s.schemas[db.ID]; ok {
		return errors.AlreadyExistsf("schema %s", db.Name)
	}

	s.schemas[db.ID] = db

	return nil
}

// DropTable deletes the given TableInfo
func (s *Schema) DropTable(id int64) (string, error) {
	table, ok := s.tables[id]
	if !ok {
		return "", errors.NotFoundf("table %s", table.Name)
	}

	delete(s.tables, id)
	delete(s.tableIDToName, id)
	return table.Name.L, nil
}

// Createtable creates new TableInfo
func (s *Schema) Createtable(schema *model.DBInfo, table *model.TableInfo) error {
	_, ok := s.tables[table.ID]
	if ok {
		return errors.AlreadyExistsf("table %s.%s", schema.Name, table.Name)
	}

	schema.Tables = append(schema.Tables, table)
	s.tables[table.ID] = table
	s.tableIDToName[table.ID] = tableName{schema: schema.Name.L, table: table.Name.L}

	return nil
}
