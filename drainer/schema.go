package drainer

import (
	"encoding/json"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/model"
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

	ignoreSchema map[int64]struct{}

	schemaMetaVersion int64

	hasImplicitCol bool
}

// TableName stores the table and schema name
type TableName struct {
	Schema string `toml:"db-name" json:"db-name"`
	Table  string `toml:"tbl-name" json:"tbl-name"`
}

// NewSchema returns the Schema object
func NewSchema(jobs []*model.Job, ignoreSchemaNames map[string]struct{}, hasImplicitCol bool) (*Schema, error) {
	s := &Schema{
		hasImplicitCol: hasImplicitCol,
	}

	err := s.reconstructSchema(jobs, ignoreSchemaNames)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// log.Infof("[local schema/table] %v", s.tableIDToName)
	// log.Infof("[local schema] %v", s.schemas)
	// log.Infof("[ignore schema] %v", s.ignoreSchema)

	return s, nil
}

func (s *Schema) String() string {
	mp := map[string]interface{}{
		"tableIDToName":     s.tableIDToName,
		"schemas":           s.schemas,
		"tables":            s.tables,
		"ignoreSchema":      s.ignoreSchema,
		"schemaMetaVersion": s.schemaMetaVersion,
		"hasImplicitCol":    s.hasImplicitCol,
	}

	data, _ := json.MarshalIndent(mp, "\t", "\t")

	return string(data)
}

// reconstructSchema reconstruct the schema infomations by history jobs
func (s *Schema) reconstructSchema(jobs []*model.Job, ignoreSchemaNames map[string]struct{}) error {
	s.tableIDToName = make(map[int64]TableName)
	s.schemas = make(map[int64]*model.DBInfo)
	s.schemaNameToID = make(map[string]int64)
	s.tables = make(map[int64]*model.TableInfo)
	s.ignoreSchema = make(map[int64]struct{})

	for _, job := range jobs {
		if job.State == model.JobStateCancelled {
			continue
		}

		switch job.Type {
		case model.ActionCreateSchema:
			schema := job.BinlogInfo.DBInfo
			if filterIgnoreSchema(schema, ignoreSchemaNames) {
				s.AddIgnoreSchema(schema)
				continue
			}

			err := s.CreateSchema(schema)
			if err != nil {
				return errors.Trace(err)
			}

		case model.ActionDropSchema:
			_, ok := s.IgnoreSchemaByID(job.SchemaID)
			if ok {
				s.DropIgnoreSchema(job.SchemaID)
				continue
			}

			_, err := s.DropSchema(job.SchemaID)
			if err != nil {
				return errors.Trace(err)
			}

		case model.ActionRenameTable:
			_, ok := s.SchemaByTableID(job.TableID)
			if !ok {
				return errors.NotFoundf("table(%d) or it's schema", job.TableID)
			}
			_, ok = s.IgnoreSchemaByID(job.SchemaID)
			if ok {
				return errors.Errorf("ignore schema %d don't support rename ddl sql %s", job.SchemaID, job.Query)
			}
			// first drop the table
			_, err := s.DropTable(job.TableID)
			if err != nil {
				return errors.Trace(err)
			}
			// create table
			table := job.BinlogInfo.TableInfo
			schema, ok := s.SchemaByID(job.SchemaID)
			if !ok {
				return errors.NotFoundf("schema %d", job.SchemaID)
			}

			err = s.CreateTable(schema, table)
			if err != nil {
				return errors.Trace(err)
			}

		case model.ActionCreateTable:
			table := job.BinlogInfo.TableInfo
			_, ok := s.IgnoreSchemaByID(job.SchemaID)
			if ok {
				continue
			}

			schema, ok := s.SchemaByID(job.SchemaID)
			if !ok {
				return errors.NotFoundf("schema %d", job.SchemaID)
			}

			err := s.CreateTable(schema, table)
			if err != nil {
				return errors.Trace(err)
			}

		case model.ActionDropTable:
			_, ok := s.IgnoreSchemaByID(job.SchemaID)
			if ok {
				continue
			}

			_, ok = s.SchemaByID(job.SchemaID)
			if !ok {
				return errors.NotFoundf("schema %d", job.SchemaID)
			}

			_, err := s.DropTable(job.TableID)
			if err != nil {
				return errors.Trace(err)
			}

		case model.ActionTruncateTable:
			_, ok := s.IgnoreSchemaByID(job.SchemaID)
			if ok {
				continue
			}

			schema, ok := s.SchemaByID(job.SchemaID)
			if !ok {
				return errors.NotFoundf("schema %d", job.SchemaID)
			}

			_, err := s.DropTable(job.TableID)
			if err != nil {
				return errors.Trace(err)
			}

			table := job.BinlogInfo.TableInfo
			if table == nil {
				return errors.NotFoundf("table %d", job.TableID)
			}

			err = s.CreateTable(schema, table)
			if err != nil {
				return errors.Trace(err)
			}

		default:
			tbInfo := job.BinlogInfo.TableInfo
			if tbInfo == nil {
				return errors.NotFoundf("table %d", job.TableID)
			}

			_, ok := s.IgnoreSchemaByID(job.SchemaID)
			if ok {
				continue
			}

			_, ok = s.SchemaByID(job.SchemaID)
			if !ok {
				return errors.NotFoundf("schema %d", job.SchemaID)
			}

			err := s.ReplaceTable(tbInfo)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

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

// IgnoreSchemaByID returns the schema that whether to be ignored
func (s *Schema) IgnoreSchemaByID(id int64) (val struct{}, ok bool) {
	val, ok = s.ignoreSchema[id]
	return
}

// TableByID returns the TableInfo by table id
func (s *Schema) TableByID(id int64) (val *model.TableInfo, ok bool) {
	val, ok = s.tables[id]
	return
}

// AddIgnoreSchema add schema into ignoreSchema
func (s *Schema) AddIgnoreSchema(schema *model.DBInfo) {
	s.ignoreSchema[schema.ID] = struct{}{}
}

// DropIgnoreSchema delete the given DBInfo in ignoreSchema
func (s *Schema) DropIgnoreSchema(id int64) {
	delete(s.ignoreSchema, id)
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
