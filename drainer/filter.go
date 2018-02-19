package drainer

import (
	"fmt"
	"regexp"
)

type filter struct {
	schema *Schema

	// cfg *SyncerConfig

	// because TiDB is case-insensitive, only lower-case here.
	ignoreSchemaNames map[string]struct{}

	reMap map[string]*regexp.Regexp

	doDBs    []string
	doTables []TableName
	prepared bool
}

func newFilter(doDBS []string, doTables []TableName, ignoreSchemas string) *filter {
	filter := &filter{
		schema:            &Schema{},
		ignoreSchemaNames: formatIgnoreSchemas(ignoreSchemas),
		reMap:             make(map[string]*regexp.Regexp),
		doDBs:             make([]string, 0, 10),
		doTables:          make([]TableName, 0, 10),
	}
	filter.genRegexMap()
	return filter
}

func (f *filter) addOneRegex(originStr string) {
	if _, ok := f.reMap[originStr]; !ok {
		var re *regexp.Regexp
		if originStr[0] != '~' {
			re = regexp.MustCompile(fmt.Sprintf("(?i)^%s$", originStr))
		} else {
			re = regexp.MustCompile(fmt.Sprintf("(?i)%s", originStr[1:]))
		}
		f.reMap[originStr] = re
	}
}

func (f *filter) genRegexMap() {
	//for _, db := range s.cfg.DoDBs {
	for _, db := range f.doDBs {
		f.addOneRegex(db)
	}

	//for _, tb := range s.cfg.DoTables {
	for _, tb := range f.doTables {
		f.addOneRegex(tb.Schema)
		f.addOneRegex(tb.Table)
	}
}

// whiteFilter whitelist filtering
func (f *filter) whiteFilter(stbs []TableName) []TableName {
	var tbs []TableName
	if len(f.doTables) == 0 && len(f.doDBs) == 0 {
		return stbs
	}
	for _, tb := range stbs {
		// if the white list contains "schema_s.table_t" and "schema_s",
		// all tables in that schema_s will pass the filter.
		if f.matchTable(f.doTables, tb) {
			tbs = append(tbs, tb)
		}
		if f.matchDB(f.doDBs, tb.Schema) {
			tbs = append(tbs, tb)
		}
	}
	return tbs
}

func (f *filter) skipSchemaAndTable(schema string, table string) bool {
	tbs := []TableName{{Schema: schema, Table: table}}
	tbs = f.whiteFilter(tbs)
	if len(tbs) == 0 {
		return true
	}
	return false
}

func (f *filter) matchString(pattern string, t string) bool {
	if re, ok := f.reMap[pattern]; ok {
		return re.MatchString(t)
	}
	return pattern == t
}

func (f *filter) matchDB(patternDBS []string, a string) bool {
	for _, b := range patternDBS {
		if f.matchString(b, a) {
			return true
		}
	}
	return false
}

func (f *filter) matchTable(patternTBS []TableName, tb TableName) bool {
	for _, ptb := range patternTBS {
		if f.matchString(ptb.Schema, tb.Schema) {
			// tb.Table == "" means create or drop database
			if tb.Table == "" || f.matchString(ptb.Table, tb.Table) {
				return true
			}
		}
	}
	return false
}
