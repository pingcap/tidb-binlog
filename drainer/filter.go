package drainer

import (
	"fmt"
	"regexp"
)

type filter struct {
	schema *Schema

	// because TiDB is case-insensitive, only lower-case here.
	ignoreDBs map[string]struct{}

	regexMap map[string]*regexp.Regexp

	doDBs    []string
	doTables []TableName
}

func newFilter(doDBS []string, doTables []TableName, ignoreDBs string) *filter {
	filter := &filter{
		schema:    &Schema{},
		ignoreDBs: formatIgnoreDBs(ignoreDBs),
		regexMap:  make(map[string]*regexp.Regexp),
		doDBs:     doDBS,
		doTables:  doTables,
	}
	filter.genRegexMap()
	return filter
}

func (f *filter) addOneRegex(originStr string) {
	if _, ok := f.regexMap[originStr]; !ok {
		var re *regexp.Regexp
		if originStr[0] != '~' {
			re = regexp.MustCompile(fmt.Sprintf("(?i)^%s$", originStr))
		} else {
			re = regexp.MustCompile(fmt.Sprintf("(?i)%s", originStr[1:]))
		}
		f.regexMap[originStr] = re
	}
}

func (f *filter) genRegexMap() {
	for _, db := range f.doDBs {
		f.addOneRegex(db)
	}

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

// SkipSchemaAndTable return true if the schema and table should be skiped
func (f *filter) SkipSchemaAndTable(schema string, table string) bool {
	tbs := []TableName{{Schema: schema, Table: table}}
	tbs = f.whiteFilter(tbs)
	if len(tbs) == 0 {
		return true
	}
	return false
}

func (f *filter) matchString(pattern string, t string) bool {
	if re, ok := f.regexMap[pattern]; ok {
		return re.MatchString(t)
	}
	return pattern == t
}

func (f *filter) matchDB(patternDBS []string, schema string) bool {
	for _, patternDB := range patternDBS {
		if f.matchString(patternDB, schema) {
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
