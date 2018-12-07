package drainer

import (
	"fmt"
	"regexp"
)

type filter struct {
	ignoreSchemaNames map[string]struct{}
	reMap             map[string]*regexp.Regexp

	doDBs    []string
	doTables []TableName
}

func newFilter(ignoreSchemaNames map[string]struct{}, doDBs []string, doTables []TableName) *filter {
	filter := &filter{
		ignoreSchemaNames: ignoreSchemaNames,
		doDBs:             doDBs,
		doTables:          doTables,
		reMap:             make(map[string]*regexp.Regexp),
	}

	filter.genRegexMap()

	return filter
}

func (s *filter) addOneRegex(originStr string) {
	if _, ok := s.reMap[originStr]; !ok {
		var re *regexp.Regexp
		if originStr[0] != '~' {
			re = regexp.MustCompile(fmt.Sprintf("(?i)^%s$", originStr))
		} else {
			re = regexp.MustCompile(fmt.Sprintf("(?i)%s", originStr[1:]))
		}
		s.reMap[originStr] = re
	}
}

func (s *filter) genRegexMap() {
	for _, db := range s.doDBs {
		s.addOneRegex(db)
	}

	for _, tb := range s.doTables {
		s.addOneRegex(tb.Schema)
		s.addOneRegex(tb.Table)
	}
}

// whiteFilter whitelist filtering
func (s *filter) whiteFilter(stbs []TableName) []TableName {
	var tbs []TableName
	if len(s.doTables) == 0 && len(s.doDBs) == 0 {
		return stbs
	}
	for _, tb := range stbs {
		// if the white list contains "schema_s.table_t" and "schema_s",
		// all tables in that schema_s will pass the filter.
		if s.matchTable(s.doTables, tb) {
			tbs = append(tbs, tb)
		}
		if s.matchDB(s.doDBs, tb.Schema) {
			tbs = append(tbs, tb)
		}
	}
	return tbs
}

func (s *filter) skipSchemaAndTable(schema string, table string) bool {
	if _, ok := s.ignoreSchemaNames[schema]; ok {
		return true
	}

	tbs := []TableName{{Schema: schema, Table: table}}
	tbs = s.whiteFilter(tbs)
	if len(tbs) == 0 {
		return true
	}
	return false
}

func (s *filter) matchString(pattern string, t string) bool {
	if re, ok := s.reMap[pattern]; ok {
		return re.MatchString(t)
	}
	return pattern == t
}

func (s *filter) matchDB(patternDBS []string, a string) bool {
	for _, b := range patternDBS {
		if s.matchString(b, a) {
			return true
		}
	}
	return false
}

func (s *filter) matchTable(patternTBS []TableName, tb TableName) bool {
	for _, ptb := range patternTBS {
		if s.matchString(ptb.Schema, tb.Schema) {
			// tb.Table == "" means create or drop database
			if tb.Table == "" || s.matchString(ptb.Table, tb.Table) {
				return true
			}
		}
	}
	return false
}
