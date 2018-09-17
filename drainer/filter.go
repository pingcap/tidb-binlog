package drainer

import (
	"fmt"
	"regexp"
)

func (s *Syncer) addOneRegex(originStr string) {
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

func (s *Syncer) genRegexMap() {
	for _, db := range s.cfg.DoDBs {
		s.addOneRegex(db)
	}

	for _, tb := range s.cfg.DoTables {
		s.addOneRegex(tb.Schema)
		s.addOneRegex(tb.Table)
	}
}

// whiteFilter whitelist filtering
func (s *Syncer) whiteFilter(stbs []TableName) []TableName {
	var tbs []TableName
	if len(s.cfg.DoTables) == 0 && len(s.cfg.DoDBs) == 0 {
		return stbs
	}
	for _, tb := range stbs {
		// if the white list contains "schema_s.table_t" and "schema_s",
		// all tables in that schema_s will pass the filter.
		if s.matchTable(s.cfg.DoTables, tb) {
			tbs = append(tbs, tb)
		}
		if s.matchDB(s.cfg.DoDBs, tb.Schema) {
			tbs = append(tbs, tb)
		}
	}
	return tbs
}

func (s *Syncer) skipSchemaAndTable(schema string, table string) bool {
	tbs := []TableName{{Schema: schema, Table: table}}
	tbs = s.whiteFilter(tbs)
	return len(tbs) == 0
}

func (s *Syncer) matchString(pattern string, t string) bool {
	if re, ok := s.reMap[pattern]; ok {
		return re.MatchString(t)
	}
	return pattern == t
}

func (s *Syncer) matchDB(patternDBS []string, a string) bool {
	for _, b := range patternDBS {
		if s.matchString(b, a) {
			return true
		}
	}
	return false
}

func (s *Syncer) matchTable(patternTBS []TableName, tb TableName) bool {
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
