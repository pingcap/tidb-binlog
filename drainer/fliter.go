package drainer

import (
	"regexp"
)

func (s *Syncer) genRegexMap() {
	for _, db := range s.cfg.DoDBs {
		if db[0] != '~' {
			continue
		}
		if _, ok := s.reMap[db]; !ok {
			s.reMap[db] = regexp.MustCompile(db[1:])
		}
	}

	for _, tb := range s.cfg.DoTables {
		if tb.Table[0] == '~' {
			if _, ok := s.reMap[tb.Table]; !ok {
				s.reMap[tb.Table] = regexp.MustCompile(tb.Table[1:])
			}
		}
		if tb.Schema[0] == '~' {
			if _, ok := s.reMap[tb.Schema]; !ok {
				s.reMap[tb.Schema] = regexp.MustCompile(tb.Schema[1:])
			}
		}
	}
}

// whiteFilter whitelist filtering
func (s *Syncer) whiteFilter(stbs []TableName) []TableName {
	var tbs []TableName
	if len(s.cfg.DoTables) == 0 && len(s.cfg.DoDBs) == 0 {
		return stbs
	}
	for _, tb := range stbs {
		if s.matchTable(s.cfg.DoTables, tb) {
			tbs = append(tbs, tb)
		}
		if s.matchDB(s.cfg.DoDBs, tb.Schema) {
			tbs = append(tbs, tb)
		}
	}
	return tbs
}

func (s *Syncer) skipDML(schema string, table string) bool {
	tbs := []TableName{{Schema: schema, Table: table}}
	tbs = s.whiteFilter(tbs)
	if len(tbs) == 0 {
		return true
	}
	return false
}

func (s *Syncer) skipDDL(schema, table string) bool {
	tbs := []TableName{{Schema: schema, Table: table}}
	tbs = s.whiteFilter(tbs)
	if len(tbs) == 0 {
		return true
	}
	return false
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
		if s.matchString(ptb.Table, tb.Table) && s.matchString(ptb.Schema, tb.Schema) {
			return true
		}

		//create database or drop database
		if tb.Table == "" {
			if s.matchString(tb.Schema, ptb.Schema) {
				return true
			}
		}
	}

	return false
}
