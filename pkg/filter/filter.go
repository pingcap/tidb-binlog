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

package filter

import (
	"fmt"
	"regexp"
	"strings"
)

// Filter to skip data by schema name and table name.
type Filter struct {
	reMap map[string]*regexp.Regexp

	doDBs    []string
	doTables []TableName

	ignoreDBs    []string
	ignoreTables []TableName
}

// NewFilter creates a instance of Filter
func NewFilter(ignoreDBs []string, ignoreTables []TableName, doDBs []string, doTables []TableName) *Filter {
	filter := &Filter{
		ignoreDBs:    ignoreDBs,
		ignoreTables: ignoreTables,
		doDBs:        doDBs,
		doTables:     doTables,
		reMap:        make(map[string]*regexp.Regexp),
	}

	filter.genRegexMap()

	return filter
}

func (s *Filter) addOneRegex(originStr string) {
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

func (s *Filter) genRegexMap() {
	for _, db := range s.doDBs {
		s.addOneRegex(db)
	}

	for _, tb := range s.doTables {
		s.addOneRegex(tb.Schema)
		s.addOneRegex(tb.Table)
	}

	for _, db := range s.ignoreDBs {
		s.addOneRegex(db)
	}

	for _, tb := range s.ignoreTables {
		s.addOneRegex(tb.Schema)
		s.addOneRegex(tb.Table)
	}
}

// whiteFilter whitelist filtering
func (s *Filter) whiteFilter(stbs []TableName) []TableName {
	var tbs []TableName
	if len(s.doTables) == 0 && len(s.doDBs) == 0 {
		return stbs
	}
	for _, tb := range stbs {
		// if the white list contains "schema_s.table_t" and "schema_s",
		// all tables in that schema_s will pass the Filter.
		if s.matchTable(s.doTables, tb) {
			tbs = append(tbs, tb)
		}
		if s.matchDB(s.doDBs, tb.Schema) {
			tbs = append(tbs, tb)
		}
	}
	return tbs
}

// blackFilter return TableName which is not in the blacklist
func (s *Filter) blackFilter(stbs []TableName) []TableName {
	var tbs []TableName
	if len(s.ignoreTables) == 0 && len(s.ignoreDBs) == 0 {
		return stbs
	}

	for _, tb := range stbs {
		if s.matchTable(s.ignoreTables, tb) {
			continue
		}
		if s.matchDB(s.ignoreDBs, tb.Schema) {
			continue
		}
		tbs = append(tbs, tb)
	}
	return tbs
}

// SkipSchemaAndTable skips data based on schema and table rules.
func (s *Filter) SkipSchemaAndTable(schema string, table string) bool {
	tbs := []TableName{{Schema: strings.ToLower(schema), Table: strings.ToLower(table)}}

	tbs = s.whiteFilter(tbs)
	tbs = s.blackFilter(tbs)
	return len(tbs) == 0
}

func (s *Filter) matchString(pattern string, t string) bool {
	if re, ok := s.reMap[pattern]; ok {
		return re.MatchString(t)
	}
	return pattern == t
}

func (s *Filter) matchDB(patternDBS []string, a string) bool {
	for _, b := range patternDBS {
		if s.matchString(b, a) {
			return true
		}
	}
	return false
}

func (s *Filter) matchTable(patternTBS []TableName, tb TableName) bool {
	for _, ptb := range patternTBS {
		if s.matchString(ptb.Schema, tb.Schema) && s.matchString(ptb.Table, tb.Table) {
			return true
		}

	}
	return false
}

// TableName specify a Schema name and Table name
type TableName struct {
	Schema string `toml:"db-name" json:"db-name"`
	Table  string `toml:"tbl-name" json:"tbl-name"`
}
