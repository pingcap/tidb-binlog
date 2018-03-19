package restore

import (
	"fmt"
	"regexp"
	"strings"

	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

// do some filters based on schema/table

// GenRegexMap generates regular expression map.
func (r *Restore) GenRegexMap() {
	for _, db := range r.cfg.DoDBs {
		r.addOneRegex(db)
	}

	for _, tb := range r.cfg.DoTables {
		r.addOneRegex(tb.Schema)
		r.addOneRegex(tb.Name)
	}
}

func (r *Restore) addOneRegex(originStr string) {
	if _, ok := r.reMap[originStr]; !ok {
		var re *regexp.Regexp
		if originStr[0] != '~' {
			re = regexp.MustCompile(fmt.Sprintf("(?i)^%s$", originStr))
		} else {
			re = regexp.MustCompile(fmt.Sprintf("(?i)%s", originStr[1:]))
		}
		r.reMap[originStr] = re
	}
}

// SkipBySchemaAndTable skips sql based on schema and table rules.
func (r *Restore) SkipBySchemaAndTable(schema string, table string) bool {
	tbs := []TableName{{Schema: strings.ToLower(schema), Name: strings.ToLower(table)}}
	tbs = r.whiteFilter(tbs)
	if len(tbs) == 0 {
		return true
	}
	return false
}

// whiteFilter whitelist filtering
func (r *Restore) whiteFilter(stbs []TableName) []TableName {
	var tbs []TableName
	if len(r.cfg.DoTables) == 0 && len(r.cfg.DoDBs) == 0 {
		return stbs
	}
	for _, tb := range stbs {
		// if the white list contains "schema_s.table_t" and "schema_s",
		// all tables in that schema_s will pass the filter.
		if r.matchTable(r.cfg.DoTables, tb) {
			tbs = append(tbs, tb)
		}
		if r.matchDB(r.cfg.DoDBs, tb.Schema) {
			tbs = append(tbs, tb)
		}
	}
	return tbs
}

func (r *Restore) matchTable(patternTBS []TableName, tb TableName) bool {
	for _, ptb := range patternTBS {
		if r.matchString(ptb.Schema, tb.Schema) {
			// tb.Table == "" means create or drop database
			if tb.Name == "" || r.matchString(ptb.Name, tb.Name) {
				return true
			}
		}
	}
	return false
}

func (r *Restore) matchDB(patternDBS []string, a string) bool {
	for _, b := range patternDBS {
		if r.matchString(b, a) {
			return true
		}
	}
	return false
}

func (r *Restore) matchString(pattern string, t string) bool {
	if re, ok := r.reMap[pattern]; ok {
		return re.MatchString(t)
	}
	return pattern == t
}

func isAcceptableBinlog(binlog *pb.Binlog, startTs, endTs int64) bool {
	return binlog.CommitTs >= startTs && (endTs == 0 || binlog.CommitTs <= endTs)
}
