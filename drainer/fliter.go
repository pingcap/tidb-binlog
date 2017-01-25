package drainer

import (
	"regexp"
)

func (d *Drainer) genRegexMap() {
	for _, db := range d.cfg.DoDBs {
		if db[0] != '~' {
			continue
		}
		if _, ok := d.reMap[db]; !ok {
			d.reMap[db] = regexp.MustCompile(db[1:])
		}
	}

	for _, tb := range d.cfg.DoTables {
		if tb.Table[0] == '~' {
			if _, ok := d.reMap[tb.Table]; !ok {
				d.reMap[tb.Table] = regexp.MustCompile(tb.Table[1:])
			}
		}
		if tb.Schema[0] == '~' {
			if _, ok := d.reMap[tb.Schema]; !ok {
				d.reMap[tb.Schema] = regexp.MustCompile(tb.Schema[1:])
			}
		}
	}
}

// whiteFilter whitelist filtering
func (d *Drainer) whiteFilter(stbs []TableName) []TableName {
	var tbs []TableName
	if len(d.cfg.DoTables) == 0 && len(d.cfg.DoDBs) == 0 {
		return stbs
	}
	for _, tb := range stbs {
		if d.matchTable(d.cfg.DoTables, tb) {
			tbs = append(tbs, tb)
		}
		if d.matchDB(d.cfg.DoDBs, tb.Schema) {
			tbs = append(tbs, tb)
		}
	}
	return tbs
}

func (d *Drainer) skipDML(schema string, table string) bool {
	tbs := []TableName{{Schema: schema, Table: table}}
	tbs = d.whiteFilter(tbs)
	if len(tbs) == 0 {
		return true
	}
	return false
}

func (d *Drainer) skipDDL(schema, table string) bool {
	tbs := []TableName{{Schema: schema, Table: table}}
	tbs = d.whiteFilter(tbs)
	if len(tbs) == 0 {
		return true
	}
	return false
}

func (d *Drainer) matchString(pattern string, t string) bool {
	if re, ok := d.reMap[pattern]; ok {
		return re.MatchString(t)
	}
	return pattern == t
}

func (d *Drainer) matchDB(patternDBS []string, a string) bool {
	for _, b := range patternDBS {
		if d.matchString(b, a) {
			return true
		}
	}
	return false
}

func (d *Drainer) matchTable(patternTBS []TableName, tb TableName) bool {
	for _, ptb := range patternTBS {
		if d.matchString(ptb.Table, tb.Table) && d.matchString(ptb.Schema, tb.Schema) {
			return true
		}

		//create database or drop database
		if tb.Table == "" {
			if d.matchString(tb.Schema, ptb.Schema) {
				return true
			}
		}
	}

	return false
}
