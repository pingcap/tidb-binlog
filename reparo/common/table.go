package common

import (
	"fmt"
)

// Column represents table column in database.
type Column struct {
	Idx      int
	Name     string
	NotNull  bool
	Unsigned bool
}

func (c *Column) String() string {
	return fmt.Sprintf("idx:%d, name:%s, not_null:%v, unsigned:%v", c.Idx, c.Name, c.NotNull, c.Unsigned)
}

// Table represents a table in database.
type Table struct {
	Schema string `toml:"db-name" json:"db-name"`
	Name   string `toml:"tbl-name" json:"tbl-name"`

	Columns      []*Column
	IndexColumns map[string][]*Column
}
