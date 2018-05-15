package table

// Column represents table column in database.
type Column struct {
	Idx      int
	Name     string
	NotNull  bool
	Unsigned bool
}

// Table represents a table in database.
type Table struct {
	Schema string
	Name   string

	Columns      []*Column
	IndexColumns map[string][]*Column
}
