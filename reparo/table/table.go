package table

type Column struct {
	Idx      int
	Name     string
	NotNull  bool
	Unsigned bool
}

type Table struct {
	Schema string
	Name   string

	Columns      []*Column
	IndexColumns map[string][]*Column
}
