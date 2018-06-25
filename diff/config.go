package diff

import "fmt"

// Config is the diff configuration.
type Config struct {
	EqualIndex       bool `toml:"equal-index" json:"equal-index"`
	EqualCreateTable bool `toml:"equal-create-table" json:"equal-create-table"`
	EqualRowCount    bool `toml:"equal-row-count" json:"equal-row-count"`
	EqualData        bool `toml:"equal-data" json:"equal-data"`
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("DiffConfig(%+v)", *c)
}
