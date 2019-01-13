package loader

import (
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	check "github.com/pingcap/check"
)

func Test(t *testing.T) { check.TestingT(t) }

type UtilSuite struct{}

var _ = check.Suite(&UtilSuite{})

func (cs *UtilSuite) SetUpTest(c *check.C) {
}

func (cs *UtilSuite) TestGetTableInfo(c *check.C) {
	db, mock, err := sqlmock.New()

	c.Assert(err, check.IsNil)
	defer db.Close()

	// (id, a1, a2, a3, a4)
	// primary key: id
	// unique key: (a1) (a2,a3)
	columnRows := sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
		AddRow("id", "int(11)", "NO", "PRI", "NULL", "").
		AddRow("a1", "int(11)", "NO", "PRI", "NULL", "").
		AddRow("a2", "int(11)", "NO", "PRI", "NULL", "").
		AddRow("a3", "int(11)", "NO", "PRI", "NULL", "").
		AddRow("a4", "int(11)", "NO", "PRI", "NULL", "")

	indexRows := sqlmock.NewRows([]string{"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment"}).
		AddRow("test1", 0, "PRIMARY", 1, "id", "", "", "", "", "", "", "", "").
		AddRow("test1", 0, "dex1", 1, "a1", "", "", "", "", "", "", "", "").
		AddRow("test1", 0, "dex2", 1, "a2", "", "", "", "", "", "", "", "").
		AddRow("test1", 0, "dex2", 2, "a3", "", "", "", "", "", "", "", "").
		AddRow("test1", 1, "dex3", 1, "a4", "", "", "", "", "", "", "", "")

	mock.ExpectQuery("show columns").WillReturnRows(columnRows)

	mock.ExpectQuery("show index").WillReturnRows(indexRows)

	info, err := getTableInfo(db, "test", "test1")
	c.Assert(err, check.IsNil)
	c.Assert(info, check.NotNil)

	c.Assert(info, check.DeepEquals, &tableInfo{
		columns:    []string{"id", "a1", "a2", "a3", "a4"},
		primaryKey: &indexInfo{"PRIMARY", []string{"id"}},
		uniqueKeys: []indexInfo{{"PRIMARY", []string{"id"}},
			{"dex1", []string{"a1"}},
			{"dex2", []string{"a2", "a3"}},
		}})
}
