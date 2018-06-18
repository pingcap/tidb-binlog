package diff

import (
	"database/sql"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

// FlashDiff contains two sql DB, used for comparing.
type FlashDiff struct {
	db1 *sql.DB
	db2 *sql.DB
}

// NewFlash returns a FlashDiff instance.
func NewFlash(db1, db2 *sql.DB) *FlashDiff {
	return &FlashDiff{
		db1: db1,
		db2: db2,
	}
}

// FlashEqual tests whether two database have same data and schema.
func (df *FlashDiff) FlashEqual() (eq bool, err error) {
	tbls1, err := getTables(df.db1)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	tbls2, err := getTables(df.db2)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	eq = equalStrings(tbls1, tbls2)
	if !eq {
		log.Infof("show tables get different table. [source db tables] %v [target db tables] %v", tbls1, tbls2)
		return false, nil
	}

	for _, tblName := range tbls1 {
		eq, err = df.equalTableRowCount(tblName)
		if err != nil || !eq {
			err = errors.Trace(err)
			return
		}
	}

	return
}

// EqualTableRowCount tests whether two database table have same row count.
func (df *FlashDiff) equalTableRowCount(tblName string) (bool, error) {
	rows1, err := getTableRowCount(df.db1, tblName)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer rows1.Close()

	rows2, err := getTableRowCount(df.db2, tblName)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer rows2.Close()

	cols1, err := rows1.ColumnTypes()
	if err != nil {
		return false, errors.Trace(err)
	}
	cols2, err := rows2.ColumnTypes()
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(cols1) != len(cols2) {
		return false, nil
	}

	row1 := newRawBytesRow(cols1)
	row2 := newRawBytesRow(cols2)
	return equalRows(rows1, rows2, row1, row2)
}
