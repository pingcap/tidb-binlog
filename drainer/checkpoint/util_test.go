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

package checkpoint

import (
	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

var _ = Suite(&testUtil{})

type testUtil struct{}

func (t *testUtil) TestG(c *C) {
	tests := []struct {
		name string
		rows []uint64
		id   uint64
		err  bool
	}{
		{"no row", nil, 0, true},
		{"on row", []uint64{1}, 1, false},
		{"multi row", []uint64{1, 2}, 0, true},
	}

	for _, test := range tests {
		db, mock, err := sqlmock.New()
		c.Assert(err, IsNil)

		rows := sqlmock.NewRows([]string{"clusterID"})
		for _, row := range test.rows {
			rows.AddRow(row)
		}

		mock.ExpectQuery("select clusterID from .*").WillReturnRows(rows)

		c.Log("test: ", test.name)
		id, err := getClusterID(db, "schema", "table")
		if test.err {
			c.Assert(err, NotNil)
			c.Assert(id, Equals, test.id)
		} else {
			c.Assert(err, IsNil)
			c.Assert(id, Equals, test.id)
		}
	}
}
