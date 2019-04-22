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

package loader

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// all DML must be the same table
// we merge consequence DML by Primary key
// after merge, only have one record for one key
// insert + delete -> delete
// insert + update -> insert
// insert + insert -> insert  invalid
// delete + delete -> delete  invalid
// delete + update -> -       invalid
// delete + insert -> insert
// update + delete -> delete
// update + update -> update
// update + insert -> -       invalid
func mergeByPrimaryKey(dmls []*DML) (types map[DMLType][]*DML, err error) {
	if len(dmls) == 0 {
		return
	}

	pks := dmls[0].primaryKeys()
	if len(pks) == 0 {
		return nil, errors.Errorf("%s.%s no pk", dmls[0].Database, dmls[0].Table)
	}

	var res = make(map[string]*DML)

	// if update primary key, replace update -> delete(old one) + insert(new one)
	var tmpDmls []*DML
	for _, dml := range dmls {
		if dml.Tp == UpdateDMLType && dml.updateKey() {
			deleteDML := &DML{
				Database: dml.Database,
				Table:    dml.Table,
				Tp:       DeleteDMLType,
				Values:   dml.OldValues,
				info:     dml.info,
			}
			tmpDmls = append(tmpDmls, deleteDML)

			insertDML := &DML{
				Database:  dml.Database,
				Table:     dml.Table,
				Tp:        InsertDMLType,
				Values:    dml.Values,
				OldValues: nil,
				info:      dml.info,
			}
			tmpDmls = append(tmpDmls, insertDML)
		} else {
			tmpDML := &DML{
				Database:  dml.Database,
				Table:     dml.Table,
				Tp:        dml.Tp,
				Values:    dml.Values,
				OldValues: dml.OldValues,
				info:      dml.info,
			}

			tmpDmls = append(tmpDmls, tmpDML)
		}
	}
	dmls = tmpDmls

	for _, dml := range dmls {
		key := dml.formatKey()
		oldDML, ok := res[key]
		if !ok {
			res[key] = dml
			continue
		}

		switch dml.Tp {
		case InsertDMLType:
			// ignore the previous delete
			if oldDML.Tp == DeleteDMLType {
			} else if oldDML.Tp == UpdateDMLType || oldDML.Tp == InsertDMLType {
				log.Warn("update-insert/insert-insert happen", zap.Reflect("before", oldDML), zap.Reflect("after", dml))
			}
			res[key] = dml
		case DeleteDMLType:
			// insert/update + delete -> delete
			res[key] = dml
		case UpdateDMLType:
			if oldDML.Tp == InsertDMLType {
				// insert-update -> insert
				dml.Tp = InsertDMLType
				dml.OldValues = nil
			} else if oldDML.Tp == UpdateDMLType {
				// update-update -> update
				dml.OldValues = oldDML.OldValues
			} else if oldDML.Tp == DeleteDMLType {
				// delete + update -> invalid
				log.Warn("abnormal case delete + update, just remain update now")
			}
			res[key] = dml

		default:
			return nil, errors.Errorf("unknown tp: %v", dml.Tp)
		}
	}

	types = make(map[DMLType][]*DML)
	for _, dml := range res {
		dmls = types[dml.Tp]
		dmls = append(dmls, dml)
		types[dml.Tp] = dmls
	}

	return
}
