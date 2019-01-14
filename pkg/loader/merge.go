package loader

import (
	"github.com/ngaut/log"
	"github.com/pkg/errors"
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
	if len(dmls) < 0 {
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
			deleteDML := new(DML)
			deleteDML.Database = dml.Database
			deleteDML.Table = dml.Table
			deleteDML.Tp = DeleteDMLType
			deleteDML.Values = dml.OldValues
			deleteDML.info = dml.info
			tmpDmls = append(tmpDmls, deleteDML)

			insertDML := new(DML)
			insertDML.Database = dml.Database
			insertDML.Table = dml.Table
			insertDML.Tp = InsertDMLType
			insertDML.Values = dml.Values
			insertDML.OldValues = nil
			insertDML.info = dml.info
			tmpDmls = append(tmpDmls, insertDML)
		} else {
			tmpDML := new(DML)
			tmpDML.Database = dml.Database
			tmpDML.Table = dml.Table
			tmpDML.Tp = dml.Tp
			tmpDML.Values = dml.Values
			tmpDML.OldValues = dml.OldValues
			tmpDML.info = dml.info

			tmpDmls = append(tmpDmls, tmpDML)
		}
	}
	dmls = tmpDmls

	for _, dml := range dmls {
		log.Debug(dml)

		key := dml.formatKey()
		log.Debug("key: ", key)
		switch dml.Tp {
		case InsertDMLType:
			oldDML, ok := res[key]
			if !ok {
				res[key] = dml
			} else {
				// ignore the previous delete
				if oldDML.Tp == DeleteDMLType {
					res[key] = dml
				} else if oldDML.Tp == UpdateDMLType || oldDML.Tp == InsertDMLType {
					log.Warnf("update-insert/insert-insert happen. before: %+v, after: %+v", oldDML, dml)
					res[key] = dml
				}
			}
		case DeleteDMLType:
			_, ok := res[key]
			if !ok {
				res[key] = dml
			} else {
				// insert/update -> delete
				res[key] = dml
			}
		case UpdateDMLType:
			oldDML, ok := res[key]
			if !ok {
				res[key] = dml
			} else {
				if oldDML.Tp == InsertDMLType {
					// insert-update -> insert
					dml.Tp = InsertDMLType
					dml.OldValues = nil
					res[key] = dml
				} else if oldDML.Tp == UpdateDMLType {
					// update-update -> update
					dml.OldValues = oldDML.OldValues
					res[key] = dml
				} else if oldDML.Tp == DeleteDMLType {
					// delete + update -> invalid
					log.Warn("abnormal case delete + update, just remain update now")
					res[key] = dml
				}
			}

		default:
			return nil, errors.Errorf("unknown tp: %v", dml.Tp)
		}
	}

	types = make(map[DMLType][]*DML)
	for _, dml := range res {
		log.Debug(dml)
		dmls = types[dml.Tp]
		dmls = append(dmls, dml)
		types[dml.Tp] = dmls
	}

	return
}
