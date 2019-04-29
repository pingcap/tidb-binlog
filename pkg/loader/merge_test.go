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
	"math/rand"

	check "github.com/pingcap/check"
)

type modelSuite struct {
}

var _ = check.Suite(&modelSuite{})

func (m *modelSuite) TestMerge(c *check.C) {
	info := &tableInfo{
		columns:    []string{"k", "v"},
		uniqueKeys: []indexInfo{{"PRIMARY", []string{"k"}}},
	}
	info.primaryKey = &info.uniqueKeys[0]

	apply := func(kv map[int]int, dmls []*DML) map[int]int {
		for _, dml := range dmls {
			switch dml.Tp {
			case InsertDMLType:
				k := dml.Values["k"].(int)
				v := dml.Values["v"].(int)
				kv[k] = v
			case UpdateDMLType:
				k := dml.Values["k"].(int)
				v := dml.Values["v"].(int)

				oldk := dml.OldValues["k"].(int)
				// oldv := dml.OldValues["v"].(int)
				delete(kv, oldk)
				kv[k] = v
			case DeleteDMLType:
				k := dml.Values["k"].(int)
				delete(kv, k)
			}
		}

		return kv
	}

	// generate dmlNum DML date
	var dmls []*DML
	dmlNum := 100000
	maxKey := 1000
	updateKeyProbability := 0.1

	var kv = make(map[int]int)
	for i := 0; i < dmlNum; i++ {
		dml := new(DML)
		dml.info = info
		dmls = append(dmls, dml)

		k := rand.Intn(maxKey)
		v, ok := kv[k]
		if !ok {
			// insert
			dml.Tp = InsertDMLType
			dml.Values = make(map[string]interface{})
			dml.Values["k"] = k
			dml.Values["v"] = rand.Int()
		} else {
			if rand.Int()%2 == 0 {
				// update
				dml.Tp = UpdateDMLType
				dml.OldValues = make(map[string]interface{})
				dml.OldValues["k"] = k
				dml.OldValues["v"] = v

				newv := rand.Int()
				dml.Values = make(map[string]interface{})
				dml.Values["k"] = k
				dml.Values["v"] = newv
				// check whether to update k
				if rand.Float64() < updateKeyProbability {
					for try := 0; try < 10; try++ {
						newk := rand.Intn(maxKey)
						if _, ok := kv[newk]; !ok {
							dml.Values["k"] = newk
							break
						}
					}
				}
			} else {
				// delete
				dml.Tp = DeleteDMLType
				dml.Values = make(map[string]interface{})
				dml.Values["k"] = k
				dml.Values["v"] = v
			}
		}

		kv = apply(kv, []*DML{dml})
	}

	kv = make(map[int]int)
	kvMerge := make(map[int]int)

	step := dmlNum / 10
	for i := 0; i < len(dmls); i += step {
		end := i + step
		if end > len(dmls) {
			end = len(dmls)
		}
		logDMLs(dmls[i:end], c)
		kv = apply(kv, dmls[i:end])

		res, err := mergeByPrimaryKey(dmls[i:end])
		c.Assert(err, check.IsNil)

		noMergeNumber := end - i
		mergeNumber := 0
		if mdmls, ok := res[DeleteDMLType]; ok {
			logDMLs(mdmls, c)
			kvMerge = apply(kvMerge, mdmls)
			mergeNumber += len(mdmls)
		}
		if mdmls, ok := res[InsertDMLType]; ok {
			logDMLs(mdmls, c)
			kvMerge = apply(kvMerge, mdmls)
			c.Logf("kvMerge: %v", kvMerge)
			mergeNumber += len(mdmls)
		}
		if mdmls, ok := res[UpdateDMLType]; ok {
			logDMLs(mdmls, c)
			kvMerge = apply(kvMerge, mdmls)
			mergeNumber += len(mdmls)
		}
		c.Logf("before number: %d, after merge: %d", noMergeNumber, mergeNumber)
		c.Logf("kv: %v kvMerge: %v", kv, kvMerge)
		c.Assert(kvMerge, check.DeepEquals, kv)
	}
}

func logDMLs(dmls []*DML, c *check.C) {
	c.Log("dmls: ", len(dmls))
	for _, dml := range dmls {
		c.Logf("tp: %v, values: %v, OldValues: %v", dml.Tp, dml.Values, dml.OldValues)
	}
}
