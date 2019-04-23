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

// Copyright 2017 PingCAP, Inc.
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

import "github.com/pingcap/errors"

// Causality provides a simple mechanism to improve the concurrency of SQLs execution under the premise of ensuring correctness.
// causality groups sqls that maybe contain causal relationships, and syncer executes them linearly.
// if some conflicts exist in more than one groups, then syncer waits all SQLs that are grouped be executed and reset causality.
// this mechanism meets quiescent consistency to ensure correctness.
type Causality struct {
	relations map[string]string
}

// NewCausality return a instance of Causality
func NewCausality() *Causality {
	return &Causality{
		relations: make(map[string]string),
	}
}

// Add add keys to Causality
func (c *Causality) Add(keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	if c.DetectConflict(keys) {
		return errors.New("some conflicts in causality, must be resolved")
	}
	// find causal key
	selectedRelation := keys[0]
	nonExistKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if val, ok := c.relations[key]; ok {
			selectedRelation = val
		} else {
			nonExistKeys = append(nonExistKeys, key)
		}
	}
	// set causal relations for those non-exist keys
	for _, key := range nonExistKeys {
		c.relations[key] = selectedRelation
	}
	return nil
}

// Get gets the token of key
func (c *Causality) Get(key string) string {
	return c.relations[key]
}

// Reset reset Causality
func (c *Causality) Reset() {
	c.relations = make(map[string]string)
}

// DetectConflict detects whether there is a conflict
func (c *Causality) DetectConflict(keys []string) bool {
	if len(keys) == 0 {
		return false
	}

	var existedRelation string
	for _, key := range keys {
		if val, ok := c.relations[key]; ok {
			if existedRelation != "" && val != existedRelation {
				return true
			}
			existedRelation = val
		}
	}

	return false
}
