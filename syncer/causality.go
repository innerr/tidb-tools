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

package main

import "github.com/juju/errors"

type causality struct {
	relations map[string]string
}

func newCausality() *causality {
	return &causality{
		relations: make(map[string]string),
	}
}

// make sure to resolve conflict before call Add
func (c *causality) add(relations []string) error {
	if len(relations) == 0 {
		return nil
	}

	if c.detectConflict(relations) {
		return errors.New("some conflicts in causality")
	}
	// find relation
	selectedRelation := relations[0]
	var nonExistRelations []string
	for _, relation := range relations {
		if val, ok := c.relations[relation]; ok {
			selectedRelation = val
		} else {
			nonExistRelations = append(nonExistRelations, relation)
		}
	}
	// set karma for those non exist key
	for _, relation := range nonExistRelations {
		c.relations[relation] = selectedRelation
	}
	return nil
}

func (c *causality) get(relation string) string {
	return c.relations[relation]
}

func (c *causality) reset() {
	c.relations = make(map[string]string)
}

// detectConflict detects whether there is a conflict
func (c *causality) detectConflict(relations []string) bool {
	if len(relations) == 0 {
		return false
	}

	var existedRelation string
	for _, relation := range relations {
		if val, ok := c.relations[relation]; ok {
			if existedRelation != "" && val != existedRelation {
				return true
			}
			existedRelation = val
		}
	}

	return false
}
