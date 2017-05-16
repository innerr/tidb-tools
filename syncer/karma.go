// Copyright 2016 PingCAP, Inc.
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

type karma map[string]string

// Add adds key
// make sure detecting conflict before call Add
func (k karma) Add(keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	if k.DetectConflict(keys) {
		return errors.New("some conflict in karma")
	}
	// find karma key
	selectedKarma := keys[0]
	var nonExistKeys []string
	for _, key := range keys {
		if val, ok := k[key]; ok {
			selectedKarma = val
		} else {
			nonExistKeys = append(nonExistKeys, key)
		}
	}
	// set karma for those non exist key
	for _, key := range nonExistKeys {
		k[key] = selectedKarma
	}
	return nil
}

func (k karma) Get(key string) string {
	return k[key]
}

func (k karma) Reset() {
	k = make(map[string]string)
}

// detectConflict detect whether there is a conflict
func (k karma) DetectConflict(keys []string) bool {
	if len(keys) == 0 {
		return false
	}

	var existedKarma string
	for _, key := range keys {
		if val, ok := k[key]; ok {
			if existedKarma != "" && val != existedKarma {
				return true
			}
			existedKarma = val
		}
	}

	return false
}
