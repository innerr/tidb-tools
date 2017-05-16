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

import (
	. "github.com/pingcap/check"
)

func (s *testSyncerSuite) TestKarma(c *C) {
	k := newKarma()
	caseData := []string{"test_1", "test_2", "test_3"}
	excepted := map[string]string{
		"test_1": "test_1",
		"test_2": "test_1",
		"test_3": "test_1",
	}
	c.Assert(k.add(caseData), IsNil)
	c.Assert(k.k, DeepEquals, excepted)
	c.Assert(k.add([]string{"test_4"}), IsNil)
	excepted["test_4"] = "test_4"
	c.Assert(k.k, DeepEquals, excepted)
	conflictData := []string{"test_4", "test_3"}
	c.Assert(k.detectConflict(conflictData), IsTrue)
	c.Assert(k.add(conflictData), NotNil)
	k.reset()
	c.Assert(k.k, HasLen, 0)
}
