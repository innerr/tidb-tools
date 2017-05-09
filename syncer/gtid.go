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

import (
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
)

// GTIDSet wraps mysql.MysqlGTIDSet
// todo: wrap mysql.GTIDSet
type GTIDSet struct {
	*mysql.MysqlGTIDSet
}

func parseGTIDSet(gtidStr string) (GTIDSet, error) {
	gs, err := mysql.ParseMysqlGTIDSet(gtidStr)
	if err != nil {
		return GTIDSet{}, errors.Trace(err)
	}

	return GTIDSet{gs.(*mysql.MysqlGTIDSet)}, nil
}

func (g GTIDSet) delete(uuid string) {
	delete(g.Sets, uuid)
}

func (g GTIDSet) contain(uuid string) bool {
	_, ok := g.Sets[uuid]
	return ok
}

func (g GTIDSet) all() map[string]*mysql.UUIDSet {
	return g.Sets
}
