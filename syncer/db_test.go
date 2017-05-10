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
	"errors"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	tmysql "github.com/pingcap/tidb/mysql"
	gouuid "github.com/satori/go.uuid"
)

func newMysqlErr(number uint16, message string) *mysql.MySQLError {
	return &mysql.MySQLError{
		Number:  number,
		Message: message,
	}
}

func (s *testSyncerSuite) TestIsRetryableError(c *C) {
	e := newMysqlErr(tmysql.ErrNoDB, "no db error")
	r := isRetryableError(e)
	c.Assert(r, IsFalse)

	e = newMysqlErr(tmysql.ErrUnknown, "i/o timeout")
	r = isRetryableError(e)
	c.Assert(r, IsTrue)

	e = newMysqlErr(tmysql.ErrDBCreateExists, "db already exists")
	r = isRetryableError(e)
	c.Assert(r, IsFalse)

	ee := errors.New("driver: bad connection")
	r = isRetryableError(ee)
	c.Assert(r, IsTrue)
}

func (s *testSyncerSuite) TestGetMasterStatus(c *C) {
	binlogPos, gtids, err := getMasterStatus(s.db)
	c.Assert(err, IsNil)
	c.Assert(binlogPos.Name, Not(Equals), "")
	c.Assert(binlogPos.Pos, Not(Equals), 0)
	// because master is reset.
	c.Assert(len(gtids), Equals, 0)
}

func (s *testSyncerSuite) TestGetServerUUID(c *C) {
	uuid, err := getServerUUID(s.db)
	c.Assert(err, IsNil)
	_, err = gouuid.FromString(uuid)
	c.Assert(err, IsNil)
}

func (s *testSyncerSuite) TestResolveDDLSQL(c *C) {

	tests := []struct {
		sql      string
		wantSqls []string
		wantOk   bool
		wantErr  bool
	}{
		{"drop table `foo`.`bar`", []string{"DROP TABLE `foo`.`bar`"}, true, false},
		{"drop table if exists `foo`.`bar`", []string{"DROP TABLE IF EXISTS `foo`.`bar`"}, true, false},
		{"rename table t1 to t2", []string{"rename table t1 to t2"}, true, false}, // no change
		{"rename table `t1` to `t2`, `t3` to `t4`", nil, false, true},             //parser not supported two ddls currently.
		{"alter table `bar` add column `id` int not null", []string{"alter table `bar` add column `id` int not null"}, true, false},
		{"alter table `bar` add column `id1` int not null, add column `id2` int not null default 1", []string{"ALTER TABLE `bar` ADD COLUMN `id1` int NOT NULL", "ALTER TABLE `bar` ADD COLUMN `id2` int NOT NULL DEFAULT 1"}, true, false},
		{"alter table `bar` add column `id1` int not null, add column `id2` int not null COMMENT 'this is id2'", []string{"ALTER TABLE `bar` ADD COLUMN `id1` int NOT NULL", "ALTER TABLE `bar` ADD COLUMN `id2` int NOT NULL COMMENT 'this is id2'"}, true, false},
		{"alter table `bar` add column `id2` int not null first", []string{"alter table `bar` add column `id2` int not null first"}, true, false},
		{"alter table `bar` add column `id1` int not null, add column `id2` int not null first", []string{"ALTER TABLE `bar` ADD COLUMN `id1` int NOT NULL", "ALTER TABLE `bar` ADD COLUMN `id2` int NOT NULL FIRST"}, true, false},
		{"alter table `bar` add column `id1` int not null, add column `id2` int not null after `id1`", []string{"ALTER TABLE `bar` ADD COLUMN `id1` int NOT NULL", "ALTER TABLE `bar` ADD COLUMN `id2` int NOT NULL AFTER `id1`"}, true, false},
		{"alter table `bar` add index (`id`)", []string{"alter table `bar` add index (`id`)"}, true, false},
		{"alter table `bar` add key (`id`)", []string{"alter table `bar` add key (`id`)"}, true, false},
		{"alter table `bar` add index `idx`(`id`, `name`), add index (`name`)", []string{"ALTER TABLE `bar` ADD CONSTRAINT INDEX `idx` (`id`, `name`)", "ALTER TABLE `bar` ADD CONSTRAINT INDEX (`name`)"}, true, false}, // doubt this. mysql doesn't have ADD CONSTRAINT INDEX syntax
		{"alter table `bar` add index `idx`(`id`, `name`), add key (`name`)", []string{"ALTER TABLE `bar` ADD CONSTRAINT INDEX `idx` (`id`, `name`)", "ALTER TABLE `bar` ADD CONSTRAINT INDEX (`name`)"}, true, false},
		{"ALTER TABLE bar ADD FULLTEXT INDEX `FullText` (`name` ASC)", nil, false, true},               // tidb not support fulltext index
		{"ALTER TABLE bar ADD FULLTEXT INDEX `fulltext` (`name`) WITH PARSER ngram", nil, false, true}, // ditto
		{"ALTER TABLE bar ADD SPATIAL INDEX (`g`)", nil, false, true},                                  // tidb not support spatial index
		{"ALTER TABLE bar ADD PRIMARY KEY (`g`), add index (`h`);", []string{"ALTER TABLE `bar` ADD CONSTRAINT PRIMARY KEY (`g`)", "ALTER TABLE `bar` ADD CONSTRAINT INDEX (`h`)"}, true, false},
		{"ALTER TABLE bar ADD c INT unsigned NOT NULL AUTO_INCREMENT,ADD PRIMARY KEY (c);", []string{"ALTER TABLE `bar` ADD COLUMN `c` int UNSIGNED NOT NULL AUTO_INCREMENT", "ALTER TABLE `bar` ADD CONSTRAINT PRIMARY KEY (`c`)"}, true, false},
		{"ALTER table bar ADD CONSTRAINT `x` index (name), add unique (`u1`), add unique key (`u2`), add unique index (`u3`);", []string{"ALTER TABLE `bar` ADD CONSTRAINT INDEX `x` (`name`)", "ALTER TABLE `bar` ADD CONSTRAINT UNIQUE INDEX (`u1`)", "ALTER TABLE `bar` ADD CONSTRAINT UNIQUE INDEX (`u2`)", "ALTER TABLE `bar` ADD CONSTRAINT UNIQUE INDEX (`u3`)"}, true, false},
		{"ALTER TABLE bar add index (`name`), add index `hash_index` using hash (`name1`) COMMENT 'a hash index'", []string{"ALTER TABLE `bar` ADD CONSTRAINT INDEX (`name`)", "ALTER TABLE `bar` ADD CONSTRAINT INDEX `hash_index` (`name1`) COMMENT 'a hash index'"}, true, false},
		{"CREATE INDEX id_index ON lookup (id) USING BTREE", nil, false, true},                                                                                                         // tidb not support USING BTREE | HASH syntax
		{"ALTER TABLE bar add index (`name`), add FOREIGN KEY (product_category, product_id) REFERENCES product(category, id) ON UPDATE CASCADE ON DELETE RESTRICT", nil, false, true}, //tidb not support ON UPDATE CASCADE ON DELETE RESTRICT
		{"ALTER TABLE bar add index (`name`), add FOREIGN KEY (product_category, product_id) REFERENCES product(category, id)", []string{"ALTER TABLE `bar` ADD CONSTRAINT INDEX (`name`)", "ALTER TABLE `bar` ADD CONSTRAINT FOREIGN KEY (`product_category`, `product_id`) REFERENCES `product` (`category`, `id`)"}, true, false},
		{"ALTER TABLE bar alter `id` set default 1, alter `name` drop default", []string{"ALTER TABLE `bar` ALTER COLUMN `id` SET DEFAULT 1", "ALTER TABLE `bar` ALTER COLUMN `name` DROP DEFAULT"}, true, false},
		{"ALTER TABLE bar change a b varchar(255), change c d varchar(255) first, change e f varchar(255) after g", nil, false, true}, // tidb not support change column  FIRST | AFTER column
		{"ALTER TABLE bar change a b varchar(255), change c d varchar(255)", []string{"ALTER TABLE `bar` CHANGE COLUMN `a` `b` varchar(255)", "ALTER TABLE `bar` CHANGE COLUMN `c` `d` varchar(255)"}, true, false},
		{"ALTER TABLE bar modify a varchar(255), modify b varchar(255) first, modify c varchar(255) after d", []string{"ALTER TABLE `bar` MODIFY COLUMN `a` varchar(255)", "ALTER TABLE `bar` MODIFY COLUMN `b` varchar(255) FIRST", "ALTER TABLE `bar` MODIFY COLUMN `c` varchar(255) AFTER `d`"}, true, false},
		{"ALTER TABLE bar drop a, drop b", []string{"ALTER TABLE `bar` DROP COLUMN `a`", "ALTER TABLE `bar` DROP COLUMN `b`"}, true, false},
		{"ALTER TABLE bar DROP PRIMARY KEY, drop a", []string{"ALTER TABLE `bar` DROP PRIMARY KEY", "ALTER TABLE `bar` DROP COLUMN `a`"}, true, false},
		{"ALTER TABLE bar drop key a, drop index b", []string{"ALTER TABLE `bar` DROP INDEX `a`", "ALTER TABLE `bar` DROP INDEX `b`"}, true, false},
		{"ALTER TABLE bar drop key a, drop FOREIGN KEY b", []string{"ALTER TABLE `bar` DROP INDEX `a`", "ALTER TABLE `bar` DROP FOREIGN KEY `b`"}, true, false},
		// {"ALTER TABLE bar ENABLE KEYS, DISABLE KEYS", []string{"ALTER TABLE `bar` ENABLE KEYS", "ALTER TABLE `bar` DISABLE KEYS"}, true, false},
		{"ALTER TABLE bar add index (id), rename to bar1", []string{"ALTER TABLE `bar` ADD CONSTRAINT INDEX (`id`)", "ALTER TABLE `bar` RENAME TO `bar1`"}, true, false},
		// {"ALTER TABLE bar rename to bar1, add index (id)", []string{"ALTER TABLE `bar` RENAME TO `bar1`", "ALTER TABLE `bar1` ADD CONSTRAINT INDEX (`id`)"}, true, false},
		{"ALTER TABLE bar add index (id), rename as bar1", []string{"ALTER TABLE `bar` ADD CONSTRAINT INDEX (`id`)", "ALTER TABLE `bar` RENAME TO `bar1`"}, true, false},
		{"ALTER TABLE bar rename index idx_1 to idx_2, rename key idx_3 to idx_4", nil, false, true}, // tidb not support rename index currently.
		{"ALTER TABLE bar ORDER BY id1, id2", nil, false, true},                                      //tidb not support ORDER BY.
		{"ALTER TABLE bar CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin", nil, false, true},         //tidb not support CONVERT TO CHARACTER SET xxx
		{"ALTER TABLE bar character set utf8 collate utf8_bin, add index (id)", nil, false, true},    // tidb not support this.
		{"ALTER TABLE bar add index (id), character set utf8 collate utf8_bin", []string{"ALTER TABLE `bar` ADD CONSTRAINT INDEX (`id`)", "ALTER TABLE `bar` DEFAULT CHARACTER SET = 'utf8' COLLATE = 'utf8_bin'"}, true, false},
		{"ALTER TABLE bar add index (id), character set utf8", []string{"ALTER TABLE `bar` ADD CONSTRAINT INDEX (`id`)", "ALTER TABLE `bar` DEFAULT CHARACTER SET = 'utf8'"}, true, false},
		{"ALTER TABLE bar add index (id), collate utf8_bin", []string{"ALTER TABLE `bar` ADD CONSTRAINT INDEX (`id`)", "ALTER TABLE `bar` DEFAULT COLLATE = 'utf8_bin'"}, true, false},

		{"ALTER TABLE bar add c1 timestamp not null on update current_timestamp, add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD CONSTRAINT INDEX (`c1`)"}, true, false},
		{"ALTER TABLE bar add c1 timestamp null on update current_timestamp, add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp NULL ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD CONSTRAINT INDEX (`c1`)"}, true, false},
		{"ALTER TABLE bar add c1 timestamp on update current_timestamp, add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD CONSTRAINT INDEX (`c1`)"}, true, false},
		{"ALTER TABLE bar add c1 timestamp null default null on update current_timestamp, add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD CONSTRAINT INDEX (`c1`)"}, true, false},

		{"ALTER TABLE bar add c1 timestamp null default 20150606 on update current_timestamp, add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp NULL DEFAULT 20150606 ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD CONSTRAINT INDEX (`c1`)"}, true, false},
		{"ALTER TABLE bar add c1 timestamp not null default 20150606 on update current_timestamp, add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp NOT NULL DEFAULT 20150606 ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD CONSTRAINT INDEX (`c1`)"}, true, false},
		{"ALTER TABLE bar add c1 timestamp default 20150606 on update current_timestamp, add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp DEFAULT 20150606 ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD CONSTRAINT INDEX (`c1`)"}, true, false},

		{"ALTER TABLE bar add c1 varchar(10) DEFAULT '' NOT NULL, add c2 varchar(10) NOT NULL DEFAULT 'foo'", []string{"ALTER TABLE `bar` ADD COLUMN `c1` varchar(10) DEFAULT '' NOT NULL", "ALTER TABLE `bar` ADD COLUMN `c2` varchar(10) NOT NULL DEFAULT 'foo'"}, true, false},
		{"ALTER TABLE bar add c1 int not null default 100000000000000, add c2 smallint not null default '100000000000000'", []string{"ALTER TABLE `bar` ADD COLUMN `c1` int NOT NULL DEFAULT 100000000000000", "ALTER TABLE `bar` ADD COLUMN `c2` smallint NOT NULL DEFAULT '100000000000000'"}, true, false},
	}

	for _, tt := range tests {
		sqls, ok, err := resolveDDLSQL(tt.sql)
		c.Assert(sqls, DeepEquals, tt.wantSqls)
		c.Assert(ok, Equals, tt.wantOk)
		if tt.wantErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
	}
}
