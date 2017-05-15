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
	"fmt"
	"strconv"
	"strings"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
)

func defaultValueToSQL(opt *ast.ColumnOption) string {
	if opt.Tp != ast.ColumnOptionDefaultValue {
		panic("unreachable")
	}

	sql := " DEFAULT "
	datum := opt.Expr.GetDatum()
	switch datum.Kind() {
	case types.KindNull:
		expr, ok := opt.Expr.(*ast.FuncCallExpr)
		if ok {
			sql += expr.FnName.O
		} else {
			sql += "NULL"
		}

	case types.KindInt64:
		sql += strconv.FormatInt(datum.GetInt64(), 10)

	case types.KindString:
		sql += formatStringValue(datum.GetString())

	default:
		panic("not implemented yet")
	}

	return sql
}

func formatStringValue(s string) string {
	if s == "" {
		return "''"
	}
	return fmt.Sprintf("'%s'", s)
}

func fieldTypeToSQL(ft *types.FieldType, isModify bool) string {
	strs := []string{ft.CompactStr()}
	if mysql.HasUnsignedFlag(ft.Flag) {
		strs = append(strs, "UNSIGNED")
	}
	if mysql.HasZerofillFlag(ft.Flag) {
		strs = append(strs, "ZEROFILL")
	}
	if mysql.HasBinaryFlag(ft.Flag) {
		strs = append(strs, "BINARY")
	}

	// TiDB doesn't support alter table change/modify column charset and collation.
	if isModify {
		return strings.Join(strs, " ")
	}

	if types.IsTypeChar(ft.Tp) || types.IsTypeBlob(ft.Tp) {
		if ft.Charset != "" && ft.Charset != charset.CharsetBin {
			strs = append(strs, fmt.Sprintf("CHARACTER SET %s", ft.Charset))
		}
		if ft.Collate != "" && ft.Collate != charset.CharsetBin {
			strs = append(strs, fmt.Sprintf("COLLATE %s", ft.Collate))
		}
	}

	return strings.Join(strs, " ")
}

// FIXME: tidb's AST is error-some to handle more condition
func columnDefToSQL(colDef *ast.ColumnDef, isModify bool) string {
	typeDef := fieldTypeToSQL(colDef.Tp, isModify)
	sql := fmt.Sprintf("%s %s", columnNameToSQL(colDef.Name), typeDef)

	for _, opt := range colDef.Options {
		switch opt.Tp {
		case ast.ColumnOptionNotNull:
			sql += " NOT NULL"
		case ast.ColumnOptionNull:
			sql += " NULL"
		case ast.ColumnOptionDefaultValue:
			sql += defaultValueToSQL(opt)
		case ast.ColumnOptionAutoIncrement:
			sql += " AUTO_INCREMENT"
		case ast.ColumnOptionUniqKey:
			sql += " UNIQUE KEY"
		case ast.ColumnOptionKey:
			sql += " KEY"
		case ast.ColumnOptionUniq:
			sql += " UNIQUE"
		case ast.ColumnOptionIndex:
			sql += " INDEX"
		case ast.ColumnOptionUniqIndex:
			sql += " UNIQUE INDEX"
		case ast.ColumnOptionPrimaryKey:
			sql += " PRIMARY KEY"
		case ast.ColumnOptionComment:
			sql += fmt.Sprintf(" COMMENT '%v'", opt.Expr.GetValue())
		case ast.ColumnOptionOnUpdate: // For Timestamp and Datetime only.
			sql += " ON UPDATE CURRENT_TIMESTAMP"
		case ast.ColumnOptionFulltext:
			panic("not implemented yet")
		default:
			panic("not implemented yet")
		}
	}

	return sql
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}

func tableNameToSQL(tbl *ast.TableName) string {
	sql := ""
	if tbl.Schema.O != "" {
		sql += fmt.Sprintf("`%s`.", tbl.Schema.O)
	}
	sql += fmt.Sprintf("`%s`", tbl.Name.O)
	return sql
}

func columnNameToSQL(name *ast.ColumnName) string {
	sql := ""
	if name.Schema.O != "" {
		sql += fmt.Sprintf("`%s`.", escapeName(name.Schema.O))
	}
	if name.Table.O != "" {
		sql += fmt.Sprintf("`%s`.", escapeName(name.Table.O))
	}
	sql += fmt.Sprintf("`%s`", escapeName(name.Name.O))
	return sql
}

func indexColNameToSQL(name *ast.IndexColName) string {
	sql := columnNameToSQL(name.Column)
	if name.Length != types.UnspecifiedLength {
		sql += fmt.Sprintf(" (%d)", name.Length)
	}
	return sql
}

func constraintKeysToSQL(keys []*ast.IndexColName) string {
	if len(keys) == 0 {
		panic("unreachable")
	}
	sql := ""
	for i, indexColName := range keys {
		if i == 0 {
			sql += "("
		}
		sql += indexColNameToSQL(indexColName)
		if i != len(keys)-1 {
			sql += ", "
		}
	}
	sql += ")"
	return sql
}

func referenceDefToSQL(refer *ast.ReferenceDef) string {
	sql := fmt.Sprintf("%s ", tableNameToSQL(refer.Table))
	sql += constraintKeysToSQL(refer.IndexColNames)
	if refer.OnDelete != nil && refer.OnDelete.ReferOpt != ast.ReferOptionNoOption {
		sql += fmt.Sprintf(" ON DELETE %s", refer.OnDelete.ReferOpt)
	}
	if refer.OnUpdate != nil && refer.OnUpdate.ReferOpt != ast.ReferOptionNoOption {
		sql += fmt.Sprintf(" ON UPDATE %s", refer.OnUpdate.ReferOpt)
	}
	return sql
}

func constraintToSQL(constraint *ast.Constraint) string {
	sql := ""
	switch constraint.Tp {
	case ast.ConstraintKey, ast.ConstraintIndex:
		sql += "ADD INDEX "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += constraintKeysToSQL(constraint.Keys)
		sql += indexOptionToSQL(constraint.Option)

	case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		sql += "ADD CONSTRAINT "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += "UNIQUE INDEX "
		sql += constraintKeysToSQL(constraint.Keys)
		sql += indexOptionToSQL(constraint.Option)

	case ast.ConstraintForeignKey:
		sql += "ADD CONSTRAINT "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += "FOREIGN KEY "
		sql += constraintKeysToSQL(constraint.Keys)
		sql += " REFERENCES "
		sql += referenceDefToSQL(constraint.Refer)

	case ast.ConstraintPrimaryKey:
		sql += "ADD CONSTRAINT "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += "PRIMARY KEY "
		sql += constraintKeysToSQL(constraint.Keys)
		sql += indexOptionToSQL(constraint.Option)

	case ast.ConstraintFulltext:
		fallthrough

	default:
		panic("not implemented yet")
	}
	return sql
}

func positionToSQL(pos *ast.ColumnPosition) string {
	var sql string
	switch pos.Tp {
	case ast.ColumnPositionNone:
	case ast.ColumnPositionFirst:
		sql = " FIRST"
	case ast.ColumnPositionAfter:
		colName := pos.RelativeColumn.Name.O
		sql = fmt.Sprintf(" AFTER `%s`", escapeName(colName))
	default:
		panic("unreachable")
	}
	return sql
}

// Convert constraint indexoption to sql. Currently only support comment.
func indexOptionToSQL(option *ast.IndexOption) string {
	if option == nil {
		return ""
	}

	if option.Comment != "" {
		return fmt.Sprintf(" COMMENT '%s'", option.Comment)
	}

	return ""
}

func tableOptionToSQL(options []*ast.TableOption) string {
	sql := ""
	if len(options) == 0 {
		return sql
	}

	charset := struct {
		exists bool
		value  string
	}{}
	collate := struct {
		exists bool
		value  string
	}{}

	for _, option := range options {
		switch option.Tp {
		case ast.TableOptionCharset:
			charset.exists = true
			charset.value = option.StrValue
		case ast.TableOptionCollate:
			collate.exists = true
			collate.value = option.StrValue
		}
	}
	if charset.exists && collate.exists {
		sql += fmt.Sprintf("DEFAULT CHARACTER SET = '%s' COLLATE = '%s'", charset.value, collate.value)
	} else if charset.exists && !collate.exists {
		sql += fmt.Sprintf("DEFAULT CHARACTER SET = '%s'", charset.value)
	} else if !charset.exists && collate.exists {
		sql += fmt.Sprintf("DEFAULT COLLATE = '%s'", collate.value)
	}

	return sql
}

func alterTableSpecToSQL(spec *ast.AlterTableSpec, ntable *newTable) string {
	sql := ""
	log.Debugf("spec.Tp: %d", spec.Tp)

	switch spec.Tp {
	case ast.AlterTableOption:
		sql += tableOptionToSQL(spec.Options)

	case ast.AlterTableAddColumn:
		sql += fmt.Sprintf("ADD COLUMN %s", columnDefToSQL(spec.NewColumn, false))
		if spec.Position != nil {
			sql += positionToSQL(spec.Position)
		}

	case ast.AlterTableDropColumn:
		sql += fmt.Sprintf("DROP COLUMN %s", columnNameToSQL(spec.OldColumnName))

	case ast.AlterTableDropIndex:
		sql += fmt.Sprintf("DROP INDEX `%s`", escapeName(spec.Name))

	case ast.AlterTableAddConstraint:
		sql += constraintToSQL(spec.Constraint)

	case ast.AlterTableDropForeignKey:
		sql += fmt.Sprintf("DROP FOREIGN KEY `%s`", escapeName(spec.Name))

	case ast.AlterTableModifyColumn:
		sql += "MODIFY COLUMN "
		sql += columnDefToSQL(spec.NewColumn, true)
		if spec.Position != nil {
			sql += positionToSQL(spec.Position)
		}

	// FIXME: should support [FIRST|AFTER col_name], but tidb parser not support this currently.
	case ast.AlterTableChangeColumn:
		sql += "CHANGE COLUMN "
		sql += fmt.Sprintf("%s %s",
			columnNameToSQL(spec.OldColumnName),
			columnDefToSQL(spec.NewColumn, true))

	case ast.AlterTableRenameTable:
		ntable.isNew = true
		ntable.table = spec.NewTable
		sql += fmt.Sprintf("RENAME TO %s", tableNameToSQL(spec.NewTable))

	case ast.AlterTableAlterColumn:
		sql += fmt.Sprintf("ALTER COLUMN %s ", columnNameToSQL(spec.NewColumn.Name))
		if options := spec.NewColumn.Options; options != nil {
			sql += fmt.Sprintf("SET DEFAULT %v", options[0].Expr.GetValue())
		} else {
			sql += "DROP DEFAULT"
		}

	case ast.AlterTableDropPrimaryKey:
		sql += "DROP PRIMARY KEY"

	case ast.AlterTableLock:
		// just ignore it
	default:
	}
	return sql
}

func alterTableStmtToSQL(stmt *ast.AlterTableStmt, ntable *newTable) string {
	var sql string
	if ntable.isNew {
		sql = fmt.Sprintf("ALTER TABLE %s ", tableNameToSQL(ntable.table))
	} else {
		sql = fmt.Sprintf("ALTER TABLE %s ", tableNameToSQL(stmt.Table))
	}
	for i, spec := range stmt.Specs {
		if i != 0 {
			sql += ", "
		}
		sql += alterTableSpecToSQL(spec, ntable)
	}

	log.Debugf("alter table stmt to sql:%s", sql)
	return sql
}
