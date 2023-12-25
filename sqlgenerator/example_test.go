package sqlgenerator_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/PingCAP-QE/schrddl/sqlgenerator"
	"github.com/stretchr/testify/require"
)

func TestReadMeExample(t *testing.T) {
	state := sqlgenerator.NewState()
	state.Config().SetMaxTable(200)
	state.SetWeight(sqlgenerator.IndexDefinitions, 0)
	state.SetWeight(sqlgenerator.PartitionDefinition, 0)
	for i := 0; i < 200; i++ {
		sql, err := sqlgenerator.CreateTable.Eval(state)
		require.NoError(t, err)
		fmt.Print(sql)
		fmt.Println(";")
	}
}

func TestQuery(t *testing.T) {
	state := sqlgenerator.NewState()
	rowCount := 10
	tblCount := 2
	for i := 0; i < tblCount; i++ {
		sql, err := sqlgenerator.CreateTable.Eval(state)
		require.NoError(t, err)
		fmt.Println(sql)
	}
	for _, tb := range state.Tables {
		state.Env().Table = tb
		for i := 0; i < rowCount; i++ {
			sql, err := sqlgenerator.InsertInto.Eval(state)
			require.NoError(t, err)
			fmt.Println(sql)
		}
	}
	queries := generateQuery(state, rowCount)
	for _, sql := range queries {
		fmt.Println(sql)
	}
}

func TestExampleInitialize(t *testing.T) {
	state := sqlgenerator.NewState()
	tableCount, columnCount := 5, 5
	indexCount, rowCount := 2, 10
	initSQLs := generateCreateTable(state, tableCount, columnCount, indexCount)
	for _, sql := range initSQLs {
		fmt.Println(sql)
	}
	insertSQLs := generateInsertInto(state, rowCount)
	for _, sql := range insertSQLs {
		fmt.Println(sql)
	}
}

//func TestExampleCTE(t *testing.T) {
//	state := sqlgenerator.NewState()
//	state.SetWeight(sqlgenerator.IndexDefinitions, 0)
//	state.SetWeight(sqlgenerator.PartitionDefinition, 0)
//	state.SetRepeat(sqlgenerator.ColumnDefinition, 5, 5)
//	rowCount := 10
//	tblCount := 2
//	for i := 0; i < tblCount; i++ {
//		sql := sqlgenerator.CreateTable.Eval(state)
//		fmt.Println(sql)
//	}
//
//	generateInsertInto(state, rowCount)
//
//	for i := 0; i < 100; i++ {
//		fmt.Println(sqlgenerator.CTEQueryStatement.Eval(state))
//	}
//
//	for i := 0; i < 100; i++ {
//		fmt.Println(sqlgenerator.CTEDMLStatement.Eval(state), ";")
//	}
//}

func TestExampleCreateTableWithoutIndexOrPartitions(t *testing.T) {
	state := sqlgenerator.NewState()
	state.Config().SetMaxTable(200)
	state.SetWeight(sqlgenerator.IndexDefinitions, 0)
	state.SetWeight(sqlgenerator.PartitionDefinition, 0)
	for i := 0; i < 200; i++ {
		sql, err := sqlgenerator.CreateTable.Eval(state)
		require.NoError(t, err)
		fmt.Println(sql)
		require.Greater(t, len(sql), 0)
		require.NotContains(t, sql, "index")
		require.NotContains(t, sql, "partition")
	}
}

func TestExampleIntegerColumnTypeChange(t *testing.T) {
	state := sqlgenerator.NewState()
	state.ReplaceRule(sqlgenerator.ColumnDefinitionType, sqlgenerator.ColumnDefinitionTypesIntegers)
	state.SetWeight(sqlgenerator.PartitionDefinitionList, 0)
	createTables := generateCreateTable(state, 5, 10, 8)
	for _, sql := range createTables {
		fmt.Println(sql)
	}
	insertSQLs := generateInsertInto(state, 20)
	for _, sql := range insertSQLs {
		fmt.Println(sql)
	}
	state.SetWeight(sqlgenerator.AlterTable, 20)
	state.SetWeight(sqlgenerator.AlterColumn, 10)
	alterTableCount := 0
	for i := 0; i < 200; i++ {
		sql, err := sqlgenerator.Start.Eval(state)
		require.NoError(t, err)
		require.Greater(t, len(sql), 0)
		fmt.Println(sql)
		if strings.Contains(sql, "alter table") {
			alterTableCount++
		}
	}
	fmt.Printf("Total alter table statements: %d\n", alterTableCount)
}

func TestExampleColumnTypeChangeWithGivenTypes(t *testing.T) {
	state := sqlgenerator.NewState()
	state.Config().SetMaxTable(100)
	state.SetRepeat(sqlgenerator.ColumnDefinition, 5, 5)
	state.ReplaceRule(sqlgenerator.ColumnDefinitionTypeOnCreate, sqlgenerator.ColumnDefinitionTypesIntegerInt)
	state.ReplaceRule(sqlgenerator.ColumnDefinitionTypeOnAdd, sqlgenerator.ColumnDefinitionTypesIntegerBig)
	state.ReplaceRule(sqlgenerator.ColumnDefinitionTypeOnModify, sqlgenerator.ColumnDefinitionTypesIntegerTiny)

	for i := 0; i < 100; i++ {
		query, err := sqlgenerator.CreateTable.Eval(state)
		require.NoError(t, err)
		require.Equal(t, 5, strings.Count(query, "int"), query)
	}
	for i := 0; i < 20; i++ {
		state.Env().Table = state.Tables.Rand()
		query, err := sqlgenerator.AddColumn.Eval(state)
		require.NoError(t, err)
		require.Contains(t, query, "bigint", query)
	}
	for i := 0; i < 20; i++ {
		randTable := state.Tables.Rand()
		state.Env().Table = state.Tables.Rand()
		query, err := sqlgenerator.AlterColumn.Eval(state)
		require.NoError(t, err)
		if len(query) == 0 {
			pk := randTable.Indexes.Primary()
			if pk != nil {
				require.Len(t, randTable.Columns.Diff(pk.Columns), 0)
			}
		} else if strings.Contains(query, "modify") || strings.Contains(query, "change") {
			require.Contains(t, query, "tinyint", query)
		}
	}
}

func TestGBKCharacters(t *testing.T) {
	state := sqlgenerator.NewState()
	state.Config().SetMaxTable(100)
	state.SetRepeat(sqlgenerator.ColumnDefinition, 5, 5)
}

func TestExampleSubSelectFieldCompatible(t *testing.T) {
	state := sqlgenerator.NewState()
	state.ReplaceRule(sqlgenerator.SubSelect, sqlgenerator.SubSelectWithGivenTp)
	query, err := sqlgenerator.CreateTable.Eval(state)
	require.NoError(t, err)
	require.Greater(t, len(query), 0)
	tbl := state.Tables.Rand()
	state.Env().Table = tbl
	state.Env().Column = tbl.Columns.Rand()
	for i := 0; i < 100; i++ {
		pred, err := sqlgenerator.Predicate.Eval(state)
		require.NoError(t, err)
		fmt.Println(pred)
	}
}

func generateCreateTable(state *sqlgenerator.State, tblCount, colCount, idxCount int) []string {
	result := make([]string, 0, tblCount)
	state.SetRepeat(sqlgenerator.ColumnDefinition, colCount, colCount)
	state.SetRepeat(sqlgenerator.IndexDefinition, idxCount, idxCount)
	for i := 0; i < tblCount; i++ {
		sql, err := sqlgenerator.CreateTable.Eval(state)
		if err != nil {
			panic(err)
		}
		result = append(result, sql)
	}
	return result
}

func generateInsertInto(state *sqlgenerator.State, rowCount int) []string {
	result := make([]string, 0, rowCount)
	for _, tb := range state.Tables {
		state.Env().Table = tb
		for i := 0; i < rowCount; i++ {
			sql, err := sqlgenerator.InsertInto.Eval(state)
			if err != nil {
				panic(err)
			}
			result = append(result, sql)
		}
	}
	return result
}

func generateQuery(state *sqlgenerator.State, count int) []string {
	result := make([]string, 0, count)
	for _, tb := range state.Tables {
		state.Env().Table = tb
		for i := 0; i < count; i++ {
			sql, err := sqlgenerator.Query.Eval(state)
			if err != nil {
				panic(err)
			}
			result = append(result, sql)
		}
	}
	return result
}

func TestHaving(t *testing.T) {
	state := sqlgenerator.NewState()
	state.Config().SetMaxTable(20)
	state.SetWeight(sqlgenerator.IndexDefinitions, 0)
	state.SetWeight(sqlgenerator.PartitionDefinition, 0)
	state.SetWeight(sqlgenerator.AggSelect, 1)
	for i := 0; i < 10; i++ {
		sql, err := sqlgenerator.CreateTable.Eval(state)
		require.NoError(t, err)
		fmt.Print(sql)
		fmt.Println(";")
	}
	for i := 0; i < 100; i++ {
		sql, err := sqlgenerator.MultiSelect.Eval(state)
		require.NoError(t, err)
		fmt.Print(sql)
		fmt.Println(";")
	}
}
