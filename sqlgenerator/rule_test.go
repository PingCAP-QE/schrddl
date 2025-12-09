package sqlgenerator_test

import (
	"strings"
	"testing"

	"github.com/PingCAP-QE/schrddl/sqlgenerator"
	"github.com/pingcap/tidb/pkg/parser"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/require"
)

func TestStart(t *testing.T) {
	state := sqlgenerator.NewState()
	defer state.CheckIntegrity()
	for i := 0; i < 300; i++ {
		res, err := sqlgenerator.Start.Eval(state)
		require.NoError(t, err)
		require.Greater(t, len(res), 0, i)
	}
	require.Equal(t, state.Env().Depth(), 0)
}

func TestCreateTable(t *testing.T) {
	state := sqlgenerator.NewState()
	defer state.CheckIntegrity()
	for i := 0; i < 300; i++ {
		res, err := sqlgenerator.CreateTable.Eval(state)
		require.NoError(t, err)
		require.Greater(t, len(res), 0, i)
	}
}

func TestCreateColumnTypes(t *testing.T) {
	state := sqlgenerator.NewState()
	defer state.CheckIntegrity()

	state.ReplaceRule(sqlgenerator.ColumnDefinitionType, sqlgenerator.ColumnDefinitionTypesIntegerInt)
	state.SetRepeat(sqlgenerator.ColumnDefinition, 5, 5)
	intColCount := 0
	for i := 0; i < 100; i++ {
		res, err := sqlgenerator.CreateTable.Eval(state)
		require.NoError(t, err)
		require.Greater(t, len(res), 0, i)
		intColCount += strings.Count(res, "int")
	}
	require.Equal(t, 100*5, intColCount)
}

func TestCreateTableLike(t *testing.T) {
	state := sqlgenerator.NewState()
	defer state.CheckIntegrity()
	_, err := sqlgenerator.CreateTable.Eval(state)
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		_, err = sqlgenerator.CreateTableLike.Eval(state)
		require.NoError(t, err)
		state.Env().Table = state.Tables.Rand()
		_, err = sqlgenerator.AddColumn.Eval(state)
		require.NoError(t, err)
		dropColTbls := state.Tables.Filter(func(t *sqlgenerator.Table) bool {
			state.Env().Table = t
			return sqlgenerator.MoreThan1Columns(state) && sqlgenerator.HasDroppableColumn(state)
		})
		if len(dropColTbls) > 0 {
			state.Env().Table = dropColTbls.Rand()
			_, err = sqlgenerator.DropColumn.Eval(state)
			require.NoError(t, err)
		}
	}
}

func TestAlterColumnPosition(t *testing.T) {
	state := sqlgenerator.NewState()
	defer state.CheckIntegrity()
	state.SetRepeat(sqlgenerator.ColumnDefinition, 10, 10)
	_, err := sqlgenerator.CreateTable.Eval(state)
	require.NoError(t, err)
	state.Env().Table = state.Tables.Rand()
	for i := 0; i < 10; i++ {
		_, err = sqlgenerator.InsertInto.Eval(state)
		require.NoError(t, err)
	}
	originCols := state.Env().Table.Columns.Copy()
	positionChanged := false
	for i := 0; i < 100; i++ {
		query, err := sqlgenerator.AlterColumn.Eval(state)
		require.NoError(t, err)
		if strings.Contains(query, "first") || strings.Contains(query, "after") {
			positionChanged = true
		}
	}
	if positionChanged {
		eq := originCols.Equal(state.Env().Table.Columns)
		require.False(t, eq)
	}
}

func TestConfigKeyUnitAvoidAlterPKColumn(t *testing.T) {
	state := sqlgenerator.NewState()
	defer state.CheckIntegrity()
	state.SetRepeat(sqlgenerator.ColumnDefinition, 10, 10)
	state.SetRepeat(sqlgenerator.IndexDefinition, 1, 1)
	state.ReplaceRule(sqlgenerator.IndexDefinitionType, sqlgenerator.IndexDefinitionTypePrimary)

	_, err := sqlgenerator.CreateTable.Eval(state)
	require.NoError(t, err)
	tbl := state.Tables.Rand()
	pk := tbl.Indexes.Rand()
	require.Equal(t, sqlgenerator.IndexTypePrimary, pk.Tp)
	pkCols := tbl.Columns.Filter(func(c *sqlgenerator.Column) bool {
		return pk.HasColumn(c)
	})
	state.Env().Table = tbl
	for i := 0; i < 30; i++ {
		_, err := sqlgenerator.AlterColumn.Eval(state)
		require.NoError(t, err)
	}
	for _, pkCol := range pkCols {
		require.True(t, tbl.Columns.Contain(pkCol))
	}
}

func TestSyntax(t *testing.T) {
	state := sqlgenerator.NewState()
	defer state.CheckIntegrity()
	tidbParser := parser.New()

	state.Config().SetMaxTable(200)
	for i := 0; i < 1000; i++ {
		sql, err := sqlgenerator.Start.Eval(state)
		require.NoError(t, err)
		_, warn, err := tidbParser.ParseSQL(sql)
		require.Lenf(t, warn, 0, "sql: %s", sql)
		require.Nilf(t, err, "sql: %s", sql)
	}
}
