package test

import (
	"github.com/PingCAP-QE/schrddl/norec"
	"github.com/PingCAP-QE/schrddl/sqlgenerator"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestNoRecSimple(t *testing.T) {
	state := sqlgenerator.NewState()
	state.SetWeight(sqlgenerator.GroupByColumnsOpt, 0)
	state.SetWeight(sqlgenerator.AggFunction, 0)
	state.SetWeight(sqlgenerator.WindowFunction, 0)
	state.SetWeight(sqlgenerator.WindowClause, 0)
	state.SetWeight(sqlgenerator.WindowFunctionOverW, 0)

	tidbParser := parser.New()

	prepareStmtCnt := 5
	for i := 0; i < prepareStmtCnt; i++ {
		sql, err := sqlgenerator.Start.Eval(state)
		require.NoError(t, err)
		println("", sql)
	}

	for i := 0; i < prepareStmtCnt; i++ {
		sql, err := sqlgenerator.DMLStmt.Eval(state)
		require.NoError(t, err)
		println("", sql)
	}

	rewriter := &norec.NoRecRewriter{}
	var sb strings.Builder

	querySQL, err := sqlgenerator.SingleSelect.Eval(state)
	require.NoError(t, err)
	println("", querySQL)
	stmts, _, err := tidbParser.Parse(querySQL, "", "")
	require.NoError(t, err)

	stmt := stmts[0]
	newStmt, _ := stmt.Accept(rewriter)
	sb.Reset()
	err = newStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutDefaultCharset, &sb))
	require.NoError(t, err)
	newQuery := sb.String()
	println("", newQuery)
}
