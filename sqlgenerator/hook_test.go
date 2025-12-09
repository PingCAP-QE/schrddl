package sqlgenerator_test

import (
	"fmt"
	"testing"

	"github.com/PingCAP-QE/schrddl/sqlgenerator"
	"github.com/stretchr/testify/require"
)

func TestHookPredNeedRollBackStmt(t *testing.T) {
	state := sqlgenerator.NewState()
	predHook := sqlgenerator.NewFnHookPred()
	rollBackStmts := []sqlgenerator.Fn{
		sqlgenerator.AlterTable, sqlgenerator.FlashBackTable, sqlgenerator.CreateTable, sqlgenerator.SplitRegion,
	}
	for _, f := range rollBackStmts {
		predHook.AddMatchFn(f)
	}
	state.Hook().Append(predHook)

	for i := 0; i < 100; i++ {
		query, err := sqlgenerator.Start.Eval(state)
		require.NoError(t, err)
		if predHook.Matched() {
			fmt.Println(query)
		}
		predHook.ResetMatched()
	}
}

func TestHookReplacer(t *testing.T) {
	replacerHook := sqlgenerator.NewFnHookReplacer()
	query := "select sleep(100000000000);"
	hijacker := sqlgenerator.NewFn(func(state *sqlgenerator.State) sqlgenerator.Fn {
		return sqlgenerator.Str(query)
	})
	replacerHook.Replace(sqlgenerator.Start, hijacker)

	state := sqlgenerator.NewState()
	state.Hook().Append(replacerHook)
	result, err := sqlgenerator.Start.Eval(state)
	require.NoError(t, err)
	require.Equal(t, result, query)
}
