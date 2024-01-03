package reduce

import (
	"github.com/PingCAP-QE/schrddl/util"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"math/rand"
)

type reduceState struct {
	ruleState map[rule]bool
}

func tryARule(check func(sql string) (bool, error), sql string, r rule, s *reduceState) (string, error) {
	n, err := util.SQL2Ast(sql)
	if err != nil {
		return sql, err
	}
	more := r.apply(&n)
	s.ruleState[r] = more
	trySQL, err := util.Ast2SQL(n)
	if err != nil {
		return "", err
	}
	ok, err := check(trySQL)
	if err != nil {
		return "", err
	}
	if ok {
		return trySQL, nil
	}
	return sql, nil
}

func ReduceSQL(check func(sql string) (bool, error), sql string) string {
	newestSQL := sql
	n, err := util.SQL2Ast(newestSQL)
	if err != nil {
		logutil.BgLogger().Warn("SQL2Ast failed", zap.Error(err))
		return newestSQL
	}

	topRule := selectStmtRule
	switch n.(type) {
	case *ast.SelectStmt:
		topRule = selectStmtRule
	default:
		return newestSQL
	}

	maxStep := 30

	s := &reduceState{
		ruleState: make(map[rule]bool),
	}

	for i := 0; i < maxStep; i++ {
		randomRule := topRule[rand.Intn(len(topRule))]
		if s.ruleState[randomRule] {
			continue
		}
		newSQL, err := tryARule(check, newestSQL, randomRule, s)
		if err != nil {
			continue
		}
		if newSQL != "" {
			newestSQL = newSQL
		}
	}

	return newestSQL
}
