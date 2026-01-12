package framework

import (
	"github.com/PingCAP-QE/schrddl/eet"
	"github.com/PingCAP-QE/schrddl/util"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"strings"
)

type eetChecker struct {
	c        *testCase
	rewriter eet.EETRewriter
	sb       strings.Builder
}

func (t *eetChecker) check(sql string, isReduce bool) (ok bool, err error) {
	cntOfOld := 0
	cntOfNew := 0
	var newQuery string

	newQuery = ""

	defer func() {
		if isReduce {
			if ok {
				t.c.cntOfOldReduce = cntOfOld
				t.c.cntOfNewReduce = cntOfNew
				t.c.reduceSQL = sql
				t.c.reduceChangedSQL = newQuery
			}
		} else {
			t.c.cntOfOldOriginal = cntOfOld
			t.c.cntOfNewOriginal = cntOfNew
			t.c.originalSQL = sql
		}
	}()

	querySQL := sql

	// send queries to tidb and check the result
	globalRunQueryCnt.Add(1)
	rs1, err := t.c.execQueryForCRC32(querySQL)
	//println(fmt.Sprintf("%s;", querySQL))
	if err != nil {
		if dmlIgnoreError(err) {
			return false, nil
		} else {
			logutil.BgLogger().Error("unexpected error", zap.String("query", querySQL), zap.Error(err))
			return false, errors.Trace(err)
		}
	}
	globalSuccessQueryCnt.Add(1)
	pd, err := t.c.getQueryPlan(querySQL)
	if err != nil {
		return false, errors.Trace(err)
	}
	t.c.queryPlanMap[pd.plan] = querySQL
	if pd.useMvIndex {
		t.c.planUseMvIndex++
	}

	stmts, _, err := t.c.tidbParser.Parse(querySQL, "", "")
	if err != nil {
		logutil.BgLogger().Error("parse error", zap.String("sql", querySQL), zap.Error(err))
		return false, errors.Trace(err)
	}
	stmt := stmts[0]
	t.rewriter.Reset()
	newStmt, _ := util.CloneAst(stmt).Accept(&t.rewriter)
	if !t.rewriter.Valid() {
		// No predicate, continue
		return false, nil
	}
	t.sb.Reset()
	err = newStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutDefaultCharset, &t.sb))
	if err != nil {
		return false, errors.Trace(err)
	}
	newQuery = t.sb.String()
	rs2, err := t.c.execQueryForCRC32(newQuery)
	//println(fmt.Sprintf("%s;", newQuery))
	if err != nil {
		if dmlIgnoreError(err) {
			return false, nil
		} else {
			logutil.BgLogger().Error("unexpected error", zap.String("query", querySQL), zap.Error(err))
			return false, err
		}
	}
	t.rewriter.Reset()

	cntOfOld = len(rs1)
	cntOfNew = len(rs2)
	return !mapEqual(rs1, rs2), nil
}
