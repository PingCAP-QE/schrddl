package framework

import (
	"github.com/PingCAP-QE/schrddl/norec"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

type tlpChecker struct {
	c        *testCase
	rewriter norec.NoRecRewriter
	sb       strings.Builder
}

func (t *tlpChecker) check(sql string, isReduce bool) (ok bool, err error) {
	cntOfOld := 0
	cntOfNew := 0
	var newQuery string
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
	rs1, err := t.c.execQueryForCnt(querySQL)
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
	newStmt, _ := stmt.Accept(&t.rewriter)
	if !t.rewriter.Valid() {
		// No predicate, continue
		return false, nil
	}
	isAgg := t.rewriter.IsAgg()
	t.sb.Reset()
	err = newStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutDefaultCharset, &n.sb))
	if err != nil {
		return false, errors.Trace(err)
	}
	newQuery = n.sb.String()

	cntOfOld = rs1
	rs2, err := n.c.execQuery(newQuery)
	//println(fmt.Sprintf("%s;", newQuery))
	if err != nil {
		if dmlIgnoreError(err) {
			return false, nil
		} else {
			logutil.BgLogger().Error("unexpected error", zap.String("query", querySQL), zap.Error(err))
			return false, err
		}
	}
	if isAgg {
		for _, row := range rs2 {
			if row[0] != "'0'" && row[0] != ddlTestValueNull {
				cntOfNew++
			}
		}
	} else if rs2[0][0] == ddlTestValueNull {
		cntOfNew = 0
	} else {
		cn, err := strconv.Atoi(strings.Trim(rs2[0][0], "'"))
		if err != nil {
			logutil.BgLogger().Error("convert error", zap.Error(err))
			return false, err
		}
		cntOfNew = cn
	}

	return cntOfOld != cntOfNew, nil
}
