package framework

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/PingCAP-QE/schrddl/norec"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type norecChecker struct {
	c        *testCase
	rewriter norec.NoRecRewriter
	sb       strings.Builder
}

func (n *norecChecker) check(sql string, isReduce bool) (ok bool, err error) {
	cntOfOld := 0
	cntOfNew := 0
	var newQuery string
	defer func() {
		if isReduce {
			if ok {
				n.c.cntOfOldReduce = cntOfOld
				n.c.cntOfNewReduce = cntOfNew
				n.c.reduceSQL = sql
				n.c.reduceChangedSQL = newQuery
			}
		} else {
			n.c.cntOfOldOriginal = cntOfOld
			n.c.cntOfNewOriginal = cntOfNew
			n.c.originalSQL = sql
		}
	}()

	querySQL := sql

	// send queries to tidb and check the result
	globalRunQueryCnt.Add(1)
	rs1, err := n.c.execQueryForCnt(querySQL)
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
	pd, err := n.c.getQueryPlan(querySQL)
	if err != nil {
		return false, errors.Trace(err)
	}

	re := regexp.MustCompile(`IndexJoin`)
	match := re.FindString(pd.plan)
	if match != "" && strings.Contains(querySQL, "inl_join") {
		n.c.aggregationAsInnerSideOfIndexJoin++
	}

	n.c.queryPlanMap[pd.plan] = querySQL
	if pd.useMvIndex {
		n.c.planUseMvIndex++
	}

	stmts, _, err := n.c.tidbParser.Parse(querySQL, "", "")
	if err != nil {
		logutil.BgLogger().Error("parse error", zap.String("sql", querySQL), zap.Error(err))
		return false, errors.Trace(err)
	}
	stmt := stmts[0]
	n.rewriter.Reset()
	newStmt, _ := stmt.Accept(&n.rewriter)
	if !n.rewriter.Valid() {
		// No predicate, continue
		return false, nil
	}
	isAgg := n.rewriter.IsAgg()
	n.sb.Reset()
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
