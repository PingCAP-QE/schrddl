package framework

import (
	"strings"

	"github.com/PingCAP-QE/schrddl/pinolo/stage2"
	"github.com/PingCAP-QE/schrddl/util"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type certChecker struct {
	c *testCase
}

func (n *certChecker) check(sql string, isReduce bool) (ok bool, err error) {
	cntOfOld := 0.0
	cntOfNew := 0.0

	defer func() {
		if cntOfOld == 0.0 && cntOfNew == 1.0 {
			ok = false
			return
		}

		if isReduce {
			if ok {
				n.c.oldEstCntReduce = cntOfOld
				n.c.newEstCntReduce = cntOfNew
				n.c.reduceSQL = sql
			}
		} else {
			n.c.oldEstCntOriginal = cntOfOld
			n.c.newEstCntOriginal = cntOfNew
			n.c.originalSQL = sql
		}
	}()

	querySQL := sql

	// send queries to tidb and check the result
	globalRunQueryCnt.Add(1)
	rs1, err := n.c.execQueryForPlanEstCnt(querySQL)
	//println(fmt.Sprintf("%s;", querySQL))
	if err != nil {
		if util.DMLIgnoreError(err) {
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

	op := strings.Split(pd.plan, ";")

	n.c.queryPlanMap[pd.plan] = querySQL
	if pd.useMvIndex {
		n.c.planUseMvIndex++
	}

	mr := stage2.MutateAll(querySQL, 1234)
	if mr.Err != nil {
		logutil.BgLogger().Error("mutate error", zap.String("sql", querySQL), zap.Error(mr.Err))
		return false, errors.Trace(mr.Err)
	}
	for _, r := range mr.MutateUnits {
		if r.Err == nil {
			pd, err := n.c.getQueryPlan(querySQL)
			if err != nil {
				return false, errors.Trace(err)
			}
			np := strings.Split(pd.plan, ";")
			if editDistance(op, np) > 1 {
				continue
			}
			n.c.checkCERTCnt++

			rs2, err := n.c.execQueryForPlanEstCnt(r.Sql)
			if err != nil {
				if util.DMLIgnoreError(err) {
					//logutil.BgLogger().Warn("ignore error", zap.String("query", r.Sql), zap.Error(err))
					return false, nil
				} else {
					logutil.BgLogger().Error("unexpected error", zap.String("query", r.Sql), zap.Error(err))
					return false, errors.Trace(err)
				}
			}

			if (r.IsUpper && rs2 < rs1) || (!r.IsUpper && rs2 > rs1) {
				cntOfOld = rs1
				cntOfNew = rs2
				n.c.reduceChangedSQL = r.Sql
				return true, nil
			}
		}
	}
	return false, nil
}

func editDistance(list1 []string, list2 []string) int {
	dp := make([][]int, len(list1)+1)
	for i := 0; i <= len(list1); i++ {
		dp[i] = make([]int, len(list2)+1)
		for j := 0; j <= len(list2); j++ {
			if i == 0 {
				dp[i][j] = j
			} else if j == 0 {
				dp[i][j] = i
			} else {
				substitutionCost := 1
				if list1[i-1] == list2[j-1] {
					substitutionCost = 0
				}
				dp[i][j] = min(dp[i-1][j-1]+substitutionCost, min(dp[i-1][j]+1, dp[i][j-1]+1))
			}
		}
	}
	return dp[len(list1)][len(list2)]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
