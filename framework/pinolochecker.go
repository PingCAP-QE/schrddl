package framework

import (
	"github.com/PingCAP-QE/schrddl/pinolo"
	"github.com/PingCAP-QE/schrddl/pinolo/stage2"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type pinoloChecker struct {
	c *testCase
}

func (n *pinoloChecker) check(sql string, isReduce bool) (ok bool, err error) {
	cntOfOld := 0
	cntOfNew := 0

	defer func() {
		if isReduce {
			if ok {
				n.c.cntOfOldReduce = cntOfOld
				n.c.cntOfNewReduce = cntOfNew
				n.c.reduceSQL = sql
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
	var rs1checkSum map[uint32]struct{}
	if EnableApproximateQuerySynthesis {
		rs1checkSum, err = n.c.execQueryForCRC32(querySQL)
	}
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
			rs2, err := n.c.execQueryForCnt(r.Sql)
			if err != nil {
				if dmlIgnoreError(err) {
					//logutil.BgLogger().Warn("ignore error", zap.String("query", r.Sql), zap.Error(err))
					return false, nil
				} else {
					logutil.BgLogger().Error("unexpected error", zap.String("query", r.Sql), zap.Error(err))
					return false, errors.Trace(err)
				}
			}
			rs2checkSum, err := n.c.execQueryForCRC32(r.Sql)
			if err != nil {
				return false, nil
			}

			if (r.IsUpper && mapLess(rs2checkSum, rs1checkSum) && !n.c.execQueryAndCheckEmpty(pinolo.BuildExcept(querySQL, r.Sql))) || (!r.IsUpper && mapMore(rs2checkSum, rs1checkSum) && !n.c.execQueryAndCheckEmpty(pinolo.BuildExcept(r.Sql, querySQL))) {
				cntOfOld = rs1
				cntOfNew = rs2
				n.c.reduceChangedSQL = r.Sql
				return true, nil
			}
		}
	}
	return false, nil
}
