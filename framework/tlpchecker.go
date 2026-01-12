package framework

import (
	"github.com/PingCAP-QE/schrddl/tlp"
	"github.com/PingCAP-QE/schrddl/util"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"strings"
)

type tlpChecker struct {
	c        *testCase
	rewriter tlp.TlpRewriter
	sb       strings.Builder
}

func (t *tlpChecker) check(sql string, isReduce bool) (ok bool, err error) {
	cntOfOld := 0
	cntOfNew := 0
	var newQuery string

	cntOfP := 0
	cntOfN := 0
	cntOfNull := 0
	cntOfAll := 0
	negativeQuery := ""
	isNullQuery := ""
	allQuery := ""

	defer func() {
		if isReduce {
			if ok {
				t.c.cntOfOldReduce = cntOfOld
				t.c.cntOfNewReduce = cntOfNew
				t.c.reduceSQL = sql
				t.c.reduceChangedSQL = newQuery

				t.c.cntOfP = cntOfP
				t.c.cntOfN = cntOfN
				t.c.cntOfNull = cntOfNull
				t.c.cntOfAll = cntOfAll
				t.c.nQuery = negativeQuery
				t.c.nullQuery = isNullQuery
				t.c.allQuery = allQuery
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
	negativeQuery = t.sb.String()
	rs2, err := t.c.execQueryForCRC32(negativeQuery)
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
	t.rewriter.SetGenerateIsNull()
	newStmt, _ = util.CloneAst(stmt).Accept(&t.rewriter)
	if !t.rewriter.Valid() {
		// No predicate, continue
		return false, nil
	}
	t.sb.Reset()
	err = newStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutDefaultCharset, &t.sb))
	if err != nil {
		return false, errors.Trace(err)
	}
	isNullQuery = t.sb.String()
	rs3, err := t.c.execQueryForCRC32(isNullQuery)
	if err != nil {
		if dmlIgnoreError(err) {
			return false, nil
		} else {
			logutil.BgLogger().Error("unexpected error", zap.String("query", querySQL), zap.Error(err))
			return false, err
		}
	}

	t.rewriter.Reset()
	t.rewriter.SetGenerateAll()
	newStmt, _ = util.CloneAst(stmt).Accept(&t.rewriter)
	if !t.rewriter.Valid() {
		// No predicate, continue
		return false, nil
	}
	t.sb.Reset()
	err = newStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutDefaultCharset, &t.sb))
	if err != nil {
		return false, errors.Trace(err)
	}
	allQuery = t.sb.String()
	rsAll, err := t.c.execQueryForCRC32(allQuery)
	if err != nil {
		if dmlIgnoreError(err) {
			return false, nil
		} else {
			logutil.BgLogger().Error("unexpected error", zap.String("query", querySQL), zap.Error(err))
			return false, err
		}
	}

	cntOfP = len(rs1)
	cntOfN = len(rs2)
	cntOfNull = len(rs3)
	cntOfAll = len(rsAll)
	unionMap := mapUnion(mapUnion(rs1, rs2), rs3)
	return !mapEqual(unionMap, rsAll), nil
}

func mapUnion(ml, mr map[uint32]struct{}) map[uint32]struct{} {
	if len(ml) == 0 {
		return mr
	}
	if len(mr) == 0 {
		return ml
	}
	for k := range mr {
		ml[k] = struct{}{}
	}
	return ml
}

func mapEqual(ml, mr map[uint32]struct{}) bool {
	if len(ml) != len(mr) {
		return false
	}
	for k := range ml {
		if _, ok := mr[k]; !ok {
			mls := make([]uint32, 0, len(ml))
			mrs := make([]uint32, 0, len(mr))
			for k1 := range ml {
				mls = append(mls, k1)
			}
			for k2 := range mr {
				mrs = append(mrs, k2)
			}
			slices.Sort(mls)
			slices.Sort(mrs)
			logutil.BgLogger().Warn("found inconsistent", zap.Uint32("k in ml", k))
			for i := range mls {
				logutil.BgLogger().Warn("mls", zap.Uint32("mls", mls[i]))
			}
			for i := range mrs {
				logutil.BgLogger().Warn("mrs", zap.Uint32("mrs", mrs[i]))
			}
			return false
		}
	}
	return true
}
