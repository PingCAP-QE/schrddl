package sqlgenerator

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	_ "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mock"
	"go.uber.org/zap"
)

// a replacement to avoid initialization loop
var CTEQueryStatementReplacement Fn

var CTEQueryStatement = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	state.env.QState = &QueryState{
		SelectedCols: map[*Table]QueryStateColumns{
			tbl: {
				Columns: tbl.Columns,
				Attr:    make([]string, len(tbl.Columns)),
			},
		},
		AggCols: make(map[*Table]Columns),
	}
	return And(WithClause, SimpleCTEQuery)
})

var CTEDMLStatement = NewFn(func(state *State) Fn {
	state.ctes = state.ctes[:0]
	return And(
		WithClause,
		DMLStmt,
	)
})

var SimpleCTEQuery = NewFn(func(state *State) Fn {
	parentCTE := state.ParentCTE()
	ctes := state.PopCTE()

	state.env.QState.SelectedCols = map[*Table]QueryStateColumns{}
	for _, cte := range ctes {
		state.env.QState.SelectedCols[cte] = QueryStateColumns{
			Columns: cte.Columns,
			Attr:    make([]string, len(cte.Columns)),
		}
	}

	if rand.Intn(10) == 0 {
		c := rand.Intn(len(ctes))
		for i := 0; i < c; i++ {
			ctes = append(ctes, ctes[rand.Intn(len(ctes))])
		}
	}

	rand.Shuffle(len(ctes), func(i, j int) {
		ctes[j], ctes[i] = ctes[i], ctes[j]
	})

	//ctes = ctes[:rand.Intn(mathutil.Min(len(ctes), 2))+1]

	cteNames := make([]string, 0, len(ctes))
	colsInfo := make(map[ColumnType][]string)
	colNames := make([]string, 0)
	colNames = append(colNames, "1")
	for i := range ctes {
		//ctes[i].AsName = fmt.Sprintf("cte_as_%d", state.alloc.AllocCTEID())
		cteNames = append(cteNames, fmt.Sprintf("%s", ctes[i].Name))
		for _, col := range ctes[i].Columns {
			if _, ok := colsInfo[col.Tp]; !ok {
				colsInfo[col.Tp] = make([]string, 0)
			}
			colsInfo[col.Tp] = append(colsInfo[col.Tp], fmt.Sprintf("%s", col.Name))
			colNames = append(colNames, fmt.Sprintf("%s", col.Name))
		}
	}

	orderByFields := make([]string, len(colNames))
	for i := range orderByFields {
		orderByFields[i] = fmt.Sprintf("%d", i+1)
	}
	return And(
		Str("("),
		Str("select"),
		Str(strings.Join(colNames, ",")),
		Str("from"),
		Str(strings.Join(cteNames, ",")),
		Str("where"),
		Predicates,
		//If(rand.Intn(10) == 0,
		//	And(
		//		Str("where exists ("),
		//		Query,
		//		Str(")"),
		//	),
		//),
		If(parentCTE == nil,
			And(
				Str("order by"),
				Str(strings.Join(orderByFields, ",")),
			),
		),
		Opt(Limit),
		Str(")"),
	)
})

var WithClause = NewFn(func(state *State) Fn {
	//validSQLPercent := 75
	state.IncCTEDeep()
	return And(
		Str("with"),
		//Or(
		//	If(ShouldValid(validSQLPercent), Str("recursive")),
		//	Str("recursive"),
		//),
		Repeat(CTEDefinition.R(1, 2), Str(",")),
		//CTEDefinition,
	)
})

var CTEDefinition = NewFn(func(state *State) Fn {
	validSQLPercent := 100
	cte := state.GenNewCTE()
	//colCnt := state.ParentCTEColCount()
	//if colCnt == 0 {
	//	colCnt = 2
	//}
	//cte.AppendColumn(state.GenNewColumnWithType(ColumnTypeInt))
	//for i := 0; i < colCnt+rand.Intn(2); i++ {
	//	cte.AppendColumn(state.GenNewColumnWithType(ColumnTypeInt, ColumnTypeChar))
	//}
	if !ShouldValid(validSQLPercent) {
		if RandomBool() && state.GetCTECount() != 0 {
			cte.Name = state.GetRandomCTE().Name
		} else {
			cte.Name = state.Tables.Rand().Name
		}
	}
	state.PushCTE(cte)

	tbl1 := state.Tables.Rand()
	state.env.QState = &QueryState{
		SelectedCols: map[*Table]QueryStateColumns{
			tbl1: {
				Columns: tbl1.Columns,
				Attr:    make([]string, len(tbl1.Columns)),
			},
		}, AggCols: make(map[*Table]Columns),
	}
	if rand.Intn(2) == 0 {
		tbl2 := state.Tables.Rand()
		state.env.QState.SelectedCols[tbl2] = QueryStateColumns{
			Columns: tbl2.Columns,
			Attr:    make([]string, len(tbl2.Columns)),
		}
	}

	ctedef, err := CTEExpressionParens.Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}

	return And(
		Str(cte.Name),
		Strs("(", PrintColumnNamesWithoutPar(cte.Columns, ""), ")"),
		Str("AS"),
		Str(ctedef),
	)
})

func evalTypeToColumnType(evalType types.EvalType) ColumnType {
	switch evalType {
	case types.ETInt:
		return ColumnTypeBigInt
	case types.ETReal:
		return ColumnTypeDouble
	case types.ETDecimal:
		return ColumnTypeDecimal
	case types.ETString:
		return ColumnTypeVarchar
	case types.ETDatetime:
		return ColumnTypeDatetime
	case types.ETTimestamp:
		return ColumnTypeTimestamp
	case types.ETDuration:
		return ColumnTypeTime
	case types.ETJson:
		return ColumnTypeJSON
	default:
		panic(fmt.Sprintf("unknown eval type %d", evalType))
	}
}

func getTypeOfExpressions(sql string, dbName string, schemas []*model.TableInfo) ([]ColumnType, error) {
	cols := make([]*expression.Column, 0)
	var names types.NameSlice
	for _, tbl := range schemas {
		column, name, err := expression.ColumnInfos2ColumnsAndNames(mock.NewContext(), model.NewCIStr(dbName), tbl.Name, tbl.Cols(), tbl)
		if err != nil {
			return nil, err
		}
		cols = append(cols, column...)
		for _, n := range name {
			names = append(names, n)
		}
	}
	stmts, _, err := parser.New().ParseSQL(sql)
	if err != nil {
		return nil, err
	}

	ts := make([]ColumnType, 0)
	fields := stmts[0].(*ast.SelectStmt).Fields.Fields
	buildOptions := expression.WithInputSchemaAndNames(expression.NewSchema(cols...), names, nil)
	for _, field := range fields {
		// A temporarily workaround for building aggregation expression
		if aggFunc, ok := field.Expr.(*ast.AggregateFuncExpr); field.Expr != nil && ok {
			newArgList := make([]expression.Expression, 0, len(aggFunc.Args))
			for _, arg := range aggFunc.Args {
				expr, err := expression.BuildSimpleExpr(mock.NewContext(), arg, buildOptions)
				if err != nil {
					return nil, err
				}
				newArgList = append(newArgList, expr)
			}
			newFunc, err := aggregation.NewAggFuncDesc(mock.NewContext(), aggFunc.F, newArgList, aggFunc.Distinct)
			if err != nil {
				return nil, err
			}
			ts = append(ts, evalTypeToColumnType(newFunc.RetTp.EvalType()))
			continue
		}

		expr, err := expression.BuildSimpleExpr(mock.NewContext(), field.Expr, buildOptions)
		if err != nil {
			return nil, err
		}
		ts = append(ts, evalTypeToColumnType(expr.GetType(mock.NewContext()).EvalType()))
	}

	return ts, nil
}

var CTESeedPart = NewFn(func(state *State) Fn {
	//validSQLPercent := 100
	//tbl := state.Tables.Rand()
	currentCTE := state.CurrentCTE()

	var cteDef string
	var err error
	var ts []ColumnType
	maxTry := 100
	for i := 0; i < maxTry; i++ {
		cteDef, err = CommonSelect.Eval(state)
		//logutil.BgLogger().Warn("cte seed part", zap.String("cteDef", cteDef))
		if err != nil {
			continue
		}
		ts, err = getTypeOfExpressions(cteDef, "test", state.tableMeta)
		if err == nil {
			break
		}
	}
	if err != nil {
		logutil.BgLogger().Warn("cte seed part", zap.String("cteDef", cteDef))
		return NoneBecauseOf(err)
	}
	//cteDef, err := CommonSelect.Eval(state)
	////logutil.BgLogger().Warn("cte seed part", zap.String("cteDef", cteDef))
	//if err != nil {
	//	return NoneBecauseOf(err)
	//}
	//ts, err := getTypeOfExpressions(cteDef, "test", state.tableMeta)
	//if err != nil {
	//	logutil.BgLogger().Warn("get type of expressions failed", zap.String("cteDef", cteDef))
	//	return NoneBecauseOf(err)
	//}
	for _, t := range ts {
		currentCTE.AppendColumn(state.GenNewColumnWithType(t))
	}

	//fields := make([]string, len(currentCTE.Columns)-1)
	//for i := range fields {
	//	switch rand.Intn(4) {
	//	case 0, 3:
	//		cols := tbl.Columns.Filter(func(column *Column) bool {
	//			return column.Tp == currentCTE.Columns[i+1].Tp
	//		})
	//		if len(cols) != 0 {
	//			fields[i] = cols[rand.Intn(len(cols))].Name
	//			continue
	//		}
	//		fallthrough
	//	case 1:
	//		fields[i] = currentCTE.Columns[i+1].RandomValue()
	//	case 2:
	//		if ShouldValid(validSQLPercent) {
	//			fields[i] = PrintConstantWithFunction(currentCTE.Columns[i+1].Tp)
	//		} else {
	//			fields[i] = fmt.Sprintf("a") // for unknown column
	//		}
	//	}
	//}
	//
	//if !ShouldValid(validSQLPercent) {
	//	fields = append(fields, "1")
	//}

	return Str(cteDef)

	//return Or(
	//	//CommonSelect,
	//	And(
	//		Str("select 1,"),
	//		Str(strings.Join(fields, ",")),
	//		Str("from"),
	//		Str(tbl.Name), // todo: it can refer the exist cte and the common table
	//	).W(5),
	//	CTEQueryStatementReplacement,
	//)
})

var CTERecursivePart = NewFn(func(state *State) Fn {
	validSQLPercent := 75
	lastCTE := state.CurrentCTE()
	if !ShouldValid(validSQLPercent) {
		lastCTE = state.GetRandomCTE()
	}
	fields := append(make([]string, 0, len(lastCTE.Columns)), fmt.Sprintf("%s + 1", lastCTE.Columns[0].Name))
	for _, col := range lastCTE.Columns[1:] {
		fields = append(fields, PrintColumnWithFunction(col))
	}
	if !ShouldValid(validSQLPercent) {
		rand.Shuffle(len(fields[1:]), func(i, j int) {
			fields[1+i], fields[1+j] = fields[1+j], fields[1+i]
		})
		if rand.Intn(20) == 0 {
			fields = append(fields, "1")
		}
	}

	// todo: recursive part can be a function, const
	return Or(
		And(
			Str("select"),
			Str(strings.Join(fields, ",")),
			Str("from"),
			Str(lastCTE.Name), // todo: it also can be a cte
			Str("where"),
			Str(fmt.Sprintf("%s < %d", lastCTE.Columns[0].Name, 5)),
			Opt(And(Str("limit"), Str(RandomNum(0, 20)))),
		),
	)
})

var CTEExpressionParens = NewFn(func(state *State) Fn {
	return And(
		Str("("),
		CTESeedPart,
		//Opt(
		//	And(
		//		Str("UNION"),
		//		UnionOption,
		//		CTERecursivePart,
		//	),
		//),
		Str(")"))
})

var UnionOption = NewFn(func(state *State) Fn {
	return Or(
		Empty,
		Str("DISTINCT"),
		Str("ALL"),
	)
})

func init() {
	CTEQueryStatementReplacement = CTEQueryStatement
}
