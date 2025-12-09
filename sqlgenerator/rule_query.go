package sqlgenerator

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/cznic/mathutil"
)

var QueryOrCTE = NewFn(func(state *State) Fn {
	return Or(
		Query.W(3),
		CTEQueryStatement,
	)
})

var Query = NewFn(func(state *State) Fn {
	return Or(
		SingleSelect.W(4),
		MultiSelect.W(4),
		UnionSelect.W(4),
		MultiSelectWithSubQuery.W(4),
		MultiSelectWithIndexJoin.W(1),
		InfoSchemaQuery.W(1),
	)
}).P(HasTables)

// infoSchemaExcludes keeps system schemas we typically don't query together with user schemas.
var infoSchemaExcludes = "('INFORMATION_SCHEMA','METRICS_SCHEMA','PERFORMANCE_SCHEMA','mysql')"

var InfoSchemaQuery = NewFn(func(state *State) Fn {
	return Or(
		InfoSchemaTablesQuery,
		InfoSchemaColumnsQuery,
		InfoSchemaSchemataQuery,
		InfoSchemaStatisticsQuery,
	)
})

var InfoSchemaLikePattern = NewFn(func(state *State) Fn {
	return Or(
		Str("'t%'"),
		Str("'test%'"),
		Str("'%col%'"),
		Str("'%a%'"),
		Str("'%1%'"),
	)
})

var InfoSchemaRlikePattern = NewFn(func(state *State) Fn {
	return Or(
		Str("'^t.*'"),
		Str("(?i)'^test.*'"),
		Str("(?i)'.*col.*'"),
		Str("(?i)'.*idx.*'"),
		Str("'.*[0-9].*'"),
	)
})

// Prefer using existing generated table names to raise match probability.
var InfoSchemaTableNameFromState = NewFn(func(state *State) Fn {
	if len(state.Tables) == 0 {
		return Empty
	}
	tbl := state.Tables.Rand()
	if tbl == nil {
		return Empty
	}
	// simple equals, like, rlike with case-insensitive variant
	return Or(
		Strs("and table_name =", fmt.Sprintf("'%s'", tbl.Name)),
		Strs("and table_name like", fmt.Sprintf("'%s%%'", tbl.Name)),
		Strs("and table_name rlike", fmt.Sprintf("(?i)'^%s.*'", tbl.Name)),
	)
})

var InfoSchemaColumnNameFromState = NewFn(func(state *State) Fn {
	if len(state.Tables) == 0 {
		return Empty
	}
	tbl := state.Tables.Rand()
	if tbl == nil || len(tbl.Columns) == 0 {
		return Empty
	}
	col := tbl.Columns.Rand()
	if col == nil {
		return Empty
	}
	return Or(
		Strs("and column_name =", fmt.Sprintf("'%s'", col.Name)),
		Strs("and column_name rlike", fmt.Sprintf("(?i)'^%s.*'", col.Name)),
	)
})

var infoSchemaTableNameFilter = NewFn(func(state *State) Fn {
	return Or(
		And(Str("and table_name like"), InfoSchemaLikePattern),
		And(Str("and table_name rlike"), InfoSchemaRlikePattern),
		InfoSchemaTableNameFromState,
	)
})

var infoSchemaSchemaNameFilter = NewFn(func(state *State) Fn {
	return Or(
		And(Str("and schema_name like"), InfoSchemaLikePattern),
		And(Str("and schema_name rlike"), InfoSchemaRlikePattern),
	)
})

var InfoSchemaTablesQuery = NewFn(func(state *State) Fn {
	return And(
		Strs("select", "table_schema, table_name, table_type"),
		Strs("from", "information_schema.tables"),
		Str("where"),
		Or(
			Str("table_schema = database()"),
			Str("table_schema not in "+infoSchemaExcludes),
		),
		Opt(infoSchemaTableNameFilter),
		Strs("order by", "table_schema, table_name"),
		Strs("limit", RandomNum(5, 50)),
	)
})

var InfoSchemaColumnsQuery = NewFn(func(state *State) Fn {
	return And(
		Strs("select", "table_schema, table_name, column_name, data_type, ordinal_position"),
		Strs("from", "information_schema.columns"),
		Str("where"),
		Or(
			Str("table_schema = database()"),
			Str("table_schema not in "+infoSchemaExcludes),
		),
		Opt(infoSchemaTableNameFilter),
		Opt(InfoSchemaColumnNameFromState),
		Opt(And(Str("and ordinal_position <="), Str(RandomNum(1, 5)))),
		Strs("order by", "table_schema, table_name, ordinal_position"),
		Strs("limit", RandomNum(5, 80)),
	)
})

var InfoSchemaSchemataQuery = NewFn(func(state *State) Fn {
	return And(
		Strs("select", "schema_name, default_character_set_name, default_collation_name"),
		Strs("from", "information_schema.schemata"),
		Str("where schema_name not in "+infoSchemaExcludes),
		Opt(infoSchemaSchemaNameFilter),
		Strs("order by", "schema_name"),
		Strs("limit", RandomNum(3, 30)),
	)
})

var InfoSchemaStatisticsQuery = NewFn(func(state *State) Fn {
	return And(
		Strs("select", "table_schema, table_name, index_name, column_name, seq_in_index, non_unique"),
		Strs("from", "information_schema.statistics"),
		Str("where"),
		Or(
			Str("table_schema = database()"),
			Str("table_schema not in "+infoSchemaExcludes),
		),
		Opt(infoSchemaTableNameFilter),
		Opt(Str("and index_name != 'PRIMARY'")),
		Strs("order by", "table_schema, table_name, index_name, seq_in_index"),
		Strs("limit", RandomNum(5, 80)),
	)
})

var QueryAll = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	orderByAllCols := PrintColumnNamesWithoutPar(tbl.Columns, "")
	return Strs("select * from", tbl.Name, "order by", orderByAllCols)
}).P(HasTables)

var UnionSelect = NewFn(func(state *State) Fn {
	tbl1, tbl2 := state.Tables.Rand(), state.Tables.Rand()
	fieldNum := mathutil.Min(len(tbl1.Columns), len(tbl2.Columns))
	state.env.Table = tbl1
	state.env.QState = &QueryState{FieldNumHint: fieldNum, SelectedCols: map[*Table]QueryStateColumns{
		tbl1: {
			Columns: tbl1.Columns,
			Attr:    make([]string, len(tbl1.Columns)),
		},
	}, AggCols: make(map[*Table]Columns),
	}
	firstSelect, err := CommonSelect.Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	setOpr, err := SetOperator.Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	state.env.Table = tbl2
	state.env.QState = &QueryState{FieldNumHint: fieldNum, SelectedCols: map[*Table]QueryStateColumns{
		tbl2: {
			Columns: tbl2.Columns,
			Attr:    make([]string, len(tbl2.Columns)),
		},
	}, AggCols: make(map[*Table]Columns),
	}
	secondSelect, err := CommonSelect.Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	return Strs(
		"(", firstSelect, ")",
		setOpr,
		"(", secondSelect, ")",
		"order by 1 limit", RandomNum(1, 1000),
	)
})

var SingleSelect = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	state.env.QState = &QueryState{
		SelectedCols: map[*Table]QueryStateColumns{
			tbl: {
				Columns: tbl.Columns,
				Attr:    make([]string, len(tbl.Columns)),
			},
		},
		TableIndexes: map[*Table]Indexes{
			tbl: tbl.Indexes,
		},
		AggCols: make(map[*Table]Columns),
	}
	return CommonSelect
})

var PostHandleWith = NewFn(func(state *State) Fn {
	ctes := state.PopCTE()

	state.env.QState.SelectedCols = map[*Table]QueryStateColumns{}

	// TODO: Pick all the CTEs now, support picking random CTEs later.
	for _, cte := range ctes {
		state.env.QState.SelectedCols[cte] = QueryStateColumns{
			Columns: cte.Columns,
			Attr:    make([]string, len(cte.Columns)),
		}
	}

	return Empty
})

var CTESelect = NewFn(func(state *State) Fn {
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
	return And(WithClause, PostHandleWith, CommonSelect)
})

var MultiSelectWithSubQuery = NewFn(func(state *State) Fn {
	tbl1 := state.Tables.Rand()
	tbl2Str, err := TableSubQuery.Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	sq := state.PopSubQuery()
	sq[0].SubQueryDef = tbl2Str
	state.env.QState = &QueryState{
		SelectedCols: map[*Table]QueryStateColumns{
			tbl1: {
				Columns: tbl1.Columns,
				Attr:    make([]string, len(tbl1.Columns)),
			},
			sq[0]: {
				Columns: sq[0].Columns,
				Attr:    make([]string, len(sq[0].Columns)),
			},
		},
		AggCols: make(map[*Table]Columns),
	}

	return CommonSelect
})

var MultiSelect = NewFn(func(state *State) Fn {
	tbl1 := state.Tables.Rand()
	tbl2 := state.Tables.Rand()
	state.env.QState = &QueryState{
		SelectedCols: map[*Table]QueryStateColumns{
			tbl1: {
				Columns: tbl1.Columns,
				Attr:    make([]string, len(tbl1.Columns)),
			},
			tbl2: {
				Columns: tbl2.Columns,
				Attr:    make([]string, len(tbl2.Columns)),
			},
		},
		AggCols: make(map[*Table]Columns),
	}
	return CommonSelect
})

var NonAggSelect = NewFn(func(state *State) Fn {
	return And(
		Str("select"), HintTiFlash, Or(HintIndexMerge, HintUseIndex), HintJoin,
		SelectFields, Str("from"), TableReference,
		WhereClause, Opt(OrderBy), Opt(Limit),
	)
})

var GroupByColumns = NewFn(func(state *State) Fn {
	aggColsMap := state.env.QState.AggCols
	if len(aggColsMap) == 0 {
		return Empty
	}
	if state.env.QueryHint == hintSingleValue {
		return Empty
	}
	groupByItems := make([]string, 0)
	for t, cols := range aggColsMap {
		for _, col := range cols {
			groupByItems = append(groupByItems, fmt.Sprintf("%s.%s", t.Name, col.Name))
		}
	}
	return Strs("group by", strings.Join(groupByItems, ","))
})

var AggSelect = NewFn(func(state *State) Fn {
	state.env.QState.IsAgg = true
	// Choose aggregate columns.
	// TODO: support expression
	groupByColsCnt := 1 + rand.Intn(3)
	tbl := state.env.QState.GetRandTable()
	groupByCols := tbl.Columns.RandGiveN(groupByColsCnt)
	state.env.QState.AggCols[tbl] = groupByCols

	return And(
		Str("select"), HintTiFlash, Or(HintIndexMerge, HintUseIndex), Opt(HintAggToCop), HintJoin,
		SelectFields, Str("from"), TableReference,
		WhereClause, GroupByColumns, WindowClause, HavingOpt, Opt(OrderBy), Opt(Limit),
	)
})

var CommonSelect = NewFn(func(state *State) Fn {
	NotNil(state.env.QState)
	if state.env.QueryHint == hintSingleValue {
		if rand.Intn(10) == 0 || state.GetWeight(AggSelect) == 0 {
			return NonAggSelect
		}
		return AggSelect
	}
	return Or(
		NonAggSelect,
		AggSelect,
	)
})

var SelectFields = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	if queryState.FieldNumHint == 0 {
		queryState.FieldNumHint = 1 + rand.Intn(5)
	}
	if state.env.QueryHint == hintSingleValue {
		queryState.FieldNumHint = 1
	}
	var fns []Fn
	for i := 0; i < queryState.FieldNumHint; i++ {
		fieldID := fmt.Sprintf("r%d", i)
		fns = append(fns, NewFn(func(state *State) Fn {
			state.env.Table = queryState.GetRandTable()
			state.env.QColumns = queryState.SelectedCols[state.env.Table]
			return And(SelectField, Str("as"), Str(fieldID))
		}))
		if i != queryState.FieldNumHint-1 {
			fns = append(fns, Str(","))
		}
	}
	return And(fns...)
})

var SelectField = NewFn(func(state *State) Fn {
	NotNil(state.env.Table)
	NotNil(state.env.QColumns)
	if state.env.QState.IsAgg {
		if len(state.env.QState.AggCols[state.env.Table]) == 0 {
			return AggFunction
		}
		return Or(
			AggFunction,
			BuiltinFunction,
			SelectFieldName,
			//WindowFunctionOverW,
		)
	}
	return Or(
		BuiltinFunction,
		SelectFieldName,
	)
})

var SelectFieldName = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	if state.env.QState.IsAgg {
		c := state.env.QState.AggCols[tbl].Rand()
		return Str(fmt.Sprintf("%s.%s", tbl.Name, c.Name))
	}
	cols := state.env.QColumns
	c := cols.Rand()
	return Str(fmt.Sprintf("%s.%s", tbl.Name, c.Name))
})

var TableReference = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	tbNames := make([]Fn, 0, len(queryState.SelectedCols))
	for t := range queryState.SelectedCols {
		if t.SubQueryDef != "" {
			tbNames = append(tbNames, Str(t.SubQueryDef))
			continue
		}
		tbNames = append(tbNames, Str(t.Name))
	}
	if len(tbNames) == 1 {
		return tbNames[0]
	}
	return Or(
		Join(tbNames, Str(",")),
		And(Join(tbNames, JoinType), Str("on"), JoinPredicate),
	)
})

var JoinType = NewFn(func(state *State) Fn {
	return Or(
		// TODO: enable outer join
		//Str("left join"),
		//Str("right join"),
		Str("join"),
	)
})

var JoinPredicate = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	var (
		preds     []string
		prevTable *Table
		prevCol   *Column
	)
	for t, cols := range queryState.SelectedCols {
		col := cols.Rand()
		if prevTable != nil {
			preds = append(preds,
				fmt.Sprintf("%s.%s = %s.%s",
					prevTable.Name, prevCol.Name,
					t.Name, col.Name))
		}
		prevTable = t
		prevCol = col
	}
	return Str(strings.Join(preds, " and "))
})

var GroupByColumnsOpt = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	var groupByItems []string
	for t, scs := range queryState.SelectedCols {
		for i, c := range scs.Columns {
			if scs.Attr[i] == QueryAggregation {
				groupByItems = append(groupByItems, fmt.Sprintf("%s.%s", t.Name, c.Name))
			}
		}
	}
	if len(groupByItems) == 0 {
		return Empty
	}
	return Opt(Strs("group by", strings.Join(groupByItems, ",")))
})

var WhereClause = NewFn(func(state *State) Fn {
	return Or(
		Empty,
		And(Str("where"), Or(Predicates, Predicate)).W(3),
	)
})

var HintNPlan = NewFn(func(state *State) Fn {
	i := strconv.Itoa(rand.Intn(10))
	return Or(Empty, Str("/*+ nth_plan("+i+") */"))
})

var HintJoin = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	if len(queryState.SelectedCols) != 2 {
		return Empty
	}
	var tbl []*Table
	for t := range queryState.SelectedCols {
		tbl = append(tbl, t)
	}
	t1, t2 := tbl[0], tbl[1]
	return Or(
		Strs("/*+  */"),
		Strs("/*+ merge_join(", t1.Name, ",", t2.Name, "*/"),
		Strs("/*+ NO_MERGE_JOIN(", t1.Name, ",", t2.Name, "*/"),
		Strs("/*+ hash_join(", t1.Name, ",", t2.Name, "*/"),
		Strs("/*+ inl_join(", t1.Name, ") */"),
		Strs("/*+ inl_join(", t2.Name, ") */"),
		Strs("/*+ inl_hash_join(", t1.Name, ",", t2.Name, ") */"),
		Strs("/*+ HASH_JOIN_BUILD(", t1.Name, ") */"),
		Strs("/*+ HASH_JOIN_PROBE(", t1.Name, ") */"),
		Strs("/*+ NO_HASH_JOIN(", t1.Name, ",", t2.Name, "*/"),
		//Strs("/*+ inl_merge_join(", t1.Name, ",", t2.Name, ") */"),
	)
})

var WindowClause = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	if !queryState.IsWindow {
		return Empty
	}
	for t := range queryState.SelectedCols {
		state.env.Table = t
	}
	return And(
		Str("window w as"),
		Str("("),
		Opt(WindowPartitionBy),
		WindowOrderBy,
		Opt(WindowFrame),
		Str(")"),
	)
})

var WindowPartitionBy = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	cols := tbl.Columns.RandNNotNil()
	return Strs("partition by", PrintColumnNamesWithoutPar(cols, ""))
})

var WindowOrderBy = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	cols := tbl.Columns.RandNNotNil()
	return Strs("order by", PrintColumnNamesWithoutPar(cols, ""))
})

var WindowFrame = NewFn(func(state *State) Fn {
	frames := []string{
		fmt.Sprintf("%d preceding", rand.Intn(5)),
		"current row",
		fmt.Sprintf("%d following", rand.Intn(5)),
	}
	get := func(idx int) interface{} { return frames[idx] }
	set := func(idx int, v interface{}) { frames[idx] = v.(string) }
	Move(rand.Intn(len(frames)), 0, get, set)
	return Strs("rows between", frames[1], "and", frames[2])
})

var WindowFunctionOverW = NewFn(func(state *State) Fn {
	NotNil(state.env.QState)
	return And(WindowFunction, Str("over w"))
}).P(func(state *State) bool {
	queryState := state.env.QState
	return len(queryState.SelectedCols) == 1
})

var WindowFunction = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	queryState.IsWindow = true
	var tbl *Table
	for t := range queryState.SelectedCols {
		tbl = t
	}
	col := Str(fmt.Sprintf("%s.%s", tbl.Name, tbl.Columns.Rand().Name))
	num := Str(RandomNum(1, 6))
	return Or(
		Str("row_number()"),
		Str("rank()"),
		Str("dense_rank()"),
		Str("cume_dist()"),
		Str("percent_rank()"),
		Strf("ntile([%fn])", num),
		Strf("lead([%fn],[%fn],NULL)", col, num),
		Strf("lag([%fn],[%fn],NULL)", col, num),
		Strf("first_value([%fn])", col),
		Strf("last_value([%fn])", col),
		Strf("nth_value([%fn],[%fn])", col, num),
	)
})

var Predicates = NewFn(func(state *State) Fn {
	var pred []string
	for i := 0; i < 1+rand.Intn(2); i++ {
		if i != 0 {
			andor, err := AndOr.Eval(state)
			if err != nil {
				return NoneBecauseOf(err)
			}
			pred = append(pred, andor)
		}
		if state.env.QState != nil {
			state.env.Table = state.env.QState.GetRandTable()
		} else if state.env.Table == nil {
			state.env.Table = state.GetRandTableOrCTE()
		}
		state.env.Column = state.env.Table.Columns.Rand()
		p, err := Predicate.Eval(state)
		if err != nil {
			return NoneBecauseOf(err)
		}
		pred = append(pred, p)
	}
	return Str(strings.Join(pred, " "))
})

var HavingPredicate = NewFn(func(state *State) Fn {
	if state.env.QState != nil {
		state.env.Table = state.env.QState.GetRandTable()
	} else if state.env.Table == nil {
		state.env.Table = state.Tables.Rand()
	}
	// choose a table with agg columns.
	for len(state.env.QState.AggCols[state.env.Table]) == 0 {
		state.env.Table = state.env.QState.GetRandTable()
	}
	state.env.Column = state.env.QState.AggCols[state.env.Table].Rand()
	tbl := state.env.Table
	randCol := state.env.Column
	colName := fmt.Sprintf("%s.%s", tbl.Name, randCol.Name)
	var pre Fn
	noJsonPre := Or(
		And(Str(colName), CompareSymbol, RandVal),
		And(Str(colName), Str("in"), Str("("), InValues, Str(")")),
		And(Str("IsNull("), Str(colName), Str(")")),
		And(Str(colName), Str("between"), RandVal, Str("and"), RandVal),
	)
	if state.env.Column.Tp == ColumnTypeJSON {
		pre = Or(
			And(Str(colName), CompareSymbol, RandVal),
			JSONPredicate,
		)
	} else {
		pre = noJsonPre
	}
	return Or(
		pre.W(5),
		And(Str("not("), pre, Str(")")),
	)
})

var HavingPredicates = NewFn(func(state *State) Fn {
	var pred []string
	for i := 0; i < 1+rand.Intn(2); i++ {
		if i != 0 {
			andor, err := AndOr.Eval(state)
			if err != nil {
				return NoneBecauseOf(err)
			}
			pred = append(pred, andor)
		}
		p, err := HavingPredicate.Eval(state)
		if err != nil {
			return NoneBecauseOf(err)
		}
		pred = append(pred, p)
	}
	return Str(strings.Join(pred, " "))
})

var Predicates2 Fn

var HavingOpt = NewFn(func(state *State) Fn {
	return Or(
		Empty,
		And(Str("having"), Or(HavingPredicates, HavingPredicates)).W(3),
	)
})

func init() {
	Predicates2 = Predicates
}

var Predicate = NewFn(func(state *State) Fn {
	if state.env.QState != nil {
		state.env.Table = state.env.QState.GetRandTable()
	} else if state.env.Table == nil {
		state.env.Table = state.Tables.Rand()
	}
	state.env.Column = state.env.Table.Columns.Rand()
	tbl := state.env.Table
	randCol := state.env.Column
	if tbl == nil {
		return NoneBecauseOf(fmt.Errorf("table is nil"))
	}
	if randCol == nil {
		return NoneBecauseOf(fmt.Errorf("column is nil, table %s", tbl.Name))
	}
	colName := fmt.Sprintf("%s.%s", tbl.Name, randCol.Name)
	var pre Fn

	fns := []Fn{
		And(Str(colName), CompareSymbol, RandVal),
		And(Str(colName), Str("in"), Str("("), InValues, Str(")")),
		And(Str("IsNull("), Str(colName), Str(")")),
		And(Str(colName), Str("between"), RandVal, Str("and"), RandVal),
	}
	if state.GetWeight(ScalarSubQuery) != 0 {
		fns = append(fns, And(Str(colName), CompareSymbol, ScalarSubQuery))
	}
	noJsonPre := Or(
		fns...,
	)
	if state.env.Column.Tp == ColumnTypeJSON {
		pre = Or(
			And(Str(colName), CompareSymbol, RandVal),
			JSONPredicate,
		)
	} else {
		pre = noJsonPre
	}
	return Or(
		pre.W(5),
		And(Str("not("), pre, Str(")")),
	)
})

var JSONPredicate = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	randCol := state.env.Column
	colName := fmt.Sprintf("%s.%s", tbl.Name, randCol.Name)
	arv, err := ArrayRandVal.Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	jsContainVal := "'" + strings.Trim(arv, "'") + "'"

	pre := Or(
		And(Str(arv), Str("MEMBER OF"), Str("("), Str(colName), Str(")")),
		And(Str("JSON_CONTAINS("), Str(colName), Str(","), Str(jsContainVal), Str(")")),
		//And(Str("JSON_CONTAINS("), ArrayRandVal, Str(","), Str(colName), Str(")")),
		And(Str("JSON_OVERLAPS("), Str(colName), Str(","), RandVal, Str(")")),
		//And(Str("JSON_OVERLAPS("), RandVal, Str(","), Str(colName), Str(")")),
		And(Str("IsNull("), Str("JSON_OVERLAPS("), RandVal, Str(","), Str(colName), Str(")"), Str(")")),
	)
	return Or(
		pre,
		And(Str("not("), pre, Str(")")),
	)
})

var InValues = NewFn(func(state *State) Fn {
	if len(state.Tables) <= 1 {
		return RandColVals
	}
	return Or(
		RandColVals,
		SubSelect,
	)
})

var RandColVals = NewFn(func(state *State) Fn {
	return Repeat(RandVal.R(1, 5), Str(","))
})

var ArrayRandVal = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	randCol := state.env.Column
	var v string
	if len(tbl.Values) == 0 || rand.Intn(3) == 0 {
		v = randomArrayJSONSubValue(randCol.SubType)
	} else {
		v = tbl.GetRandArraySubVal(randCol)
	}
	if len(v) == 0 {
		v = randomArrayJSONSubValue(randCol.SubType)
	}
	return Str(v)
})

var RandVal = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	randCol := state.env.Column
	var v string
	if len(tbl.Values) == 0 || rand.Intn(3) == 0 {
		v = randCol.RandomValue()
	} else {
		v = tbl.GetRandRowVal(randCol)
	}
	if len(v) == 0 {
		v = randCol.RandomValue()
	}
	return Str(v)
})

var SubSelect = NewFn(func(state *State) Fn {
	tbl := state.Env().Table
	availableTbls := state.Tables
	if state.Env().IsIn("CommonDelete") || state.Env().IsIn("CommonUpdate") {
		availableTbls = availableTbls.Filter(func(t *Table) bool {
			return t.ID != tbl.ID
		})
	}
	subTbl := availableTbls.Rand()
	subCol := subTbl.Columns.Rand()
	return And(
		Str("select"), Str(subCol.Name), Str("from"), Str(subTbl.Name),
		Str("where"), Predicates2,
	)
})

var SubSelectWithGivenTp = NewFn(func(state *State) Fn {
	randCol := state.env.Column
	subTbl, subCol := GetRandTableColumnWithTp(state.Tables, randCol.Tp)
	return And(
		Str("select"), Str(subCol.Name), Str("from"), Str(subTbl.Name),
		Str("where"), Predicate,
	)
}).P(HasSameColumnType)

var ForUpdateOpt = NewFn(func(state *State) Fn {
	return Opt(Str("for update"))
})

var HintTiFlash = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	var tbs []string
	for t := range queryState.SelectedCols {
		if t.TiflashReplica > 0 {
			tbs = append(tbs, t.Name)
		}
	}
	if len(tbs) == 0 {
		return Empty
	}
	return Strs("/*+ read_from_storage(tiflash[", strings.Join(tbs, ","), "]) */")
})

var HintIndexMerge = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	var tbs []string
	for t := range queryState.SelectedCols {
		tbs = append(tbs, t.Name)
	}
	return Strs("/*+ use_index_merge(", strings.Join(tbs, ","), ") */")
})

var HintAggToCop = NewFn(func(state *State) Fn {
	return And(
		Str("/*+"),
		Opt(Str("agg_to_cop()")),
		Or(Empty, Str("hash_agg()"), Str("stream_agg()")),
		Str("*/"),
	)
})

// It only works with singleSelect
var HintUseIndex = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	if queryState.TableIndexes == nil {
		return Empty
	}
	var tbs []*Table
	for t := range queryState.SelectedCols {
		tbs = append(tbs, t)
	}
	tb := tbs[rand.Int()%len(tbs)]
	if rand.Int()%2 == 0 && len(queryState.TableIndexes[tb]) != 0 {
		return Strs("/*+ use_index(", tb.Name, ",", queryState.TableIndexes[tb].Rand().Name, ") */")
	} else {
		return Strs("/*+ use_index(", tb.Name, ") */")
	}
})

var SetOperator = NewFn(func(state *State) Fn {
	return Or(
		Str("union"),
		Str("union all"),
		Str("except"),
		Str("intersect"),
	)
})

var OrderBy = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	var fields strings.Builder
	if queryState == nil {
		return Empty
	}
	for i := 0; i < queryState.FieldNumHint; i++ {
		if i != 0 {
			fields.WriteString(",")
		}
		fields.WriteString(fmt.Sprintf("r%d", i))
	}
	return Strs("order by", fields.String())
})

var Limit = NewFn(func(state *State) Fn {
	return Strs("limit", RandomNum(100000000, 1000000000))

	//return Strs("limit", RandomNum(1000000, 2147483646))
})

var Query2 Fn

func init() {
	Query2 = Query
}

var SubQuery = NewFn(func(state *State) Fn {
	return And(Str("("), Query, Str(")"))
})

var ScalarSubQuery = NewFn(func(state *State) Fn {
	state.env.QueryHint = hintSingleValue
	return And(Str("("), Query2, Str(")"))
})

var TableSubQuery = NewFn(func(state *State) Fn {
	state.IncSubQueryDeep()
	st := state.GenSubQuery()
	def, err := SingleSelect.Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	var cts []ColumnType
	cts, err = getTypeOfExpressions(def, "test", state.tableMeta)
	if err != nil {
		return NoneBecauseOf(err)
	}
	for _, t := range cts {
		st.AppendColumn(state.GenNewColumnWithType(t))
	}
	// Reset column name
	for i, c := range st.Columns {
		c.Name = fmt.Sprintf("r%d", i)
	}
	state.PushSubQuery(st)

	return And(Str("("), Str(def), Str(") "), Str(st.Name))
})
