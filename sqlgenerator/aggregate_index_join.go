package sqlgenerator

import (
	"fmt"
	"math/rand"
	"sync"
)

type JoinColumn struct {
	outerTable   *Table
	innerTable   *Table
	outerColumns []*Column
	innerColumns []*Column
}

var allJoinColumns sync.Map

var indexJoinType = map[ColumnType][]ColumnType{
	ColumnTypeBoolean: {
		ColumnTypeBoolean, ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt,
		ColumnTypeBigInt, ColumnTypeDouble, ColumnTypeDecimal, ColumnTypeYear, ColumnTypeBit},
	ColumnTypeTinyInt: {
		ColumnTypeBoolean, ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt,
		ColumnTypeBigInt, ColumnTypeDouble, ColumnTypeDecimal, ColumnTypeYear, ColumnTypeBit},
	ColumnTypeSmallInt: {
		ColumnTypeBoolean, ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt,
		ColumnTypeBigInt, ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDecimal, ColumnTypeYear, ColumnTypeBit},
	ColumnTypeMediumInt: {
		ColumnTypeBoolean, ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt,
		ColumnTypeBigInt, ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDecimal, ColumnTypeYear, ColumnTypeBit},
	ColumnTypeInt: {
		ColumnTypeBoolean, ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt,
		ColumnTypeBigInt, ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDecimal, ColumnTypeYear, ColumnTypeBit},
	ColumnTypeBigInt: {
		ColumnTypeBoolean, ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt,
		ColumnTypeBigInt, ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDecimal, ColumnTypeYear, ColumnTypeBit},
	ColumnTypeFloat:   {ColumnTypeFloat, ColumnTypeDouble},
	ColumnTypeDouble:  {ColumnTypeFloat, ColumnTypeDouble},
	ColumnTypeDecimal: {ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDecimal},
	ColumnTypeChar: {
		ColumnTypeFloat, ColumnTypeDouble, ColumnTypeChar, ColumnTypeVarchar,
		ColumnTypeDate, ColumnTypeDatetime, ColumnTypeTimestamp},
	ColumnTypeVarchar: {
		ColumnTypeFloat, ColumnTypeDouble, ColumnTypeChar, ColumnTypeVarchar,
		ColumnTypeDate, ColumnTypeDatetime, ColumnTypeTimestamp},
	ColumnTypeDate: {ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDate, ColumnTypeDatetime, ColumnTypeTimestamp},
	ColumnTypeTime: {
		ColumnTypeFloat, ColumnTypeDouble, ColumnTypeChar, ColumnTypeVarchar,
		ColumnTypeDate, ColumnTypeTime, ColumnTypeDatetime, ColumnTypeTimestamp},
	ColumnTypeDatetime:  {ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDate, ColumnTypeDatetime, ColumnTypeTimestamp},
	ColumnTypeTimestamp: {ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDate, ColumnTypeDatetime, ColumnTypeTimestamp},
	ColumnTypeYear: {
		ColumnTypeBoolean, ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt,
		ColumnTypeBigInt, ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDecimal, ColumnTypeDate,
		ColumnTypeDatetime, ColumnTypeTimestamp, ColumnTypeYear, ColumnTypeBit},
	ColumnTypeBit: {
		ColumnTypeBoolean, ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt,
		ColumnTypeBigInt, ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDecimal, ColumnTypeYear, ColumnTypeBit},
}

func PrepareIndexJoinColumns(s *State) {
	var CheckFunc = func(left, right ColumnType) bool {
		if matches, ok := indexJoinType[left]; ok {
			for _, match := range matches {
				if right == match {
					return true
				}
			}
		}
		return false
	}

	joinColumns := make([]*JoinColumn, 0)

	// Enumerate possible join columns
	for i := 0; i < len(s.Tables); i++ {
		for j := 0; j < len(s.Tables); j++ {
			outerTable := s.Tables[i]
			innerTable := s.Tables[j]

			var comb = JoinColumn{
				outerTable:   outerTable,
				innerTable:   innerTable,
				outerColumns: make([]*Column, 0),
				innerColumns: make([]*Column, 0),
			}

			for _, outerCol := range outerTable.Columns {
				for _, innerIndex := range innerTable.Indexes {
					if CheckFunc(outerCol.Tp, innerIndex.Columns[0].Tp) {
						comb.outerColumns = append(comb.outerColumns, outerCol)
						comb.innerColumns = append(comb.innerColumns, innerIndex.Columns[0])
					}
				}
			}

			if len(comb.innerColumns) > 0 {
				joinColumns = append(joinColumns, &comb)
			}
		}
	}

	allJoinColumns.Store(s, joinColumns)
}

func RemoveIndexJoinColumns(s *State) {
	allJoinColumns.Delete(s)
}

func RandJoinColumn(s *State) (*Table, *Table, *Column, *Column) {
	val, ok := allJoinColumns.Load(s)
	if !ok {
		return nil, nil, nil, nil
	}

	joinColumns, ok := val.([]*JoinColumn)
	if !ok {
		return nil, nil, nil, nil
	}

	totalNum := 0
	for _, joinColumn := range joinColumns {
		totalNum += len(joinColumn.innerColumns)
	}

	if totalNum == 0 {
		return nil, nil, nil, nil
	}

	idx := rand.Intn(totalNum)
	for _, joinColumn := range joinColumns {
		if idx < len(joinColumn.innerColumns) {
			return joinColumn.outerTable, joinColumn.innerTable,
				joinColumn.outerColumns[idx], joinColumn.innerColumns[idx]
		}
		idx -= len(joinColumn.innerColumns)
	}

	return nil, nil, nil, nil
}

var MultiSelectWithIndexJoin = NewFn(func(state *State) Fn {
	tbl1, tbl2, col1, col2 := RandJoinColumn(state)
	if tbl1 == nil {
		return NoneBecauseOf(fmt.Errorf("not initialized"))
	}

	// Generate subquery
	state.IncSubQueryDeep()
	st := state.GenSubQuery()
	state.env.QState = &QueryState{
		SelectedCols: map[*Table]QueryStateColumns{
			tbl2: {
				Columns: tbl2.Columns,
				Attr:    make([]string, len(tbl2.Columns)),
			},
		},
		AggCols: make(map[*Table]Columns),
	}
	state.env.QState.SelectedCols[tbl2].Attr[col2.Idx] = ChosenSelection

	def, err := SimpleAggSelect.Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	var cts []ColumnType
	cts, err = getTypeOfExpressions(def, "test", state)
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

	tbl2Str := fmt.Sprintf("(%s) %s", def, st.Name)
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

	joinHint := Str(fmt.Sprintf("/*+ inl_join(%s) */", sq[0].Name))
	joinPredicate := Str(
		fmt.Sprintf("on %s.%s = %s.%s",
			tbl1.Name, col1.Name, st.Name, sq[0].Columns[0].Name))

	tblNames := []Fn{Str(tbl1.Name), Str(tbl2Str)}
	join := Join(tblNames, Or(Str("left join"), Str("inner join")))

	return And(
		Str("select"), joinHint, SelectFields,
		Str("from"), join, joinPredicate, Opt(OrderBy), Opt(Limit),
	)
})

var SimpleAggSelect = NewFn(func(state *State) Fn {
	state.env.QState.IsAgg = true
	tbl := state.env.QState.GetRandTable()

	groupByColsCnt := rand.Intn(3)
	groupByCols := tbl.Columns.RandGiveN(groupByColsCnt)
	for i, attr := range state.env.QState.SelectedCols[tbl].Attr {
		if attr == ChosenSelection {
			groupByCols = append([]*Column{tbl.Columns[i]}, groupByCols...)
		}
	}
	state.env.QState.AggCols[tbl] = groupByCols

	return And(
		Str("select"), Opt(HintAggToCop), SimpleSelectFields, Str("from"),
		TableReference, WhereClause, GroupByColumns,
	)
})

var SimpleSelectFields = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	queryState.FieldNumHint = 2 + rand.Intn(4)

	tbl := queryState.GetRandTable()

	var fns []Fn

	// We need at least one column for join and one aggregation function
	fns = append(fns, NewFn(func(state *State) Fn {
		state.env.Table = tbl
		return Str(fmt.Sprintf("%s.%s as r0", tbl.Name, state.env.QState.AggCols[tbl][0].Name))
	}))
	fns = append(fns, Str(","))
	fns = append(fns, NewFn(func(state *State) Fn {
		state.env.Table = tbl
		state.env.QColumns = queryState.SelectedCols[state.env.Table]
		return And(AggFunction, Str("as r1"))
	}))

	for i := 2; i < queryState.FieldNumHint; i++ {
		fieldID := fmt.Sprintf("r%d", i)
		fns = append(fns, Str(","))
		fns = append(fns, NewFn(func(state *State) Fn {
			state.env.Table = tbl
			state.env.QColumns = queryState.SelectedCols[state.env.Table]
			return And(Or(SelectFieldName, AggFunction), Str("as"), Str(fieldID))
		}))
	}
	return And(fns...)
})
