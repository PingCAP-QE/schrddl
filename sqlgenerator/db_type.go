package sqlgenerator

import (
	"math/rand"

	"github.com/pingcap/tidb/pkg/parser/model"
)

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

type State struct {
	hooks  *Hooks
	weight map[string]int
	repeat map[string]Interval
	prereq map[string]func(*State) bool

	Tables        Tables
	droppedTables Tables
	joinColumns   []*JoinColumn

	ctes     [][]*Table
	subQuery [][]*Table
	alloc    *IDAllocator

	env *Env

	prepareStmts []*Prepare

	tableMeta []*model.TableInfo

	fnStack string
}

type JoinColumn struct {
	outerTable   *Table
	innerTable   *Table
	outerColumns []*Column
	innerColumns []*Column
}

type Table struct {
	ID        int
	Name      string
	AsName    string
	Columns   Columns
	Indexes   Indexes
	Collate   *Collation
	Clustered bool

	TiflashReplica int

	Values            [][]string
	ColForPrefixIndex Columns

	// ChildTables records tables that have the same structure.
	// A table is also its ChildTables.
	// This is used for SELECT OUT FILE and LOAD DATA.
	ChildTables []*Table
	SubQueryDef string
}

type Column struct {
	ID        int
	Idx       int
	Name      string
	Tp        ColumnType
	Collation *Collation

	IsUnsigned bool
	Arg1       int // optional
	Arg2       int // optional

	// for ColumnTypeJSON
	Array   bool
	SubType string

	Args       []string // for ColumnTypeSet and ColumnTypeEnum
	DefaultVal string
	IsNotNull  bool
}

type Index struct {
	ID           int
	Name         string
	Tp           IndexType
	Columns      Columns
	ColumnPrefix []int
}

type Prepare struct {
	ID   int
	Name string
	Args []func() string
}

func NewState() *State {
	s := &State{
		hooks:  &Hooks{},
		weight: make(map[string]int),
		repeat: make(map[string]Interval),
		prereq: make(map[string]func(*State) bool),
		alloc:  &IDAllocator{},
		env:    &Env{},
	}
	s.hooks.Append(NewFnHookScope(s))
	return s
}

func (s *State) Hook() *Hooks {
	return s.hooks
}

func (s *State) Env() *Env {
	return s.env
}

func (s *State) Config() *ConfigurableState {
	return (*ConfigurableState)(s)
}

func (s *State) ReplaceRule(fn Fn, newFn Fn) {
	replacer := s.hooks.Find(HookNameReplacer)
	if replacer == nil {
		replacer = NewFnHookReplacer()
		s.hooks.Append(replacer)
	}
	replacer.(*FnHookReplacer).Replace(fn, newFn)
}

func (s *State) CleanReplaceRule(fn Fn) {
	replacer := s.hooks.Find(HookNameReplacer)
	if replacer == nil {
		return
	}
	replacer.(*FnHookReplacer).RemoveReplace(fn)
}

func (s *State) GetWeight(fn Fn) int {
	if !s.GetPrerequisite(fn)(s) {
		return 0
	}
	if w, ok := s.weight[fn.Info]; ok {
		return w
	}
	return fn.Weight
}

func (s *State) GetRepeat(fn Fn) (lower int, upper int) {
	if w, ok := s.repeat[fn.Info]; ok {
		return w.lower, w.upper
	}
	return fn.Repeat.lower, fn.Repeat.upper
}

func (s *State) GetPrerequisite(fn Fn) func(state *State) bool {
	if p, ok := s.prereq[fn.Info]; ok {
		return p
	}
	if fn.Prerequisite != nil {
		return fn.Prerequisite
	}
	return func(state *State) bool {
		return true
	}
}

func (s *State) RemoveRepeat(fn Fn) {
	if _, ok := s.repeat[fn.Info]; ok {
		delete(s.repeat, fn.Info)
	}
}

func (s *State) RemoveWeight(fn Fn) {
	if _, ok := s.weight[fn.Info]; ok {
		delete(s.weight, fn.Info)
	}
}

func (s *State) PickRandomCTEOrTableName() string {
	names := make([]string, 0, 10)
	for _, cteL := range s.ctes {
		for _, cte := range cteL {
			names = append(names, cte.Name)
		}
	}

	for _, tbl := range s.Tables {
		names = append(names, tbl.Name)
	}

	return names[rand.Intn(len(names))]
}

func (s *State) GetRandomCTE() *Table {
	ctes := make([]*Table, 0, 10)
	for _, cteL := range s.ctes {
		for _, cte := range cteL {
			ctes = append(ctes, cte)
		}
	}

	return ctes[rand.Intn(len(ctes))]
}

func (s *State) GetCTECount() int {
	c := 0
	for _, cteL := range s.ctes {
		c += len(cteL)
	}

	return c
}

// QueryState represent an intermediate state during a query generation.
type QueryState struct {
	SelectedCols map[*Table]QueryStateColumns
	IsWindow     bool
	// AggCols is the aggregate columns in the select list. It's the available columns used in
	// select field, having clause and order by clause.
	AggCols      map[*Table]Columns
	IsAgg        bool
	FieldNumHint int
}

type QueryStateColumns struct {
	Columns
	Attr []string
}

func (q QueryState) GetRandTable() *Table {
	idx := rand.Intn(len(q.SelectedCols))
	for t := range q.SelectedCols {
		if idx == 0 {
			return t
		}
		idx--
	}
	return nil
}

type MultiObjs struct {
	items []string
}

func NewMultiObjs() *MultiObjs {
	return &MultiObjs{}
}

func (m *MultiObjs) SameObject(name string) bool {
	if m == nil {
		return false
	}
	for _, i := range m.items {
		if i == name {
			return true
		}
	}
	return false
}

func (m *MultiObjs) AddName(name string) {
	m.items = append(m.items, name)
}

func (s *State) SetTableMeta(tableMeta []*model.TableInfo) {
	s.tableMeta = tableMeta
}

func (s *State) PrepareIndexJoinColumns() {
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
				s.joinColumns = append(s.joinColumns, &comb)
			}
		}
	}
}

func (s *State) RandJoinColumn() (*Table, *Table, *Column, *Column) {
	totalNum := 0
	for _, joinColumn := range s.joinColumns {
		totalNum += len(joinColumn.innerColumns)
	}

	if totalNum == 0 {
		return nil, nil, nil, nil
	}

	idx := rand.Intn(totalNum)
	for _, joinColumn := range s.joinColumns {
		if idx < len(joinColumn.innerColumns) {
			return joinColumn.outerTable, joinColumn.innerTable,
				joinColumn.outerColumns[idx], joinColumn.innerColumns[idx]
		}
		idx -= len(joinColumn.innerColumns)
	}

	return nil, nil, nil, nil
}
