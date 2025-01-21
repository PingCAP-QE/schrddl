package sqlgenerator

import (
	"fmt"
	"math/rand"

	"github.com/cznic/mathutil"
)

var IndexDefinitions = NewFn(func(state *State) Fn {
	return Repeat(IndexDefinition.R(0, 4), Str(","))
})

var IndexDefinition = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	newIdx := &Index{ID: state.alloc.AllocIndexID()}
	state.env.Index = newIdx
	// Example:
	//   unique key idx_1 (a, b, c)
	//   primary key (a(2), b(3), c)
	ret, err := And(
		IndexDefinitionType,
		IndexDefinitionName,
		IndexDefinitionColumns,
		IndexAddGlobalIndexKeyword,
		IndexDefinitionClustered,
	).Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	// It is possible that no column can be used to build an index.
	if len(newIdx.Columns) == 0 {
		return Empty
	}
	tbl.AppendIndex(newIdx)
	if state.env.MultiObjs != nil {
		state.env.MultiObjs.AddName(newIdx.Name)
	}
	return Str(ret)
})

var IndexDefinitionType = NewFn(func(state *State) Fn {
	return Or(
		IndexDefinitionTypeUnique,
		IndexDefinitionTypeNonUnique,
		IndexDefinitionTypePrimary,
	)
})

var IndexDefinitionName = NewFn(func(state *State) Fn {
	idx := state.env.Index
	idx.Name = fmt.Sprintf("idx_%d", idx.ID)
	if idx.Tp == IndexTypePrimary {
		return Empty
	}
	return Str(idx.Name)
})

var IndexDefinitionColumns = NewFn(func(state *State) Fn {
	return And(Str("("), Repeat(IndexDefinitionColumn.R(1, 3), Str(",")), Str(")"))
})

var IndexDefinitionColumn = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	idx := state.env.Index
	partCol := state.env.PartColumn
	// For non-global index, we should add `partCol`.
	if partCol != nil && !idx.Global && !idx.Columns.Contain(partCol) {
		state.env.IdxColumn = partCol
		return IndexDefinitionColumnNoPrefix
	}
	totalCols := tbl.Columns.Filter(func(c *Column) bool {
		return !idx.HasColumn(c) && !state.env.MultiObjs.SameObject(c.Name)
	})
	if idx.Tp == IndexTypePrimary {
		// All parts of a PRIMARY KEY must be NOT NULL and non-generated.
		totalCols = totalCols.Filter(ColNotGenerated).Filter(func(c *Column) bool {
			return c.IsNotNull && c.Tp != ColumnTypeJSON
		})
	}
	if len(totalCols) == 0 {
		return Empty
	}
	state.env.IdxColumn = totalCols.Rand()
	return IndexDefinitionColumnCheckLen
})

var IndexDefinitionColumnCheckLen = NewFn(func(state *State) Fn {
	idx := state.env.Index
	col := state.env.IdxColumn
	currentLength := 0
	for _, c := range idx.Columns {
		currentLength += c.EstimateSizeInBytes()
	}
	if currentLength+col.EstimateSizeInBytes() > DefaultKeySizeLimit {
		return Empty
	}
	return Or(
		IndexDefinitionColumnNoPrefix.P(IndexColumnCanHaveNoPrefix),
		IndexDefinitionColumnPrefix.P(IndexColumnPrefixable),
	)
})

// var randArrayTp = []string{"SIGNED", "UNSIGNED", "CHAR(64)", "binary(64)", "SIGNED", "UNSIGNED", "CHAR(64)", "binary(64)", "date", "datetime", "time", "double"}
var randArrayTp = []string{"SIGNED", "UNSIGNED", "CHAR(64)", "binary(64)", "SIGNED", "UNSIGNED", "CHAR(64)", "binary(64)", "double"}

var IndexDefinitionColumnNoPrefix = NewFn(func(state *State) Fn {
	idx := state.env.Index
	col := state.env.IdxColumn
	idx.AppendColumn(col, 0)
	if col.Tp == ColumnTypeJSON {
		return Str(fmt.Sprintf("(cast(%s as %s array))", col.Name, col.SubType))
	}
	return Str(col.Name)
}).P(func(state *State) bool {
	col := state.env.IdxColumn
	return !col.Tp.NeedKeyLength()
})

var IndexDefinitionColumnPrefix = NewFn(func(state *State) Fn {
	idx := state.env.Index
	col := state.env.IdxColumn
	maxLength := mathutil.Min(col.Arg1, 5)
	if maxLength == 0 {
		maxLength = 5
	}
	prefix := 1 + rand.Intn(maxLength)
	idx.AppendColumn(col, prefix)
	return Strs(col.Name, "(", Num(prefix), ")")
})

var IndexDefinitionTypeUnique = NewFn(func(state *State) Fn {
	idx := state.env.Index
	idx.Tp = IndexTypeUnique
	return Str("unique key")
})

var IndexDefinitionTypeNonUnique = NewFn(func(state *State) Fn {
	idx := state.env.Index
	idx.Tp = IndexTypeNonUnique
	return Str("key")
})

var IndexDefinitionTypePrimary = NewFn(func(state *State) Fn {
	if state.env.Table.Indexes.Primary() != nil {
		return None("pk exists")
	}
	if !state.env.Table.Columns.Found(func(c *Column) bool {
		return c.DefaultVal != "null"
	}) {
		return None("all columns are default null")
	}
	idx := state.env.Index
	idx.Tp = IndexTypePrimary
	return Str("primary key")
})

var IndexDefinitionGlobalIndex = NewFn(func(state *State) Fn {
	idx := state.env.Index
	idx.Global = true
	return Str("/*T![global_index] global */")
})

var IndexAddGlobalIndexKeyword = NewFn(func(state *State) Fn {
	parCols := state.env.PartColumn
	if parCols != nil {
		return Or(
			Empty,
			IndexDefinitionGlobalIndex,
		)
	} else {
		return Empty
	}
})

var IndexDefinitionClustered = NewFn(func(state *State) Fn {
	idx := state.env.Index
	if idx.Tp != IndexTypePrimary {
		return Empty
	}
	if idx.Global {
		return IndexDefinitionKeywordNonClustered
	}
	return Or(
		IndexDefinitionKeywordClustered,
		IndexDefinitionKeywordNonClustered,
	)
})

var IndexDefinitionKeywordClustered = NewFn(func(state *State) Fn {
	if state.env.IsIn("AddIndex") {
		return None("add clustered primary key is not supported")
	}
	tbl := state.env.Table
	tbl.Clustered = true
	return Str("/*T![clustered_index] clustered */")
})

var IndexDefinitionKeywordNonClustered = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	tbl.Clustered = false
	return Str("/*T![clustered_index] nonclustered */")
})
