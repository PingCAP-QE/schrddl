package sqlgenerator

import (
	"fmt"
	"github.com/ngaut/log"
	"math/rand"
	"strings"

	"github.com/cznic/mathutil"
)

var IndexDefinitions = NewFn(func(state *State) Fn {
	return Repeat(IndexDefinition.R(0, 4), Str(","))
})

var IndexDefinition = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	newIdx := &Index{ID: state.alloc.AllocIndexID()}
	state.env.Index = newIdx
	var ret string
	var err error
	if !strings.Contains(state.FnStack, "CreateTable") && rand.Intn(3) == 0 && len(tbl.Columns.Filter(func(c *Column) bool {
		return c.Tp == ColumnTypeVector
	})) > 0 {
		ret, err = And(
			Strs("vector index"),
			IndexDefinitionName,
			IndexDefinitionVectorColumn,
		).Eval(state)
		if err != nil {
			log.Fatal(err)
			return NoneBecauseOf(err)
		}
	} else {
		// Example:
		//   unique key idx_1 (a, b, c)
		//   primary key (a(2), b(3), c)
		ret, err = And(
			IndexDefinitionType,
			IndexDefinitionName,
			IndexDefinitionColumns,
			IndexAddGlobalIndexKeyword,
			IndexDefinitionClustered,
		).Eval(state)
		if err != nil {
			return NoneBecauseOf(err)
		}
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

var IndexDefinitionVectorFunc = NewFn(func(state *State) Fn {
	col := state.env.Table.Columns.Filter(func(c *Column) bool {
		return c.Tp == ColumnTypeVector
	}).Rand()
	if col == nil {
		return NoneBecauseOf(fmt.Errorf("no vector column"))
	}
	idx := state.env.Index
	state.env.IdxColumn = col
	idx.AppendColumn(col, 0)
	return Or(
		Strs("vec_cosine_distance(", col.Name, ")"),
		Strs("vec_l2_distance(", col.Name, ")"),
	)
})

var IndexDefinitionVectorColumn = NewFn(func(state *State) Fn {
	return And(Str("(("), IndexDefinitionVectorFunc, Str("))"), Str(" USING HNSW"))
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
		return !idx.HasColumn(c) && !state.env.MultiObjs.SameObject(c.Name) && c.Tp != ColumnTypeVector
	})
	if idx.Tp == IndexTypePrimary {
		// All parts of a PRIMARY KEY must be NOT NULL.
		totalCols = totalCols.Filter(func(c *Column) bool {
			return c.DefaultVal != "null" && c.Tp != ColumnTypeJSON
		})
	}
	if idx.Global {
		totalCols = totalCols.Filter(func(c *Column) bool {
			return partCol.Name != c.Name
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
	parCols := state.env.PartColumn
	idx.Tp = IndexTypeUnique
	if parCols != nil {
		// If it's a partition table, random create an unique index.
		return Or(
			Str("unique key"),
			IndexDefinitionUniqueGlobalIndex,
		)
	}
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

// Only set `idx.Global = true` will print keyword in `IndexAddGlobalIndexKeyword` later.
// We could delete it after global index support non-unique keys.
var IndexDefinitionUniqueGlobalIndex = NewFn(func(state *State) Fn {
	idx := state.env.Index
	idx.Global = true
	return Str("unique key")
})

var IndexAddGlobalIndexKeyword = NewFn(func(state *State) Fn {
	idx := state.env.Index
	if idx.Global {
		return Str("global")
	} else {
		return Empty
	}
})

var IndexDefinitionClustered = NewFn(func(state *State) Fn {
	idx := state.env.Index
	if idx.Tp != IndexTypePrimary || idx.Global {
		return Empty
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
