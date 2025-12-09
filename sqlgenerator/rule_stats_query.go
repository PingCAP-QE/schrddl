package sqlgenerator

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/cznic/mathutil"
)

// Column filter functions
func ColIsNullable(col *Column) bool {
	return !col.IsNotNull
}

func ColIsString(col *Column) bool {
	return col.Tp.IsStringType()
}

// Statistics validation specific query generation rules

// StatsSingleColumnQuery single table single column query (for validating histogram and CMSketch)
var StatsSingleColumnQuery = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	if len(tbl.Columns) == 0 {
		return NoneBecauseOf(fmt.Errorf("no columns available"))
	}

	col := tbl.Columns.Rand()
	state.env.Column = col
	state.env.Table = tbl

	return And(
		Str("SELECT * FROM"),
		Str(tbl.Name),
		Str("WHERE"),
		StatsFilterCondition,
	)
}).P(HasTables)

// StatsMultiColumnQuery single table multi-column query (for validating multi-column selectivity calculation)
var StatsMultiColumnQuery = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	if len(tbl.Columns) < 2 {
		return NoneBecauseOf(fmt.Errorf("need at least 2 columns"))
	}

	// Randomly select 2-3 columns
	maxCols := mathutil.Min(3, len(tbl.Columns))
	numCols := 2
	if maxCols > 2 {
		numCols = 2 + rand.Intn(maxCols-1)
	}
	// RandN doesn't accept parameters, need to manually select
	allCols := make(Columns, len(tbl.Columns))
	copy(allCols, tbl.Columns)
	rand.Shuffle(len(allCols), func(i, j int) {
		allCols[i], allCols[j] = allCols[j], allCols[i]
	})
	selectedCols := allCols[:numCols]
	state.env.Columns = selectedCols
	state.env.Table = tbl

	return And(
		Str("SELECT * FROM"),
		Str(tbl.Name),
		Str("WHERE"),
		StatsMultiFilterConditions,
	)
}).P(HasTables)

// StatsRangeQuery range query (for validating range estimation)
var StatsRangeQuery = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	// Select numeric or date type columns
	var col *Column
	for attempts := 0; attempts < 10; attempts++ {
		c := tbl.Columns.Rand()
		if c.Tp == ColumnTypeInt || c.Tp == ColumnTypeBigInt ||
			c.Tp == ColumnTypeDatetime || c.Tp == ColumnTypeTimestamp {
			col = c
			break
		}
	}
	if col == nil {
		return NoneBecauseOf(fmt.Errorf("no suitable column for range query"))
	}

	state.env.Column = col
	state.env.Table = tbl

	return And(
		Str("SELECT * FROM"),
		Str(tbl.Name),
		Str("WHERE"),
		StatsRangeFilterCondition,
	)
}).P(HasTables)

// StatsFilterCondition single column filter condition
var StatsFilterCondition = NewFn(func(state *State) Fn {
	col := state.env.Column
	if col == nil {
		return NoneBecauseOf(fmt.Errorf("no column set"))
	}

	// Randomly select filter type
	filters := []Fn{
		StatsEqualFilter.W(3),
		StatsNotEqualFilter.W(1),
		StatsGreaterThanFilter.W(2),
		StatsLessThanFilter.W(2),
		StatsInFilter.W(2),
	}

	// If column is nullable, add NULL-related filters
	if !col.IsNotNull {
		filters = append(filters, StatsIsNullFilter.W(1), StatsIsNotNullFilter.W(1))
	}

	return Or(filters...)
})

// StatsEqualFilter equality filter
var StatsEqualFilter = NewFn(func(state *State) Fn {
	col := state.env.Column
	val := col.RandomValue()
	return Strs(col.Name, "=", val)
})

// StatsNotEqualFilter inequality filter
var StatsNotEqualFilter = NewFn(func(state *State) Fn {
	col := state.env.Column
	val := col.RandomValue()
	return Strs(col.Name, "!=", val)
})

// StatsGreaterThanFilter greater than filter
var StatsGreaterThanFilter = NewFn(func(state *State) Fn {
	col := state.env.Column
	val := col.RandomValue()
	return Strs(col.Name, ">", val)
})

// StatsLessThanFilter less than filter
var StatsLessThanFilter = NewFn(func(state *State) Fn {
	col := state.env.Column
	val := col.RandomValue()
	return Strs(col.Name, "<", val)
})

// StatsInFilter IN filter
var StatsInFilter = NewFn(func(state *State) Fn {
	col := state.env.Column
	count := 2 + rand.Intn(9) // 2-10
	vals := col.RandomValuesAsc(count)
	valsStr := strings.Join(vals, ", ")
	return Strs(col.Name, "IN", "(", valsStr, ")")
})

// StatsIsNullFilter IS NULL filter
var StatsIsNullFilter = NewFn(func(state *State) Fn {
	col := state.env.Column
	return Strs(col.Name, "IS NULL")
})

// StatsIsNotNullFilter IS NOT NULL filter
var StatsIsNotNullFilter = NewFn(func(state *State) Fn {
	col := state.env.Column
	return Strs(col.Name, "IS NOT NULL")
})

// StatsRangeFilterCondition range filter condition
// Generates a range that matches the actual data generation range to avoid out-of-bounds queries
var StatsRangeFilterCondition = NewFn(func(state *State) Fn {
	col := state.env.Column

	// Generate a range that matches the data generation range
	// This ensures the query range overlaps with actual data
	var val1, val2 string
	switch col.Tp {
	case ColumnTypeTinyInt:
		if col.IsUnsigned {
			// Unsigned: 0 to 255
			v1 := rand.Intn(256)
			v2 := v1 + rand.Intn(256-v1)
			val1 = strconv.FormatInt(int64(v1), 10)
			val2 = strconv.FormatInt(int64(v2), 10)
		} else {
			// Signed: -128 to 127
			v1 := -128 + rand.Intn(256)
			v2 := v1 + rand.Intn(128-v1)
			if v2 > 127 {
				v2 = 127
			}
			val1 = strconv.FormatInt(int64(v1), 10)
			val2 = strconv.FormatInt(int64(v2), 10)
		}
	case ColumnTypeSmallInt:
		if col.IsUnsigned {
			// Unsigned: 0 to 65535
			v1 := rand.Intn(65536)
			v2 := v1 + rand.Intn(65536-v1)
			val1 = strconv.FormatInt(int64(v1), 10)
			val2 = strconv.FormatInt(int64(v2), 10)
		} else {
			// Signed: -32768 to 32767
			v1 := -32768 + rand.Intn(65536)
			v2 := v1 + rand.Intn(32768-v1)
			if v2 > 32767 {
				v2 = 32767
			}
			val1 = strconv.FormatInt(int64(v1), 10)
			val2 = strconv.FormatInt(int64(v2), 10)
		}
	case ColumnTypeMediumInt:
		if col.IsUnsigned {
			// Unsigned: 0 to 16777215
			v1 := rand.Intn(16777216)
			v2 := v1 + rand.Intn(16777216-v1)
			val1 = strconv.FormatInt(int64(v1), 10)
			val2 = strconv.FormatInt(int64(v2), 10)
		} else {
			// Signed: -8388608 to 8388607
			v1 := -8388608 + rand.Intn(16777216)
			v2 := v1 + rand.Intn(8388608-v1)
			if v2 > 8388607 {
				v2 = 8388607
			}
			val1 = strconv.FormatInt(int64(v1), 10)
			val2 = strconv.FormatInt(int64(v2), 10)
		}
	case ColumnTypeInt:
		if col.IsUnsigned {
			// Unsigned: 0 to 4294967295
			// Use a smaller range to match data generation (0 to 1000000 for Zipfian)
			maxVal := uint64(1000000)
			v1 := rand.Uint64() % maxVal
			v2 := v1 + rand.Uint64()%(maxVal-v1)
			val1 = strconv.FormatUint(v1, 10)
			val2 = strconv.FormatUint(v2, 10)
		} else {
			// Signed: -2147483648 to 2147483647
			// Use a smaller range to match data generation (-1000000 to 1000000 for Zipfian)
			min := int64(-1000000)
			max := int64(1000000)
			v1 := min + rand.Int63n(max-min)
			v2 := v1 + rand.Int63n(max-v1)
			if v2 > max {
				v2 = max
			}
			val1 = strconv.FormatInt(v1, 10)
			val2 = strconv.FormatInt(v2, 10)
		}
	case ColumnTypeBigInt:
		if col.IsUnsigned {
			// Unsigned: use a reasonable range (0 to 1000000 for Zipfian)
			maxVal := uint64(1000000)
			v1 := rand.Uint64() % maxVal
			v2 := v1 + rand.Uint64()%(maxVal-v1)
			val1 = strconv.FormatUint(v1, 10)
			val2 = strconv.FormatUint(v2, 10)
		} else {
			// Signed: use a reasonable range (-1000000 to 1000000 for Zipfian)
			// This matches the data generation range in RandomValueWithSkew
			min := int64(-1000000)
			max := int64(1000000)
			v1 := min + rand.Int63n(max-min)
			v2 := v1 + rand.Int63n(max-v1)
			if v2 > max {
				v2 = max
			}
			val1 = strconv.FormatInt(v1, 10)
			val2 = strconv.FormatInt(v2, 10)
		}
	case ColumnTypeDatetime, ColumnTypeTimestamp:
		// Use the same range as RandGoTimes: 1970-01-02 to 2037-01-01
		min := time.Date(1970, 1, 2, 0, 0, 1, 0, time.UTC)
		max := time.Date(2037, 1, 0, 0, 0, 0, 0, time.UTC)
		delta := max.Sub(min)
		offset1 := time.Duration(rand.Int63n(int64(delta)))
		offset2 := time.Duration(rand.Int63n(int64(delta / 10))) // Range up to 10% of total
		t1 := min.Add(offset1)
		t2 := t1.Add(offset2)
		if t2.After(max) {
			t2 = max
		}
		val1 = fmt.Sprintf("'%s'", t1.Format("2006-01-02 15:04:05"))
		val2 = fmt.Sprintf("'%s'", t2.Format("2006-01-02 15:04:05"))
	default:
		// Fallback to original method
		val1, val2 = col.RandomValueRange()
	}

	return Strs(col.Name, "BETWEEN", val1, "AND", val2)
})

// StatsMultiFilterConditions multi-column filter conditions
var StatsMultiFilterConditions = NewFn(func(state *State) Fn {
	cols := state.env.Columns
	if len(cols) == 0 {
		return NoneBecauseOf(fmt.Errorf("no columns set"))
	}

	var conditions []Fn
	for _, col := range cols {
		state.env.Column = col
		cond, err := StatsFilterCondition.Eval(state)
		if err != nil {
			continue
		}
		conditions = append(conditions, Str(cond))
	}

	if len(conditions) == 0 {
		return NoneBecauseOf(fmt.Errorf("failed to generate conditions"))
	}

	// Use And to connect all conditions
	allConditions := make([]Fn, 0, len(conditions)*2-1)
	for i, cond := range conditions {
		if i > 0 {
			allConditions = append(allConditions, Str("AND"))
		}
		allConditions = append(allConditions, cond)
	}

	return And(allConditions...)
})

// StatsQueryWithFullScan query with forced full table scan (using hint)
var StatsQueryWithFullScan = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	return And(
		Str("SELECT /*+ IGNORE_INDEX("+tbl.Name+") */ * FROM"),
		Str(tbl.Name),
		Opt(StatsFilterCondition),
	)
}).P(HasTables)
