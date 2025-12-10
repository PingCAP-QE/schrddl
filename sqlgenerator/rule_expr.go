package sqlgenerator

import (
	"fmt"
	"math/rand"
)

func CopyTypeInfo(dst, src *Column) {
	dst.Tp = src.Tp
	dst.Collation = src.Collation
	dst.IsUnsigned = src.IsUnsigned
	dst.Arg1 = src.Arg1
	dst.Arg2 = src.Arg2
	dst.Array = src.Array
	dst.SubType = src.SubType
	dst.Args = make([]string, len(src.Args))
	copy(dst.Args, src.Args)
	dst.DefaultVal = src.DefaultVal
	dst.IsNotNull = src.IsNotNull
}

// TODO(joechenrh): support more flexible function generation
var GenerateFunction = NewFn(func(state *State) Fn {
	prevColumn := state.env.Table.Columns.Rand()
	prevNameFn := Str(prevColumn.Name)
	prevTp := prevColumn.Tp
	col := state.env.Column

	CopyTypeInfo(col, prevColumn)
	col.GeneratedFrom = prevColumn.Idx + 1

	var fns []Fn
	switch prevTp {
	case ColumnTypeInt, ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeBigInt:
		if RandomBool() {
			col.Tp = ColumnTypeInt
		}
		fns = []Fn{Strf("[%fn] * 16", prevNameFn)}
	case ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDecimal:
		if RandomBool() {
			col.Tp = ColumnTypeDouble
		}
		fns = []Fn{Strf("[%fn] * 16.16", prevNameFn)}
	case ColumnTypeChar, ColumnTypeVarchar, ColumnTypeText,
		ColumnTypeBlob, ColumnTypeBinary, ColumnTypeVarBinary:
		fns = []Fn{
			Strf("concat([%fn], [%fn])", prevNameFn, prevNameFn),
			Strf("elt(1, [%fn], 'a string')", prevNameFn),
			Strf("elt(1, 'a string', [%fn])", prevNameFn),
		}
	case ColumnTypeTime, ColumnTypeTimestamp, ColumnTypeDate, ColumnTypeDatetime:
		col.Tp = ColumnTypeDatetime
		fns = []Fn{Strf("date_add([%fn], interval 1 day)", prevNameFn)}
	default:
		fns = []Fn{prevNameFn}
	}

	return And(
		Str(col.TypeString()),
		Str("as("),
		Or(fns...),
		Str(")"),
		Or(
			Str("virtual"),
			Str("stored"),
		),
	)
})

var BuiltinFunction = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	cols := state.env.QColumns.Columns
	if state.env.QState.IsAgg {
		// should use agg columns in select fields.
		cols = state.env.QState.AggCols[tbl]
	}
	strCols := cols.Filter(func(c *Column) bool {
		return c.Tp.IsStringType()
	}).Or(cols)
	intCols := cols.Filter(func(c *Column) bool {
		return c.Tp.IsIntegerType()
	}).Or(cols)
	mk := func(colName string) Fn {
		return Str(fmt.Sprintf("%s.%s", tbl.Name, colName))
	}
	s1, s2 := mk(strCols.Rand().Name), mk(strCols.Rand().Name)
	i1, i2 := mk(intCols.Rand().Name), mk(intCols.Rand().Name)
	chs := Str(Collations[CollationType(rand.Intn(int(CollationTypeMax)-1)+1)].CharsetName)
	ns := RandomNums(0, 10, 2)
	n1, n2 := Str(ns[0]), Str(ns[1])
	return Or(
		Strf("ascii([%fn])", s1),
		Strf("bin([%fn])", i1),
		Strf("bit_length([%fn])", s1),
		Strf("char([%fn], [%fn] using [%fn])", i1, i2, chs),
		Strf("char_length([%fn])", s1),
		Strf("character_length([%fn])", s1),
		Strf("concat([%fn], [%fn])", s1, s2),
		Strf("concat_ws(',', [%fn], [%fn])", s1, s2),
		Strf("elt(2, [%fn], [%fn])", s1, s2),
		Strf("export_set([%fn], [%fn], [%fn], '-', 8)", n1, s1, s2),
		Strf("field([%fn], [%fn], [%fn])", s1, s1, s2),
		Strf("find_in_set([%fn], [%fn])", s1, s2),
		Strf("format([%fn], [%fn])", i1, Str(RandomNum(0, 4))),
		Strf("from_base64([%fn])", s1),
		Strf("hex([%fn])", s1),
		Strf("insert([%fn], [%fn], [%fn], [%fn])", s1, n1, n2, s2),
		Strf("instr([%fn], [%fn])", s1, s2),
		Strf("lower([%fn])", s1),
		Strf("lcase([%fn])", s1),
		Strf("left([%fn], [%fn])", s1, n1),
		Strf("length([%fn])", s1),
		Strf("locate([%fn], [%fn])", s1, s2),
		Strf("lpad([%fn], [%fn], [%fn])", s1, n1, s2),
		Strf("ltrim([%fn])", s1),
		Strf("make_set([%fn], [%fn], [%fn])", n1, s1, s2),
		Strf("mid([%fn], [%fn], [%fn])", s1, n1, n2),
		Strf("oct([%fn])", i1),
		Strf("octet_length([%fn])", i1),
		Strf("ord([%fn])", s1),
		Strf("position([%fn] in [%fn])", s1, s2),
		Strf("quote([%fn])", s1),
		// regex-match patterns tuned to likely random ASCII/GBK strings
		Strf("[%fn] rlike '.*'", s1),                   // match anything
		Strf("[%fn] rlike '^[[:alnum:]_]{1,20}$'", s1), // common alnum/underscore
		Strf("[%fn] rlike '^[A-Za-z].*'", s1),          // starts with letter
		Strf("[%fn] rlike '^[0-9].*'", s1),             // starts with digit
		Strf("[%fn] rlike '.*[A-Za-z].*'", s1),         // contains letter
		Strf("[%fn] rlike '.*[0-9].*'", s1),            // contains digit
		Strf("[%fn] rlike '^.{1,10}$'", s1),            // length up to 10
		Strf("[%fn] rlike '[A-Za-z0-9_]{3,8}'", s1),    // mid-length token
		Strf("[%fn] rlike '^[[:alpha:]]+$'", s1),       // pure alpha
		Strf("[%fn] rlike '^[[:digit:]]{1,3}$'", s1),   // short digits
		Strf("[%fn] rlike 'foo.*'", s1),                // prefix literal
		Strf("[%fn] rlike '.*bar'", s1),                // suffix literal
		// case-insensitive variants to match mixed-case random strings
		Strf("[%fn] rlike '(?i)^foo.*'", s1),
		Strf("[%fn] rlike '(?i).*bar$'", s1),
		Strf("[%fn] rlike '(?i)^[a-z]{1,5}$'", s1),
		Strf("[%fn] rlike '(?i)[a-z0-9]{2,6}'", s1),
		Strf("lower([%fn]) rlike '^a.*'", s1),
		// safe pattern without injecting raw regex meta from data
		Strf("lower([%fn]) rlike '^[a-z0-9_]{0,32}$'", s1),
		// TODO: fix OOM.
		//Strf("repeat([%fn], [%fn])", s1, i1),
		Strf("replace([%fn], [%fn], [%fn])", s1, s2, s1),
		Strf("reverse([%fn])", s1),
		Strf("right([%fn], [%fn])", s1, n1),
		Strf("rpad([%fn], [%fn], [%fn])", s1, n1, s2),
		Strf("rtrim([%fn])", s1),
		Strf("space([%fn])", n1),
		Strf("strcmp([%fn], [%fn])", s1, s2),
		Strf("substr([%fn], [%fn])", s1, n1),
		Strf("substring([%fn], [%fn])", s1, n1),
		Strf("substring_index([%fn], [%fn], [%fn])", s1, Str("','"), n1),
		Strf("to_base64([%fn])", s1),
		Strf("trim([%fn])", s1),
		Strf("ucase([%fn])", s1),
		Strf("unhex([%fn])", s1),
		Strf("upper([%fn])", s1),
		//Strf("weight_string([%fn])", s1), bug
	)
})

var AggFunction = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	cols := state.env.QColumns
	intCols := cols.Filter(func(c *Column) bool {
		return c.Tp.IsIntegerType()
	}).Or(cols.Columns)
	col := intCols.Rand()
	for i, c := range cols.Columns {
		if c.ID == col.ID {
			cols.Attr[i] = QueryAggregation // group by clause needs this.
			break
		}
	}
	c1 := Str(fmt.Sprintf("%s.%s", tbl.Name, col.Name))
	distinctOpt := Opt(Str("distinct"))
	return Or(
		Strf("count([%fn] [%fn])", distinctOpt, c1),
		Strf("sum([%fn] [%fn])", distinctOpt, c1),
		Strf("avg([%fn] [%fn])", distinctOpt, c1),
		Strf("max([%fn] [%fn])", distinctOpt, c1),
		Strf("min([%fn] [%fn])", distinctOpt, c1),
		Strf("group_concat([%fn] [%fn] order by [%fn])", distinctOpt, c1, c1),
		Strf("bit_or([%fn])", c1),
		Strf("bit_xor([%fn])", c1),
		Strf("bit_and([%fn])", c1),
		//Strf("var_pop([%fn])", c1),
		//Strf("var_samp([%fn])", c1),
		//Strf("stddev_pop([%fn])", c1),
		//Strf("stddev_samp([%fn])", c1),
		// Strf("json_objectagg([%fn], [%fn])", c1, c2),
		//Strf("approx_count_distinct([%fn])", c1),
		//Strf("approx_percentile([%fn], [%fn])", c1, Str(RandomNum(0, 100))),
	)
})

var CompareSymbol = NewFn(func(state *State) Fn {
	return Or(
		Str("="),
		Str("<"),
		Str("<="),
		Str(">"),
		Str(">="),
		Str("<>"),
		Str("!="),
		Str("<=>"),
	)
})
