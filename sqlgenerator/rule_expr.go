package sqlgenerator

import (
	"fmt"
	"math/rand"
)

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
	vecCols := cols.Filter(func(c *Column) bool { return c.Tp == ColumnTypeVector })
	mk := func(colName string) Fn {
		return Str(fmt.Sprintf("%s.%s", tbl.Name, colName))
	}
	s1, s2 := mk(strCols.Rand().Name), mk(strCols.Rand().Name)
	i1, i2 := mk(intCols.Rand().Name), mk(intCols.Rand().Name)
	var v1, v2 Fn
	if len(vecCols) > 0 {
		v1, v2 = mk(vecCols.Rand().Name), mk(vecCols.Rand().Name)
	}
	hasVecCol := func(s *State) bool {
		return len(vecCols) >= 1
	}

	chs := Str(Collations[CollationType(rand.Intn(int(CollationTypeMax)-1)+1)].CharsetName)
	ns := RandomNums(0, 10, 2)
	n1, n2 := Str(ns[0]), Str(ns[1])
	return Or(
		Strf("VEC_L1_DISTANCE([%fn], [%fn])", v1, v2).P(hasVecCol),
		Strf("VEC_L2_DISTANCE([%fn], [%fn])", v1, v2).P(hasVecCol),
		Strf("VEC_NEGATIVE_INNER_PRODUCT([%fn], [%fn])", v1, v2).P(hasVecCol),
		Strf("VEC_COSINE_DISTANCE([%fn], [%fn])", v1, v2).P(hasVecCol),
		Strf("VEC_L2_NORM([%fn], [%fn])", v1, v2).P(hasVecCol),
		Strf("vec_dims([%fn])", v1).P(hasVecCol),
		Strf("vec_as_text([%fn])", v1).P(hasVecCol),
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
		// TODO: fix bug for tiflash
		//Strf("lpad([%fn], [%fn], [%fn])", s1, n1, s2),
		Strf("ltrim([%fn])", s1),
		Strf("make_set([%fn], [%fn], [%fn])", n1, s1, s2),
		Strf("mid([%fn], [%fn], [%fn])", s1, n1, n2),
		Strf("oct([%fn])", i1),
		Strf("octet_length([%fn])", i1),
		Strf("ord([%fn])", s1),
		Strf("position([%fn] in [%fn])", s1, s2),
		Strf("quote([%fn])", s1),
		// TODO: fix OOM.
		//Strf("repeat([%fn], [%fn])", s1, i1),
		Strf("replace([%fn], [%fn], [%fn])", s1, s2, s1),
		Strf("reverse([%fn])", s1),
		Strf("right([%fn], [%fn])", s1, n1),
		// TODO: fix bug for tiflash
		// Strf("rpad([%fn], [%fn], [%fn])", s1, n1, s2),
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
	)
})
