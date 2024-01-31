package sqlgenerator

var AlterAddIndex = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	state.env.Table = tbl
	return And(Str("alter table"), Str(tbl.Name), Str("add"), IndexDefinition)
})

var SetVariable = NewFn(func(state *State) Fn {
	return Or(
		And(Str("set"), Str("tidb_max_chunk_size"), Str("="), Or(Str("32"), Str("4096"))),
		And(Str("set"), Str("tidb_enable_clustered_index"), Str("="), Or(Str("0"), Str("1"))),
	)
})

var QPGMuattor = []Fn{
	CreateTable,
	AlterAddIndex,
}

var NonDDLMutator = []Fn{
	AnalyzeTable,
	SetVariable,
}
