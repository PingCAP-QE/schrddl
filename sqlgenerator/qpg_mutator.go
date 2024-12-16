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
		//And(Str("set"), Str("sql_mode"), Str("="), Or(Str("''"), Str("default"))),
		And(Str("set"), Str("tidb_enable_chunk_rpc"), Str("="), Or(Str("0"), Str("1"))),
		And(Str("set"), Str("tidb_enable_clustered_index"), Str("="), Or(Str("0"), Str("1"), Str("'INT_ONLY'"))),
		And(Str("set"), Str("tidb_enable_table_partition"), Str("="), Or(Str("0"), Str("1"))),
		And(Str("set"), Str("tidb_enable_window_function"), Str("="), Or(Str("0"), Str("1"))),
		And(Str("set"), Str("tidb_enable_non_prepared_plan_cache"), Str("="), Or(Str("0"), Str("1"))),
		And(Str("set"), Str("tidb_enable_non_prepared_plan_cache_for_dml"), Str("="), Or(Str("0"), Str("1"))),
		And(Str("set"), Str("tidb_enable_parallel_apply"), Str("="), Or(Str("0"), Str("1"))),
		And(Str("set"), Str("tidb_enable_pipelined_window_function"), Str("="), Or(Str("0"), Str("1"))),
		And(Str("set"), Str("tidb_enable_vectorized_expression"), Str("="), Or(Str("0"), Str("1"))),
		And(Str("set"), Str("tidb_executor_concurrency"), Str("="), Or(Str("1"), Str("5"))),
		And(Str("set"), Str("tidb_index_join_batch_size"), Str("="), Or(Str("1"), Str("16"), Str("64"), Str("25000"))),
		And(Str("set"), Str("tidb_index_lookup_size"), Str("="), Or(Str("16"), Str("64"), Str("20000"))),
		And(Str("set"), Str("tidb_init_chunk_size"), Str("="), Or(Str("2"), Str("32"))),
		And(Str("set"), Str("tidb_partition_prune_mode"), Str("="), Or(Str("'dynamic'"), Str("'static'"))),
		And(Str("set"), Str("tidb_ddl_enable_fast_reorg"), Str("="), Or(Str("'0'"), Str("'1'"))),
		And(Str("set"), Str("tidb_enable_dist_tasktidb_enable_dist_task"), Str("="), Or(Str("'0'"), Str("'1'"))),
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
