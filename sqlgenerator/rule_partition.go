package sqlgenerator

import "math/rand"

var PartitionDefinition = NewFn(func(state *State) Fn {
	partColumn := state.env.PartColumn
	if partColumn == nil {
		return Empty
	}
	var supportsPartitionTypes []Fn

	supportsPartitionTypes = append(supportsPartitionTypes, Empty)

	if partColumn.Tp.IsKeyPartitionType() {
		supportsPartitionTypes = append(supportsPartitionTypes, PartitionDefinitionKey)
	}

	if partColumn.Tp.IsRangePartitionType() {
		supportsPartitionTypes = append(supportsPartitionTypes, PartitionDefinitionRange)
	}

	if partColumn.Tp.IsListPartitionType() {
		supportsPartitionTypes = append(supportsPartitionTypes, PartitionDefinitionList)
	}

	if partColumn.Tp.IsHashPartitionType() {
		supportsPartitionTypes = append(supportsPartitionTypes, PartitionDefinitionHash)
	}

	if partColumn.Tp.IsColumnsPartitionType() {
		supportsPartitionTypes = append(supportsPartitionTypes, PartitionDefinitionListColumns, PartitionDefinitionRangeColumns)
	}

	return Or(supportsPartitionTypes...)
})

var PartitionDefinitionHash = NewFn(func(state *State) Fn {
	partitionedCol := state.env.PartColumn
	partitionNum := RandomNum(1, 6)
	return And(
		Str("partition by hash ("),
		Str(partitionedCol.Name),
		Str(")"),
		Str("partitions"),
		Str(partitionNum),
	)
})

var PartitionDefinitionKey = NewFn(func(state *State) Fn {
	partitionedCol := state.env.PartColumn
	partitionNum := RandomNum(1, 6)
	return And(
		Str("partition by key ("),
		Str(partitionedCol.Name),
		Str(")"),
		Str("partitions"),
		Str(partitionNum),
	)
})

var PartitionDefinitionRange = NewFn(func(state *State) Fn {
	partitionedCol := state.env.PartColumn
	partitionCount := rand.Intn(5) + 1
	vals := partitionedCol.RandomValuesAsc(partitionCount)
	if rand.Intn(2) == 0 {
		partitionCount++
		vals = append(vals, "maxvalue")
	}
	return Strs(
		"partition by range (",
		partitionedCol.Name, ") (",
		PrintRangePartitionDefs(vals),
		")",
	)
})

var PartitionDefinitionRangeColumns = NewFn(func(state *State) Fn {
	partitionedCol := state.env.PartColumn
	partitionCount := rand.Intn(5) + 1
	vals := partitionedCol.RandomValuesAsc(partitionCount)
	if rand.Intn(2) == 0 {
		partitionCount++
		vals = append(vals, "maxvalue")
	}
	return Strs(
		"partition by range columns(",
		partitionedCol.Name, ") (",
		PrintRangePartitionDefs(vals),
		")",
	)
})

var PartitionDefinitionList = NewFn(func(state *State) Fn {
	partitionedCol := state.env.PartColumn
	listVals := partitionedCol.RandomValuesAsc(20)
	listGroups := RandomGroups(listVals, rand.Intn(3)+1)
	return Strs(
		"partition by list (",
		partitionedCol.Name, ") (",
		PrintListPartitionDefs(listGroups),
		")",
	)
})

var PartitionDefinitionListColumns = NewFn(func(state *State) Fn {
	partitionedCol := state.env.PartColumn
	listVals := partitionedCol.RandomValuesAsc(20)
	listGroups := RandomGroups(listVals, rand.Intn(3)+1)
	return Strs(
		"partition by list columns(",
		partitionedCol.Name, ") (",
		PrintListPartitionDefs(listGroups),
		")",
	)
})
