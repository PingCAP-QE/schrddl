package sqlgenerator

import (
	"fmt"
	"math/rand"

	"github.com/cznic/mathutil"
)

var ColumnDefinitions = NewFn(func(state *State) Fn {
	return Repeat(ColumnDefinition.R(1, 10), Str(","))
})

var ColumnDefinition = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	partialCol := &Column{ID: state.alloc.AllocColumnID(), Idx: len(tbl.Columns)}
	state.env.Column = partialCol
	// Example:
	//   a varchar(255) collate utf8mb4_bin not null
	//   b bigint unsigned default 100
	ret, err := And(
		ColumnDefinitionName,
		Or(
			And(
				ColumnDefinitionTypeOnCreate,
				ColumnDefinitionCollation,
				ColumnDefinitionUnsigned,
				ColumnDefinitionNotNull,
				ColumnDefinitionDefault,
				ColumnDefinitionOnUpdate,
			).W(8),
			ColumnDefinitionGenerated.P(MoreThan1Columns).W(0),
		),
	).Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	tbl.AppendColumn(partialCol)
	return Str(ret)
})

var ColumnDefinitionName = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Name = fmt.Sprintf("col_%d", col.ID)
	return Str(col.Name)
})

var ColumnDefinitionTypeOnCreate = NewFn(func(state *State) Fn {
	return ColumnDefinitionType
})

var ColumnDefinitionTypeOnAdd = NewFn(func(state *State) Fn {
	return ColumnDefinitionType
})

var ColumnDefinitionTypeOnModify = NewFn(func(state *State) Fn {
	return ColumnDefinitionType
})

var ColumnDefinitionType = NewFn(func(state *State) Fn {
	return Or(
		ColumnDefinitionTypesIntegers.W(5),
		ColumnDefinitionTypesFloatings.W(3),
		ColumnDefinitionTypesStrings.W(6),
		ColumnDefinitionTypesBinaries.W(3),
		ColumnDefinitionTypesTimes.W(5),
		ColumnDefinitionTypesBit.W(0), // Disabled: BIT type has large estimation errors
		ColumnDefinitionTypesJSON.W(6),
	)
})

var ColumnDefinitionCollation = NewFn(func(state *State) Fn {
	col := state.env.Column
	if !col.Tp.IsStringType() {
		return Empty
	}
	switch col.Tp {
	case ColumnTypeBinary, ColumnTypeVarBinary, ColumnTypeBlob:
		col.Collation = Collations[CollationBinary]
		return Empty
	default:
		// Exclude GBK collations (CollationGBKChineseCI and CollationGBKBin)
		// Retry until we get a non-GBK collation
		for attempts := 0; attempts < 10; attempts++ {
			collationType := CollationType(rand.Intn(int(CollationTypeMax)-1) + 1)
			if collationType != CollationGBKChineseCI && collationType != CollationGBKBin {
				col.Collation = Collations[collationType]
				if oldCol := state.env.OldColumn; oldCol != nil && !CharsetCompatible(oldCol, col) {
					continue
				}
				return Opt(Strs("collate", col.Collation.CollationName))
			}
		}
		// Fallback to utf8mb4 if all attempts failed
		col.Collation = Collations[CollationUtf8mb4GeneralCI]
		return Opt(Strs("collate", col.Collation.CollationName))
	}
})

var ColumnDefinitionNotNull = NewFn(func(state *State) Fn {
	col := state.env.Column
	if RandomBool() {
		col.IsNotNull = true
		return Str("not null")
	} else {
		return Empty
	}
})

var ColumnDefinitionDefault = NewFn(func(state *State) Fn {
	col := state.env.Column
	if RandomBool() || col.Tp.DisallowDefaultValue() {
		return Empty
	}

	if col.Tp.SupportCurrentTimestamp() && RandomBool() {
		return Str("default CURRENT_TIMESTAMP")
	}

	return Strs("default", col.RandomValue())
})

var ColumnDefinitionOnUpdate = NewFn(func(state *State) Fn {
	col := state.env.Column
	if RandomBool() || !col.Tp.SupportCurrentTimestamp() {
		return Empty
	}

	return Str("ON UPDATE CURRENT_TIMESTAMP")
})

var ColumnDefinitionGenerated = NewFn(func(state *State) Fn {
	return And(
		GenerateFunction,
		ColumnDefinitionNotNull,
	)
})

var ColumnDefinitionUnsigned = NewFn(func(state *State) Fn {
	col := state.env.Column
	if !col.Tp.IsIntegerType() {
		return Empty
	}
	if RandomBool() {
		col.IsUnsigned = true
		return Str("unsigned")
	} else {
		return Empty
	}
})

var ColumnDefinitionTypesStrings = NewFn(func(state *State) Fn {
	return Or(
		ColumnDefinitionTypesChar,
		ColumnDefinitionTypesVarchar,
		ColumnDefinitionTypesText,

		ColumnDefinitionTypesEnum,
		ColumnDefinitionTypesSet,
	)
})

var ColumnDefinitionTypesBinaries = NewFn(func(state *State) Fn {
	return Or(
		ColumnDefinitionTypesBlob,
		ColumnDefinitionTypesBinary,
		ColumnDefinitionTypesVarbinary,
	)
})

var ColumnDefinitionTypesTimes = NewFn(func(state *State) Fn {
	if !ModifyColumnCompatible(state.env.OldColumn, ColumnTypeTime) {
		return None("unsupported change to time")
	}
	return Or(
		ColumnDefinitionTypesDate,
		ColumnDefinitionTypesTime,
		ColumnDefinitionTypesDateTime,
		ColumnDefinitionTypesTimestamp,
		ColumnDefinitionTypesYear,
	)
})

var ColumnDefinitionTypesIntegers = NewFn(func(state *State) Fn {
	return Or(
		ColumnDefinitionTypesIntegerBool,
		ColumnDefinitionTypesIntegerTiny.W(0), // Disabled: TINYINT type has large estimation errors
		ColumnDefinitionTypesIntegerSmall,
		ColumnDefinitionTypesIntegerInt,
		ColumnDefinitionTypesIntegerMedium,
		ColumnDefinitionTypesIntegerBig,
	)
})

var ColumnDefinitionTypesFloatings = NewFn(func(state *State) Fn {
	return Or(
		ColumnDefinitionTypesFloat,
		ColumnDefinitionTypesDouble,
		ColumnDefinitionTypesDecimal,
	)
})

var ColumnDefinitionTypesIntegerBool = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeBoolean
	return Str(col.TypeString())
})

var ColumnDefinitionTypesIntegerTiny = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeTinyInt
	return Str(col.TypeString())
})

var ColumnDefinitionTypesIntegerSmall = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeSmallInt
	return Str(col.TypeString())
})

var ColumnDefinitionTypesIntegerMedium = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeMediumInt
	return Str(col.TypeString())
})

var ColumnDefinitionTypesIntegerInt = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeInt
	return Str(col.TypeString())
})

var ColumnDefinitionTypesIntegerBig = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeBigInt
	return Str(col.TypeString())
})

var ColumnDefinitionTypesFloat = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Arg1 = 0
	col.Arg2 = 0
	col.Tp = ColumnTypeFloat
	return Str(col.TypeString())
})

var ColumnDefinitionTypesDouble = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Arg1 = 0
	col.Arg2 = 0
	col.Tp = ColumnTypeDouble
	return Str(col.TypeString())

})

var ColumnDefinitionTypesDecimal = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Arg1 = 1 + rand.Intn(65)
	upper := mathutil.Min(col.Arg1, 30)
	col.Arg2 = 1 + rand.Intn(upper)
	col.Tp = ColumnTypeDecimal
	return Str(col.TypeString())
})

var ColumnDefinitionTypesBit = NewFn(func(state *State) Fn {
	if !ModifyColumnCompatible(state.env.OldColumn, ColumnTypeSet) {
		return None("unsupported change to bit")
	}
	col := state.env.Column
	col.Arg1 = 1 + rand.Intn(62)
	col.Tp = ColumnTypeBit
	return Str(col.TypeString())
})

var ColumnDefinitionTypesChar = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Arg1 = 1 + rand.Intn(255)
	col.Tp = ColumnTypeChar
	return Str(col.TypeString())
})

var ColumnDefinitionTypesBinary = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Arg1 = 1 + rand.Intn(255)
	col.Tp = ColumnTypeBinary
	return Str(col.TypeString())
})

var ColumnDefinitionTypesVarchar = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Arg1 = 1 + rand.Intn(512)
	col.Tp = ColumnTypeVarchar
	return Str(col.TypeString())
})

var ColumnDefinitionTypesText = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Arg1 = 1 + rand.Intn(512)
	col.Tp = ColumnTypeText
	return Str(col.TypeString())
})

var ColumnDefinitionTypesBlob = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Arg1 = 1 + rand.Intn(512)
	col.Tp = ColumnTypeBlob
	return Str(col.TypeString())
})

var ColumnDefinitionTypesVarbinary = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Arg1 = 1 + rand.Intn(512)
	col.Tp = ColumnTypeVarBinary
	return Str(col.TypeString())
})

var ColumnDefinitionTypesEnum = NewFn(func(state *State) Fn {
	if !ModifyColumnCompatible(state.env.OldColumn, ColumnTypeEnum) {
		return None("unsupported change to enum")
	}
	col := state.env.Column
	col.Args = []string{"Alice", "Bob", "Charlie", "David"}
	col.Tp = ColumnTypeEnum
	return Str(col.TypeString())
})

var ColumnDefinitionTypesSet = NewFn(func(state *State) Fn {
	if !ModifyColumnCompatible(state.env.OldColumn, ColumnTypeSet) {
		return None("unsupported change to set")
	}
	col := state.env.Column
	col.Args = []string{"Alice", "Bob", "Charlie", "David"}
	col.Tp = ColumnTypeSet
	return Str(col.TypeString())
})

var ColumnDefinitionTypesDate = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeDate
	return Str(col.TypeString())
})

var ColumnDefinitionTypesTime = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeTime
	return Str(col.TypeString())
})

var ColumnDefinitionTypesDateTime = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeDatetime
	return Str(col.TypeString())
})

var ColumnDefinitionTypesTimestamp = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeTimestamp
	return Str(col.TypeString())
})

var ColumnDefinitionTypesYear = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeYear
	return Str(col.TypeString())
})

var ColumnDefinitionTypesJSON = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	if oldCol := state.env.OldColumn; oldCol != nil {
		if tbl.Indexes.Found(func(index *Index) bool {
			return index.HasColumn(oldCol)
		}) {
			// JSON column cannot be used in key specification.
			return None("json column cannot be used in key")
		}
	}
	col := state.env.Column
	col.Tp = ColumnTypeJSON
	// TODO: support non-array JSON column.
	col.Array = true
	col.SubType = randArrayTp[rand.Intn(len(randArrayTp))]
	return Str(col.TypeString())
})
