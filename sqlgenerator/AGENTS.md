# sqlgenerator/

> Probabilistic SQL generation framework using functional DSL

## OVERVIEW

44 files implementing a grammar-based SQL generator. Uses weighted random selection with prerequisites to generate valid DDL/DML statements.

## STRUCTURE

```
sqlgenerator/
├── generator_*.go    # DSL core: Fn, NewFn, Or, And, Str
├── db_type.go        # State, Table, Column, Index (core types)
├── db_*.go           # State operations: mutator, retriever, generator
├── rule.go           # Entry point: Start, all DDL/DML rules
├── rule_*.go         # Domain-specific rules (query, column, index, cte, expr)
├── hook*.go          # Execution hooks: Pred, Replacer, Scope, TxnWrap
└── *_test.go         # Tests with testify
```

## WHERE TO LOOK

| Task | File | Entry |
|------|------|-------|
| Add new DDL rule | `rule.go` | Add to `Start` Or() |
| Add new DML rule | `rule.go` | Add to `DMLStmt` |
| Add column type | `db_constant.go` | `ColumnType` enum |
| Modify table generation | `db_generator.go` | `GenNewTable`, `GenNewColumn` |
| Add query clause | `rule_query.go` | `Query`, `SelectFields`, etc. |
| Add expression type | `rule_expr.go` | Expression generation |
| Hook execution | `hook.go` | `FnHook` interface |

## KEY TYPES

| Type | Purpose |
|------|---------|
| `State` | Holds tables, columns, hooks, config during generation |
| `Fn` | Function node with Gen, Weight, Repeat, Prerequisite |
| `Table` | Table metadata with Columns, Indexes |
| `Column` | Column definition with Tp, Collation, constraints |

## DSL PRIMITIVES

```go
NewFn(func(state *State) Fn { ... })  // Create rule
Or(a, b, c)                            // Weighted choice
And(a, b, c)                           // Concatenate
Str("literal")                         // String literal
Strs("a", "b", "c")                    // Random choice from strings
fn.W(10)                               // Set weight
fn.P(prerequisite)                     // Set prerequisite
fn.R(1, 5)                             // Repeat range
fn.Eval(state)                         // Evaluate to string
```

## HOOKS

| Hook | Purpose |
|------|---------|
| `NewFnHookPred` | Conditional execution |
| `NewFnHookReplacer` | Replace rules dynamically |
| `NewFnHookScope` | Scoped rule overrides |
| `NewFnHookTxnWrap` | Transaction wrapping |
| `NewFnHookDebug` | Debug output |

## ANTI-PATTERNS

- Float/Double precision deprecated (db_generator.go:61)
- Always use ASCII-only strings (stats_extension.go:144)
- Avoid 1970-01-01 boundary date (db_generator.go:592)
- Never BEGIN inside existing transaction (hook_txn_wrap.go:48)

## TODO ITEMS

- rule_cte.go:339/364/370 - CTE recursion incomplete
- rule_query.go:335/437 - Expression/outer join generation
- rule_column.go:379 - JSON non-array columns
- rule_expr.go:22/144 - Function generation flexibility, OOM risk
