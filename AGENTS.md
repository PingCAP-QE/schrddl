# AGENTS.md - schrddl

**Generated:** 2026-01-08 | **Commit:** 6b11acd | **Branch:** master

> DDL correctness testing framework for TiDB via randomized concurrent DDL/DML

## STRUCTURE

```
schrddl/
├── main.go           # CLI entry, flags, mode selection
├── ddl/              # DDL test execution (run, ops, meta)
├── sqlgenerator/     # Probabilistic SQL generation DSL
├── Dockerfile        # Build: hub.pingcap.net/qa/schrddl-master
└── .github/workflows/build.yml  # CI: docker build + push
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| CLI flags | `main.go:30-42` | addr, db, mode, concurrency, tables, time |
| Test execution | `ddl/run.go` → `ddl/ddl.go` | Run() → Initialize() → Execute() |
| DDL operations | `ddl/ddl_ops.go` | 68KB, all DDL ops |
| DML operations | `ddl/dml_ops.go` | Insert/Update/Delete |
| SQL generation entry | `sqlgenerator/rule.go:Start` | DSL root |
| State/Table/Column | `sqlgenerator/db_type.go` | Core data structures |
| DSL primitives | `sqlgenerator/generator_lib.go` | Or/And/Str/Repeat |
| Hooks (txn, debug) | `sqlgenerator/hook*.go` | Pred/Replacer/Scope/TxnWrap |

## CODE MAP

| Symbol | Type | Location | Refs | Role |
|--------|------|----------|------|------|
| `State` | struct | db_type.go:9 | 334 | SQL generation context |
| `Fn` | struct | generator_types.go:23 | 341 | DSL function node |
| `Table` | struct | db_type.go:31 | - | Table metadata |
| `Column` | struct | db_type.go:52 | - | Column definition |
| `DDLCase` | struct | ddl/ddl.go:59 | - | Test case container |
| `testCase` | struct | ddl/ddl.go | - | Execution context |

## CONVENTIONS

### Imports (3-section)
```go
import (
    "context"  // stdlib

    "github.com/juju/errors"  // third-party
    "github.com/ngaut/log"

    "github.com/PingCAP-QE/schrddl/sqlgenerator"  // local
)
```

### Logging
- Use `github.com/ngaut/log` (NOT stdlib `log`)
- Fatal: `log.Fatalf("[ddl] msg %v", err)`
- Warn: `log.Warnf(...)` for non-fatal issues

### Error Handling
- Wrap: `errors.Trace(err)` or `errors.Annotatef(err, "context")`
- Check immediately, return early

### DSL Pattern
```go
var RuleName = NewFn(func(state *State) Fn {
    return Or(
        ChildRule.W(10).P(Prerequisite),  // Weight + Prereq
        AnotherRule.W(5),
    )
})
```

## ANTI-PATTERNS (THIS PROJECT)

| Pattern | Reason | Location |
|---------|--------|----------|
| Nested BEGIN in txn | Forbidden at hook_txn_wrap.go:48 | Causes transaction errors |
| Float/Double precision | Deprecated at db_generator.go:61 | Precision issues |
| 1970-01-01 date | db_generator.go:592 | Avoid boundary date |
| Non-ASCII strings | stats_extension.go:144 | Force ASCII to avoid encoding issues |
| blob column conversion | ddl_ops.go:993 | Depends on TiDB bug fix |

## TODO/KNOWN ISSUES

- CTE recursion/reference incomplete (rule_cte.go)
- Outer join generation not implemented (rule_query.go)
- JSON non-array columns not supported (rule_column.go)
- KindBit default value bug (datatype.go:112)

## COMMANDS

```bash
# Build
go build -o schrddl

# Test
go test ./...
go test -v -run TestReadMeExample ./sqlgenerator/

# Run (requires TiDB)
./schrddl -addr "127.0.0.1:4000" -db "test" -mode "parallel" -concurrency 20 -time 2h

# Docker
docker build -t hub.pingcap.net/qa/schrddl-master .
```

## NOTES

- Pre-existing `go vet` warnings (format strings) - build still passes
- Tests in `ddl/` require TiDB connection for full integration
- Global variables control behavior: `EnableTransactionTest`, `RCIsolation`, `Prepare`, `GlobalSortUri`
- `timeoutExitLoop` goroutine forces `os.Exit(0)` after test duration
