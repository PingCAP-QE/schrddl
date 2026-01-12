# ddl/

> DDL test execution engine

## OVERVIEW

10 files implementing DDL/DML test execution against TiDB. Manages table metadata, executes operations, validates correctness.

## STRUCTURE

```
ddl/
├── run.go                  # Entry: Run(), DB connection, signal handling
├── ddl.go                  # DDLCase, executors (Parallel/Serial/Transaction)
├── ddl_ops.go              # DDL operations (68KB, largest file)
├── dml_ops.go              # DML operations (Insert/Update/Delete)
├── ddl_multi_schema_change.go  # Multi-schema change support
├── meta.go                 # Table/Column metadata, type mapping
├── datatype.go             # Field types, random data generation
├── util.go                 # Utilities
└── *_test.go               # Tests
```

## WHERE TO LOOK

| Task | File | Entry |
|------|------|-------|
| Start execution | `run.go:Run()` | Main entry |
| Add DDL operation | `ddl_ops.go` | Add ddlTestOpExecutor |
| Add DML operation | `dml_ops.go` | dmlTestOpExecutor |
| Table metadata | `meta.go` | ddlTestTable, ddlTestColumn |
| Data type generation | `datatype.go` | RandDataType, TestFieldType |
| Execution modes | `ddl.go` | ParallelExecuteOperations, SerialExecuteOperations |

## KEY TYPES

| Type | Purpose |
|------|---------|
| `DDLCase` | Test case container with config |
| `DDLCaseConfig` | Concurrency, MySQLCompatible, TablesToCreate |
| `testCase` | Runtime execution context |
| `ddlTestOpExecutor` | DDL operation executor |
| `dmlTestOpExecutor` | DML operation executor |
| `ddlTestTable` | Table metadata for validation |
| `ddlTestColumn` | Column metadata |

## EXECUTION FLOW

```
Run() → openDB() → NewDDLCase() → Initialize() → Execute()
                                                    ↓
                              ParallelExecuteOperations (if parallel mode)
                              SerialExecuteOperations (if serial mode)
                              TransactionExecuteOperations (if txn enabled)
```

## GLOBAL VARIABLES

| Variable | Purpose |
|----------|---------|
| `EnableTransactionTest` | Enable transaction executor |
| `RCIsolation` | RC isolation level |
| `Prepare` | Use prepared statements |
| `CheckDDLExtraTimeout` | Extra DDL timeout |
| `GlobalSortUri` | Dist task sort URI |

## ANTI-PATTERNS

- blob column conversion depends on TiDB bug fix (ddl_ops.go:993)
- filedTypeM > 7 treated as bug (meta.go:691)
- KindBit default value bug unfixed (datatype.go:112)
- KindJSON virtual column check bug unfixed (datatype.go:133)

## NOTES

- Uses unsafe pointer aliasing (ddl_ops.go:15,225) - be careful
- Heavy error logging with stack traces in ddl_ops.go
- Signal handling (HUP/INT/TERM/QUIT) in run.go
