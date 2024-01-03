# Unify Test Framework

## What is Unify Test Framework?

## How to run?
    
```bash
go build
./schrddl --mode="parallel" --concurrency=1
```

## Fuzz support:

- [ ] SelectStmt:
  - [ ] SelectStmtFromTable "FROM" TableRefsClause WhereClauseOptional SelectStmtGroup HavingClause WindowClauseOptional OrderByOptional SelectStmtLimitOpt SelectLockOpt SelectStmtIntoOption

- [ ] SelectStmtBasic
    - [ ] DistinctOpt
    - [x] TableOptimizerHints

- [ ] Field
    - [ ] `*`
    - [ ] Identifier.*
    - [ ] Identifier.Identifier
    - [ ] Expression as xxx