# Unify Test Framework

## What is Unify Test Framework?

Unify Test Framework is a test framework for TiDB. It is designed to test the correctness of SQLs.

It builds table schema randomly by create table SQLs, and then generates some random data by DMLs.
After that, it executes SQLs and verify if it is correct.

## How to run?
    
### Prerequisites

Start a TiDB server with 4000 port and 10080 status port.

Recommend to use TiUP to start a TiDB cluster.

``` bash
tiup playground nightly --db=1 --kv=1 --tiflash=0
```

### Build and Run


```bash
go build
./schrddl
```

You can change the concurrency by setting `--concurrency` flag.

```bash
./schrddl --mode="parallel" --concurrency=20
```

## Fuzz support:

- [x] Non Aggregation Select
- [x] Aggregation Select
- [x] Join
- [x] Multi-value Index
- [x] Non-recursive CTE with common select
- [ ] Nested CTE
- [ ] Recursive CTE
- [ ] Subquery