# schrddl

Randomized concurrent DDL/DML correctness testing framework for TiDB.

`schrddl` continuously generates and executes mixed workloads (DDL + DML), then checks whether TiDB’s behavior matches the expected in-memory/metadata model (and, in parallel mode, uses TiDB DDL job ordering to replay operations locally).

## What This Repo Contains

- `main.go`: CLI entry and flags.
- `ddl/`: DDL/DML execution engine (parallel/serial/txn modes, metadata, validators).
- `sqlgenerator/`: probabilistic SQL generator (functional DSL + hooks).

## Requirements

- Go `1.24+` (see `go.mod`)
- A TiDB instance reachable via MySQL protocol
- A dedicated database/schema for testing (the tool creates/drops/renames tables, indexes, schemas, etc.)

> Warning: do **not** run this against a production cluster.

## Build

```bash
go build -o schrddl
```

## Run (Requires TiDB)

```bash
./schrddl \
  -addr "127.0.0.1:4000" \
  -db "test" \
  -mode "parallel" \
  -concurrency 20 \
  -tables 1 \
  -time 2h
```

### Common Flags

- `-addr`: TiDB address in `host:port` (default `127.0.0.1:4000`)
- `-db`: database/schema name (default `test`)
- `-mode`: `serial` or `parallel`
- `-concurrency`: number of concurrent workers (default `20`)
- `-tables`: initial tables to create per worker (default `1`, but currently clamped to a minimum of `2`)
- `-time`: total running time (default `2h`)
- `-mysql-compatible`: disable TiDB-only checks/features where applicable
- `-output`: log file path (empty means stdout)

### Advanced Flags

- `-txn`: enable transactional DML executor
- `-rc-txn`: use read-committed isolation (also sets TiDB global `transaction_isolation`)
- `-prepare`: use prepared statements for DML
- `-check-ddl-extra-timeout`: extra timeout for background “DDL queue drained” checks
- `-global-sort-uri`: enable dist task and set `tidb_cloud_storage_uri` (for features that require global sort storage)

## Notes / Behavior

- Connection DSN is currently fixed as `root:@tcp(<addr>)/<db>` (no user/password flags).
- The runner sets some TiDB globals (e.g. time zone, `max_execution_time`; and dist-task globals when `-global-sort-uri` is set).
- The process has a hard exit loop after the configured duration (see `main.go:67`), so expect `os.Exit(0)` shortly after `-time`.

## Development

Run unit tests:

```bash
go test ./...
```

Generate example SQL (prints to stdout):

```bash
go test -v -run TestReadMeExample ./sqlgenerator/
```

## Docker

```bash
docker build -t hub.pingcap.net/qa/schrddl-master .
```
