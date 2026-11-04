# schrddl

Randomized concurrent DDL/DML correctness testing framework for TiDB.

`schrddl` continuously generates and executes mixed workloads (DDL + DML), then checks whether TiDB’s behavior matches the expected in-memory/metadata model (and, in parallel mode, uses TiDB DDL job ordering to replay operations locally).

## What This Repo Contains

- `main.go`: CLI entry and flags.
- `ddl/`: DDL/DML execution engine (parallel/serial/txn modes, metadata, validators).
- `framework/`: unified SQL correctness framework (optional engine).
- `sqlgenerator/`: probabilistic SQL generator (functional DSL + hooks).

## Requirements

Profiling is disabled by default. To enable it for the framework engine, bind an explicit
loopback IP (for example `-pprof-addr 127.0.0.1:6060`) and pass `-pprof-token-file /path/to/token`.
The token file must be owner-only (mode `0600`) and contain a random token of at least 32
characters. Clients must send `Authorization: Bearer TOKEN`; keep the token out of command-line
arguments and shared logs. The server has HTTP timeouts and never exposes the cmdline endpoint.

- Go `1.24+` (see `go.mod`)
- A TiDB instance reachable via MySQL protocol
- A dedicated database/schema for testing (the tool creates/drops/renames tables, indexes, schemas, etc.)

> Warning: do **not** run this against a production cluster.

## Build

```bash
go build -o schrddl
```

## Run (Requires TiDB)

Database authentication and TLS are configured through the environment. Mount the
password as an owner-only (`0600`) file; do not put secrets in command-line arguments.
Use a dedicated workload user with the privileges required by the selected tests.

```bash
export SCHRDDL_DB_USER=ddl_test
export SCHRDDL_DB_PASSWORD_FILE=/run/secrets/db-password
export SCHRDDL_TLS_CA=/run/secrets/db-ca.pem
./schrddl \
  -addr "127.0.0.1:4000" \
  -db "test" \
  -mode "parallel" \
  -concurrency 20 \
  -tables 1 \
  -time 2h
```

## Engines

`schrddl` supports two engines:

- `-engine ddl` (default): legacy randomized concurrent DDL/DML engine in `ddl/`.
- `-engine framework`: unified SQL correctness framework in `framework/` (optional oracles such as NoREC/TLP/EET/CERT).

Framework-specific flags:

- `-aqs`: enable Approximate Query Synthesis
- `-cert`: enable CERT
- `-tlp`: enable TLP
- `-eet`: enable EET
- `-pprof-addr`: start authenticated pprof on an explicit loopback address (e.g. `127.0.0.1:6060`)
- `-pprof-token-file`: required owner-only token file when profiling is enabled

Note: `-global-sort-uri` only applies to `-engine ddl`.

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

- Every database connection, including setup, workers and diagnostic dumps, uses
  `SCHRDDL_DB_USER` and `SCHRDDL_DB_PASSWORD_FILE`. There is no default user.
- TLS 1.2+ with certificate and hostname verification is required by default. System
  roots are used unless `SCHRDDL_TLS_CA` selects a PEM CA bundle. Optional mutual TLS
  uses `SCHRDDL_TLS_CERT` and `SCHRDDL_TLS_KEY` together.
- Only isolated disposable test clusters should use `SCHRDDL_ALLOW_INSECURE=true`;
  this explicitly permits plaintext and an omitted password file. The user must
  still be configured explicitly.
- Diagnostic dumps are local SQL files with owner-only permissions. They use a
  read-only repeatable-read transaction and the same verified connector as workers.
- The runner sets some TiDB globals (e.g. time zone, `max_execution_time`; and dist-task globals when `-global-sort-uri` is set).
- The process has a hard exit loop after the configured duration (see `main.go`), so expect `os.Exit(0)` shortly after `-time`.

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

## Credentials and rotation

`-global-sort-uri` accepts only credential-free `s3://`, `gs://`, `gcs://` or
`azblob://` storage locations. The only query options allowed are `region`,
`force-path-style` and `storage-class`. Configure workload identity / instance roles
on the TiDB storage workers; credentials are never accepted in the URI, arguments,
or `tidb_cloud_storage_uri`. The URI is bound as a SQL parameter.

The issue-creation helper reads `GITHUB_TOKEN_FILE` (an owner-only mounted secret)
and never prompts for or echoes a PAT. Use a fine-grained, expiring token scoped to
the target repository. Do not commit secret files or include their contents in logs.

For database passwords and PATs: issue a replacement, atomically replace the mounted
secret file, restart clients to discard pooled connections / cached credentials,
verify the replacement, then revoke the old credential at its provider. Password
files are read when each new pool is created; replacing a file does not invalidate
existing sessions. Use the same restart procedure for CA/client-certificate changes.
If credentials were previously passed in argv or a cloud-storage URI, revoke them,
clear the old TiDB global value, and remove exposed copies from logs/artifacts under
the applicable retention policy. Cloud workload identities use provider-managed
short-lived credentials and avoid persisting long-lived keys in TiDB metadata.
