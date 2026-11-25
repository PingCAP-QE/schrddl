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
export SCHRDDL_TLS_CA_SHA256=$(sha256sum /run/secrets/db-ca.pem | cut -d' ' -f1)
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
- `-rc-txn`: use read-committed isolation (the cluster-wide `transaction_isolation`
  global is only set with `SCHRDDL_ALLOW_GLOBAL_TUNING=true`)
- `-prepare`: use prepared statements for DML
- `-check-ddl-extra-timeout`: extra timeout for background “DDL queue drained” checks
- `-global-sort-uri`: enable dist task and set `tidb_cloud_storage_uri` (for features that require global sort storage).
  The two globals are cluster-wide changes, so the flag also needs
  `SCHRDDL_ALLOW_GLOBAL_TUNING=true` and a privileged account; the run fails without them.

## Notes / Behavior

- Every database connection, including setup, workers and diagnostic dumps, uses
  `SCHRDDL_DB_USER` and `SCHRDDL_DB_PASSWORD_FILE`. There is no default user.
- TLS 1.2+ with certificate and hostname verification is required by default. System
  roots are used unless `SCHRDDL_TLS_CA` selects a PEM CA bundle. Optional mutual TLS
  uses `SCHRDDL_TLS_CERT` and `SCHRDDL_TLS_KEY` together.
- `SCHRDDL_TLS_CA` is the trust anchor every database connection and status API request
  sends its credentials to, so it is validated and pinned. The file must be a regular
  file of at most 1 MiB, owned by root or the process user, and not writable by group
  or others. `SCHRDDL_TLS_CA_SHA256`, the SHA-256 of the PEM file
  (`sha256sum db-ca.pem`), is required whenever `SCHRDDL_TLS_CA` is set: another file
  in its place is rejected before any connection is opened, even when it has safe
  ownership and permissions. Symlinks are followed, as secret volumes mount them; the
  file they lead to is what is checked. The bundle is re-read and re-verified for every
  new connection, so rotate the CA and its pin together and restart.
- Schema metadata for the framework engine is fetched from the TiDB status API through
  the shared client in `dbconfig` (no `curl`): over verified TLS, with the workload
  account as basic authentication. Responses are validated (status, content type, JSON,
  size bound) before being trusted, and redirects are never followed. The status address
  is derived from `-addr` (SQL port + 6080, e.g. 4000 → 10080) or set explicitly with
  `SCHRDDL_STATUS_ADDR`, whose host must be the `-addr` host, so an override can only
  select a different port.
- Only isolated disposable test clusters should use `SCHRDDL_ALLOW_INSECURE=true`;
  this explicitly permits plaintext and an omitted password file, is reported with a
  warning, and the user must still be configured explicitly. It never sends the
  database password over plaintext: status API requests carry no credentials in this
  mode.
- Diagnostic dumps are local SQL files with owner-only permissions. They use a
  read-only repeatable-read transaction and the same verified connector as workers.
- The workload account does not need `SUPER`/`SYSTEM_VARIABLES_ADMIN`. Every
  cluster-wide `SET GLOBAL` statement in the runner is issued through one guard
  (`util.ApplyGlobalTuning`) and only when `SCHRDDL_ALLOW_GLOBAL_TUNING=true`
  is set for a privileged account. Without exception, that covers runner setup
  (`max_execution_time`, `transaction_isolation` under `-rc-txn`,
  `tidb_enable_global_index`, resetting `tidb_cloud_storage_uri`), the cluster
  time zone, the dist-task globals required by `-global-sort-uri`, the
  failure-scene GC freeze, and the cluster-wide settings the random SQL generator
  produces (`tidb_ddl_enable_fast_reorg`, `tidb_enable_dist_task`,
  `tidb_row_format_version`, `tidb_enable_clustered_index`). Generated
  statements are classified before they are executed: session-scoped `SET`
  statements always run, a cluster-wide one is skipped with one warning per
  setting and never fails the worker. `-global-sort-uri` cannot run without its
  globals, so that run fails early instead.
- After a fatal worker error the runner best-effort freezes cluster GC to preserve
  the failure scene. The freeze is a cluster-wide change and needs
  `SCHRDDL_ALLOW_GLOBAL_TUNING=true`; without it GC keeps running and that is
  logged. The freeze is bounded (`SCHRDDL_GC_FREEZE_WINDOW`, default
  `72h`, capped at `720h`, `0` disables it), logs the previous value and an exact
  restore statement, applies at most once, and self-restores when the window
  elapses; GC is never disabled indefinitely and a missing privilege is only logged.
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
