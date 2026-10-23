# Storage readiness and acknowledged writes

This branch adds per-storage readiness and an opt-in acknowledged-write service to Zenoh. The sibling RocksDB fork implements local WAL synchronization. These changes operate at the storage layer and contain no agent-platform concepts.

## Build and verify on Linux

Keep these checkouts adjacent:

```text
~/Projects/zenoh
~/Projects/zenoh-backend-rocksdb
```

On macOS, the scripts run in MSL `Dev`. On Linux, they use the current checkout directly. Rust 1.97.1, a C/C++ compiler, libclang, Python 3, and network access for initial dependencies are required. Build output stays in `${XDG_CACHE_HOME:-$HOME/.cache}/zenoh-fork/target`.

```sh
./contrib/storage-improvements/verify
```

The launcher builds the router, storage manager, and sibling backend. The backend pins an immutable commit in the Zenoh fork; keep this checkout at that revision when building a matching router and plugins. Its Cargo wrapper can use the adjacent Zenoh source with `ZENOH_LOCAL_SOURCE=1` when developing changes across both repositories. Local overrides can update the backend lockfile; restore its committed lockfile before building the pinned combination.

Both dynamic libraries and the router must use the same Zenoh revision, Rust toolchain, and compatible feature set. The launcher explicitly enables the backend-traits default feature in the host build to match the backend. Do not mix these libraries with release binaries.

The live proof uses a checksum-pinned native Python client wheel, disposable credentials, temporary Linux databases, and loopback listeners. It stops the routers and removes temporary databases afterward. Results and sanitized logs are written under `.local/storage-improvements/`, which Git ignores. `verify --skip-build` reuses the existing binaries.

## Storage readiness

Each storage accepts two new optional boolean fields:

```json
{
  "key_expr": "application/state/**",
  "required": true,
  "acknowledged_writes": true,
  "volume": {"id": "rocksdb", "dir": "state", "create_db": true}
}
```

Both default to `false`, preserving optional startup and disabling the command service unless selected explicitly.

Query the router's admin space at:

```text
@/<router-zid>/router/status/plugins/storage_manager/readiness
```

The response contains:

- `ready`: every configured required storage is ready;
- `healthy`: every configured storage, including optional storages, is ready;
- `storages`: a map of storage name to `required`, `state`, and optional `error`.

States are `initializing`, `ready`, `failed`, and `stopped`. Ready means the backend initialized and the subscriber/queryable interfaces were declared. It does not establish a successful write or synchronized disk commit. Failed initialization remains visible even if no worker was created. Worker panics, channel failure, or observed write failure make readiness fail; a storage must be recreated to clear a sticky failure.

An optional failed storage leaves unrelated stores running. A required storage's synchronous backend initialization failure returns an error from the storage-manager plugin. Configure the plugin itself with `__required__: true` to make `zenohd` fail startup on that error. Asynchronous declaration/worker failures are exposed through readiness rather than terminating the router. Admission controllers must check readiness after startup and while operating.

Ordinary read errors are logged without automatically failing readiness, because existing backends can represent a missing record as an error. Readiness is not a continuous disk probe. Existing backend status responses remain available; administrative status waits are bounded.

## Acknowledged-write endpoint

When `acknowledged_writes` is enabled, a storage exposes a separate concrete command endpoint:

```text
@storage/<router-zid>/router/<storage-name>/write
```

For non-router plugin hosts, the third segment is the host mode. This endpoint is separate from ordinary stored-data query expressions. Grant query/reply permission on it only to authorized writers. Granting read queries over the data namespace must not grant command access. Live subscribers also require the appropriate egress publication permissions.

Send a Zenoh query with JSON metadata in its attachment and the raw value in its payload. Query encoding identifies the value encoding. Deletes carry no payload. Metadata is limited to 4096 bytes and rejects unknown fields.

```json
{
  "protocol": 1,
  "operation_id": "write-42",
  "operation": "put",
  "key": "application/state/task-42",
  "durability": "local_sync"
}
```

`operation` is `put` or `delete`. Keys must be concrete and included by the storage's configured key expression. Wildcards and out-of-scope keys are rejected.

| Durability | Acknowledgement means |
| --- | --- |
| `processed` | The backend accepted the write. Its own buffering and persistence policy still applies. |
| `local_sync` | The backend accepted the write and completed its explicit local stable-storage synchronization operation. |

Backends must explicitly advertise synchronization support. The default trait implementation rejects it. A request for unsupported `local_sync` fails before mutation; no silent downgrade occurs. The RocksDB fork uses `flush_wal(true)` after the write batch containing payload and recovery metadata.

A successful response contains protocol, operation ID, concrete key, command service identity, requested durability, disposition (`inserted`, `replaced`, or `deleted`), and timestamp. An outdated write is rejected rather than presented as a successful synchronized write. The same timestamp is used to notify live subscribers and other replicas after storage processing. Notification success and replica persistence are not prerequisites for a local commit acknowledgement.

## Errors and uncertain outcomes

Preflight rejection returns an error reply with `may_have_applied: false`. A backend write or synchronization error returns `may_have_applied: true` conservatively: the write may already be present even though the requested synchronization failed.

The Rust `zenoh_ext::acknowledged_storage_write` helper is behind the `unstable` feature. It validates input, rejects ambiguous or mismatched replies, distinguishes structured rejection from unknown outcome, and disables query consolidation when collecting responder results. An absent, malformed, ambiguous, or lost reply is `UnknownOutcome`.

Operation IDs correlate requests and replies. They do not implement durable deduplication. Retry only after reconciling uncertain state; this API does not provide exactly-once execution, compare-and-swap, queue leases, or a multi-key transaction. `local_sync` is local to the selected backend, not a replica quorum. Disk and controller behavior must honor the synchronization primitive for power-loss durability.

## Review and release boundaries

The local baseline revisions are recorded in `baselines.json`. These changes are developed in the onexay forks; they are not a published release or an upstream contribution. Upstream acceptance requires the project's contributor process. These patches do not change the Zenoh wire protocol or the semantics of native pub/sub `put`.

See [verification results](reports/verification.md). Full-platform integration, power-cut testing, disk-full fault injection, replica-quorum acknowledgements, and durable operation-ID deduplication remain outside this patch.
