# Storage improvement verification

The changes were exercised on Ubuntu Linux ARM64 in MSL `Dev`, using Rust 1.97.1 and the adjacent Zenoh/RocksDB checkouts identified in `../baselines.json`.

## Checks

The selected core crates passed 29 Rust tests, the RocksDB crate passed its database test, and all 10 live integration checks passed. These are targeted checks, not the complete upstream multi-transport test suite.

| Check | Coverage |
| --- | --- |
| Core Rust unit tests | Configuration opt-in/defaults, readiness transitions, explicit synchronization, write/sync failure propagation, error replies, request validation, unavailable service, and lost acknowledgement after request handling. Existing selected-crate tests run alongside the additions. |
| RocksDB unit test | Put, replacement, and deletion with synchronization, closing and reopening the actual database. |
| Live Linux proof | Optional failure visibility, required startup failure, command permission separation, default-off behavior, unsupported durability rejection before mutation, volatile processed writes, out-of-scope/wildcard rejection, live subscriber notification, and synchronized put/delete recovery after `SIGKILL`. |
| Static checks | Rust formatting, targeted Clippy with warnings denied, ShellCheck for launchers, and Python syntax parsing. |

Commands from the Zenoh checkout:

```sh
./contrib/storage-improvements/cargo test \
  -p zenoh-plugin-storage-manager -p zenoh_backend_traits -p zenoh-ext \
  --no-default-features --features zenoh/transport_tcp,zenoh-ext/unstable --lib
./contrib/storage-improvements/cargo clippy \
  -p zenoh-plugin-storage-manager -p zenoh_backend_traits -p zenoh-ext \
  --no-default-features --features zenoh/transport_tcp,zenoh-ext/unstable \
  --lib --tests -- -D warnings
./contrib/storage-improvements/cargo fmt --all -- --check
./contrib/storage-improvements/verify
```

From the RocksDB checkout:

```sh
./contrib/storage-improvements/cargo test --features zenoh/runtime_plugins --lib
./contrib/storage-improvements/cargo clippy --features zenoh/runtime_plugins --lib --tests -- -D warnings
./contrib/storage-improvements/cargo fmt --all -- --check
```

The latest live result is `.local/storage-improvements/proof.json`. Router logs in that directory exclude startup configuration lines and redact disposable credential values. No proof daemon remains running afterward.

## Interpreting the crash results

The put test receives a `local_sync` acknowledgement and immediately kills the router before any read-back of that value. A restarted router reads the expected replacement value. The deletion test likewise kills immediately after acknowledgement, then verifies absence after restart and readiness. This avoids treating a pre-crash read or graceful backend shutdown as the persistence boundary.

These results exercise process-crash recovery. They do not simulate a power cut, faulty drive cache, disk-full conditions, or replica quorum. The implementation's stable-storage guarantee derives from the backend synchronization primitive; broader hardware failure guarantees need additional testing.

## Contribution status

Source changes and development tooling are prepared for review in the onexay forks. The RocksDB fork pins the matching Zenoh fork commit. Upstream submission and releases remain separate work; no upstream acceptance is implied.
