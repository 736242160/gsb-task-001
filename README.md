# gsb-kv: Embedded MVCC + WAL Key/Value Store

A small embedded key/value storage engine written in pure Go (standard
library only). It provides snapshot-isolation transactions, multi-version
concurrency control (MVCC), and a CRC-protected write-ahead log (WAL).

## Quick Start

```go
import kv "gsb-kv"

e, err := kv.Open("./data")            // creates dir + wal.log if needed
defer e.Close()

tx, _ := e.Begin(false)                // read-write transaction
_ = tx.Set("foo", []byte("bar"))
_ = tx.Commit()                        // WAL fsync, then global visibility

rt, _ := e.Begin(true)                 // read-only snapshot
v, err := rt.Get("foo")                // []byte("bar")
_ = rt.Rollback()

_ = e.Vacuum()                         // reclaim obsolete versions
```

`OpenWithOptions(dir, Options{SyncWAL: false})` disables per-commit fsync for
bulk-loading; the default (`DefaultOptions`) is durable.

## Concurrency Model (Snapshot Isolation)

- `Begin` allocates a monotonic **begin timestamp** (`read_tx_id`). A
  transaction reads the newest version with `created_tx_id < begin_tx_id`,
  exactly its snapshot. In-flight or later-committed data is invisible.
- `Commit` allocates a separate monotonic **commit timestamp** that stamps
  every new version (`created_tx_id`).
- First-committer-wins: before committing, if the head version of any written
  key has `created_tx_id >= begin_tx_id`, the commit fails with
  `ErrWriteConflict` and the transaction is automatically rolled back.
- Commit is a single critical section: the commit record is written and
  fsynced to the WAL **before** versions are published into the index, and the
  timestamp/WAL/visibility order is atomic.
- `Engine` uses an `sync.RWMutex` (concurrent snapshot reads) plus a dedicated
  mutex serializing WAL appends.

## MVCC Storage & Vacuum

- Each key points to a version chain, newest first; every version records
  `key`, `value`, `created_tx_id`, `deleted_tx_id`, and `is_deleted`
  (tombstone).
- The active-transaction tracker computes the **watermark**: the earliest
  begin timestamp still in flight.
- `Vacuum()` keeps for each key all versions `created_tx_id >= watermark` and
  the single newest version below it (the agreed view for all live snapshots).
  When no transaction is active it keeps only the head and deletes keys whose
  head is a tombstone.

## WAL Format & Crash Recovery

Each record is a self-describing frame:

```
| magic(4) "KVWL" | crc32(4) | payloadLen(4) |
| txID(8) | recordType(1) | keyLen(4) key | valLen(4) value |
```

Record types: `TxBegin`, `Set`, `Delete`, `TxCommit`, `TxRollback`.
The commit frame stores the commit timestamp in `txID` and the begin
timestamp in its 8-byte value.

On `Open`, frames are replayed in order:

- committed transactions are rebuilt in commit-timestamp order;
- transactions that began but never committed are discarded completely;
- a torn tail or CRC32 mismatch truncates the file to the last valid commit
  point and `Open` returns the warning `ErrWALCorrupt` (the returned engine is
  usable); a clean replay returns `nil`.

## Testing

```sh
go test -race -count=1 ./...
```

Covered: `TestSnapshotIsolation`, `TestWriteConflict`, `TestCrashRecovery`,
`TestVacuum`, plus CRC corruption, rollback replay, concurrent read/write
retries, and sentinel-error cases.
