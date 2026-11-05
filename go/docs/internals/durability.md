# Durability and resume

An S3 run with `-out` keeps a journal so that `-resume` continues an interrupted run. A resume never
re-lists a published key and never skips one, and the final dataset is byte-identical to an uninterrupted
run's. Package `internal/durable` holds the journal; `internal/listing/journal.go` connects it to the router
and the provider. Filesystem runs opt out: they are fast enough to restart.

## Progress states

A key passes through five states. Only the last two survive a crash, and they are never ahead of each other:

| State | Where it lives | After a crash |
|---|---|---|
| fetched | a response body in the client | fetched again |
| emitted | rows in the order stage (memory, or a disposable spill file) | listed again from the durable frontier |
| encoded | an open or closed `.tmp` part | discarded unless its journal record is durable |
| durable | a synced journal record for a synced part | kept; the part is renamed if the rename was lost |
| committed | renamed into `data/`, later the manifest and `_SUCCESS` | kept |

The durable frontier K is the max key of the last record, and records exist only for a contiguous ordinal
prefix (part i is published only after part i−1). No worker's cursor is ever persisted: a worker that has
fetched far past K holds rows that exist only in memory, so the resume lists from K, not from any cursor.

| Crash boundary | Recovery |
|---|---|
| response fetched, nothing emitted (`response-read`) | fetched again; the frontier does not move |
| split decided, children partly queued (`split`) | the tiling is rebuilt from K and the recorded boundaries; the split is lost harmlessly |
| rows buffered or queued to an encoder (`range-done`, `encoding`) | relisted from K |
| part closed and synced, no record (`part-closed`, `journal-append`) | the orphan `.tmp` is removed and the part is reproduced byte for byte |
| record written, not synced (`journal-sync`) | a torn tail is ignored; a whole unsynced frame that survived is a valid record of a synced part |
| record durable, rename absent (`recorded`, `rename`) | the `.tmp` is checked against the record (size, MD5), then renamed |
| rename done, directory sync absent | the same check: the part is either at its final name or still at `.tmp` |
| several parts closed out of order | only the contiguous ordinal prefix was published; later parts are reproduced |
| manifest written, `_SUCCESS` absent (`manifest`) | the resume lists nothing after K and republishes manifest, reports and `_SUCCESS` |
| `_SUCCESS` durable (`success`) | committed; `-resume` reports `ErrComplete` |

Every boundary in the left column is a named crash point (`internal/faults`). `TestCrashMatrix` stops the run
there without cleanup, and `TestSIGKILLAtEveryPoint` has the real process SIGKILL itself there. Both then
resume and compare with an uninterrupted run.

**The promise:**
- On a static source, recovery is byte-identical to an uninterrupted run.
- On a live bucket, durable output is exactly-once: no published key is listed again into the dataset, and
  none is skipped. Objects that change during the run change the rows, and so the part boundaries, as they
  would in any run.
- When K holds scalars S3 refuses in `start-after`, the resume fetches from K's safe prefix and drops keys up
  to K. Such keys are fetched again but never published twice.

## The unit is the part

Parts are published as soon as they close, in ordinal order, by the router's `Publish` hook:

1. The part's encoder closes the file and fsyncs it (`data/part-NNNNN.parquet.tmp`).
2. One journal record is appended and fsynced: the part (ordinal, rows, bytes, MD5, min and max key), the
   router's frozen part limit, the total rows so far, and the range boundaries created since the
   previous record.
3. The part is renamed to `data/part-NNNNN.parquet` and the directory is fsynced.

This is commit-before-emit (I1) at part granularity: no row is published before the record that covers it.
Rows may sit in memory, or in an unpublished `.tmp` part, beyond the last record; a crash loses them and the
resume lists them again. `manifest.json`, the run reports and `_SUCCESS` are written at the end as before.

## Resume

`swath list s3://… -out DIR -resume`:

1. Takes the output lock (`-out/.swath.lock`, `flock`) before reading, recovering, removing or writing
   anything. It is held through the run's cleanup and publication. Fresh runs and overwrites take it too,
   and it lives outside `.swath/`, so replacing a previous run never removes a live owner's lock.
2. Checks the identity in the journal header against the request (`durable.Check`, a pure function). The
   provider, bucket, prefix, endpoint, region, schema, key cap, writer (`parquet.WriterID`: encodings,
   geometry, part-size rule), part size and `-fetch-owner` must match; a mismatch is a typed
   `*durable.MismatchError`. Endpoints are compared after `NormalizeEndpoint` (scheme and host case, a
   trailing slash), the only equivalences allowed: another endpoint may serve another bucket of the same
   name. Workers, encoders, memory, `-seed`, `-no-probe` and credentials may change.
3. Recovers `data/`. Every retained part must match its record's size and MD5 (`ErrPartAltered` otherwise).
   A part whose record is durable but whose rename did not happen is renamed now. Every unrecorded file in
   `data/` is removed.
4. Lists `(K, ⊤]`, where K is the last recorded part's max key, tiled at the recorded boundaries above K:
   `(K, b1], …, (bn, ⊤]`. If K holds scalars S3 refuses in `start-after`, the first range starts at K's safe
   prefix and drops keys up to K.
5. Continues the router at the next ordinal with the recorded limit, so part boundaries fall exactly where
   the uninterrupted run would have put them.

A record also carries whether its part is the final one (`file_final`). Once the final part is durable, a
resume does not list again: it only publishes. On a changed bucket, listing again could add a second final
part. One record carries at most 4,096 boundaries (the most recent), so the journal grows with parts, not
splits. A record over the frame limit is refused on append as well as on read.

A journaled run that fails with an ordinary error (not a crash) leaves its temporary parts in place: one
may already be recorded (its rename failed), and only `-resume`'s reconciliation decides which are.
`-resume` on a complete dataset returns `ErrComplete`; on a directory without a journal,
`ErrNothingToResume`. A new run over an interrupted one needs `-overwrite`.

## Why splits need no transaction

Splits are owner-only (no thieves), so there is no stale-snapshot race and no compare-and-swap. A split moves
DECIDED (its boundaries appended to the provider's list, in memory, under the provider mutex) → DURABLE (taken
into the next part's record) → CHILDREN_EMITTED. Resume correctness depends only on K. The boundaries only
choose how `(K, ⊤]` is cut, and every cut is a valid tiling. A split that never became durable costs
parallelism on resume, never a key. The only work under the split mutex is the in-memory append.

## The journal

`DIR/.swath/journal` is append-only. Each frame is a 4-byte little-endian length, a CRC-32C (Castagnoli) over the length and
the payload, and the payload (JSON); journal version 2. The first frame is the header (tool, format, identity, creation time).
Every later frame is a part record, with the ordinals dense from 0. One writer goroutine owns the file;
`Append` returns after `fsync`.

An incomplete last frame, or a bad checksum on the last frame, is a torn tail, unless a part is published
under its final name beyond the recorded ones. A rename only follows an acknowledged record, so such a part
proves a record was lost (a damaged length, or bit rot in the last frame). The resume then refuses
(`ErrCorrupt`) before deleting anything. It was never acknowledged, so
nothing depends on it, and a reopen truncates it. A bad checksum with data after it is interior corruption
(`ErrCorrupt`), and the resume refuses. So does an unknown header version. A journal whose header never
became durable means nothing was durable, and the run starts over. `swath inspect DIR` prints the header, every record and any torn
tail as JSON lines.

**The contract, precisely.**
- **Process crash (SIGKILL, panic, OOM kill) on a running kernel.** Every acknowledged step survives, because
  the page cache does. This is what the crash matrix and the SIGKILL tests demonstrate.
- **Power loss or a kernel crash.** Recovery holds when the filesystem honours `fsync` on files and
  directories (ext4, XFS and APFS do). Before a journal record names a part:
  - the part file is synced, and so is its directory entry in `data/`;
  - `data/` and `.swath/` were created with their entries in the output directory synced;
  - the journal frame is synced before `Append` returns;
  - a rename is followed by a sync of `data/`.

  Nothing tests this: SIGKILL cannot show power-loss behaviour.
- **Unsupported on Windows:** directories cannot be synced and the lock is not enforced, so neither
  guarantee holds there; a journaled run prints a warning (`durable.Enforced` is false).
