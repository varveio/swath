# Durability and resume

An S3 or GCS run with an output directory keeps a journal so that `swath resume DIR` continues an interrupted run. A resume never
publishes a key twice and never skips one, and the final dataset is byte-identical to an uninterrupted
run's. Package `internal/journal` holds the journal; `internal/listing/journal.go` connects it to the router
and the provider. Filesystem runs opt out: they are fast enough to restart.

## Progress states

A key passes through five states. Only the last two survive a crash. Durable progress always runs ahead of
the rename into `data/` (a record is synced before its part is renamed), and recovery closes that gap:

| State | Where it lives | After a crash |
|---|---|---|
| fetched | a response body in the client | fetched again |
| emitted | rows in the order stage (memory, or a disposable spill file) | listed again from the durable frontier |
| encoded | an open or closed part in `.swath/parts/` | discarded unless its journal record is durable; a claimed part is set aside (below) |
| durable | a synced journal record for a synced part | kept; the part is renamed if the rename was lost |
| published | renamed into `data/` (early, part by part; not yet a dataset) | kept |

The dataset is committed only when `manifest.json` names every part. The durable frontier K is the max key of the last record, and records exist only for a contiguous ordinal
prefix (part i is published only after part i−1). No worker's cursor is ever persisted: a worker that has
fetched far past K holds rows that exist only in memory, so the resume lists from K, not from any cursor.

| Crash boundary | Recovery |
|---|---|
| response fetched, nothing emitted (`response-read`) | fetched again; the frontier does not move |
| split decided, children partly queued (`split`) | in memory only: the resume starts from K; the split is lost harmlessly |
| rows buffered or queued to an encoder (`range-done`, `encoding`) | relisted from K |
| part closed and synced, not claimed (`part-closed`) | the orphan in `.swath/parts/` is removed and the part is reproduced byte for byte |
| part claimed, record not yet written (`journal-append`) | the claimed part is set aside and must be reproduced exactly (below): on disk this is the same as an acknowledged record lost whole since |
| record written, not synced (`journal-sync`) | a whole unsynced frame that survived is a valid record of a synced part, and recovery syncs the journal before acting on it; a torn tail (power loss) is dropped, and its claimed part is set aside and must be reproduced exactly (below) |
| record durable, rename absent (`recorded`, `rename`) | the part in `.swath/parts/` is checked against the record (size, MD5), then renamed into `data/` |
| record durable, rename absent, the record truncated or zeroed since (damage, not a crash) | reads as a torn tail: the part is set aside in `.swath/suspect/`; the resume records that ordinal again only if it reproduces the part byte for byte, and otherwise refuses (`ErrAmbiguousTail`) with the part kept (`TestAmbiguousTailRefusesOnChangedSource`) |
| record durable, rename absent, the whole frame lost since (the journal ends cleanly at the frame before) | reads as an append that never began: the claimed part is set aside the same way, reproduced or refused (`TestAcknowledgedWholeLastFrameLossPreservesPart`) |
| rename done, directory sync absent (`renamed-unsynced`) | the same check: the part is either at its final name or still in `.swath/parts/`; recovery syncs `data/` before it removes the temporary parts |
| recovery itself interrupted after its renames (`recovered`, a resume-only point) | the next resume recovers again from the same journal (`TestRepeatedRecovery`) |
| several parts closed out of order | only the contiguous ordinal prefix was published; later parts are reproduced |
| every part published, no manifest (`published`) | the final part is durable: the resume does not list, and installs the manifest |
| manifest installed (`manifest`) | committed; `swath resume` reports `ErrComplete`. The run reports are written after the commit and cannot revoke it |

Every boundary in the left column is a named crash point (`internal/faults`). `TestCrashMatrix` stops the run
there without cleanup, and `TestSIGKILLAtEveryPoint` has the real process SIGKILL itself there, for S3 and GCS. Both then
resume and compare with an uninterrupted run.

**The promise:**
- On a static source, recovery is byte-identical to an uninterrupted run.
- On a live bucket, durable output is exactly-once: no published key is listed again into the dataset, and
  none is skipped. Objects that change during the run change the rows, and so the part boundaries, as they
  would in any run.
- When K is not a bound the provider accepts (for S3, scalars outside the pivot alphabet), the resume fetches
  from K's longest accepted prefix and drops keys up to K, also in ranges split off before the cursor passes
  K. Such keys are fetched again but never published twice.

## The unit is the part

Parts are published as soon as they close, in ordinal order, by the router's `Publish` hook. That hook is
the one ordered committer: the router calls it for one part at a time (part i after part i−1), and it owns
the append, the sync and the rename; the journal's `Append` is a plain synchronous call, with no writer
goroutine, request or acknowledgement channel.

1. The part's encoder closes the file and fsyncs it (`.swath/parts/part-NNNNN.parquet`).
2. The committer claims it: renames it to `part-NNNNN.parquet.claimed` and fsyncs `.swath/parts/`. Only the
   part being recorded is ever claimed, so recovery can tell it from parts that closed early and were waiting
   for their turn, which no record can name.
3. One journal record is appended and fsynced: the ordinal, the part's `dataset.PartInfo` (rows, bytes,
   MD5, min and max key: the same type the manifest lists) and the writer state (the router's frozen part
   limit, the total rows so far). The journal is about committed output only: no split history, no cursor.
4. The part is renamed to `data/part-NNNNN.parquet` and the directory is fsynced.

This is commit-before-emit (I1) at part granularity: no row is published before the record that covers it.
Rows may sit in memory, or in an unpublished `.tmp` part, beyond the last record; a crash loses them and the
resume lists them again. At the end, `manifest.json` is installed by an atomic rename and a directory sync:
that is the commit. The run reports follow it.

## Resume

`swath resume DIR` (it replays the command recorded in the journal header; only the runtime flags `--workers`, `--encoders`, `--memory`, `--spill-*` and `--report-format` may change):

1. Takes the output lock (`DIR/.swath/lock`, `flock`) before recovering, removing or writing anything (the
   command reads the journal header first, to replay its command, and the read-only refusals come before the
   lock so a refused directory is left as it was). It is held through the run's cleanup, publication and the
   command's `error.json` and progress writes (`listing.Config.Settle`, `Progress.Owned`); a failure under the
   lock (a damaged journal, a mismatch, a failed recovery) is settled before it is released, and a resume whose
   source does not open takes the lock to settle, so the interrupted run's outcome never stands as current
   (`TestOutcomeIsCurrent`). Every run with an
   output takes it: fresh runs, overwrites and filesystem runs, which keep no journal.
   Replacing a previous run empties `.swath/` except the lock, so a live owner's lock is never unlinked.
2. Checks the identity in the journal header against the request (`journal.Check`, a pure function). The
   pipeline freezes the schema, key cap, writer recipe (`parquetout.Recipe`: encodings, geometry, part-size rule) and
   part size; the source names the rest as fields with a canonical form (`source.Identified`). For S3 that
   is the provider, the source (bucket and prefix), the effective route and `-fetch-owner`. The route is
   what the client builds, not what was asked: the URL the bucket's requests go to (`--endpoint` or the
   regional AWS host with its partition, FIPS and dual-stack variant; the endpoint's path bytes; path-style or
   virtual-host addressing) and the signing region, so a resume in an environment or profile that resolves
   another region is refused even when neither flag was given. A field added, dropped or changed is a typed
   `*journal.MismatchError`. The route's canonical form is `objhttp.CanonicalRoute` (shared with GCS) (scheme and host case), the only
   equivalence allowed beyond the client's own folding of one trailing endpoint slash: another endpoint, path
   or addressing style may serve another bucket of the same name. A library caller's supplied
   `s3.Options.Client` must name its route (`s3.Routed`), or the run declines resume. Workers, encoders,
   memory, `-seed`, `-probe` and
   credentials may change. A run with `--max-keys` keeps no journal and cannot resume: the cap counts what
   one process listed.
3. Recovers `data/`. The journal is synced first (its accepted frames are acted on). Every retained part
   must match its record's size and MD5 (`ErrPartAltered` otherwise). A part whose record is durable but whose
   rename did not happen is renamed now. Every unrecorded file in `data/` is removed, `data/` is synced, the
   part of a torn last record is set aside (the journal, below), and only then are the temporary parts removed.
4. Has the source restart after K, the last recorded part's max key (`source.Resumable.Restart`), from its
   normal start-up. S3 lists one range `(K, ⊤]` and splits it as on a fresh run. If K is not a bound
   the provider accepts, the range starts at K's longest accepted prefix and drops keys up to K.
5. Continues the router at the next ordinal with the recorded limit, so part boundaries fall exactly where
   the uninterrupted run would have put them.

A record also carries whether its part is the final one. Once the final part is durable, a
resume does not list again: it only publishes. On a changed bucket, listing again could add a second final
part. The journal grows with parts only. A record over the frame limit is refused on append as well as on read.

**The trade-off.** Earlier versions also journaled every split boundary so a resume could start with the
interrupted run's tiling. That costs journal growth and ties the journal to S3's range model; without it a
resume ramps up from one range, as a fresh run does from the root (splits engage after the first page of
each range). The ramp-up cost has not been measured.

A journaled run that fails with an ordinary error (not a crash) leaves its temporary parts in place: one
may already be recorded (its rename failed), and only `swath resume`'s reconciliation decides which are.
`swath resume` on a complete dataset returns `ErrComplete`; on a directory without a journal,
`ErrNothingToResume`. A new run over an interrupted one needs `-overwrite`.

## Why splits need no transaction

Splits are owner-only (no thieves), so there is no stale-snapshot race and no compare-and-swap, and they are
never durable: resume correctness depends only on K, and any tiling of `(K, ⊤]` is valid. Nothing blocks
under the split mutex.

## The journal

`DIR/.swath/journal` is append-only. Each frame is a 4-byte little-endian length, a CRC-32C (Castagnoli) over the length and
the payload, and the payload (JSON); journal version 6 (version 3 made the identity provider-neutral, version 4 writes part
bounds as base64, version 5 drops the split checkpoints and shares `PartInfo` with the manifest, version 6
refuses version 5 journals: a run with `--max-keys` could write one, and neither its identity nor its records
hold the cap or the S3 route, so through the library it could match an uncapped resume and a retained final
record would skip listing;
an older journal is refused, so an interrupted run of an older build is finished with that build or started
over with `-overwrite`). The first frame is the header (tool, format, identity, creation time).
Every later frame is a part record, with the ordinals dense from 0. `Append` writes and returns after
`fsync`; its one caller is the ordered committer.

Only an append that never completed is a torn tail, and only at the end: fewer bytes than the last frame's
length says, or a whole last frame with a bad checksum whose payload ends in a zero byte (bytes that never
reached the disk read as zeros; a payload is JSON and ends in `}`). A reopen truncates it. A record cut short
this way after its sync is indistinguishable from a torn append unless its part was renamed (below), and a
record lost whole, the journal ending exactly at the frame before it, is indistinguishable from an append that
never began. So neither a torn tail nor a clean end is taken as proof that nothing depends on the next
ordinal: whenever that ordinal's part is claimed (synced and renamed `.claimed` in `.swath/parts/` before its
append began), it is moved to `.swath/suspect/` before the temporary parts are removed, and the resume must
reproduce it byte for byte (size and MD5) before it records that ordinal again. A suspect already set aside
by an earlier resume is never replaced by a later claimed part. The cost: a crash or failure between the claim
and the end of the append, followed by a source change inside that part, refuses rather than relists. On a static source it does, and the resume
continues as if the append had never happened; when the source changed in that part's range, or no longer
yields a part there, the resume refuses with `ErrAmbiguousTail` and keeps the part, at every later attempt too.
The operator decides: `--overwrite` starts over, and removing `.swath/suspect/` accepts the source as it is now.
Everything else is corruption (`ErrCorrupt`), and the resume refuses before deleting or renaming
anything: a bad checksum with data after it; a whole last frame with a bad checksum whose last byte is not
zero, `}` or any other (an acknowledged record damaged since, its closing byte included: its part may be
durable only in `.swath/parts/`, which nothing else names); a length over the frame limit; a length that
overruns the file over bytes that hold a whole frame under a smaller length (the checksum covers the length,
so a match proves the damage); a part published under its final name beyond the recorded ones; an unknown
header version. A journal file that is empty or whose header frame is incomplete means the header never
became durable, and the run starts over, but only when no part file exists in `data/` or `.swath/parts/`:
the header is synced before any part exists, so a part proves damage. `swath show --journal DIR` prints the header, every record and any torn
tail as JSON lines.

**The contract, precisely.**
- **Process crash (SIGKILL, panic, OOM kill) on a running kernel.** Every acknowledged step survives, because
  the page cache does. This is what the crash matrix and the SIGKILL tests demonstrate.
- **Power loss or a kernel crash.** Recovery holds when the filesystem honours `fsync` on files and
  directories (ext4, XFS and APFS do). Before a journal record names a part:
  - the part file is synced, and so is its claimed name in `.swath/parts/`;
  - `data/` and `.swath/` were created with their entries in the output directory synced;
  - the journal frame is synced before `Append` returns;
  - a rename is followed by a sync of `data/`;
  - recovery syncs the journal before renaming on its strength, and `data/` before removing temporary parts.

  Nothing tests this: SIGKILL cannot show power-loss behaviour.
- **Unsupported on Windows:** directories cannot be synced and the lock is not enforced, so neither
  guarantee holds there; a journaled run prints a warning (`journal.Enforced` is false).
