# Durability and resume

An S3 or GCS run with an output directory keeps a journal so that `swath resume DIR` continues an interrupted run. A resume never
publishes a key twice and never skips one, and the final dataset holds an uninterrupted run's rows: byte-identical
to it on a static source when no part was cut by time, the same rows with timing-dependent part boundaries when
`--part-interval` cut one (the promise below). Package `internal/journal` holds the journal; `internal/listing/journal.go` connects it to the router
and the provider. Filesystem runs opt out: they are fast enough to restart.

## Progress states

A key passes through five states. Only the last two survive a crash. Durable progress always runs ahead of
the rename into `data/` (a record is synced before its part is renamed), and recovery closes that gap:

| State | Where it lives | After a crash |
|---|---|---|
| fetched | a response body in the client | fetched again |
| emitted | rows in the order stage (memory, or a disposable spill file) | listed again from the durable frontier |
| certified | rows in a sealed durable spill file whose ledger record survived ([ordering.md](ordering.md#durable-spill)) | replayed from the file (rows above K only), not listed |
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
| part claimed, record not yet written (`journal-append`) | the claimed part is set aside and must be reproduced exactly (below): on disk this is the same as an acknowledged record lost whole since. The resume closes that ordinal after the claimed part's row count, so a part the part interval closed is reproduced whatever the resume's timing or interval (`TestTimeCutClaimResumes`), and writes it with the claimed part's columns and `created_by`, so a build that adds an optional column reproduces it (`TestClaimedPartKeepsItsColumns`, also for a torn record). A reproduced part that reaches the size target at that boundary was closed by size, and when it is the first such part it calibrates the size ratio as the original did, before its successor is routed (`TestCalibratingClaimRecoversRepeatedly`, `journal-append#2` in both crash matrices) |
| record written, not synced (`journal-sync`) | a whole unsynced frame that survived is a valid record of a synced part, and recovery syncs the journal before acting on it; a torn tail (power loss) is dropped, and its claimed part is set aside and must be reproduced exactly (below) |
| record durable, rename absent (`recorded`, `rename`) | the part in `.swath/parts/` is checked against the record (size, SHA-256), then renamed into `data/` |
| record durable, rename absent, the record truncated or zeroed since (damage, not a crash) | reads as a torn tail: the part is set aside in `.swath/suspect/`; the resume records that ordinal again only if it reproduces the part byte for byte, and otherwise refuses (`ErrAmbiguousTail`) with the part kept (`TestAmbiguousTailRefusesOnChangedSource`) |
| record durable, rename absent, the whole frame lost since (the journal ends cleanly at the frame before) | reads as an append that never began: the claimed part is set aside the same way, reproduced or refused (`TestAcknowledgedWholeLastFrameLossPreservesPart`) |
| rename done, directory sync absent (`renamed-unsynced`) | the same check: the part is either at its final name or still in `.swath/parts/`; recovery syncs `data/` before it removes the temporary parts |
| recovery itself interrupted after its renames (`recovered`, a resume-only point) | the next resume recovers again from the same journal (`TestRepeatedRecovery`) |
| several parts closed out of order | only the contiguous ordinal prefix was published; later parts are reproduced |
| every part published, no manifest (`published`) | the final part is durable: the resume does not list, and installs the manifest |
| manifest installed (`manifest`) | committed; `swath resume` reports `ErrComplete`. The run reports are written after the commit and cannot revoke it |

Every boundary in the left column is a named crash point (`internal/faults`). `TestCrashMatrix` stops the run
there without cleanup, and `TestSIGKILLAtEveryPoint` has the real process SIGKILL itself there, for S3 and GCS. Both then
resume and compare with an uninterrupted run: byte for byte with the part interval off, and row for row (no
refusal) with it on, where parts are closed by time (a trickling source in `TestCrashMatrix`, a slower bucket
with a 1 ms interval in `TestSIGKILLAtEveryPoint`).

The durable spill has crash points of its own. `faults.SpillPoints`: the sync of each directory the run
creates, a durable file's first write (its header, with the capture start at a range's start), later frames
and trailer writes, its seal's fsync, the flusher's directory sync, ledger append, ledger sync and unlink.
`faults.RecoveryPoints`: a resume's ledger truncation, the moment after it (truncated, not yet synced), its
retirements' append and sync, an unlink, the directory sync, a recovery replay's start and each captured
frame it replays. `TestDurableKillMatrix` (in process: each point's 1st and 4th occurrence; writes torn at
an explicit offset inside a header, a capture start, a rows frame, a trailer, a tail and a ledger record;
lost unsynced state) and `TestSIGKILLDurableSpill` (the real process) run them on a slow-frontier fixture
and fail if a point is not reached. In process the fixture is gated, not timed: each seal waits until the seal
before it has a ledger append of its own (so four seals are four appends for the fourth-occurrence ledger
points), and the torn-ledger test holds the frontier until its tear, so the part records it needs follow.
`TestDurableRepeatedRecovery` interrupts a resume twice inside recovery, each schedule on its own copy of a
damaged snapshot, with the truncation or the retirements' append lost where the schedule says so. Each
then resumes to the uninterrupted dataset. In the real process the same seal gating is
`SWATH_TEST_SERIAL_SEALS`, a test hook beside `SWATH_TEST_KILL_AT`: each seal waits until the earlier seals'
records were appended by ledger cycles of their own, so `TestSIGKILLDurableSpill` reaches its fourth-cycle
arms by construction. Its recovery arms seed their damaged ledger from a run whose first part's close is held
(`SWATH_TEST_HANG_AT=part-closed:1`: no part published, K at ⊥) and killed at the sixth ledger sync, so certificates stay live above K for the recovery
replay.

The same schedule also runs, opt-in, against MinIO and against production key lists, through the real
binary: SIGKILLs at the durable steps, after a part record, mid recovery replay, with unsynced state lost,
and at 40 % and 70 % of the keys. Each resume is checked for oracle equality, reconciliation, and that no
data request starts inside a replayed certificate ([testing.md](../testing.md#durable-spill-campaign-opt-in)).
The results, and the full-scale Batch runs (64 lists of 20 to 150 million keys, killed at 40 % and 70 %), are
in [measurements.md](../measurements.md#durable-spill).

**Priors and certificates.** A prior (`--seed-from`: a previous dataset's or a cut file's keys used as seed
tiles), like `--seed`, only shapes the tiling of the live gaps. On a resume with reused certificates the
prior's tiles of `(K, ⊤]` are cut around the certificates (`keyrange.aroundReuse`, as for the seed and the
plain resume tiling). A certificate becomes a reused range that the order stage replays, and no data request
starts inside its `[max(cs, K), ce)`. A prior confers no coverage: no key range is skipped or certified because
a prior listed it. Coverage above K comes only from certificates that this run's ledger and files prove, and
a certificate never depends on a prior. The tests are `keyrange` `TestPriorTilesOnlyGaps` (prior cuts inside
and around spans; without the cut the resume listed every key) and the campaign's `seed-from` arm.

**The recovery point's age.** Durable progress advances a part at a time, and a part closes at its size
target or, on a slow frontier, once it has been open for the part interval (default 30 s,
[parquet-output.md](parquet-output.md)). `progress.json` reports the age (`part_age_ms`: since the last part
was published, or since the start). What a crash loses is not bounded by that age: rows listed past the
frontier (in memory or spill) and parts encoded after a gap in the ordinals are listed again too.

**The part limit.** A dataset has at most 100,000 parts (`part-00000` to `part-99999`: ordinals have five digits, `dataset.MaxParts`); at the defaults (64 MiB parts, about 30 bytes per row) it takes about 200 billion objects or 35 days of uninterrupted 30-second time cuts; in practice a small `--part-size` or `--part-rotation-max-rows` reaches it. A run that reaches it publishes and records every
part up to 99,999 and stops (exit 1, `cause` `part_limit`, not resumable: the part size and row cap are in
the identity, so a resume would stop at the same part); its journal and parts are kept, and the run starts
over with a larger `--part-size` ([parquet-output.md](parquet-output.md)). Its readers accept a manifest of every dataset within the limit:
`dataset.ManifestLimits` sizes the manifest for 100,000 parts with keys up to 4,096 bytes (about 1.1 GiB),
where the seekparquet module's default (64 MiB) would refuse, for example, 30,000 one-row parts of 1,024-byte
keys (about 82 MB). `TestManifestLimitsReadSwathsLargest` proves the sizing by arithmetic from a measured part
entry, reads a 3,000-part manifest within it, and shows a smaller limit refusing the same manifest.

**The promise:**
- On a static source, recovery is byte-identical to an uninterrupted run when no part was cut by time;
  otherwise it is the same rows, with boundaries that depend on when each attempt cut.
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
   SHA-256, min and max key: the same type the manifest lists) and the writer state (the router's frozen part
   limit, the total rows so far). The journal is about committed output only: no split history, no cursor.
4. The part is renamed to `data/part-NNNNN.parquet` and the directory is fsynced.

This is commit-before-emit (I1) at part granularity: no row is published before the record that covers it.
Rows may sit in memory, or in an unpublished `.tmp` part, beyond the last record; a crash loses them and the
resume lists them again. At the end, `manifest.json` is installed by an atomic rename and a directory sync:
that is the commit. The run reports follow it.

The part file itself is written by the seekparquet module's part writer (`write.NewPart`; `Seal` writes the
footer, syncs and closes the file, and returns the part's rows, size, exact bounds and the SHA-256 of every
byte written, footer included, computed as the bytes went out). Its writes and sync go through swath's
`faults` (the disk-fault matrix). The claim, the record, the rename into `data/` and the manifest's
installation stay swath's (`listing.runJournal.publish`, `dataset.PublishPartWith`/`PublishPartIn`,
`dataset.Publish` through `dataset.Replace`), not the module's `local.PublishPart` and `local.Commit`: those
take no fault hooks, so the crash matrix's `renamed-unsynced` point and the disk-fault matrix's manifest write
and sync could not be reached through them; and at the pinned version `PublishPart` renames over an existing
final name rather than refusing it. The manifest's bytes are the module's (`seekparquet.MarshalManifest`).

Every JSON file swath replaces goes through one primitive, `dataset.Replace`, with a temporary file of its
own beside the target (`.NAME.*.tmp`, so concurrent writers never share one; an overwrite removes leftovers)
and a stated durability: `Durable` (file synced before the rename, directory after) for `manifest.json` and
`run.json`, and for the outcome files a supervisor acts on, `error.json` and the `--report` file; `Atomic`
(never torn, not synced) for the file rewritten as the run goes, `.swath/progress.json`.

## Resume

`swath resume DIR` (it replays the command recorded in the journal header; only the runtime and operational flags that the generated usage in [../cli.md](../cli.md) lists (from `runtimeFlags` and `operationalFlags`, which `swath resume --help` prints; `--limiter-slow-start` among them) may be given again, the last value winning; the source and output identity stays frozen, so an override that resolves another route is refused by the identity check below ([../cli.md](../cli.md))):

1. Takes the output lock (`DIR/.swath/lock`, `flock`) before recovering, removing or writing anything (the
   command reads the journal header first, to replay its command, and the read-only refusals come before the
   lock so a refused directory is left as it was). It is held through the run's cleanup, publication and the
   command's `error.json` and progress writes (`listing.Config.Settle`, `Progress.Owned`); a failure under the
   lock (a damaged journal, a mismatch, a failed recovery) is settled before it is released, and a resume whose
   source does not open takes the lock to settle, so the interrupted run's outcome never stands as current
   (`TestOutcomeIsCurrent`). Such an early failure settles only the run it saw: it takes `journal.Witness` (a
   digest of the journal and `error.json`) before the lock and records its outcome only if, under the lock, the
   dataset is uncommitted and the witness unchanged; a run another process completed, replaced or continued
   meanwhile keeps its outcome (`TestOutcomeSettlementAfterConcurrentCommit`). A directory without a journal is
   nothing to resume and is left untouched, without a lock (`TestNothingToResumeLeavesDirectoryUntouched`). Every run with an
   output takes it: fresh runs, overwrites and filesystem runs, which keep no journal.
   Replacing a previous run empties `.swath/` except the lock, so a live owner's lock is never unlinked.
   A lock on an inode does not prove the pathname still names that directory: the lock records the output
   directory's and the lock file's identity (device and inode, `os.SameFile`) and `Lock.Verify` checks both
   before each operation that creates, writes, renames or removes something in the output by its pathname:
   the router's parts directory and each part file created, the claim and the rename into `data/`,
   recovery's renames and removals, cleanup of temporary parts, the spill directory and each spill file
   created, appended to (a frame opens the file by name: two stats per frame) or removed, the spill cleanup,
   the manifest, the run report, the outcome file (also a refused command's, `settleOutside`), and the
   command's writers through `Progress.Owned` (`progress.json` every 2 s, goroutine dumps, a forced exit's
   outcome). Writes through a descriptor already open (the journal, a part being encoded) go to the locked
   inode, so into the moved directory, never its replacement. A directory moved or replaced during the run
   stops it with `journal.ErrOutputReplaced`, and the moved directory resumes at its new path
   (`TestReplacedOutputIsNeverTouched`; `TestReplacedOutputWithLiveWriters`, where the replacement is a copy
   holding the run's own spill and progress files, rewritten every millisecond). The part files are created,
   claimed and renamed into `data/` relative to the locked directory (an `os.Root` opened after a check,
   before the router starts), so a replacement between a check and those actions cannot reach the impostor
   (`TestReplacedAfterCheckGetsNoPart`, the fault point `part-create` between the router's check and the
   create; under a load average of 13-20 `TestReplacedOutputIsNeverTouched` had seen a part created by
   pathname in the impostor's `.swath/parts`). Resume recovery (step 3) opens a root before it reads the
   journal and does through it: the journal's read, sync and reopen; the lstat, open and verification of
   parts; the renames into `data/`, each followed by the data directory's sync (`dataset.PublishPartIn`), and
   into `.swath/suspect/`, followed by syncs of `.swath/suspect/` and `.swath/`; the removal of unrecorded
   entries and stale suspects; the removal and re-creation of `.swath/parts/`; and its directory syncs. Still by pathname with no identity check before
   them (nothing is locked yet, or the lock's identities are not yet compared): taking the lock (after lstat
   checks of `.swath/`, the journal and the lock file) and `journal.Witness` (a read, before the lock and in
   the settlement before its identity check). Still by pathname, each after an identity check: a fresh run's
   preparation and the restart of a run whose journal header never became durable, the journal's creation
   for a fresh run, the live run's claim-directory sync and the data directory's sync after each
   live publication, the final publication of a run without a journal, the manifest, the run report and the
   outcome file (`dataset.Replace`), the workspace and spill cleanup, and `progress.json` and stack dumps
   under `Progress.Owned`. What is not excluded: a replacement between a check and its action for those
   pathname operations, and helpers of several steps (preparation, the manifest's write and rename) check
   once before they start, so that window spans the helper. The root itself is opened by pathname and the
   lock's identity checked by pathname after, so a replacement and its undoing around the opening is not
   excluded either. Below the root, recovery's type checks (step 3) close the case of a link already in
   place when the resume starts; a path below the root replaced after those checks and before an action is
   the same check/action window: the root refuses a link that leaves the output, but follows one that stays
   inside it, and a part's identity check after it is opened (`dataset.OpenRegularIn`, `os.SameFile`)
   proves the inode, not that each path component stayed the same.
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
   row budget and credentials may change; so may the seed for a library caller (`-seed` is not part of
   the identity), but the CLI's `swath resume` replays the recorded `--seed` and does not accept it. A run with `--max-keys` keeps no journal and cannot resume: the cap counts what
   one process listed.
3. Recovers `data/`. Every path it acts on is checked first by lstat, never following a link. Before the
   lock is taken, for the run or for a refusal's settlement, an existing `.swath/` must be a directory and an
   existing journal and lock file regular files (`journal.TakeLock`, so the lock file is never created through
   a link, nor in an output refused for its journal: `TestLockRefusesLinkedJournal`); a journal read types
   `.swath/` before the journal (`TestReadRefusesLinkedWorkspace`). Before the journal is read,
   `.swath/` must be a directory, `data/`, `.swath/parts/` and `.swath/suspect/` directories where they
   exist, and `.swath/journal` a regular file (this also precedes the restart of a run whose journal header
   never became durable, which empties the workspace; that restart also requires every entry of
   `.swath/suspect/` and `.swath/parts/` to be a regular file first, `TestRestartRefusesLinkedSuspect`). Before recovery renames or removes anything, `data/`
   must exist as a directory and every retained part, a claimed part and every entry of `.swath/suspect/`
   (the one read and the ones removed) must be regular files (`TestRecoveryRefusesLinkedSuspectUnchanged`). Anything else (an interrupted output whose `data/` or journal became a link to something
   elsewhere, which cleanup would delete from or the reopen would truncate) is refused with
   `dataset.ErrUnsafeLayout` (`listing.ErrUnsafeLayout`; a `*LayoutError` naming the path; exit 1, class
   `fatal`, `cause` `unsafe_layout`, not resumable, whatever stop cause was selected meanwhile,
   `TestTerminalCausesOutrankStops`; the report is written, and `error.json` when the settlement can take the
   lock, which a refused workspace or journal prevents) (`TestRecoveryRefusesSymlinkedData`, `TestRecoveryRefusesSymlinkedSuspectDir`,
   `TestRecoveryRefusesNonRegularPart`, `TestRecoveryRefusesLinkedJournal`, `TestLockRefusesLinkedWorkspace`,
   `TestRestartRefusesLinkedParts`, `TestRestartRefusesLinkedSuspect`). Recovery then reads, syncs and reopens the journal and reads, renames,
   removes and syncs parts through the output root (`os.Root`), as part creation does; an unrecorded entry of
   `data/` that is a link is removed itself, never what it names.
   The journal is synced first (its accepted frames are acted on). Every retained part
   must match its record's size and SHA-256 (`ErrPartAltered` otherwise). A part whose record is durable but whose
   rename did not happen is renamed now. Every unrecorded file in `data/` is removed, `data/` is synced, the
   part of a torn last record is set aside (the journal, below), and only then are the temporary parts removed.
   Verification reads every retained part in full, so it costs O(dataset bytes): measured once (2026-11-06,
   a 16 GiB, 4-vCPU VM, virtio disk, 1 GiB in 16 files of 64 MiB, `TestVerifyThroughput`): 247, 306 and
   376 MiB/s with the page cache dropped before each of three reps (the host's own cache likely warms), 511
   MiB/s warm, where MD5 on one core was the limit (the digest was MD5 then). SHA-256 replaced it in journal
   version 9: on a CPU with the SHA extensions it hashes about twice as fast as MD5 (1,504 against 692 MiB/s
   on one core, Xeon Platinum 8581C, 2026-11-08), so the cold rates above stay disk-bound; without them it is
   slower than MD5. At the default part size, 10,000 retained parts (640 GB) take roughly 30 to 45 minutes. The read stops when the run is stopped (a signal, `--max-duration`) and
   before recovery renames or removes anything, and its bytes count as progress (`progress.json`
   `verified_bytes`, included in `advance`), so the stall watchdog does not stop a long verification
   (`TestRecoveryVerificationIsCancellableAndReported`).
4. Has the source restart after K, the last recorded part's max key (`source.Resumable.Restart`), from its
   normal start-up. S3 and GCS tile `(K, ⊤]` as a fresh run tiles the scope: with the seed (the default) from
   delimiter listings of every folder above K, keeping only cuts above K, else as one range `(K, ⊤]` that
   splits ([providers.md](providers.md#range-engine-internalsourcekeyrange), "A resumed scope"). If K is not a
   bound the provider accepts, the first tile starts at K's longest accepted prefix and drops keys up to K.
5. Continues the router at the next ordinal with the recorded limit, so size cuts fall exactly where the
   uninterrupted run would have put them (a cut by `--part-interval` depends on timing in either run).

A record also carries whether its part is the final one. Once the final part is durable, a
resume does not list again: it only publishes. On a changed bucket, listing again could add a second final
part. The journal grows with parts only. A record over the frame limit is refused on append as well as on read.

**The trade-off.** The journal records no split boundary, so a resume cannot start with the interrupted
run's tiling: recording one would cost journal growth and tie the journal to the range engine's model. A
resume tiles `(K, ⊤]` afresh instead (the seed's listings above K, or one range), and splits engage after the
first page of each range, as on a fresh run. The ramp-up cost has not been measured.

A journaled run that fails with an ordinary error (not a crash) leaves its temporary parts in place: one
may already be recorded (its rename failed), and only `swath resume`'s reconciliation decides which are.
`swath resume` on a committed dataset meets `ErrComplete`: complete coverage exits 0 (class `completed`, the
report saying `complete`; idempotent, as Java's), partial coverage exits 1 (`incomplete`); on a directory without a journal (an empty
workspace included), `ErrNothingToResume`; neither changes the directory. A new run over an interrupted one needs `-overwrite`.

## The outcome of an attempt

The durable output says what was committed; how an attempt ended is a separate record. Each invocation gets an
attempt (`listing.Attempt`: a random 128-bit id and its start time). The report a supervisor names with
`--report FILE` (outside the output, a fresh path per attempt) is written when the attempt ends, on every
outcome, refusals before the run included, and carries the attempt and the outcome; `error.json` in the output
carries the same attempt and outcome when the attempt could record it under the lock. The contract for a
supervisor is therefore the exit code plus that report; `error.json` is the dataset-local convenience, current
only when its attempt id is the report's. An attempt that cannot write `error.json` (the storage refuses the
write, another process owns the output, or the run changed before this process held the lock) leaves the
previous file in place, perhaps with the same class and exit code, and says why in the report's
`outcome.unrecorded` (never empty then) and on stderr (`TestAttemptIdentifiesTheOutcome`,
`TestOutcomeSettlementAfterConcurrentCommit`). Nothing can promise a new file on
storage that refuses writes: without a report, only the exit code is current. A report that cannot be written
is named with its attempt id on stderr. A stop (a signal, `--max-duration`, `--stall-timeout`) that the run does
not finish within 15 s (I/O that does not return) ends the process. Its exit timer is armed first, before any
lock, file or diagnostic, so the exit comes at most 5 s after the grace whatever blocks, a full stderr pipe or a
classification on a disk that does not return included (`TestDeadlineExitsAfterGrace`,
`TestForcedExitWithFullStderr`, `TestForcedExitBackstopFirst`). The forced exit takes one snapshot of the
terminal result, waiting at most 2 s, and its exit code, `error.json` and report all derive from it: the run's
classification once made, else the forced one, frozen then so nothing later reclassifies the run. The commit is
one step with its account: the manifest's durable installation and the freezing of the committed result happen
under one gate, so a forced exit sees either a run not committed (and a commit after it is refused) or the
committed result (complete exits 0; partial coverage stays incomplete and nonzero) (`TestForcedExitAtCommit`,
`TestForcedSnapshotIsTheResult`). A snapshot that cannot be taken in time writes no outcome and exits with the
stop's code, or with the result's if it arrives before the exit. A manifest renamed into place whose directory
sync then fails is visible but not committed: the run fails, and `swath resume` finds the dataset complete. A
supervisor's own process deadline covers a process that cannot even do that.

## Why splits need no transaction

Splits are owner-only (no thieves), so there is no stale-snapshot race and no compare-and-swap, and they are
never durable: resume correctness depends only on K, and any tiling of `(K, ⊤]` is valid. Nothing blocks
under the split mutex.

## The journal

`DIR/.swath/journal` is append-only. Each frame is a 4-byte little-endian length, a CRC-32C (Castagnoli) over the length and
the payload, and the payload (JSON); journal version 9 (version 3 made the identity provider-neutral, version 4 writes part
bounds as base64, version 5 drops the split checkpoints and shares `PartInfo` with the manifest, version 6
refuses version 5 journals: a run with `--max-keys` could write one, and neither its identity nor its records
hold the cap or the S3 route, so through the library it could match an uncapped resume and a retained final
record would skip listing; version 7 marks the claimed-part protocol: a version 6 run left the part of its
last record at the unclaimed temporary name until the rename into `data/`, and version 7 recovery protects
only the claimed name, so it would remove that acknowledged part as unrecorded when the last record is torn or
lost and could then commit a changed replacement; a version 6 journal is refused before recovery reads,
moves or removes any part or touches the journal; version 8 adds the run UUID to the header, made when the
run starts and kept across every resume, and keeps the durable spill across a resume; version 9 records each
part's SHA-256 where earlier versions recorded its MD5, so recovery, which compares a retained part with its
record by that digest, refuses versions 7 and 8 before it touches anything, as an earlier build refuses
version 9).
An older or newer journal is refused with a `journal.VersionError` (exit 1, class `fatal`, not resumable by this build) and the directory's parts and journal
are left as they are: an interrupted run of an older build is finished with that build or started over with
`--overwrite`. The first frame is the header (tool, format, identity, creation time, and the run UUID).
Every later frame is a part record, with the ordinals dense from 0. `Append` writes and returns after
`fsync`; its one caller is the ordered committer. A record must follow the one before it
(`journal.Follows`: the next ordinal, no record after the final one, a nonempty part strictly after the last
record's max key, a running row total that adds up): `Append` refuses one that does not before writing
anything (`*dataset.InvariantError`), and `Read` treats a journal whose records do not chain as
`ErrCorrupt`, so a resume never continues after overlapping parts. The router checks the same adjacency
before it calls the committer ([parquet-output.md](parquet-output.md#publication-gate-and-order)).
A run that fails on a protocol violation (`source.ErrProtocol`: an invalid page the range engine cannot
recover from, keys out of order or repeated, a repeated continuation token, a truncated page without a token
or without a key in its range) ends its journal with a terminal `unresumable` frame (cause `protocol`, the
reason, the time), synced, as Java's `markRunUnresumable`: a resume would walk back into the same service.
No frame may follow it (`Append` refuses, `Read` treats one as `ErrCorrupt`), and `swath resume` and a resumed
run refuse the run with a `journal.UnresumableError` (exit 1, `cause` `protocol`), the parts and journal left
as they are; `swath list --overwrite` starts over (`TestProtocolViolationIsUnresumable`, `TestUnresumableMark`).
The frame is a later type within version 7: a build without it reads a marked journal as corrupt, which
refuses the resume too. Transient oddities are not violations and keep their handling: an unparsed body is
retried, unsorted common prefixes are normalized, empty continuing pages are followed.

Only an append that never completed is a torn tail, and only at the end: fewer bytes than the last frame's
length says, or a whole last frame with a bad checksum whose payload ends in a zero byte (bytes that never
reached the disk read as zeros; a payload is JSON and ends in `}`). A reopen truncates it. A record cut short
this way after its sync is indistinguishable from a torn append unless its part was renamed (below), and a
record lost whole, the journal ending exactly at the frame before it, is indistinguishable from an append that
never began. So neither a torn tail nor a clean end is taken as proof that nothing depends on the next
ordinal: whenever that ordinal's part is claimed (synced and renamed `.claimed` in `.swath/parts/` before its
append began), it is moved to `.swath/suspect/` before the temporary parts are removed, and the resume must
reproduce it byte for byte (size and SHA-256) before it records that ordinal again. The claimed part carries its
boundary: its footer's row count, synced with it before the append, and the resume closes the reproduced part
after exactly that many rows (`parquetout.Options.Cut`), neither earlier by the part interval nor later by
size. Without it, a part first closed by the interval would take more rows in a faster resume, or one with
another `--part-interval`, and an unchanged source would be refused. The footer also says how the part was
written: its columns and its `created_by`. The resume writes the reproduced part with those (the schema's
columns that the footer names, in order) and the parts after it with its own, so a resume by a build that
writes an optional column the claimed part lacks (`time_created` for GCS), or that names itself differently in
`created_by`, still reproduces an unchanged source's part (`TestClaimedPartKeepsItsColumns`,
`parquetout` `TestCutReproducesClaimedFooter`). An unreadable footer gives no boundary, and the byte comparison
decides as before. A suspect already set aside
by an earlier resume is never replaced by a later claimed part. The cost: a crash or failure between the claim
and the end of the append, followed by a source change inside that part, refuses rather than relists. On a static source it does, and the resume
continues as if the append had never happened; when the source changed in that part's range, or no longer
yields a part there, the resume refuses with `ErrAmbiguousTail` and keeps the part, at every later attempt too.
A resume on a machine of the other encoder path (amd64 with AVX2 against any other) can refuse the same way on
an unchanged source, when the part has dictionary pages the two paths encode differently
([parquet-output.md](parquet-output.md#byte-identity)); resuming on a machine of the original path reproduces it.
The operator decides: `--overwrite` starts over, and removing `.swath/suspect/` accepts the source as it is now.
Everything else is corruption (`ErrCorrupt`), and the resume refuses before deleting or renaming
anything: a bad checksum with data after it; a whole last frame with a bad checksum whose last byte is not
zero, `}` or any other (an acknowledged record damaged since, its closing byte included: its part may be
durable only in `.swath/parts/`, which nothing else names); a length over the frame limit; a length that
overruns the file over bytes that hold a whole frame under a smaller length (the checksum covers the length,
so a match proves the damage); a part published under its final name beyond the recorded ones; an unknown
header version. A journal file that is empty or whose header frame is incomplete means the header never
became durable, and the run starts over, but only when no part file exists in `data/` or `.swath/parts/`:
the header is synced before any part exists, so a part proves damage. A part directory that exists but cannot
be listed is an error, never "no part" (`TestDamagedHeader`). `swath show --journal DIR` prints the header, every record and any torn
tail as JSON lines.

**The contract, precisely.**
- **Process crash (SIGKILL, panic, OOM kill) on a running kernel.** Every acknowledged step survives, because
  the page cache does. This is what the crash matrix and the SIGKILL tests demonstrate.
- **A write or an fsync that fails** (a part file, a journal record, the manifest; half the bytes written then
  `ENOSPC`, or the bytes written and the sync failing with `EIO`): the run fails with that error and no
  manifest, the published parts stay, and a resume completes the uninterrupted run's dataset
  (`TestDiskFaultMatrix`, through the `faults.SetIO` seam); a full disk exits 74, class `disk_full`, resumable
  (`TestDiskFullExits74`). The kernel's behaviour after a failed fsync
  (pages it may drop or mark clean) is not modelled.
- **Power loss or a kernel crash.** Recovery holds when the filesystem honours `fsync` on files and
  directories (ext4, XFS and APFS do). Before a journal record names a part:
  - the part file is synced, and so is its claimed name in `.swath/parts/`;
  - `data/` and `.swath/` were created with their entries in the output directory synced;
  - the journal frame is synced before `Append` returns;
  - a rename is followed by a sync of `data/`;
  - recovery syncs the journal before renaming on its strength, and `data/` before removing temporary parts.

  Nothing tests this: SIGKILL cannot show power-loss behaviour.
- **Refused on Windows:** the output lock is enforced (`LockFileEx`, exclusive and failing at once, with the
  same identity checks as on Unix), but directories cannot be synced (`dataset.DirSyncEnforced` is false;
  `syncDir` is a no-op there). A process crash loses nothing; after a power loss NTFS replays its metadata
  log to a consistent state that need not include the latest creations and renames, so an acknowledged
  part could lose its name. A journaled run and `swath resume` are therefore refused with
  `listing.ErrUnenforced` before the output is created or touched (`TestUnenforcedPlatformRefusesJournal`).
  The seekparquet module's own publication refuses the same way (`local.ErrDirSyncUnsupported` unless
  `Ops.AllowUnsynced`); swath does not publish through it (above).
  What still runs: `--no-journal` runs of object stores and filesystem runs, which are not resumable. CI's
  `go-xos` job runs the suite on Windows (`TestWindowsLock` included); the journaled-run tests skip there
  (`datasettest.RequireJournal`). `Replace` retries its rename there for up to two seconds while another
  handle holds the target (`ERROR_ACCESS_DENIED`, `ERROR_SHARING_VIOLATION`: a reader of the report, a
  concurrent replacement), which leaves the old file in place until it succeeds.
