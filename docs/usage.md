# swath — usage reference

## Overview

`swath` is a high-performance Java 25 CLI designed for very large Amazon S3
bucket and prefix listings. On supported general-purpose S3 buckets, it handles
deep prefix trees, flat random-key spaces, and badly skewed distributions
without manual partitioning. Active pipeline buffers are bounded by
configuration rather than object count; finalized-part metadata grows with part
count, and `--sort` staging metadata grows with segment count.
Managed Parquet directory datasets can resume after a crash or Ctrl+C; finalized
parts are preserved and an unfinalized tail may be re-listed. Output is Parquet,
JSONL, TSV, or an aligned `table` view.

For running swath against a live bucket — credentials, the minimal IAM policy,
public buckets, S3-compatible endpoints, and the LIST-call cost model — see
[`operating.md`](operating.md).

---

## Build and install

**Requirement:** JDK 25 (no `--enable-preview`).

```
# Build and run all tests
./gradlew build

# Create a standalone distribution under swath-cli/build/install/swath/
./gradlew :swath-cli:installDist

# Make this shell find the installed launcher:
export PATH="$PWD/swath-cli/build/install/swath/bin:$PATH"
```

Run directly via Gradle for development:

```
./gradlew :swath-cli:run --args="list s3://my-bucket/prefix -o out/"
```

---

## Commands

| Command | Status | Description |
| --- | --- | --- |
| `swath list <s3-uri> [options]` | implemented | List a bucket or prefix |
| `swath resume <dir>` | implemented | Resume the run whose checkpoint is co-located in `<dir>` |
| `swath help [command]` | implemented | Show help |

`inspect` (bucket-shape probe) and `diff` (bucket diff) are planned but not yet
wired — see [`ROADMAP.md`](../ROADMAP.md).

Verbs are explicit — there is no default command, so `swath s3://my-bucket` on
its own is a usage error (exit 2). If the first argument looks like a store
URI, swath prints a hint pointing at the verb you meant, e.g.
`did you mean: swath list s3://my-bucket?`.

The repository also contains a separate S3 listing replay server for testing
listing clients against swath Parquet captures. See
[`docs/swath-replay-server.md`](swath-replay-server.md).

---

## `swath list` synopsis

```
swath list <s3-uri> [options]
```

`<s3-uri>` is an S3 URI of the form `s3://bucket` or `s3://bucket/prefix`.
The path is literal: `s3://bucket/a%20b` lists prefix/key bytes `a%20b`, not
`a b`.

v0.1 supports general-purpose S3 buckets whose listing contract provides global
lexicographic order and `StartAfter`. Directory buckets (the `--x-s3` naming
form) do not provide that contract and are refused before the first LIST
request. Opaque-token sequential directory-bucket support is planned.

### Options

#### Tuning (`--tune`)

```console
swath list s3://my-bucket/archive/ \
  --tune engine.readahead=on \
  --tune parquet.writers=4
```

`--tune KEY=VALUE` is repeatable. It holds the expert settings that should not
crowd the everyday flag surface. Invalid keys and values fail before swath opens
a checkpoint or contacts the object store. Run `swath list ... --tune help` for
the registry, or `--tune KEY=?` for one key's accepted values.

| Key | Type / range | Default | Stability | Resume class | Applies to | Effect |
| --- | --- | --- | --- | --- | --- | --- |
| `engine.readahead` | `on` or `off` | `off` | experimental | free | fresh `list` | Enable speculative dense-tail readahead. This can trade more API calls and memory for lower wall time. |
| `seed.mode` | `shallow`, `none`, or `hints` | `shallow` | stable (`hints` reserved) | identity | fresh `list` | Choose initial keyspace discovery. `hints` is reserved but not implemented. |
| `parquet.writers` | integer `2..4` | `3` | stable | free | fresh `list` | Set the bounded Parquet writer pool. A file-shaped Parquet destination still resolves to one writer. |
| `summary.interval` | positive duration | `--progress-interval` when given, else `30s` | stable | free | fresh `list` | Set report heartbeat cadence; accepts values such as `2s`, `500ms`, or `PT2S`. |
| `sort.ignore-disk-check` | `on` or `off` | `off` | diagnostic | free | fresh `list` and `resume` | Skip the pre-run and periodic sort disk-space guard. Use only after sizing the staging volume independently. |

These settings feed the same resolved fields as the engine, Parquet, report,
and sort paths; the umbrella changes their CLI spelling, not their behavior.
`swath resume` restores persisted run context and accepts only tune keys whose
applicability includes `resume`; run-shape keys are rejected.
The diagnostic `--engine-toggle NAME=VALUE` (below) remains available for the
other engine ablations that don't fit the `--tune` registry.

#### Output

| Flag | Default | Description |
| --- | --- | --- |
| `-o, --output PATH` | stdout | Output destination: a path with a known extension (`.tsv`/`.jsonl`/`.parquet`) is FILE kind; anything else is a directory dataset; `-` or omitted means stdout. FILE-kind Parquet uses a one-writer dataset directory today, not one physical file |
| `--output-type file\|dir` | inferred from `-o` | Override the file-vs-directory inference above for a pathological path name |
| `--format auto\|table\|tsv\|jsonl\|parquet` | `auto` — `table` on a terminal, `tsv` when piped | Output format; an explicit `--format` (including `auto`) must agree with a known `-o` extension |
| `--tune parquet.writers=N` | `3` | Number of parallel Parquet writers (must be 2–4; a single-file `-o *.parquet` destination collapses to 1) |
| `--parquet-part-size SIZE` | `256mb` | Parquet part-file rotation target size (e.g. `128mb`, `1gb`) |
| `--part-rotation-interval DURATION` | `30s` | Rotate a Parquet part after it has been open this long, even below `--parquet-part-size` (bounds the resume `durable_cursor` lag; e.g. `10s`, `500ms`; `0`/`none` disables it) |
| `--part-rotation-max-rows N` | `2000000` | Rotate a Parquet part after this many rows, even below `--parquet-part-size`; `0` disables it |
| `--sort` / `--no-sort` | `--no-sort` | Globally sort the Parquet output by key (sorted output). `--format parquet` only; needs a checkpoint (`--checkpoint auto` or a path) |
| `--tune sort.ignore-disk-check=on` | off | Skip `--sort`'s disk pre-check + periodic in-run disk guard; see "Sorted output" below |

Parquet output requires `-o <path>`. A directory-shaped path (no recognized
extension, e.g. `-o out/`) is created if it does not exist, and multiple part
files are written in parallel under a `data/` subdirectory; a consumer
`manifest.json` at the dataset root is kept current alongside them. A
`.parquet`-extension path (e.g. `-o out.parquet`) selects FILE kind and collapses
to a single writer, but the current implementation still creates a dataset
directory at that path. `--sort` requires the directory-dataset form (its
checkpoint-resumable merge/publish state machine needs the dataset directory
as its handle) — a single-file `-o *.parquet` is rejected under `--sort`.
FILE-kind destinations are non-resumable: creating one requires an explicit
`--checkpoint none` acknowledgment — every other checkpoint mode, including the
`auto` default, is rejected (single-file output cannot carry a checkpoint) — for
a checkpointed, resumable run, use the directory-dataset form.
FILE-kind Parquet is currently written as a one-writer dataset directory under
the requested path; that physical layout is an implementation detail and does
not make the run resumable.

The output kind and checkpoint mode together determine durability, summary
defaults, and resume support:

| Output | Accepted checkpoint modes | Default JSON summary | Resume |
| --- | --- | --- | --- |
| stdout | `auto` or `none` (both ephemeral); an explicit path is refused | none | no |
| FILE-kind text (`*.tsv`, `*.jsonl`) | `none` only | none | no |
| FILE-kind Parquet (`*.parquet`) | `none` only | `<output>/_swath_summary.json` inside the one-writer dataset directory | no |
| DIRECTORY-dataset Parquet | `auto` (managed, co-located) or an explicit SQLite path | `<output>/_swath_summary.json` | `swath resume <output>` for the managed `auto` layout |

`--checkpoint none` changes durability, not the scan algorithm: it opens an
ephemeral checkpoint store and runs the same work-stealing engine, including
ready-queue/thief activity and `--trace` events. For supported CLI resume, use
the managed `auto` layout; `swath resume` accepts a dataset directory, not an
arbitrary SQLite path.

**On-disk layout:**

```
<output-dir>/
  data/
    part-00000.parquet
    part-00001.parquet
    ...
  manifest.json
  .swath-state.json
  _SUCCESS
  symlink.txt
```

- **`data/`** holds *only* `*.parquet` part files — no manifest, no markers —
  so a `data/*` glob is always safe (DuckDB's directory glob does not honor
  the Hadoop `_`-prefix skip rule).
- **`manifest.json`** is a **consumer manifest** in the S3-Inventory schema,
  plus additive sortedness fields: `{ "sourceBucket", "version",
  "creationTimestamp", "fileFormat": "Parquet", "fileSchema", "sorted" (bool),
  "sortKey" (non-null iff `sorted`), "files": [{"key": "data/<part>", "size",
  "MD5checksum", "rowCount", "minKey"?, "maxKey"?}, ...] }`. `rowCount` is on
  every file (sorted or not); `minKey`/`maxKey` (plain UTF-8 key text, not
  base64/hex — each file's TRUE first/last key, never Parquet footer stats)
  are present only when `sorted`, letting a consumer verify the cross-file
  range-disjoint invariant (`files[i].maxKey < files[i+1].minKey`, unsigned
  byte) without opening a single Parquet file. It is committed atomically
  (`manifest.json.tmp`, fsync, rename) on each part finalize and at run end. The
  in-memory file list and each rewritten manifest are `O(parts)`; because the
  complete list is rewritten at every finalize, total manifest serialization
  work over a run is `O(parts²)`. Keep part counts reasonable by using the
  default size/time/row rotation settings or larger parts where appropriate.
- **`.swath-state.json`** is an **internal** file (not part of the
  deliverable) holding the resume identity (`args_hash`, checkpoint `run_id`)
  that swath uses to detect a stale/foreign dataset on `swath resume`.
- **`_SUCCESS`** (empty) is the whole-snapshot completion marker, written
  **last**, after the manifest.
- **`symlink.txt`** lists the `data/<part>` paths for Hive/Athena/Trino
  auto-discovery.
- **`.swath/`** (not shown above) is an **internal, transient** directory that
  holds the co-located resume checkpoint (`.swath/checkpoint.sqlite`) while a run
  is in flight. It is deleted on clean completion, so a finished dataset carries no
  checkpoint bookkeeping — only the artifacts above.

Any pre-existing dataset root or `data/`, `_staging/`, and `.swath/` entry must
not be a symbolic link; those three managed child entries must also be
directories when present. swath's fixed checkpoint, marker, and
atomic-temporary files must not be symbolic links. swath checks these paths
without following links and refuses a link planted before startup, before it
validates, creates, opens, lists, truncates, or deletes managed contents; remove
the link or choose another output directory. Directory datasets use a
single-process ownership model: concurrently replacing entries while swath is
running is outside the supported threat model, so this startup refusal is not a
claim of race safety against a hostile process mutating the dataset at the same
time.

**The output is a directory of parts — read it as one dataset.** The files
under `data/` plus `manifest.json` *are* the deliverable, in the standard
multi-part columnar layout every query engine reads transparently
(`duckdb "SELECT … FROM read_parquet('out/data/*.parquet')"`, Athena, Spark,
Trino). **There is no compaction/merge step** — by design: a post-run merge
would re-read and re-write the entire very large dataset for no
functional gain, and one giant file is worse for parallel reads. If you want
fewer/larger files, raise `--parquet-part-size`. A `.parquet`-extension `-o` selects
FILE kind and one writer, but it still creates a dataset directory under that
path today — a true single-physical-file form is planned but not shipped; if you
need one physical file now, merge downstream
(`duckdb COPY (...) TO 'one.parquet'`). **A part is not guaranteed key-sorted** — it holds the
pages of several listing nodes, so it generally spans multiple disjoint key ranges (see
"Ordering" below). Sorted
output, within and across parts, is `--sort` (see "Sorted output" below).

On `swath resume` after a crash, any incomplete part the crash left half-written is
**automatically deleted** (it was never durable and never in `manifest.json`) and
its keys are re-listed from the last durable cursor — **you never clean up
partial files by hand**, and finalized parts are never rewritten.

#### Sorted output (`--sort`)

`--sort` produces a **globally key-sorted Parquet dataset** (with a
sortedness stamp in its footer) instead of the unordered multi-part dataset —
a single file at small scale, or **range-disjoint parts in key order** once the
output exceeds the roll threshold (`-Dswath.sort.final-file-bytes`, default
~1 GiB).
Semantics:

- **Parquet only.** `--sort` with any text format is an error (exit 2); a sorted
  text sink is deferred. It also **needs a checkpoint** — `--checkpoint none` +
  `--sort` is rejected, because the sort tracks its work durably.
- **How it works.** The listing streams into one ordered sort lane that buffers
  pages into heap-adaptive, checkpoint-tracked *page-run staging segments*
  (row-oriented `.pageseg` files — CRC32C-framed, codec-compressed page
  records — **not** columnar Parquet; only the FINAL output is Parquet)
  under the **visible** `_staging/` directory inside the output directory
  *(a mid-sort run is observable with a plain `ls`)*. When the listing finishes, a k-way
  merge streams the segments into the final **`part-00001.parquet`** (uniform
  naming shared with the unsorted path — no `sorted-` prefix; `%05d`
  zero-padded, lexical name order == key order) under `data/` (like any
  part), then writes `manifest.json` (with `sorted: true`), then
  `.swath-state.json`, then finally `_SUCCESS` (the publish commit point).
  The staging directory is deleted after a successful publish.
- **Crash-state legibility.** A dataset root's on-disk state is fully
  distinguishable from four signals alone — `_SUCCESS` present, `_staging/`
  present, `manifest.json`'s `sorted` field, and whether `data/part-w*` finals
  exist:

  | State | `_SUCCESS` | `_staging/` | `manifest.sorted` | `data/part-w*` |
  |---|---|---|---|---|
  | Fresh (never run) | absent | absent | — | absent |
  | Crashed, no-sort, mid-listing | absent | absent | — | present |
  | Crashed, `--sort`, mid-listing/merge | absent | present | — | — |
  | Complete, no-sort | present | absent | `false` | present |
  | Complete, `--sort` | present | absent | `true` | absent |
- **Requires a directory-dataset destination.** `--sort`'s checkpoint-resumable
  merge/publish state machine needs the dataset directory as its handle, so a
  single-file `-o out.parquet` is rejected under `--sort` (exit 2) — pass a
  directory-shaped `-o` (no recognized extension) instead.
- **`swath resume` works.** A crash mid-listing re-lists only the non-durable tail
  (durable segments are kept); a crash mid-merge or mid-publish re-runs **only the
  merge** (zero new LIST fetches). A `--sort` / `--no-sort` mismatch on resume is
  **refused**, exactly like a changed filter or output format — pass the same
  `--sort` you used originally. A **staging-format mismatch** is refused the same
  way: a checkpoint that staged the older `parquet-segment` segments cannot
  be resumed by the current `page-run` code — `--restart` it.
- **Memory.** Peak heap is a function of the heap you grant (`-Xmx`), never of
  object count: bigger heap ⇒ bigger segments ⇒ fewer files ⇒ a single-pass merge.
- **Sizing `-Xmx` against `--concurrency`.** Listing-phase staging pressure can
  rise with concurrency, especially during retries. Use the three gauges in §1
  of `docs/metrics-and-observability.md` to size and diagnose your own run:
  `swath.sort.staging.bytes.peak` (peak
  live, not-yet-durable staging bytes — compare against `T × swath.sort.segment-bytes` to see if it's
  proportional), `swath.sort.off_thread.buffers.peak` (should never exceed the configured
  `.buffers - 1` bound — if it does, that is itself a bug signal, not tuning) and
  `swath.sort.handoff.queue.depth.peak` (should also stay within that same bound; an unbounded
  climb here is the prime leak suspect). The `swath.steal_reason{outcome=SORT,
  reason=backpressure_engaged}` engagement counter marks whenever admission actually hit the
  off-thread bound, for correlating against the timing of the retry storm.
- **Parallel range merge (off by default).**
  <a id="parallel-range-merge-off-by-default"></a>
  `-Dswath.sort.merge-parallelism=R` (R > 1) splits the merge phase into R independent key ranges
  instead of one, merged concurrently on up to `min(R, availableProcessors)` threads. It is **off by default and not a released contract** — the decision to ship multi-file
  sorted output produced by a concurrent merge is reserved, and one gap is still open (below).

  *How it stays a global sort.* The keyspace is partitioned into R **contiguous, disjoint key
  ranges**, sampled from the staging segments' own key distribution. Each range independently
  k-way-merges every staging segment, emitting only the rows whose key falls in its `[lo, hi)`,
  and writes its own part file(s). Because the ranges are contiguous, disjoint and in key order,
  concatenating their outputs in range order **is** the global sort, so the parts are renamed into
  one ascending `part-00001.parquet`... sequence. Row-to-range assignment is an exact per-row key
  compare, so every row for a given key (its versions, and any cross-`row_type` rows) stays
  together in exactly one range. Boundary choice therefore affects only how EVENLY the ranges are
  balanced — never correctness. A keyspace with fewer than two distinct sample keys cannot be split
  and falls back to the untouched serial path, recording `SORT.merge_range_unsplittable` so the
  fallback shows up in the run's metrics instead of being indistinguishable from never asking.

  *What each range avoids reading.* A range only decodes the units of a segment that can hold one
  of its keys: Parquet staging prunes by row group; page-run staging steps over pages whose
  `[minKey, maxKey]` cannot reach the range, without decoding their rows. Note the page-run skip
  avoids the row **decode**, not the read — the format has no per-page offset index, so each range
  still reads and CRC-verifies the framed pages it steps over.

  *Cost, and the ceiling on `R`.* More ranges means more concurrent merge streams, so both peak heap
  and the process descriptor budget divide across them: each range's merge fan-in is its share of
  `merge-budget-bytes` and of the fd limit. Once that per-range fan-in falls below the
  staging-segment count, every range would **cascade** (merge in several passes, rewriting its rows
  each time) — slower than the serial merge rather than faster. So the usable ceiling is

  ```
  R_max ≈ merge-budget-bytes / (segments × per-stream-bytes)
  ```

  **`R` is clamped to that bound rather than honoured past it.** A run that asks for more logs
  `sort_merge_range_clamped` at WARN with the requested and effective values, and fires the
  `merge_range_clamped` counter; if not even one range fits, the run takes the serial merge. Raise
  `heap-fraction` (or the heap) to lift the bound — the remedy is more merge budget, not a bigger `R`.

  Note this ceiling **tightens as a listing grows**: segment count rises with object count, so an `R`
  that runs single-pass on a 10 M-object bucket can hit the clamp on a billion-object one at the same
  heap. That is the clamp working, and the WARN line is where you see it.

  On page-run staging there is also **read amplification**: because the format carries no per-page
  offset index, a range reads a prefix of each segment ending at its own upper bound, so the ranges
  together read about `(R+1)/2` times the bytes a serial merge reads while each decodes only ~`1/R`
  of the rows. Measured at `R=8` on a 9.9 M-key fixture: 4.50×. Decode dominates read at that
  scale — the range reading 8× the bytes of another finished within ~2 % of it — but on staging
  much larger than page cache the extra reads are real, so treat `R` as a throughput/IO trade
  rather than a free win.

  *Known gap.* Parts produced by the parallel path carry a range-local `file_index` and none is
  marked `file_final`, so they do not carry the self-describing multi-file completeness proof a
  serial `--sort` output does. Consumers that verify that stamp (including swath's own replay
  server in `--serving-mode sorted`) will reject the output even though its content is a correct
  global sort. This is why the path is off by default.
- **Disk sizing.** The staging volume (wherever `-o`'s output directory lives) holds BOTH
  the staging segments AND, during the final merge, the merged output being written
  concurrently. Measure staging and final-output bytes on a representative sample,
  scale that observation for your target, and leave headroom for both at once. Key
  length and entropy materially affect the ratio. Undersizing this volume means a
  listing can die at the merge, after all the LIST cost is already spent.
  Staging is **row-oriented, LZ4/ZSTD-compressed page-run** files, not
  columnar Parquet — so it trades disk for speed (the `swath.sort.segment-codec` knob trades ratio
  for speed in turn: `ZSTD1` is smallest-on-disk, `LZ4` (default) is fastest, `NONE` skips
  compression). The final Parquet output rolls into **~1 GiB parts** by default (`swath.sort.final-file-bytes` /
  MAX→1 GiB), so the merged output is no longer necessarily a single file. Also **raise
  `ulimit -n`** (e.g.
  `ulimit -n 65536`, or let the launcher do it): a single-pass merge opens up to ~fan-in (default
  10000) page-run readers at once, and a low fd limit degrades the merge to a slower multi-pass
  cascade.
  Two checks guard against this (`io.varve.swath.sort.SortDiskGuard`), both skippable with
  **`--tune sort.ignore-disk-check=on`**:
  - A **startup pre-check**: refuses immediately (exit 1) if the volume is already below a 1 GiB
    floor, or — on `swath resume` of a run that previously staged real segments — if `3x` what's
    already staged exceeds current free space.
  - A **periodic in-run guard**: while the listing runs, polls actual staging bytes written so far
    against usable free space; if `3x` the observed staging size would exceed free space, it logs
    `sort_disk_exhaustion_imminent` (carrying `error_class=sort_disk_exhausted
    stop_reason=sort_disk_exhausted resumable=true` — a distinct, greppable marker so an external
    supervisor can tell this apart from an unclassified crash) and aborts immediately
    (`Runtime.halt`, exit 1) rather than continuing for hours toward a doomed merge — the
    checkpoint stays durable/resumable (the crash-only contract) exactly as it would after any
    other abrupt process death, so a later `swath resume` (once the volume has more room) continues
    cleanly. The startup pre-check's refusal logs the same classification via its own
    `sort_disk_precheck_refused` marker.
  - Neither check can know a BRAND-NEW run's eventual bucket size in advance (there is no cheap
    a-priori object-count signal) — the periodic guard is what catches that case, typically within
    minutes of real data flowing, not by predicting up front. Size the volume generously for a
    known-large bucket, or pass `--tune sort.ignore-disk-check=on` if you've already sized it yourself.

**Dev knobs** (`-D` system properties; every default is stated below):
`swath.sort.segment-bytes` (heap-adaptive flush gate), `.segment-entries`
(secondary cap), `.heap-fraction` (the adaptive ratio, default `0.08`), `.buffers`
(in-flight sealed buffers, default `2`, **must be `>= 2`** — rejected otherwise:
fewer would either deadlock or silently exceed the documented live-buffer memory cap),
`.fan-in` (merge fan-in, **default `10000`** — but the pass
width actually used is fd- and budget-bounded, see `.merge-budget-bytes` and `ulimit -n` above),
`.segment-codec` (page-run staging payload codec — `NONE`|`LZ4`|`ZSTD1`, default `LZ4`;
trades staging-disk ratio for pack/merge CPU),
`.final-file-bytes` (roll threshold for multi-file output; **default `1 GiB`**),
`.final-row-group-bytes` (served file's seek granularity),
`.merge-per-stream-bytes` (the per-open-page-run-stream memory a merge holds — for page-run
input read exactly from each segment's trailer `maxRecordLen`; the divisor that bounds
`effectiveFanIn`), `.segment-row-group-bytes` (**COLUMNAR-Parquet-staging-only** —
governs the legacy columnar staging path, not the default page-run staging),
`.merge-budget-bytes` (merge-phase memory budget, same heap-adaptive
shape as `.segment-bytes`; caps how many segment streams a merge pass holds open at once, per
`.merge-per-stream-bytes`). The summary reports effective fan-in and observed
sort outcomes, but does not echo every configured system property; retain the
invocation when those settings matter to later analysis.

**Which knob actually binds.** The merge pass width used at runtime is
`effectiveFanIn = min(fan-in, max(2, merge-budget-bytes / merge-per-stream-bytes))` (further
clamped by the fd budget from `ulimit -n`) —
so tuning `.fan-in` alone can have **no observable effect** at a small heap: the
memory budget (`.merge-budget-bytes`, heap-adaptive) is the binding constraint
until it can afford at least `.fan-in` open streams, after which `.fan-in` itself
binds. The run-summary's `sort` block echoes `effective_fan_in` directly — the
realized pass width for that run, so you can read off which arm bound without
computing the formula yourself. For an older summary that predates this field
(`effective_fan_in: null`), fall back to comparing `segments` against `passes`:
if `segments / passes` is well below the configured `.fan-in`, the merge-budget
arm was binding, not `.fan-in`.

**Merge-phase liveness.** `segments`/`passes` are populated exactly ONCE,
after the whole merge finishes — a mid-merge `_swath_summary.json` snapshot legitimately
shows them flat/zero for the whole (potentially multi-minute) merge. Watch
`sort.merge_progress_units` instead to confirm a merge is genuinely advancing
(the SAME `swath.progress.units` tally the in-JVM liveness watchdog trusts), or
tail the log for the periodic `progress … phase=merging` record, emitted by the
run's single progress reporter at the `--progress-interval` cadence (default:
30 s) — its `rows_merged` field counts the rows the merge itself has moved,
against the exact staged-row total it was handed.

**Finalize/publish liveness.** The tail AFTER the merge — footer-fsyncing
each final part and streaming every part through MD5 to build `manifest.json` —
emits **byte-keyed** liveness ticks
(one per 64 MiB hashed, plus a pre-fsync tick per part) — visible post-hoc as the
`swath.steal_reason{outcome=SORT,reason=finalize_progress_tick}` counter — so a
slow-but-progressing finalize stays alive while a genuinely stalled one still trips.
The one residual is a single `fsync` of a very large part: that is one blocking
syscall with no intra-call ticks, so on large `--sort` runs over slow disk
raise `--idle-timeout` above the time to fsync your largest part.

#### Sizing sorted output

**How to size a box for a large `--sort` run.** With page-oriented staging (see
"Sorted output" above), disk grows with the captured data. Merge memory is
bounded by the configured heap and effective fan-in, while the current merge
and Parquet encode path is serial. Size from a representative run rather than a
universal per-object or machine-size claim.

**Disk — the gate; spend here.** Size the staging volume for staging *and* the
final output at once: staging is still resident while the merged output is being
  written, so `peak disk ≈ staging + output`, and both scale with captured data.
  Measure both on a representative sample, scale for the target run, and provision
  comfortably above that peak — a run that runs out of room at the merge has
already spent its entire LIST bill. Beyond capacity, disk *speed* matters: the
merge reads every staging byte back and writes every output byte, so NVMe
shortens it directly. Staging is transient — it drains as the merge proceeds —
while the final Parquet is what you keep, at roughly half the peak. If you are
capacity-bound, **`-Dswath.sort.segment-codec=ZSTD1`** trades merge CPU for
smaller staging. Watch free space during the merge; `SortDiskGuard` is a backstop
against exhaustion, not a substitute for sizing (whether the in-run guard covers
the *final-encode* phase is not yet cleanly verified — see above).

**RAM.** The merge holds one page per open segment —
the frontier — not the dataset, so its working set is a function of the heap you
grant and the merge fan-in, not the total row count. A single-pass merge holds only while
`segments < effective_fan_in`, and once the segments a run produces approach that
budget the merge cascades into multiple passes. Read `effective_fan_in` straight
off the run summary's `sort` block (see "Which knob actually binds" above); if a
merge cascades, raise the heap or `swath.sort.fan-in` / `.merge-budget-bytes`
rather than the box.

**CPU — modest; the merge is single-threaded.**
- **Merge and Parquet encode run on about one thread.** Extra cores sit idle
  through the merge phase; parallel encode is planned but not shipped.
- **Listing, by contrast, is parallel** — more cores speed the *listing* phase up
  to the network/API ceiling, but only if the bucket's shape parallelizes.

**Two things gate scale beyond hardware:**
1. **Serial encode.** The large majority of merge time is the single-threaded
   Parquet encoder — the merge ordering itself is nearly free — so until a
   parallel encoder pool lands, merge wall time is what it is and extra CPU
   cannot buy it down.
2. **The dense-directory listing tail.** No hardware fixes a single dense
   directory that paginates one page per round-trip; that is an engine fix, not a
   provisioning one. **Total wall = listing (bucket-shape-dependent, plus that
   tail risk) + the merge.**

Measure your own bucket before committing to a volume or heap size: bytes per
object vary with key shape, and segment count determines whether the merge can
stay single-pass. Total wall time also includes the listing phase, which is
network-, API-, and bucket-shape-dependent.

#### Run summary (JSON sidecar)

| Flag | Default | Description |
| --- | --- | --- |
| `--report PATH` | `<output>/_swath_summary.json` for every non-stdout Parquet destination, including FILE-kind `*.parquet`; otherwise none | Write a machine-readable JSON run-summary to `PATH` |
| `--tune summary.interval=DURATION` | `--progress-interval` when given, else `30s` | JSON run-summary flush cadence, e.g. `2s` or `500ms` |

The summary is operational data, not a sanitized telemetry envelope. It records
the target and raw arguments and can include filters, seed prefixes, slow-range
bounds, and key samples. Review and redact it before sharing if bucket names,
prefixes, endpoints, paths, filters, or keys are sensitive.

#### S3 connection

| Flag | Default | Description |
| --- | --- | --- |
| `--region REGION` | SDK default region provider chain | AWS region; explicit value overrides the SDK chain |
| `--profile NAME` | default credential chain | AWS credentials profile |
| `--no-sign-request` | off | Anonymous requests (public buckets) |
| `--endpoint-url URL` | — | Custom S3-compatible endpoint (LocalStack, MinIO, etc.) |
| `--force-path-style` / `--no-force-path-style` | on when `--endpoint-url` is set | Force path-style S3 addressing |
| `--bearer-token-command CMD` | — | Shell command whose stdout is a fresh OAuth bearer token, used instead of AWS SigV4 signing for every request |
| `--bearer-token-refresh-interval DURATION` | `45m` | How often to re-run `--bearer-token-command` for a fresh token; rejected without that flag |
| `--fetch-owner` | off | Request the `Owner` field from S3 (`FetchOwner=true`); populates `owner_id` and `owner_display_name` in output |
| `--requester-pays requester` | off | Requester-pays buckets: send `x-amz-request-payer: requester` on every S3 request (only accepted value: `requester`) |
| `--metrics-endpoint URL` | environment or off | Export OTLP metrics to URL; overrides `SWATH_OTLP_ENDPOINT` |
| `--no-metrics` | off | Disable metrics export even when the endpoint is configured in the environment |

swath bundles the aws-sdk `sts` module so the default credential chain resolves OIDC
web-identity credentials (`AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE`, e.g. GKE/EKS
workload identity), auto-refreshing from the token file, for signed private-bucket
listing.

##### Listing a GCS bucket via its S3-compatible XML API

GCS's XML API is largely `ListObjectsV2`-compatible, including the `start-after`
range primitive swath's engine depends on — see
[`docs/internals/s3-implementation-compatibility.md`](internals/s3-implementation-compatibility.md)
for compatibility caveats verified against LocalStack/MinIO (GCS itself has not been run
through that same conformance suite yet). Rather than minting GCS HMAC interoperability
keys, point swath at a command that prints a fresh Google OAuth bearer token — GCS's XML
API accepts `Authorization: Bearer <token>` directly:

```sh
swath list s3://some-gcs-bucket/prefix/ \
  --endpoint-url https://storage.googleapis.com \
  --force-path-style \
  --bearer-token-command 'gcloud auth print-access-token' \
  --format parquet --sort \
  -o ./out
```

`--bearer-token-command` replaces SigV4 signing entirely (`--profile`/`--no-sign-request`/
`AWS_ACCESS_KEY_ID` are ignored for signing when it's set, though the SDK's normal
credential resolution still runs harmlessly in the background). The command re-runs on
`--bearer-token-refresh-interval` (default 45m — comfortably under a typical ~1h Google
OAuth access-token TTL); it isn't real expiry-aware, since a bearer token string alone
carries no portable expiry. This is a **listing-only** path: swath's output is always a
local path (`-o`) today, so it doesn't touch GCS's XML multipart-upload precondition gap
that rules the same mechanism out for a future GCS output/checkpoint destination.

**Resuming a bearer-auth run.** Unlike `--profile`/`--region`/`--no-sign-request`, which are
soft-restored from the checkpoint, `--bearer-token-command` is deliberately **never stored**.
Storing it would mean a checkpoint decides what a later `swath resume` executes — and nothing
obliges the command to *mint* a token, so a `--bearer-token-command 'echo <token>'` shortcut
would leave that literal token at rest in the checkpoint. The token itself never leaves memory
today, and persisting the command is the one thing that would change that. Re-pass both flags
on resume:

```sh
swath resume ./out --bearer-token-command 'gcloud auth print-access-token'
```

Omit it and the resumed run falls back to SigV4 signing, which a bearer-auth endpoint will
reject. Re-passing only `--bearer-token-refresh-interval` — the likeliest version of that
slip — is rejected outright (exit 2) rather than silently signing with SigV4.

#### Seeding

| Flag | Default | Description |
| --- | --- | --- |
| `--tune seed.mode=shallow\|none\|hints` | `shallow` | Seed strategy for the work-stealing engine. `shallow` runs a `delimiter=/` pass up front to discover top-level prefixes and create parallel starting ranges (recommended across supported general-purpose, globally ordered S3 key distributions). `none` starts from a single root range and relies on stealing alone. `hints` is not yet implemented (throws an error). |

#### Concurrency and liveness

| Flag | Default | Description |
| --- | --- | --- |
| `--concurrency N` | `64` | Target (ceiling) concurrency `T` for the work-stealing engine. Live concurrency is adjusted adaptively (AIMD) within `[1, T]`. |
| `--progress-interval DURATION` | `1s` redrawing, `30s` appended | Override the progress cadence (see §Progress) and opt into progress, e.g. `2s`; `1s` is the supported floor. |
| `--object-listing-queue-size N` | `50000` | In-flight entry budget for the listing queue |
| `--request-rate N` | unset | Cap aggregate S3 API requests per second; `0` also disables the cap |
| `--engine-toggle owner_split=off` | on | Diagnostic owner-side self-splitting ablation; the default keeps the optimization enabled |
| `--max-duration DURATION` | unset (no timebox) | Stop after DURATION (exit `124`). The partial is resumable only for a managed directory-dataset run with durable state |
| `--idle-timeout DURATION` | `120s` | Liveness-watchdog stall window: abort if no activity is observed for this long; `0`/`none`/`off` disables it |
| `--no-progress-timeout DURATION` | `10m` | Liveness-watchdog backstop: abort if no committed progress is observed for this long, even while activity continues; `0`/`none`/`off` disables it |

#### Diagnostic-tier ablation (`--engine-toggle`)

**EXPERIMENTAL / DIAGNOSTIC — a measurement tool, not a supported configuration, with one
documented exception: the rollback pair described below.** `--engine-toggle
NAME=VALUE` (repeatable) is a single ablation namespace so a per-mechanism A/B measurement of the
`WorkStealingScan` engine runs from one binary instead of a bespoke flag per experiment. Every
structure toggle defaults to `on` (`readahead` defaults to `off`; `mass_aware_seed` and
`rate_anchored_sensing` are default ON, opt-out `NAME=off`; `tail_floor` is the one
value-taking toggle, default `reach_floored`); **the defaults are the only supported
configuration, except for the documented `rate_anchored_sensing=off` + `tail_floor=current`
rollback below** —
toggling never changes correctness (I2/I3 tiling holds either way), only which optimization
mechanisms engage. An unknown name, a malformed value (not `on`/`off` — or, for `tail_floor`, not
one of its modes), or a contradictory combination
(the same name repeated with both values) is a startup validation error (exit 2).

| Name | Default | Effect when `off` |
| --- | --- | --- |
| `owner_split` | on | Owner-side proactive self-splitting is disabled, leaving pure thief-halving. |
| `density_ewma` | on | The far-ahead steal pivot fraction becomes a constant `0.75` instead of the EWMA-driven density signal, at both bounded-range steal sites (the thief and the owner-split site). The open-frontier extrapolation path is unaffected. |
| `radix_bands` | on | `SeedStep` skips subdividing a dense flat region at seed time — it stays one un-subdivided range instead of alphabet-uniform radix bands. |
| `structure_probes` | on | The thief's demand-driven `delimiter=/` structure probing is disabled entirely; both the empty-upper and parent-empty routes fall straight to their non-structure fallback (bisection / flat-leaf density reflection). |
| `far_ahead` | on | The bounded-range steal fraction is fixed at the plain `0.5` byte-midpoint. Composes with `density_ewma`: `far_ahead=off` always wins (checked first), regardless of `density_ewma`. |
| `alphabet_pivots` | on | The alphabet-aware pivot synthesis is disabled — `interpolate` uses the plain code-point overload (no observed-alphabet digest consult) at both call sites (the thief and the owner-split site). |
| `reflect` | on | The density-reflected pivot placement (the reflected empty-upper pivot and the reflect clamp, and by extension `reflect_lift` below — the lift is itself a reflection application, gated on `reflect() && reflect_lift()`, never `reflect_lift()` alone) is disabled: the thief's empty-upper branch no longer re-seeds the blind bisection with `StealMath.extrapolate` (it falls straight to the uniform-midpoint bisection), the owner-split no longer clamps an overshooting interpolated pivot down to the reflected mass horizon, and the reflect-lift never fires even if `reflect_lift` is separately on. Off ⇒ the exact prior placement at all three sites (full reflection ablation). |
| `confetti_feedback` | on | The realized-child-mass feedback gate is disabled: owner-split children are no longer tagged/classified, and a demand-gate/floor-eligible carve is never suppressed by an observed run-level confetti rate. Off ⇒ the exact prior behavior (only the demand gate and the child-tail floor bound owner-split carving). |
| `reflect_lift` | on | The zero-page-per-carve reflect-lift is disabled IN ISOLATION (the reflected empty-upper pivot and its clamp stay active — see `reflect` above, which kills the lift too when off): a degenerate owner-split pivot that would leave the owner a sub-one-page kept share is no longer lifted up to the density-reflected pivot. Off ⇒ the exact prior behavior (every carve publishes at the cursor's degenerate successor unchanged; the confetti feedback gate remains the sole backstop). |
| `fanout_tiling` | on | `SeedStep`'s zero-probe `key=value/` partition-fanout tiling is disabled — a truncated partition fan-out is no longer tiled along its first-page child prefixes. NOTE: with `mass_aware_seed` at its ON default the un-tiled cut may instead be sample-proven heavy and BANDED; a pure fan-out-tiling ablation needs `mass_aware_seed=off` too. |

(`mass_aware_seed`, `rate_anchored_sensing` and `readahead` are documented in the "New-mechanism
performance toggles" section below, as is the value-taking `tail_floor` arm.)

Each disabled toggle's effect is provable post-hoc from the metrics alone: turning a mechanism off
silences its own §5 counters (e.g. `structure_probes=off` ⇒ `swath.probe.structure_fetches` stays
zero), and a `TOGGLE.<name>_off` engagement mark additionally fires once per run for every disabled
toggle so an analyst never has to infer the ablation from absence alone. A startup INFO log line
(`engine_toggles_effective ...`) and the JSON run-summary's `engine_flags` block (every toggle name,
alongside `max_duration_ms`) both echo the effective state whenever any toggle is non-default.

#### New-mechanism performance toggles — defaults and cost profile

Unlike the ablation toggles above (default `on` = the shipped behavior), four toggles are
**new mechanisms** — all passed through the same `--engine-toggle` option, but distinct from
the on/off ablation list — and are the only knobs a perf-focused user needs
to consider. `mass_aware_seed` and `rate_anchored_sensing` are **default `on`** — opt-*out*
(`--engine-toggle NAME=off`), not opt-in; `readahead` remains **opt-in**, default `off`;
`tail_floor` selects a *mode* rather than on/off, default `reach_floored`.

`rate_anchored_sensing` and `tail_floor` are a pair, and turning both off together is the one
**supported** non-default configuration:
`--engine-toggle rate_anchored_sensing=off --engine-toggle tail_floor=current` restores the legacy
scheduling arms exactly. Output is unaffected either way — the pair changes scheduling, not the key
set.

> Note: that rollback is a *deviation from the defaults*, so it prints the
> `engine_toggles_effective` startup line. That line names the pair as a supported rollback rather
> than calling the whole non-default surface unsupported — the warning is about the other toggles,
> not this one.

| Name | Default | Change from default when |
| --- | --- | --- |
| `mass_aware_seed` | **on** | You need the older, non-mass-aware seed exactly, for a diagnostic A/B (`--engine-toggle mass_aware_seed=off`). It is what carries the badly skewed buckets: `pdbsnapshots` only reaches completion with it on, `genome-browser` completes materially faster, and `meeo` covers far more of the keyspace under the same time cap. It is also cheaper *per object* even on the hive-partitioned buckets where it spends more absolute API calls (`blockchain`) — because it enumerates proportionally more of them. There is no ordinary reason to turn it off. |
| `rate_anchored_sensing` | **on** | You need the legacy position sensor exactly, for a diagnostic A/B or a rollback (`--engine-toggle rate_anchored_sensing=off`). On the default arm, remaining work is read as the range's own proven mass (`max(keysEmitted, page)`), which cursor-anchored geometry may lift by up to sixteen and cut by at most four. It replaces the legacy local-density-times-span reading at the two fleet-level sites that steer on it — victim choice and the owner-split gate chain (`readahead`'s own engage gate keeps the older reading either way: it scores one owner's local runway, not a fleet-wide ranking) — which matters on a deep-nested keyspace where the old window's consumed span underflows to zero and a range's emitted keys drop out of its own estimate. It changes which range is stolen from and when an owner carves, never what is emitted. A run marks itself `TOGGLE.rate_anchored_sensing_on` and emits the sensor's own classification counters, one namespace per site (`SENSING_OWNER.*`, `SENSING_STEAL.*` — metrics-internals.md §5a). |
| `tail_floor` | **`reach_floored`** | You need the legacy floor arithmetic exactly (`tail_floor=current`), for a diagnostic A/B or a rollback, or you are racing the other cure arm (`est_direct`). The default cures a **wide-flat** keyspace, where the legacy floor is measurably blind: it scores the child tail as `est × max(0, min(1, densityRatio) − f)`, and on a wide-flat tail the trailing-density ratio reads ~3e-4 against `f`≈0.5, so the product is **exactly zero regardless of `est`** — a range with 10^5–10^6 keys left is refused every time (measured: 5,326 of 5,326 owner attempts on one `nara` tail range, which then drained serially for the rest of the run). `tail_floor=est_direct` blocks iff `est <= 2×max-keys`, dropping the byte-geometry window entirely (under `rate_anchored_sensing` the estimate is already realized-mass-anchored, so multiplying it by geometry double-counts); `tail_floor=reach_floored` keeps the window product but floors the reach term at 1/16, so a thin trailing density shrinks the child's share instead of erasing it (it admits from ~32 pages of estimate up). Both are strictly more permissive than `current` and neither changes where a pivot lands, so tiling is untouched — they change **how often an owner carves**, never what is emitted. `reach_floored` is the default; `est_direct` stays available as the other raced arm (composable with `rate_anchored_sensing`, deliberately not coupled to it). A run on an arm marks itself `TOGGLE.tail_floor_<mode>_on` and counts every decision the mode flipped, per consult site (`TAIL_FLOOR.gate_*`, `TAIL_FLOOR.clamp_*`, `TAIL_FLOOR.lift_*` — metrics-internals.md §5). |
| `readahead` | **off** | You want the lowest wall-clock on a drain-heavy bucket (`encode` and `pmc` are the shapes it has been measured engaging on) and can pay for it: readahead trades materially more API calls and a materially higher peak RSS for less wall time. Leave it off when API cost or memory matters more than latency. |

**When both apply to the same cut.** On a truncated cut that is BOTH a `key=value/` partition
fan-out and would otherwise be mass-aware-sampled, `fanout_tiling` (when on) wins outright — the
sample is short-circuited, at zero extra probes — so the two compose identically whether
`mass_aware_seed` is off or on for that shape. Mass-aware banding still fully owns every heavy
NON-partition cut.

Maximum-performance recipe (readahead on top of the now-default `mass_aware_seed`):
`swath list s3://BUCKET ... --tune engine.readahead=on`

Cost notes: none of the four performance toggles has an unbounded direct call-amplification mechanism —
readahead speculation is engage-gated and window-bounded (K=8 contiguous guesses per engaged range),
mass-aware sampling is carved out of the fixed 256-probe seed budget (≤32 probes), and
rate-anchored sensing and the tail-floor arms are local estimate arithmetic (a `tail_floor` arm's
extra cost is one more owner-side split per admitted carve — bounded by the same
one-carve-per-32-pages rate limit as any other, plus one duplicate floor evaluation per consult so
the run can report what the arm changed).
The api multipliers only cost money on requester-pays/private buckets (public-bucket LIST
is free); the RSS cost is real everywhere. Each of the four announces itself via a once-per-run
`TOGGLE.<name>_on` engagement mark (`TOGGLE.tail_floor_<mode>_on` for the value-taking one), so a
run's summary always shows what was enabled.

#### Run trace (`--trace`, V1)

| Flag | Default | Description |
| --- | --- | --- |
| `--trace PATH` | off | **DIAGNOSTIC.** Write an opt-in JSONL "flight recorder" of the `WorkStealingScan` run to `PATH`: one event per line for range seeding, claims, page commits, steal attempts, and splits (with pivot-mechanism attribution). See docs/internals/metrics-internals.md §7 for the full schema. Works with both durable and `--checkpoint none` runs |

Diagnostic tier, like `--engine-toggle`: off by default, zero cost when unset (the no-op
`TraceSink` guards every event-object allocation), meant for post-hoc "why was this run slow/
what did it do" diagnosis rather than routine operation. **Sensitivity:** unlike the summary
JSON, a trace carries real key names on nearly every event — treat it with the same care as the
output listing itself before sharing one.

#### Filtering

Filters apply after listing; they do not reduce API calls.

| Flag | Description |
| --- | --- |
| `--include REGEX` | Keep keys matching this Java regex |
| `--exclude REGEX` | Drop keys matching this Java regex |
| `--min-size SIZE` | Keep objects >= this size (e.g. `1k`, `256mb`) |
| `--max-size SIZE` | Keep objects <= this size |
| `--modified-since DATE` | Keep rows modified at or after this UTC date/time |
| `--modified-until DATE` | Keep rows modified at or before this UTC date/time |
| `--storage-class CLASS[,CLASS]` | Keep objects in these storage classes (e.g. `STANDARD,GLACIER`) |

#### Checkpoint and resume

| Flag | Default | Description |
| --- | --- | --- |
| `--checkpoint PATH\|none\|auto` | `auto` | Checkpoint store location. `auto` co-locates the checkpoint inside a directory-dataset output at `<dir>/.swath/checkpoint.sqlite` (the output dir is the run handle) and is ephemeral for stdout; `none` uses an ephemeral store (no resume possible); an explicit path uses that SQLite file. FILE-kind output rejects every mode except `none` |
| `swath resume <dir>` | — | Resume the run whose checkpoint is co-located in `<dir>` |
| `--restart` | off | Discard any prior checkpoint and start fresh |
| `--overwrite` / `--force` | off | Discard a **completed** dataset at `-o <dir>` and re-list it |

`swath resume` and `--restart`/`--overwrite` are mutually exclusive. `--checkpoint
none` with `swath resume` is an error. FILE-kind destinations require `--checkpoint
none`, and stdout rejects an explicit `--checkpoint <path>` (it would create a
checkpoint nothing can ever resume); checkpoint/resume is supported only for
directory-dataset destinations. The public resume command opens only the managed
co-located layout created by `auto`: `swath resume <dir>` does not accept a raw
SQLite checkpoint path.

The checkpoint is **deleted on clean completion** — a finished dataset keeps its
`manifest.json`, `_swath_summary.json`, `_SUCCESS`, and `data/`, but carries no checkpoint
bookkeeping. A fresh `swath list -o <dir>` over a **completed** dataset is refused
(steering to `--overwrite`) purely from the on-disk markers, so the guard holds even
though the checkpoint is gone.

#### Global flags (`-v`/`-q`/`--color`, applies to every command)

`-v`, `-q`, and `--color` are accepted **before or after** the verb — both
`swath -v list …` and `swath list -v …` work. Occurrences on the two sides add up, so
`swath -v list … -v` is `-vv` (DEBUG), exactly as if you had written it on one side.

| Flag | Level |
| --- | --- |
| `-v` | INFO |
| `-vv` | DEBUG |
| `-vvv` | TRACE |
| `-q` | ERROR |
| `-qq` | off (logging silenced entirely) |

`-q` wins over `-v` when both are given — e.g. `-vvv -q` still logs at ERROR.

| Flag | Default | Description |
| --- | --- | --- |
| `-q, --quiet` | off | Lowers the log level (see above) and suppresses the startup destination echo. `-qq` silences logging, but not the terminal `swath: …` error line printed on failure — that goes straight to the CLI's error stream, not through the logger |
| `--color` | `auto` | Colors the live progress line and the end-of-run summary block: `auto` (color only when stderr is a terminal), `always`, or `never`. See [Color](#color) below |

---

## Checkpoint and resume

The **output directory is the run handle.** With the default `--checkpoint auto`,
a directory-dataset run co-locates its SQLite checkpoint at
`<dir>/.swath/checkpoint.sqlite`. The checkpoint records:

- every listing range and its checkpoint cursor,
- every finalized Parquet part file.

If the run is interrupted (Ctrl+C, crash, OOM), run `swath resume <dir>` to
continue from durable state. Finalized parts are retained exactly once; an
unfinalized Parquet tail may be re-listed from its `durable_cursor`.

```
# First run (interrupted)
swath list s3://my-bucket/ -o out/ --format parquet

# Resume it by its output directory — the run handle
swath resume out/
```

`swath resume <dir>` opens `<dir>/.swath/checkpoint.sqlite` directly and restores
the run's listing identity from it. A directory-dataset output is the **only**
resumable regime — the output directory is the run handle. A **stream** (stdout)
or **FILE-kind** run keeps no checkpoint (zero litter) and is **not resumable**;
re-run the command to produce it again, or write to a directory dataset if you
need resumability.

On clean completion the checkpoint is deleted, so `swath resume <completed-dir>`
reports **"already complete"** and exits `0`.

### Resume constraints

- The listing arguments that determine *what is listed* must be unchanged. A
  mismatch in bucket, prefix, or endpoint causes an error (use `--restart`).
- Changing a filter (`--include`, `--min-size`, etc.) or output format between
  runs is refused in v1.0; use `--restart`.
- `--checkpoint none` runs the same work-stealing scan with an ephemeral
  checkpoint store. It has no durable state and cannot be resumed.

### Delivery guarantees by output format

| Sink | Guarantee on resume |
| --- | --- |
| **DIRECTORY-DATASET Parquet** | **Exactly-once durable dataset.** Finalized part files (footer fsynced) are never rewritten; an unfinalized tail may be discarded and re-listed from its `durable_cursor`. |
| **stdout stream** | **Non-resumable.** Ephemeral, one-shot — re-run to reproduce. |
| **FILE-kind text/Parquet** | **Non-resumable.** For a resumable run use a directory-dataset destination (`-o <dir>`); a successful single-file text publication atomically replaces the destination. FILE-kind Parquet currently uses a one-writer dataset directory under the path; a true single-physical-file form is planned but not shipped. |

---

## Output format — Parquet schema

When `--format parquet`, each object is written as one row with the following
columns:

| Column | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `key` | `BINARY` | no | Raw key bytes (byte-exact, not UTF-8 coerced) |
| `size` | `INT64` | yes | Object size in bytes; null for `COMMON_PREFIX` and `DELETE_MARKER` rows |
| `last_modified` | `INT64` TIMESTAMP(MICROS,UTC) | yes | null for `COMMON_PREFIX` rows |
| `etag` | `BINARY` (UTF8) | yes | Quotes stripped; multipart ETag (`hex-N`) kept verbatim |
| `storage_class` | `BINARY` (UTF8) | yes | e.g. `STANDARD`, `GLACIER` |
| `version_id` | `BINARY` (UTF8) | yes | Versioned listings only — always null today |
| `is_latest` | `BOOLEAN` | yes | Versioned listings only — always null today |
| `is_delete_marker` | `BOOLEAN` | no | Written on every row; `false` for ordinary objects and common prefixes |
| `owner_id` | `BINARY` (UTF8) | yes | Populated when `--fetch-owner` |
| `owner_display_name` | `BINARY` (UTF8) | yes | Populated when `--fetch-owner` |
| `checksum_algorithm` | `BINARY` (UTF8) | yes | When present |
| `checksum_type` | `BINARY` (UTF8) | yes | When present |
| `row_type` | `BINARY` (UTF8) | no | Written on every row: `OBJECT` \| `COMMON_PREFIX` \| `DELETE_MARKER` |

Versioned listing (`--all-versions`) is **planned but not built in v1.0**. What that costs you
is narrow and specific: `version_id` and `is_latest` are always null, and no `DELETE_MARKER`
row is ever produced — so `is_delete_marker` is `false` on every row a run writes. The columns
themselves are always present and always written: `row_type` and `is_delete_marker` carry a
real value on every row, today, so a consumer can filter on them without a null check. See
[`ROADMAP.md`](../ROADMAP.md).

Writer settings are pinned: `parquet.block.size` 64 MB, `parquet.page.size` 1 MB,
dictionary encoding on, ZSTD level 3.

Local Parquet durability in v1.0 targets POSIX/Linux/macOS filesystems. swath
does not weaken file fsync; directory fsync is attempted for finalized parts and
manifest renames, then debug-logged and skipped on filesystems/OSes that do not
support fsync on directories.

### Ordering

Each Parquet part file holds the pages of several listing nodes, concatenated
in the order the writer emitted them (not key order). Within a single node's
pages, rows are in the order S3 returned them (unsigned-byte key order) — but a
part file spans multiple disjoint key ranges, so **a part is not itself
key-sorted**, and **global ordering across part files is not guaranteed** when
using multiple parallel writers (the default). Use DuckDB or another tool to
sort after the fact if a globally-sorted output is required, or pass `--sort`
(see "Sorted output" above) to have swath produce the dataset already in key
order: one sorted file at small scale, and **range-disjoint parts in key order**
— every key in `part-00001` below every key in `part-00002`, so reading the
parts in name order reads the dataset in key order — once the output crosses the
`-Dswath.sort.final-file-bytes` roll threshold (default ~1 GiB).

---

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success, empty result, an already-complete resume, or stdout closed by the downstream reader (broken pipe) |
| `1` | Unrecoverable error: listing failure, non-disk-full output write failure, checkpoint corruption |
| `2` | Bad arguments, invalid URI, invalid configuration, or a guarded refusal (unfinished/foreign output dir, format/extension mismatch) |
| `74` | The output filesystem ran out of space (`EX_IOERR`) — retry with a larger workspace. The partial is resumable only when the run has managed durable dataset state |
| `75` | Retryable stuck state (`EX_TEMPFAIL`). The partial is resumable only when the run has managed durable dataset state |
| `124` | Stopped by `--max-duration`. The partial is resumable only when the run has managed durable dataset state |
| `130` | Cancelled by SIGINT (Ctrl+C) |
| `143` | Cancelled by SIGTERM (the default `kill`) |

---

## Observability

### Progress

One reporter covers the whole run — the seed step, listing, the sort merge and the final write —
and emits one record per tick on **stderr** (stdout stays data). It takes one of two forms, never
both at once: the operator-facing line when a display is wanted, and the structured `progress` log
record otherwise. Whichever is installed, a tick renders exactly once — and `--no-progress` installs
neither, so it silences the log record as well as the display.

The operator line takes one of two **forms**, carrying identical content either way. On a terminal
it redraws in place — each tick overwrites the last, so an hour-long run occupies a single line
instead of scrolling the session away. Anywhere else it is one plain, newline-terminated record per
tick: no carriage returns, no escape sequences, so a redirected stderr stays readable as-is and a
captured log never fills with control characters. Only the framing differs; no field appears in one
form and not the other, so a run's captured output says exactly what its terminal showed.

```
  seeding · 12/64 probes (19%) · last probe 3.1s ago · 21.7s elapsed · 64 API calls · <$0.001 (est. @ $0.005/1k LIST)
  listing · 4,231,000 objects · 128,000 keys/s (avg 96,000) · 4,231 pages · 512 in flight · 1m12s elapsed · 8,900 API calls · ~$0.045 (est. @ $0.005/1k LIST)
  merging · 24,000,000 rows merged · 24 segments · 2m05s elapsed · 8,900 API calls · ~$0.045 (est. @ $0.005/1k LIST)
  writing · 3,100,000/12,000,000 rows (26%) · 24 segments · 2m28s elapsed · 8,900 API calls · ~$0.045 (est. @ $0.005/1k LIST)
```

Each phase shows what it actually has. Seeding reports probes completed against the seed's probe
budget and the age of the last completed probe — the signal that tells a healthy seed from a hung
one, since a seeding run emits no objects, fetches no pages and holds no workers. Listing reports
objects emitted this session (recovered rows from a resume are shown separately as `(+N
recovered)`, so a resumed run neither displays a zero it did not earn nor jumps by billions at the
end), live and average object rate, pages and in-flight ranges. A merge reports the rows it has
moved, and a percentage only for its final pass (`writing`): a cascading merge rewrites every staged
row once per pass, so work done legitimately exceeds the staged rows until then. Every line then carries session elapsed, API calls, and the estimated spend
with the rate it assumed — withheld entirely under `--endpoint-url`, where the provider's LIST
pricing is unknowable, exactly as the end-of-run block withholds it. **No key text ever appears**:
keys can carry arbitrary bytes, and a line that echoed them could be made to forge one.

There is deliberately **no ETA and no percentage for listing**: an unsorted scan has no honest
denominator — the object total is not known until the run ends. Seeding and the merge's final pass
do have exact ones (the probe budget; the staged rows), so those carry a completion figure and
listing does not.

Whether the line appears:

| situation | progress |
| --- | --- |
| stderr is a terminal, no `-q`, no `-v` | on (the default) |
| stderr is a file or a pipe | off — a summary prints once, progress repeats |
| `-q` / `-qq` | off, unless `--progress` |
| `-v` or higher | off — INFO logging is on, so the structured `progress` record is the surface |
| `--progress` | on, past every gate above, including off a terminal and under `-q` |
| `--no-progress` | off, everywhere — display and structured record alike, including with an explicit `--progress-interval` |
| `--progress-interval DURATION` | on — asking for a cadence is asking for progress |

A redrawing line is also bounded to the terminal's width, dropping whole trailing fields rather
than cutting through one — the fields are ordered most-important-first for exactly that reason. The
width is re-read every tick, so a window resized mid-run is honoured by the next frame. A terminal
that will not report its width, or one under 24 columns, keeps the plain records instead: an erase
whose reach cannot be predicted is worse than a line that scrolls. `TERM=dumb` does the same.

The default cadence depends on the form, because the cost of a tick does. A redrawn frame replaces
the one before it and leaves nothing behind, so it ticks **every second** — a counter that only
moved every 30 s would read as a hung run, which is the misreading this display exists to prevent.
An appended record is a line in a captured log forever, so it stays at **30 s**. Either way the
first record lands a couple of seconds in rather than a whole cadence later, so a run that finishes
in 22 seconds is not silent.

`--progress-interval` overrides both (e.g. `2s` for dense sampling on short runs); **`1s` is the
supported floor** and anything faster is rejected rather than clamped — a ten-hour run at `1ms`
would attempt some 36 million records, and a cadence below one a second outruns both a reader and a
captured log. Note that `--tune summary.interval` keeps following the *configured* interval (30 s
unless you pass one), not the display's faster tick: a JSON sidecar flush is an atomic file rewrite,
where a repaint costs nothing.

Under `-v`, the same tick is logged instead as one structured `progress` record — run id, phase,
session and phase elapsed, API calls, retries and (where the provider's pricing is knowable) cost,
with the same phase-shaped tail — which is what an external supervisor tailing the log reads. See
[`metrics-and-observability.md`](metrics-and-observability.md#4--v-progress-record-30-s-default---progress-interval)
for its field list.

### End-of-run summary

When a run ends, swath prints a short summary block to **stderr** (stdout stays data):

```
  1,204,993 objects in 4m12s · 4,781 keys/s
  1,208 API calls · 1.00 per 1k objects · in flight avg 52.00 · peak 64
  ~$0.006 (est. @ $0.005/1k LIST)
  12 files · 84.0 MB written · peak RSS 512.0 MB
```

The headline's elapsed figure is the **listing clock** — the same one `keys/s` divides
by — which starts only AFTER a fresh run's seed step (probing the bucket's shape to tile the
initial worklist). On a run whose seed step took a while, the headline instead carries a second
figure, the whole session (seeding included — the same span the live progress line already
reports), clearly labeled so which one the rate is keyed to is never ambiguous:

```
  3,270,132 objects in 1m43s listing · 31,750 keys/s · 2m22s total
```

That second figure only appears when it would actually differ from the listing one by more than
about a second (`SummaryRenderer.SESSION_DELTA_MIN`) — a resumed run, or any run whose seed step was
cheap, keeps the single-figure form above rather than printing two near-identical numbers. `--report`
carries both unconditionally: `duration_ms` (listing, unchanged) and the additive `session_duration_ms`
(the whole invocation) — see
[`metrics-and-observability.md`](metrics-and-observability.md#2-list_run_summary-one-line-at-run-end).

A **faults line** — `throttled N · retried M · errors K` — is inserted only when one of those
counts is non-zero, so its presence is itself the signal: `throttled` counts real S3 backpressure
(503 SlowDown / transient 5xx), `retried` counts client-side transients that were retried and
recovered, and the two are deliberately never folded together. A run that stopped short leads with
`INCOMPLETE (<reason>)`, plus `— resume: swath resume <dir>` when the run left something resumable
and resuming could actually help — a crash is a deterministic failure a resume would hit again, and
a seed failure marks the run so a resume is refused outright, so both get the marker without the
invitation. Runs that stop before the engine starts (a failed seed probe, for instance) get the
block too, from the same numbers the report records.
A run stopped by a closed downstream (`swath list | head`) is not an incident: it prints nothing by
default, and reads `stopped early — downstream closed` if you asked for the block explicitly.

The block prints when the run **earned** it: the operator's whole wait — seeding included — was
longer than 1.5 s, it produced durable output, or it stopped for any reason other than finishing —
and not under `-q`. Terminal detection does not
enter into it: a summary redirected into `2> run.log` carries the same content it would on a
terminal, because for an overnight or fleet run the captured log is the artifact. `--stats` forces
the block past every one of those gates (short run, `-q`, redirected stderr alike), `--no-stats`
suppresses it everywhere.

| you want | use |
| --- | --- |
| the numbers, machine-readable | `--report PATH` (or the default `_swath_summary.json` sidecar) |
| the numbers on a fast run, or under `-q` | `--stats` |
| nothing at all on stderr | `--no-stats` |

Automation should read `--report`, not scrape the block: it is a stable JSON document, and it
carries strictly more than the human block does.

With `-v`, the same figures are also logged as the `list_run_summary` line — a fuller field dump,
kept because existing tooling scrapes it — carrying total objects, elapsed time, chosen strategy,
API call count (LIST + probes) and estimated cost at $0.005/1000 LIST requests, output file count
with compressed size, and the following efficiency/resource fields:

| Field | Description |
| --- | --- |
| `api_calls_per_1k_objects` | `api_calls × 1000 / objects` — surfaces probe overhead at a glance |
| `peak_rss_bytes` | Peak resident set size (`/proc/self/status` `VmHWM`); `-1` on non-Linux |
| `peak_heap_bytes` | Sum of JVM heap-pool peak usages |
| `cpu_seconds` | Process CPU time delta over the run |
| `cpu_efficiency` | `cpu_seconds / wall_seconds` — mean core utilization |

A separate `list_run_diagnostics` INFO line carries internal counters:
`strategy`, `steal_reasons`, `probe_fetches`, `empty_upper_bisections`,
`splits_committed`, `unsplittable_victims`, `peak_in_flight`, page-shape
fields, and throttle fields. These are diagnostics only, not Micrometer
meters.

### Color

`--color=auto|always|never` (default `auto`) governs swath's two operator surfaces on stderr — the
live progress line and the end-of-run summary block. Both draw from one palette, so a run cannot dim
and accent by different rules in flight than it does at the end — dim
labels/units, one accent for the headline figures (objects/elapsed/rate), red for the `INCOMPLETE`
marker. This is purely cosmetic: as with terminal detection generally (see above), it decides *form*,
never *whether* the block prints.

`auto` colors only when **stderr** is a terminal (stdout's tty-ness, used for `--format auto`, is a
separate question — see [Output](#output)); it also respects the same conventions other CLIs do:

| Signal | Effect |
| --- | --- |
| `NO_COLOR` set to any value | disables color |
| `TERM=dumb` | disables color |
| `CLICOLOR_FORCE` set to any value | forces color even off a terminal (the `gh` convention) |

An explicit `--color=always`/`--color=never` wins over **all** of the above, `NO_COLOR` included —
per [no-color.org](https://no-color.org), a command-line argument overrides the environment variable.
swath does not honor `FORCE_COLOR`; that convention is JS-ecosystem, with no CLI-native precedent.

### Metrics (Micrometer)

The following counters/gauges are maintained:

| Metric | Description |
| --- | --- |
| `swath.api.calls{strategy}` | Total S3 API calls by strategy |
| `swath.api.latency{op}` | S3 API latency (timer) |
| `swath.entries.emitted` | Total entries emitted |
| `swath.bytes.estimated` | Total bytes estimated |
| `swath.workers.active` | AIMD concurrency target `T` (gauge) |
| `swath.steals{result}` | Range-steal attempts by result |
| `swath.errors{type}` | Errors by type |

Three separate timers distinguish network latency, queue-wait (backpressure),
and rate-limit-wait so that GC pauses, throttling, and downstream stalls are
never conflated.

A Prometheus scrape endpoint (`--metrics-port`) is planned for v1.1.

### Logging

SLF4J + Logback; structured fields in `snake_case`. The run's `args_hash`,
strategy, and checkpoint path are logged at startup. Use `-v` / `-vv` / `-vvv`
to raise the log level, or `-q` / `-qq` to lower it (`-q` wins if both are given).

---

## How it works

`swath` uses a **work-stealing parallel scan** (`WorkStealingScan`). A shallow
`delimiter=/` probe discovers the top-level prefix structure and seeds an
initial set of listing ranges. Each range is a half-open key interval
`(A, B]` — the range holder emits every S3 key strictly greater than `A` and
at most `B`. Idle workers steal the upper half of a busy peer's range by
probing a midpoint key and atomically splitting the range at a page boundary;
the split is guarded against races. Managed directory-dataset resume preserves
finalized parts and may re-list only a nondurable tail. Live
concurrency is adjusted adaptively (AIMD: back off on S3 `SlowDown` / 503,
recover by adding one worker per clean 10-second window). The range set always
partitions the full keyspace with no gaps and no overlap during a scan.
Memory is bounded by the configured queue sizes and Parquet writer pool, never
by the number of objects in the bucket.
