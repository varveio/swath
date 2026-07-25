# swath — internals overview

swath is a Java 25 CLI that lists object stores. This page describes the
shape of the system as it runs today: the problem it solves, what it guarantees,
the two laws its engine mechanisms follow, how it parallelizes a listing, and what
it emits. It is the narrative entry point to the internals tier; the precise
specifications live alongside it and govern wherever this page is less exact:

- the listing engine, correctness proof, `byteMidpoint`, extrapolation, AIMD,
  seeding, versioned listing, and Express One Zone — [`algorithms.md`](algorithms.md);
- the component map and how a run flows — [`architecture.md`](architecture.md);
- the build contracts — core types (`KeyBytes`, `ListEntry`, `PageBatch`), the
  SPI, the SQLite and Parquet schemas, resume, `args_hash`, and the per-sink
  delivery guarantees — [`contracts.md`](contracts.md);
- worked engine traces of hard bucket shapes — [`walkthroughs.md`](walkthroughs.md).

The engine is one `WorkStealingScan` over `KeyBytes` ranges on JDK 25.

## The problem

Listing a large S3 bucket is hard for one reason: S3 only lets you page through a
bucket sequentially. One `ListObjectsV2` call returns up to 1000 keys in sorted
order plus a continuation token for the next 1000, and the only way to start
somewhere other than the beginning is `start_after=<key>` — "give me the keys
after this one." There is no "give me keys 50,000–51,000" and no "how many keys
are under this prefix." The key distribution inside a bucket is opaque until you
list some of it.

That makes parallel listing a guessing problem. To hand a second worker a useful
starting point you need a real key from the middle of the bucket, and the only way
to learn one is to list up to it. So swath does not divide a known range into equal
pieces; it **guesses disjoint key ranges blind, starts workers on the guesses, and
corrects them as real keys come back.**

The properties swath targets on supported general-purpose S3 buckets, with no
preconfiguration:

- it handles varied key distributions — no "run once sequentially to make hints,
  then again in parallel";
- it is designed for very large listings without buffering object rows in heap;
  active buffers are configuration-bounded, while finalized-part metadata is
  `O(parts)` and sorted staging metadata is `O(segments)`;
- managed Parquet directory datasets resume after Ctrl+C or a crash with
  exactly-once durable output and bounded tail re-listing; stdout and FILE-kind
  destinations are non-resumable, one-shot output;
- it emits formats downstream tools accept — Parquet for analytics, JSONL for
  streams, TSV for grep, table for humans;
- every transient error retries; every permanent one surfaces with context.

## Scope

swath is **LIST-only**. It exists for buckets where a precomputed listing
(AWS S3 Inventory / S3 Metadata tables) is not a usable option — not enabled, too
stale, or on a bucket you don't own. If a fresh Inventory or Metadata table is
available and queryable, that is strictly cheaper than running swath, and swath is
not the tool for the job. Consequently swath never uses, routes to, ships, or
recommends S3 Inventory / S3 Metadata (or any precomputed-listing service) as part
of its own answer — every shape it handles, including the hardest dense-directory
tails, is solved on live `ListObjectsV2` alone. This is a hard non-goal, not an
open question.

v0.1 is an S3 CLI, not a cloud-agnostic product or supported Java library. Its
internal `StoreCapabilities` and `PageFetcher` seams express a cloud-agnostic
design intent, but they are unsupported and may change without compatibility.
The range engine can apply only to a backend that guarantees one global
lexicographic order and supports a client-chosen exclusive lower bound equivalent
to `StartAfter`; opaque-token-only stores need a different sequential or
prefix-partitioned path. Only the S3 fetcher and direct S3 CLI wiring ship today.

Within S3, v0.1 supports general-purpose buckets. Directory buckets do not
provide the required ordering/`StartAfter` contract and are refused before the
first LIST request; opaque-token sequential support remains planned. This is the
lister itself, not a fleet system; a bucket fleet uses it as its data plane.

## Requirements

### Functional

- List a bucket or prefix to Parquet, JSONL, TSV, or table text.
- Stream output — peak memory ≪ bucket size.
- Recursive (full) listing ships; non-recursive (one delimiter level) mode is
  planned, not yet wired — there is no `--delimiter`/`--recursive` flag, and in
  v1.0 `CommonPrefix` rows have no user-facing path (see [`contracts.md`](contracts.md)
  §1.2).
- Versioned listing (`ListObjectVersions`) — planned, not yet wired (the S3 fetcher
  throws on a versions request; the run is hardwired to `OBJECTS`).
- Diff two buckets/prefixes via a streaming merge-join — planned; the `diff`
  subcommand is a stub today (see the diff feature in [`../../ROADMAP.md`](../../ROADMAP.md)).
- A filter chain: include/exclude regex, size, mtime, storage class, plus a
  sandboxed expression language for combinations (the `--expr` expression language
  is deferred to v1.1 — see Filters below).
- One engine always (`WorkStealingScan`) — no user-facing strategy flag to tune
  and no per-bucket strategy router.
- Pause (Ctrl+C → graceful), resume (continue at next invocation), restart (force
  fresh).

### Operational

- Single CLI binary: an **uber-jar** from one Gradle build (the `application` plus
  Shadow plugins); a `jlink` runtime image is planned, not yet wired.
- **JDK 25 LTS** runtime, compiled `--release 25`, **no preview APIs** in any
  shipped artifact — `ScopedValue` and non-pinning `synchronized` are final in 25,
  and the uber-jar must run without `--enable-preview`.
- Concurrency (`--concurrency`) and rate limit (`--request-rate`) are configurable
  via **CLI flags** (not env vars); retry policy is internal, watchdog-derived
  state, not a user knob. The only `SWATH_*` env configuration is the OTLP
  endpoint/interval (see the metrics bullet below).
- Structured logging (SLF4J + Logback) at INFO/DEBUG/TRACE.
- Optional OTLP metrics export (`--metrics-endpoint`, with `--no-metrics` as the
  explicit egress kill-switch; also driven by `SWATH_OTLP_ENDPOINT` and cadence
  from `SWATH_OTLP_INTERVAL`). A Prometheus scrape endpoint is planned, not yet
  wired (Micrometer is the facade, but only the OTLP registry ships).
- Progress reporting: a 30-s **INFO** progress line (rate, ETA, oldest pending
  range) ships for every run; a planned JLine TTY display (live console redraw) is
  not yet wired — on a TTY the same 30-s INFO path is used today.
- Exit codes: 0 success, 1 listing/output/checkpoint error, 2 config error/refusal,
  124 stopped by `--max-duration`, 130 SIGINT, 143 SIGTERM, plus exit 75
  `STUCK`/`EX_TEMPFAIL` for liveness failures. A partial is resumable only when
  the run has managed directory-dataset state — see [`contracts.md`](contracts.md) §5.

### Non-functional

- **Throughput target (unverified):** within ~10% of `s3-fast-list` at the same
  concurrency. No release-candidate measurement currently establishes this;
  see [`../performance.md`](../performance.md).
- **Active buffers are configuration-bounded** (invariant I11): queues, writer
  row groups, and merge buffers do not accumulate object rows for the whole
  listing. Whole-process memory is not N-independent, however: finalized-part
  metadata is `O(parts)` and sorted staging metadata is `O(segments)`. The
  current public Parquet heap gate covers 100,000 keys, not larger-scale runs.
  Sizing and evidence limits live in [`contracts.md`](contracts.md) §4/§7.
- **Crash anywhere → honest, per-destination guarantees.** Stdout and FILE-kind
  output are non-resumable; commit-before-emit means interrupted stdout/text may
  omit an in-flight page. Managed directory-dataset Parquet is exactly-once
  durable output via the `durable_cursor` model: finalized parts are retained and
  only the nondurable tail is re-listed. See [`contracts.md`](contracts.md) §5.

## Design laws

Two laws govern how new engine mechanisms are designed. They are not aesthetic
preferences — they encode where estimation goes wrong on real buckets, so a mechanism
that violates either tends to cost more than it saves. A short vocabulary first: a bucket's
**mass** is where its keys actually are in the keyspace (real S3 buckets are wildly
non-uniform — keys cluster in a thin slice of the possible byte values); a **pivot**
is a boundary key at which swath splits one worker's range into two so a second
worker can help; and an **estimator** is any calculation of *where* to place that
pivot or *how much work* a range still holds. The engine chooses pivots on live
buckets whose shape it cannot see in advance, so how it estimates is the whole game.

**L1 — Placement: condition estimates on observed mass; uniform priors are
bootstrap-only.** Pivot placement and remaining-size estimation prefer estimators
built from what the run has actually seen — the density at which keys have drained
so far (reflected forward to guess where the *next* equal share of keys lies), the
per-position alphabet observed in returned keys, the real keys on pages already
fetched, and the directory boundaries a `delimiter=/` probe returns. A uniform prior
over raw byte/code-point space — "assume keys are spread evenly, so the pivot is the
arithmetic midpoint" — is legitimate *only* to bootstrap the very first cut before
any keys have been observed. Beyond that it systematically aims pivots into empty
space, because real mass sits in a thin low sliver of the byte range.

- *Why it holds — the failure mode.* On mass-skewed buckets a uniform-prior pivot lands
  in empty byte-space above the real keys, so the engine has to walk it back toward the
  cursor (the `PIVOT.step_back` path). Even the splits that then commit move almost
  nothing: the pivot is byte-valid but sits just above the cursor, so the "child" range
  inherits a single page of keys instead of half the work. That is a property of
  mass-blind placement on skewed buckets, not a quirk of one bucket.
- *Why it holds — the fix.* Every estimator conditioned on observed mass places well:
  open-frontier density extrapolation (`extrapolate` / `forwardReflect`), the flat-leaf
  density pivot that divides a single dense directory, and the `delimiter=/` structure
  probes that align cuts to real directory boundaries. Every uniform-prior mechanism
  over-fires on skew — the empty-upper bisection storms, the `step_back` retries above,
  and owner-split "confetti" (many thin one-page children).
- *How to apply when adding a mechanism.* Before you compute a pivot or a size, ask
  which rung of the evidence ladder you are standing on — real observed keys >
  `CommonPrefix` boundaries > drain-density reflection > uniform interpolation. Start
  as high as the data you already hold allows, and treat a uniform-prior calculation
  as a fallback you must *justify*, not the default.

**L2 — Triggers: mechanisms may be global and shape-blind, but their triggers must
be local and demand-driven.** The *mechanism* — how a split is performed — can be one
shape-agnostic primitive applied everywhere (swath has exactly one: the CAS-guarded
split transaction). What must be local is the *trigger* — the decision of *when and
where* to fire it. A trigger keyed on a per-range or per-victim signal, or on genuine
unmet demand, adapts to a mixed bucket; a trigger keyed on a global failure or pacing
signal punishes the whole run for one bad region and starves the parts that were
doing fine.

- *Why it holds.* Firing an owner-side split eagerly on every draining page sheds thin
  far-slices and starves idle thieves. Gating that identical split on real demand — fire
  only when live work (queued plus active nodes) has fallen below the fixed
  worker count (`outstanding < workerCount`) —
  cuts over-fetch and lifts throughput on a skewed bucket. A *global* futility pacing
  signal (slow every steal down when the run as a whole looks unproductive) starves
  mixed buckets, where most regions are healthy while one bad region sets the pace; the
  *per-victim* progress gate (`markStolen`, which only makes the one worker just carved
  briefly ineligible) is the storm-stopper that ships. The nuance that keeps this from
  being "never gate globally": a gate keyed on a genuinely global *demand* quantity —
  live-node count versus the fixed worker count — is fine, because demand is a property of
  the whole worklist by definition. The adaptive effective target is diagnostic and does
  not drive this gate. It is global *failure/pacing* signals that starve.
- *How to apply when adding a mechanism.* Keep the mechanism uniform, but ask what
  your trigger reads. If it reads a per-range or per-victim fact, or an unmet-demand
  count, it is on solid ground. If it reads a run-wide failure rate or slows everything
  on one region's behalf, expect it to regress the mixed buckets — split the trigger so
  each region pays only for its own shape.

*(The per-mechanism detail lives in [`algorithms.md`](algorithms.md) §3.3.)*

## How swath parallelizes — the seed + steal hybrid

This section explains, in plain terms, why swath lists a bucket fast and how it
splits the work. [`algorithms.md`](algorithms.md) has the full mechanism and the
correctness proof; [`walkthroughs.md`](walkthroughs.md) traces five hard bucket shapes
end-to-end (deep tree, dense flat tail, skewed mass, saturated wide, crash/resume) —
the actual engine event sequence, the counters that fire, and the invariants that
carry each one.

### Why parallel listing is hard, in plain terms

The Problem section above defines the sequential pagination constraint and the
blind-range-guessing model. This section starts with the mechanisms that make and
correct those guesses.

**Which algorithm kicks in for which bucket shape** (brief; see the sections below for
detail):
1. A shallow `delimiter=/` **seed** at the start, if the bucket has directory-like
   structure — S3 hands you the top-level prefixes for one API call, so tree-shaped
   buckets parallelize from page 1.
2. **Work-stealing byte-midpoint splits** correct imbalance on the fly as workers
   finish at different rates.
3. A **structure probe** kicks in when a byte-midpoint guess lands in an empty gap (a
   clustered/deep common prefix with no keys nearby).
4. **Far-ahead / owner-side split placement** — when a directory is dense and
   fast-draining enough that even a valid split pivot can get overrun before it
   commits, the range owner places the pivot far ahead of the current cursor and
   commits the split at page-commit time, gated to fire only while there is idle
   capacity (live queued-plus-active work below the fixed worker count). This ships **default on**;
   `--engine-toggle owner_split=off` is a diagnostic opt-out, not a production fallback.
5. **Pivot placement for the residual dense tail.** Under fast, low-latency stealing, a
   naive pivot can land essentially one page ahead of the draining cursor rather than
   near the range's real midpoint, so the worker advances past it before the split
   commits and the steal is lost. Placement for this residual dense tail is still an
   active area of work; the shipped mitigations — far-ahead / owner-side placement and
   opt-in speculative readahead — are covered under "Current limit" below.

### The constraint

S3's `ListObjectsV2` is **sequential within a key range**: to fetch page N you need
the last key of page N-1 (`start-after` pagination). So a single range is listed one
page at a time. The only way to go faster than `aws s3 ls` is to **split the keyspace
into sub-ranges and list them in parallel** — each worker paginating its own range from
its own `start-after`.

The hard part is choosing *where* to split, because you don't know the key distribution
up front.

### Two ways to split — and why neither alone is enough

**Work-stealing by byte-midpoint (dynamic, but distribution-blind).** Idle workers steal
from busy ones: a thief splits a victim's range `(cursor, hi]` at a pivot
`m = byteMidpoint(cursor, hi)` — the midpoint over **byte/code-point value space**. The
thief takes `(m, hi]`, the victim keeps `(cursor, m]`; both paginate in parallel.

This works beautifully when keys are spread across the byte space — **fan-out** buckets
(uuids, hashes, many top-level prefixes). The midpoint lands among real keys → balanced
halves → high parallelism. A large fan-out bucket reaches many-way parallelism from
stealing alone.

But it's **blind to the key distribution**: `byteMidpoint` is the *median key* only if
keys are uniform. On a **clustered / deep-nested** bucket — all keys sharing a long
common prefix (`dataset/2020…/Path1/…`) — the keys occupy a tiny band while `hi` is far
away, so the midpoint lands in **empty byte-space**. The retry-toward-the-cursor produces
lopsided slivers, not halves, and pinning down the real median would take many single-key
probes (an API-call storm). Net: it can't divide the work → **near-serial regardless of
size** — a deeply clustered bucket lists essentially one-way no matter how many objects
it holds.

**`delimiter=/` seeding (structural — the key distribution, for free).** S3 will hand you
the structure for free: one `delimiter=/` call returns the **common prefixes** (the
bucket's "directories"). Those are exactly the natural split boundaries — and they're
**populated** boundaries, roughly balanced by the bucket's own layout. So instead of
*guessing* split points by value (and missing on clustered data), the **seed asks S3 where
the keys are** and lays down ranges aligned to the directory structure.

Effect on a deep-nested bucket: the seed discovers hundreds of populated boundaries and
lifts parallelism from a handful of workers to the full pool, for a large speedup — with
byte-exact output.

### The hybrid: seed up front, steal during

Neither is sufficient alone:

- **Seeding alone** can't rebalance dynamically (one directory may hold most of the
  objects, or be internally skewed) and is a one-shot probe with a bounded budget.
- **Stealing alone** is distribution-blind (the failure above).

So swath does **both**: the seed gives cheap **structural** parallelism at the start;
work-stealing then provides **dynamic** rebalancing throughout the run (redistributing
skew within and across seed ranges). `algorithms.md` §8 calls it "Seeding (the HYBRID)."

### When the seed kicks in

- **Once, up front, on a *fresh* run** — never on `swath resume` (that reloads the
  existing partition).
- Default **`--tune seed.mode=shallow`**: one `delimiter=/` probe at the listing prefix;
  if common prefixes exist (most buckets), seed a range per prefix, adaptively descending
  *narrow* sub-levels within a bounded probe budget. Cost is ~1 RPC per probe — it never
  walks per-directory, so even a 1-object-per-leaf explosion costs one
  probe + a flat scan, not a tree walk.
- **Flat bucket / no common prefixes** — a *non-truncated* probe (a small flat bucket that
  fits one page) falls back to a single root range → pure byte-midpoint (fine for
  flat/uniform keys). A *truncated* no-prefix probe with direct objects is a dense flat
  root: it is classified a `flatWideRegion` and **radix-banded** at seed time by default
  (leading-byte cut-points), not left as one root range (`algorithms.md` §8).
- **`--tune seed.mode=none`** = the old single-root behavior (stealing only).

Work-stealing then runs continuously for the rest of the listing.

### Structure discovery during stealing (shipped)

The seed discovers structure only at the start and only to its probe-budget depth. A seed
range that is *itself* an internally-deep **funnel** (e.g. `YYYY/MM/<uuid>/…` that funnels
into one big day before exploding) would revert to plain byte-midpoint inside that range if
stealing were static. The fix is **demand-driven `delimiter=/` discovery during stealing**
(shipped): when a thief's byte-midpoint empty-uppers on a **bounded** range, it probes
`delimiter=/` at that range's frontier and splits at the median discovered `CommonPrefix` —
making structure discovery recursive at *any* depth, not just at seed time.

### Current limit

The original serial-tail gap was root-caused and the structural part fixed: the cause was a
**degenerate parent-empty split pivot** — a split whose parent range was empty, so its child
inherited the whole tail by baton-passing — and it's now detected and routed through the
structure probe / density extrapolation instead.

The **remaining** residual is a *race*, not an inherent limit: late reactive stealing — a
thief probing near the drainer's own cursor and placing a single-page pivot — loses to a
drainer advancing a full page (~1000 keys) per round-trip, so the pivot gets passed before
the split commits. That does **not** prove LIST can't partition a dense directory; it proves
*that particular* pivot-placement timing loses the race.

Far-ahead / owner-side split placement (above) structurally removes that overrun and cuts
API cost, and ships default on with a demand gate so it only fires while there is idle
capacity. Further shrinking the residual is open work — intra-range speculative readahead
(implemented, default off) is the current strongest candidate.

**Where we genuinely cannot beat serial.** Inside *one* directory — a single flat prefix
with no sub-structure — the only tool S3 gives you is `start_after` pagination: one network
round-trip per ~1000 keys, strictly sequential. A directory with 110k keys is ~110 sequential
round-trips, and no number of extra workers helps, because handing a worker a valid starting
point deep inside that directory requires already knowing a key that far in — which costs
exactly the listing you were trying to parallelize. You can sometimes split a flat directory
by *guessing* name-prefix bands (e.g. hex uuid `0`–`f`) when the names are uniformly
distributed; when they're not (or the split can't be committed faster than the drainer
advances past it), exact `start_after` pagination of a single dense directory hits a
sequential LIST floor. Speculative readahead — issuing guessed `start_after` cursors ahead of
the drainer and reconciling them (implemented, opt-in) — can pipeline that chain where interior
keys are predictable enough to guess; on a genuinely opaque directory it cannot, so the
sequential floor remains the honest worst case. **This is a single-directory floor that bounds
only that directory's tail — a bucket with many such directories still parallelizes fine across
them** (only the last, densest directory serializes at the very end of the run). And it is
**not** "solved" by reading a precomputed inventory: swath is LIST-only by design (see the Scope
section above) — it exists specifically for buckets that can't use S3 Inventory / S3 Metadata,
so every shape, including this one, has to be solved on live LIST alone.

## Output

Four formats from one binary, via a sealed `EntryFormatter` ([`contracts.md`](contracts.md) §2):

- **Parquet** — streaming row-group writes via parquet-mr to a canonical superset schema
  (byte-exact `key` as `BINARY`; ETag quotes stripped, multipart `hex-N` kept verbatim, never
  panic-parsed). 2–4 sticky writers, size-rotated parts, swath's own `manifest.json`. Schema and
  writer settings are pinned in [`contracts.md`](contracts.md) §4.
- **JSONL** — one object per line (no enclosing array, so `tail -f`/`jq` work); Jackson escapes
  control chars per spec.
- **TSV** and **table** — human/grep formats.

**Control-character escaping is always on for text sinks.** S3 keys can contain newlines, tabs,
and ANSI escapes; TSV/table replace `\x00..\x1f` and `\x7f` with `\xNN` hex. JSONL is the lossless
text choice; JSON (Jackson) and Parquet (binary) are inherently safe.

**Shipped delivery and resume guarantees — state them exactly as
[`contracts.md`](contracts.md) §5, never more strongly:**

| Destination | Guarantee |
| --- | --- |
| **stdout / FILE-kind text** | Non-resumable, one-shot. Commit-before-emit can leave rows absent after interruption (at-most-once). |
| **FILE-kind Parquet** | Non-resumable; FILE kind requires `--checkpoint none`. |
| **managed directory-dataset Parquet** | **Exactly-once durable dataset** via `durable_cursor`: retain finalized parts, discard an unfinalized tail, and re-list only that bounded tail on `swath resume <dir>`. |

The deferred `--resume-output` journal is not a shipped option and does not make
stdout or FILE-kind destinations resumable today.

The checkpoint/resume internals behind this (two-cursor model, `args_hash` resume gate, sort+resume
constraints) are in [`contracts.md`](contracts.md) §4–§5 and [`algorithms.md`](algorithms.md) §4.
`--checkpoint none` runs an in-memory worklist with no resume.

## Filters

A chain of sealed `Filter` predicates evaluated in order, fail-fast: include/exclude **regex**,
**size**, **mtime**, **storage class**, and an **`--expr` JEXL** expression for combinations (e.g.
`entry.size > 1mb && entry.lastModified > date("2026-01-01")`). *(The `--expr` JEXL filter is
**deferred to v1.1**; the regex/size/mtime/storage-class filters ship in v1.0 and `ExpressionFilter`
stays as a dormant seam.)* The JEXL engine is sandboxed via custom `JexlPermissions` (no reflection,
I/O, or `System` access), compiled once at startup with AST validation rejecting unknown
variables/properties before any object is evaluated; a small set of helpers (`mb`, `gb`, `kb`,
`date`, `regex`) is registered. Filters are **not** part of `args_hash`, so the optional
`--resume-output` journal *(v1.1)* (which stores raw, pre-filter entries) lets a filter change be
re-applied on replay — see [`contracts.md`](contracts.md) §5. In v1.0 a filter/format change on
resume is refused (use `--restart`).

## Acknowledgements and prior art

swath is a clean-room synthesis. Concept-by-concept attribution:

- **[`s3ls-rs`](https://github.com/nidor1998/s3ls-rs)** (Apache-2.0, `nidor1998` and contributors) —
  pipeline shape, the `PageFetcher` trait abstraction, recursive `delimiter=/` discovery (now the
  engine's seed step), the stuck-token defence, control-character escaping, the cancellation token
  wired through every stage, broken-pipe handling, and the sealed-error-to-exit-code mapping. swath's
  architectural reference.

- **`s3-fast-list`** (MIT-0, AWS Samples, daiyy@amazon.com) — pre-segmented range partitioning (a
  planned alternate `--tune seed.mode=hints`, not yet wired), the proven `start_after` + client-side
  end-key stop primitive at the heart of the worker loop, Parquet as a first-class output, the
  bidirectional diff concept, the sandboxed expression-filter idea (Rhai → JEXL), and the
  self-bootstrapping `.ks` output.

- **`S3P`** (MIT, Shane Brinkman-Davis Delamore) — the adaptive-bisection idea: subdivide an unknown
  keyspace using `start_after` and observed page boundaries. swath keeps the zero-config property but
  replaces overlap-as-discovery with demand-driven range stealing and 1-key probes — no ~2× API bill,
  no dedup pass, no double-emit.
  <https://shanebdavis.medium.com/s3p-massively-parallel-s3-copying-9a9e466d0d74>

- **`PS3`** (jboothomas, MIT) — brute-force character-by-character expansion, echoed in the
  density-extrapolation seed for the single open-ended frontier range.
  <https://jboothomas.medium.com/fast-listing-s3-objects-from-buckets-with-millions-billions-of-items-380052fb6faf>

- **Pure Storage / Joshua Robinson** — the alphabet-partitioning benchmark (67B objects, 430K
  keys/sec) and the Python→Go speedup data point that grounds the language-choice conversation.
  <https://blog.everpuredata.com/purely-technical/listing-67-billion-objects-in-1-bucket/>

### Legal note

Architectural patterns and algorithms aren't copyrightable; swath is a clean-room synthesis in Java
that copies no code from the above. None of the source licenses (Apache-2.0, MIT-0, MIT) compel
attribution for code swath doesn't ship — these acknowledgements are professional courtesy, not legal
requirement. If during implementation a concrete snippet, comment, or test is borrowed verbatim, add
an inline source comment and (if Apache-2.0) a NOTICE entry. The default expectation is that no code is
copied.
