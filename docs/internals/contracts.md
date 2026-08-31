# Contracts & data model

Authoritative contract registry. Every section here is a contract the
implementation builds to; where prose elsewhere in the design docs disagrees,
this document wins. Algorithm details are in [`algorithms.md`](algorithms.md).

---

## 0. Load-bearing invariants

Each row is a named invariant the whole system upholds. They are stated here on
their own terms — every downstream contract in this document exists to keep one
of them true.

| # | Invariant |
| --- | --- |
| I1 | **Commit-before-emit:** a page's checkpoint commits before its entries are pushed downstream. |
| I2 | **Keyspace partition:** the range set always partitions the keyspace — pairwise-disjoint, no gaps. |
| I3 | **Boundary belongs to the left:** the boundary key belongs to the LEFT interval (`(A,B]`: emit `k<=B`; right worker `start_after=B`). |
| I4 | **Atomic split:** a split is its **own** atomic operation at a page boundary, against the already-committed cursor, that rejects a stale-snapshot second-thief attempt — serialized through the checkpoint-writer. (Guarded by `(cursor IS NULL OR cursor < pivot) AND range_end IS oldHi AND status<>COMPLETED` — `cursor IS NULL` keeps a fresh root splittable. `--checkpoint none` runs the identical guarded SQL against an ephemeral in-memory store.) |
| I5 | **Resume preserves cursor:** `IN_PROGRESS → PENDING` on resume preserves `cursor` (resume mid-range, not from `range_start`) — **except for Parquet (exactly-once)**, where the cursor is instead reset to `COALESCE(durable_cursor, range_start)` so only the not-yet-durable tail re-lists, and COMPLETED-but-not-output-complete nodes reopen (see `loadResumable`, §2, and §4.1). |
| I6 | **Durability ⇔ finalized:** a part file's rows are durable **iff** it is `finalized` (footer fsynced); a node is **output-complete** iff `COMPLETED` and `durable_cursor == cursor` (all its pages in finalized parts). |
| I7 | **No permit held across child work:** a worker permit / slot is never held while waiting on child work (worklist ⇒ no deadlock class). |
| I8 | **Shutdown order:** drop the downstream receiver before joining the producer; `shutdownNow()` before `close()`. |
| I9 | **No infinite loop:** a stuck token / truncated-without-token result is an error, never an infinite loop. |
| I10 | **Byte-exact keys:** keys are byte-exact end-to-end (compare/split/journal/Parquet); never `String.compareTo`. |
| I11 | **Bounded active buffers, explicit metadata growth:** active row/page/writer/merge buffers are functions of configured knobs, not object count; finalized-part metadata is `O(parts)` and sorted staging metadata is `O(segments)`. |
| I12 | **Safe split pivots:** every split pivot `m` is valid UTF-8 with `a <_u m <_u b` and **synthesizes no code point in `E = {U+0000..U+001F} ∪ {U+007F} ∪ {U+0080..U+009F} ∪ {U+FDD0..U+FDEF} ∪ {every plane's trailing pair xFFFE/xFFFF} ∪ {U+0025}`** (no surrogate); `byteMidpoint`/`forwardReflect` return `null` iff no such safe `m` exists strictly between. The C1 control block and every supplementary noncharacter (not just the BMP pair) belong to `E`: a synthesized `start-after` cursor landing on either can 400 on real S3, even though both are XML-1.0-legal. `U+0025` (`%`) is in `E` for a **different** reason: it is XML-1.0-legal and real S3 never 400s on it, but a lone/trailing `%` echoed verbatim by a nonconformant endpoint such as the tested LocalStack build (not re-percent-encoded, unlike real S3 and the tested MinIO build) crashes the AWS SDK's own response-unmarshalling `URLDecoder` — see [`s3-implementation-compatibility.md`](s3-implementation-compatibility.md). The synthesized divergence/append character is drawn from the safe set, so for buckets whose keys are themselves XML-safe (the overwhelming common case) every pivot is a valid S3 `start-after`. swath does not, and cannot via pivot construction, sanitize bytes copied from the bounds. Bounds may carry unsafe code points (the asymmetry: full-space interpretation, safe-set synthesis); `start-after` transport/encoding is unchanged. |

> **On I4 "own atomic operation" and writer batching.** The guarantee is
> *logical*: a split is a standalone, CAS-guarded `UPDATE`+`INSERT`, never folded
> into a page commit's cursor advance. The single checkpoint-writer may
> *physically* group several queued operations into one SQLite transaction for
> throughput (the writer protocol in [`algorithms.md`](algorithms.md) §4.1), but
> that batching preserves arrival order and only makes the split's two statements
> land together — it never merges a split's logic into a page commit. So the split
> stays its own atomic operation.

---

## 1. Core types

### 1.1 `KeyBytes` — **keys are bytes, not `String`**

```java
public final class KeyBytes implements Comparable<KeyBytes> {
    private final byte[] raw;            // key exactly as S3 returned it, AFTER url-decode
    public static KeyBytes of(byte[] raw);
    public static int compareUnsigned(byte[] a, byte[] b);   // S3's order
    @Override public int compareTo(KeyBytes o);              // delegates to compareUnsigned
    public byte[] raw();                 // public boundary copy
    public byte[] rawUnsafe();           // internal no-copy hot path; treat as immutable
    public String asString();            // lazy UTF-8 decode; output/filter boundary ONLY
    public int length();                 // raw.length (≤ 1024 for S3)
}
```

- **Never** compare keys via `String.compareTo` (UTF-16 ≠ S3 UTF-8 byte order for
  supplementary code points ≥ U+10000; see [`algorithms.md`](algorithms.md) §1.1
  for exactly where the two orders diverge). All ordering/midpoint/stop-checks use
  `compareUnsigned`.
- The canonical key is `byte[]` through the engine, checkpoint, journal, and
  Parquet (binary column). Public `raw()` returns a defensive copy; internal
  engine/checkpoint/output code uses the no-copy accessor by convention.
  `asString()` is used only by text formatters and the JEXL/regex filter view.

### 1.2 `ListEntry` (sealed) and row emission

```java
public sealed interface ListEntry permits ObjectEntry, CommonPrefixEntry, DeleteMarkerEntry {
    KeyBytes key();
}
public final class ObjectEntry implements ListEntry {
    // Accessors: key, size, lastModifiedText, lastModifiedEpochMicros, etag,
    // storageClass, versionId, isLatest, ownerId, ownerDisplayName,
    // checksumAlgorithm, checksumType
}
public record CommonPrefixEntry(KeyBytes key) implements ListEntry {}   // key = the prefix
public record DeleteMarkerEntry(KeyBytes key, String versionId, boolean isLatest,
                                String lastModifiedText, String ownerId) implements ListEntry {}
```

`lastModifiedText` preserves the object store's XML text as the primary representation. Direct,
unsorted text sinks write that value without parsing or canonicalizing it. For entries constructed
from source text, `ObjectEntry` parses the value on the first typed access and caches the
epoch-microsecond result; an invalid value remains attributed to its entry and fails on that access.
Typed consumers are the mtime filter, the current Parquet timestamp writer, and sorted spill
encoding. The current sorted spill format therefore canonicalizes timestamp text;
the canonical UTC form emitted by S3 and reconstructed by sorted spill uses a byte-exact arithmetic
parse/format path, while every alternate or unusual accepted form retains the general formatter
fallback. This is an implementation optimization, not a grammar or representation change.
Preserving it through sorted output requires a separately versioned spill change. Entries supplied
by typed stores/fixtures retain the compatibility
constructor that starts from epoch microseconds and seeds the cache directly. Thus an ordinary
unsorted TSV/JSONL listing avoids the timestamp parse-and-format round trip, while the shipped Parquet
schema below remains unchanged until the separately benchmarked string-schema decision is made. If
a typed consumer cannot parse the source text, the run fails through its listing/output error path;
an affected dataset part or sorted segment is not published.

Emission rules:
- **Objects** are always emitted.
- **`CommonPrefixEntry`** is emitted **only in non-recursive (`delimiter`)
  mode** (the user asked for a folder listing). The recursive default never
  emits common prefixes as rows — they are an internal seed artifact.
- **`DeleteMarkerEntry`** is emitted **only with `--all-versions`**, unless
  `--no-delete-markers`.

> **Planned / not yet wired (v1.0):** the three `ListEntry` variants and their
> emission rules are the shipped model, but the CLI paths that would surface the
> non-object rows are not built yet. Versioned listing (`VERSIONS` mode,
> `--all-versions`/`--no-delete-markers`, and therefore `DeleteMarkerEntry`
> emission) is **planned** — `S3PageFetcher` throws for a `VERSIONS` request and
> `StoreCapabilities.supportsVersions` is `false`. There is likewise no
> user-facing non-recursive/`--delimiter` folder-listing flag, so in v1.0
> `CommonPrefixEntry` is only ever an internal seed artifact and never an emitted
> row. In practice v1.0 emits `ObjectEntry` rows only.

### 1.3 `PageBatch` and the channel envelope — **page-granular pipeline**

The pipeline passes **batches**, not single entries (reduces queue
contention and per-object overhead). A batch is **dual-form**: it carries
**exactly one of** `entries` (a raw `ListEntry` list — the non-`--sort`
text/parquet-direct pipelines) **or** `packed` (a `PackedPage` the fetch worker
already packed — `--sort` mode, so the channel and the sort drain hold a compact
packed page instead of parsed entry objects, and packing runs on the fetch
worker, not the single drain thread). The canonical constructor **rejects** a
batch that has neither or both non-null; `isPacked()` distinguishes them and
`entryCount()` is its logical row count. A terminal batch carries `nodeCompleted=true` so the
assertion-only sort tripwire can release per-node state. When filtering removes every terminal
row, the worker emits a zero-row completion batch; `channelWeight()` prices that control batch as
one unit so completion signals remain bounded by the same channel gate (I11). Channel close/failure is carried
by a sealed envelope, never by polluting `ListEntry`:

```java
public record PageBatch(long nodeId, long pageSeq, List<ListEntry> entries,
                        PackedPage packed, boolean nodeCompleted) {}
// exactly-one-of(entries, packed) enforced by the canonical constructor.
// Convenience: 3-arg PageBatch(nodeId, pageSeq, entries) (packed=null) and
// static PageBatch.ofPacked(nodeId, pageSeq, packed) (entries=null).

// PackedPage (swath-model seam): the drain-thread view of a packed page — entryCount,
// objectCount, commonPrefixCount, deleteMarkerCount, totalObjectSize, firstKey, lastKey — without decoding
// the payload; the sort package downcasts to recover the concrete packed block.
public interface PackedPage {
    long entryCount(); long objectCount(); long commonPrefixCount();
    long deleteMarkerCount(); long totalObjectSize();
    byte[] firstKey(); byte[] lastKey();
}

public sealed interface Msg<T> permits Item, End, Failure {}
public record Item<T>(T value) implements Msg<T> {}
public record End<T>() implements Msg<T> {}
public record Failure<T>(Throwable cause) implements Msg<T> {}
```

- **Exactly one producer per channel.** The listing engine fans out
  internally (N workers) but a single drain emits exactly one `End` after
  the internal quiescence join. If anyone later wires N workers directly to
  one channel, `End` must become a join/`Failure`+count — call this out.

---

## 2. Interfaces

These are internal seams used by the CLI, tests, simulator, and replay tooling. They are
not a supported Java API or third-party SPI; v0.x does not promise source or binary
compatibility.

The CLI is swath's only supported public API before 1.0. Public Java types exist where the
modules need them, but they remain internal implementation seams rather than an embedding API. In
particular, `SortConfig` is an immutable CLI-configuration snapshot; its former record/canonical
constructor was internal and unsupported, and its flat accessors and copy methods do not establish
a generic Java configuration API.

### Store and page model

`PageFetcher.fetchPage(PageRequest)` is the engine's only listing call. A request carries
listing mode, maximum page size, byte prefix/delimiter/lower and upper bounds, any
protocol marker, and an attempt-timeout escalation level. A `ListPage` returns entries,
common prefixes, truncation/marker state, status, and observed latency.

`StoreCapabilities` declares start-key, range-bound, delimiter, version, lexical-order,
page-size, and pagination capabilities. The shipped S3 path consumes `maxKeysCap`; no
capability router or general `Store`/`Strategy` abstraction ships. S3 object listing uses
byte keys and KEY pagination. Other stores remain design intent and must satisfy the
range engine's global-order and exclusive-lower-bound requirements before reuse.

The attempt-timeout escalation value is a level, not a duration. Retry policy decides when
to climb; the store maps a level to a call-class-specific budget. S3 doubles the base per
rung (worker scans 10/20/40 seconds, point probes 3/6/12 seconds), so escalation cannot
shrink a timeout.

### Checkpoint store

`CheckpointStore` owns run identity, the durable worklist, output-part state, and sorted
phase. Its behavioral contract is:

| Operation | Required effect |
| --- | --- |
| `openRun` | Create or select a run by `args_hash`; enforce resume/restart exclusion. |
| `insertNodes` | Insert the whole seed set atomically (I2). |
| `loadResumable` | Reopen incomplete nodes; for managed Parquet, reset to durable cursors and reopen output-incomplete nodes (I5–I6). |
| `commitPage` / `commitPageAsync` | Advance cursor and status in one transaction; caller awaits acknowledgement before emit (I1). |
| `splitNode` | Narrow the parent and insert the child in one CAS-guarded transaction (I4); return `SPLIT_ABORTED` on a stale view. |
| `partFinalized` | Record a footer-fsynced part and advance covered durable cursors atomically (I6). |
| `markOutputComplete` | Latch completed nodes only after every writer has finalized. |
| sorted-phase methods | Persist `LISTING`, `MERGING`, or `PUBLISHED` for crash recovery. |
| terminal methods | Record completion/failure without overwriting a stronger terminal fact. |

All SQLite writes funnel through one checkpoint-writer thread. The null-safe split guard is
conceptually:

```sql
(cursor IS NULL OR cursor < :pivot)
AND range_end IS :old_hi
AND status <> 'COMPLETED'
```

The parent bound is restored in memory when the transaction reports no changed row.

`Filter` and `EntryFormatter` are sealed internal families. The current filters cover key
regex, size, modification time, and storage class; the current formatters cover JSONL, TSV,
aligned table, and Parquet. The diagnostic discard sink deliberately bypasses the formatter
family: its consumer drains raw `PageBatch` values and retains the standard row tally/emission
metrics without constructing a writer. `Scope` is the repository's virtual-thread lifecycle helper and
provides fork, coordinated cancellation, join, and close without preview APIs.

### 2.1 The policy/executor split

`io.varve.swath.engine.policy` decides; `io.varve.swath.engine` observes live state,
performs I/O and mutation, and records outcomes. The simulator drives the same policy
interfaces in virtual time.

The normative rules are:

1. **Source-agnostic values.** Policy views, decisions, mutations, and probe outcomes carry
   byte keys, counts, booleans, and policy-domain enums. They never carry `WorkerState`,
   `ListPage`, an SDK/protocol type, or another live executor object.
2. **Deterministic decisions.** A policy decision is a function of its explicit view and
   inputs. Policy types do not hold metrics/traces, mutate atomic state, or read ambient
   clocks/randomness. Time and randomness arrive as values or injected interfaces.
3. **Executor-owned effects.** Only executors acquire locks, issue store calls, update
   `WorkerState`, perform checkpoint CAS operations, or emit metrics/traces.
4. **Exactly-once engagements.** Decisions return bounded reason enums/`Engagement` values.
   The executor drains each returned action once and records each engagement once. A
   disabled or newly selected algorithm path must still leave an explicit mark.
5. **CAS remains the safety boundary.** Views are observations, not locks. A decision made
   from a stale view may waste a probe or lose a split race; the guarded transaction must
   still prevent a gap, overlap, or duplicate child.

The policy families have three shapes:

| Shape | View and execution model |
| --- | --- |
| Thief | `VictimView` snapshots candidates; `StealAttemptView` freezes the chosen range and observations. `ThiefPolicy` may request bounded probes before returning mutations and a split proposal. |
| Owner split | `OwnerSplitView` is captured under the owner's lock after a page commit. `OwnerSplitGovernor` is zero-probe and returns a gate/carve decision. |
| Seed | `HybridSeedPlanner` owns its private frontier, cut set, and budget before workers start. It therefore needs no live-state view; `SeedStep` translates each page into a source-agnostic `SeedProbeOutcome`. |

`AlphabetDigest` crosses the seam only as an immutable snapshot. Per-victim futility
counters remain independent atomic read-modify-write values; they must not be collapsed
into a combined write-back. Fleet-wide idle pacing may use one immutable state record
because every transition remains inside `IdleStealBackoff`'s synchronization. Production
AIMD still uses `ConcurrencyGauge`; `ConcurrencyPolicy` is a simulator port, not evidence
that the production controller has been extracted or proved equivalent under concurrency.

Some unlocked observations can change between snapshot and effect. The accepted
consequence is limited to a missed/wasted heuristic probe or a CAS-aborted split. Any
change that can affect range ownership, cursor order, output durability, or termination is
not covered by that relaxation and requires the corresponding invariant proof and test.

`DecisionPathPurityTest` mechanically checks the policy package and field-reachable project
types for held `RunMetrics`/`TraceSink`, atomic mutation, ambient clock/random calls, and
views that expose live mutable state. Decision-trace goldens check sequential policy/executor
value equivalence; contention tests check terminal outcome/counter conservation. Neither
substitutes for PROP, RES, or CONC tests at the transaction and lifecycle boundaries.

---

<a id="3-sqlite-checkpoint-schema"></a>

## 3. SQLite checkpoint schema

```sql
-- Open order is a contract, and this is it.
PRAGMA busy_timeout=5000;    -- first: connection state, not file state, so the gate can wait out a lock
PRAGMA user_version;         -- THE GATE, read before anything writes: journal_mode=WAL is a persistent
                            -- header change, so a file swath is going to refuse must be refused
                            -- while it is still byte-identical.
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;   -- WAL+NORMAL may lose the last few commits on power loss; safe here —
                            -- the resume cursor/durable_cursor only REGRESS (never corrupt), so at
                            -- worst a small tail re-lists. Use FULL only if zero-loss resume is required.
PRAGMA foreign_keys=ON;      -- a no-op inside a transaction, hence before the creation one below
PRAGMA user_version=1;       -- the checkpoint schema version, stamped at creation in the SAME
                            -- transaction as the DDL (SQLite makes both transactional), and required
                            -- to match EXACTLY on every open. Atomic creation is what keeps the
                            -- window between the last CREATE TABLE and the stamp harmless: an
                            -- interrupted create — including a WAL tail lost to power failure —
                            -- leaves an empty file the next open creates cleanly, not a durable
                            -- half-built schema that every later open (--restart and resume included,
                            -- since both run behind this gate) would refuse. All DDL
                            -- below is IF NOT EXISTS / additive ALTER, so without the stamp a
                            -- foreign, damaged, or future-version file would be half-adopted rather
                            -- than refused. There is no migration path: swath refuses anything else
                            -- (version 0 ⇒ not a swath checkpoint; higher ⇒ upgrade swath) and the
                            -- remedy is a fresh run. Column additions stay in-version via the
                            -- idempotent ALTER TABLE ADD COLUMN backfill below.

CREATE TABLE run_meta (
  id INTEGER PRIMARY KEY,
  store_scheme TEXT NOT NULL,              -- s3 | gs | az | r2 | …  (store identity)
  endpoint TEXT,                           -- custom --endpoint-url, if any (part of identity)
  bucket TEXT NOT NULL, prefix BLOB NOT NULL,
  args_hash TEXT NOT NULL,                 -- §5; gates resume eligibility
  strategy TEXT NOT NULL,                  -- the literal --strategy SELECTOR value; v1.0 always stores
                                            -- 'auto' (the probe-resolved engine is NOT stored, so a bucket
                                            -- crossing the tiny threshold between runs still resumes, §5)
  filter_spec TEXT, output_format TEXT,    -- original filter/format (resume-safety check, §5)
  mode TEXT NOT NULL CHECK (mode IN ('OBJECTS','VERSIONS')),
  started_at INTEGER NOT NULL, finished_at INTEGER,
  status TEXT NOT NULL CHECK (status IN ('RUNNING','COMPLETED','FAILED')),
  -- Run-context columns (soft-restored on resume; each backfills via idempotent ALTER TABLE
  -- ADD COLUMN onto a checkpoint DB created before it, gaining its default):
  no_sign_request INTEGER NOT NULL DEFAULT 0,
  profile TEXT, region TEXT,
  fetch_owner INTEGER NOT NULL DEFAULT 0,
  raw_output INTEGER NOT NULL DEFAULT 0,
  output_path TEXT,
  sort_enabled INTEGER NOT NULL DEFAULT 0, -- §6: gates the --sort/--no-sort resume mismatch refusal
  sort_phase TEXT,                         -- §6: LISTING | MERGING | PUBLISHED (NULL for non-sort runs)
  fatal_error INTEGER,                     -- NULLABLE. NULL/0 = not fatal at the store-level resume gate
                                            -- (broken-pipe FAILED, older DB without the column, or a row
                                            -- nothing has flagged yet); 1 = a deterministic in-process
                                            -- fatal error (swath resume refuses, §5). Shipped stdout is ephemeral.
  request_payer INTEGER NOT NULL DEFAULT 0);

-- The worklist IS this table. One row per range/prefix/inventory-file node.
CREATE TABLE listing_node (
  id INTEGER PRIMARY KEY,
  run_id INTEGER NOT NULL REFERENCES run_meta(id),
  parent_id INTEGER REFERENCES listing_node(id),
  kind TEXT NOT NULL CHECK (kind IN ('RANGE','PREFIX','INVENTORY_FILE')),
                                           -- PREFIX = portable engine for stores w/o a usable lexical lower bound
                                           --          (opaque-marker-only stores)
  range_start BLOB,                        -- A (exclusive lower);  NULL = ⊥
  range_end   BLOB,                        -- B (inclusive upper);  NULL = frontier/unbounded
  cursor      BLOB,                        -- last committed in-range source key (= start_after for ordinary resume); RANGE position.
                                           --   For S3 NO continuation_token: pagination is always start_after=cursor.
  opaque_token TEXT,                       -- resume cursor for OPAQUE_MARKER stores (PREFIX nodes; unused by S3)
  durable_cursor BLOB,                     -- last key whose pages are in FINALIZED Parquet parts (I6)
  key_marker BLOB, version_id_marker TEXT, -- intra-node version pagination (mode=VERSIONS)
  inventory_uri TEXT,                      -- kind=INVENTORY_FILE: the file to consume
  status TEXT NOT NULL CHECK (status IN ('PENDING','IN_PROGRESS','COMPLETED')),
  generation INTEGER NOT NULL DEFAULT 0,   -- v1.0 BUMPS it as bookkeeping (split, lease, resume-revert) but does NOT read it for correctness — the split guard is `(cursor IS NULL OR cursor < pivot) AND range_end IS oldHi AND status <> COMPLETED` (I4). Load-bearing only for multi-host idempotence (later).
  owner_lease TEXT,                        -- v1.0 sets it on IN_PROGRESS and clears it on resume-revert, but does NOT read it for correctness (the status flip covers staleness). Intended for multi-host worker-epoch use.
  pages_emitted INTEGER NOT NULL DEFAULT 0, api_calls INTEGER NOT NULL DEFAULT 0,
  unsplittable INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL);
CREATE INDEX idx_node_ready ON listing_node(run_id, status);

-- Output durability (I6). Parts are decoupled from nodes: a part holds pages
-- from many nodes (all on one sticky writer); a node's pages span a contiguous
-- run of its writer's parts. durable_cursor (above) advances when a part finalizes.
CREATE TABLE part_file (
  id INTEGER PRIMARY KEY,
  run_id INTEGER NOT NULL REFERENCES run_meta(id),
  writer_id INTEGER NOT NULL,              -- sticky: node_id % num_writers
  path TEXT NOT NULL, format TEXT NOT NULL,
  finalized INTEGER NOT NULL DEFAULT 0,    -- 1 ⇔ footer flushed+fsynced (durable); else discard on resume
  rows INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0,
  format_version INTEGER,                  -- page-run header version; NULL for ordinary output and legacy rows
  extension_type INTEGER);                 -- page-run trailer extension type; NULL with format_version

-- output_journal (for --resume-output, v1.1): RAW, PRE-FILTER, byte-exact page entries.
-- NOT created in v1.0 DDL — CREATE TABLE is deferred to v1.1. Shape reserved here for
-- reference only. Storage vehicle (this BLOB column or append-only CBOR sidecar files)
-- is an open v1.1 implementation choice; the replay contract does not depend on it.
-- v1.1 DDL (do not execute in v1.0):
--   CREATE TABLE output_journal (
--     node_id INTEGER NOT NULL, page_seq INTEGER NOT NULL,
--     payload BLOB NOT NULL,                   -- CBOR of List<ListEntry>, pre-filter
--     UNIQUE (node_id, page_seq));             -- page_seq continues from MAX+1 on resume (monotonic)
```

State machine: `PENDING → IN_PROGRESS` (lease, bump generation) `→ COMPLETED`
(or back to `PENDING` on resume, **keeping `cursor`**, clearing `owner_lease`).

---

<a id="4-parquet-output-schema--canonical-superset--etag-rule"></a>

## 4. Parquet output schema — **canonical superset + ETag rule**

One `MessageType`, used by every parallel writer. Logical types per Parquet
spec; timestamps `INT64` `TIMESTAMP(MICROS, UTC)`. The source listing model preserves raw
last-modified text, so this sink performs the timestamp parse lazily while writing; text sinks do
not parse it.

| Column | Physical / logical | Null? | Notes |
| --- | --- | --- | --- |
| `key` | `BINARY` | no | raw key bytes (byte-exact; not UTF-8-coerced); = the prefix for `COMMON_PREFIX` rows |
| `size` | `INT64` | **yes** | bytes; **null** for `COMMON_PREFIX` and `DELETE_MARKER` rows |
| `last_modified` | `INT64` TIMESTAMP(MICROS,UTC) | **yes** | **null** for `COMMON_PREFIX` rows |
| `etag` | `BINARY` (UTF8) | yes | **quotes stripped**; multipart form `hex-N` kept verbatim as string; never panic-parse |
| `storage_class` | `BINARY` (UTF8) | yes | STANDARD, GLACIER, … |
| `version_id` | `BINARY` (UTF8) | yes | versioned only |
| `is_latest` | `BOOLEAN` | yes | versioned only |
| `is_delete_marker` | `BOOLEAN` | no | false for objects |
| `owner_id` | `BINARY` (UTF8) | yes | when `--fetch-owner` |
| `owner_display_name` | `BINARY` (UTF8) | yes | when `--fetch-owner` |
| `checksum_algorithm` | `BINARY` (UTF8) | yes | when present |
| `checksum_type` | `BINARY` (UTF8) | yes | when present |
| `row_type` | `BINARY` (UTF8) | no | `OBJECT` \| `COMMON_PREFIX` \| `DELETE_MARKER` |

This schema is a canonical **superset**: a consumer selects the columns it
needs. Writer settings are **pinned** (not defaults): `parquet.block.size`,
`parquet.page.size`, dictionary on, ZSTD-3.

All final Parquet producers use one physical-writer contract in `output.parquet`: pinned common
configuration plus caller-selected row-group/page geometry and `WriteSupport`, streamed
emitted-byte and optional digest accounting, an optional data-only sync on the same output
channel, and a close that makes metadata publishable only after the file fsync and parent-directory
durability attempt complete. This is a low-level byte/durability boundary, not a shared lifecycle:
direct output still owns sticky lanes, rolling parts, checkpoint callbacks, and direct publication;
sorted output still owns staging/merge, late global footer stamps, ordered rolling, and sorted
publication. A periodic data sync never substitutes for either lifecycle's final close or advances
its checkpoint/publication state.

### 4.1 Multi-writer + manifest — **own manifest, not parquet `_metadata`**

- **2–64 writers**, default 3, decoupled from listing concurrency (not one per worker).
  Parquet counts 2–4 are the measured release envelope; expert counts 5–64 are admitted only when
  the JVM maximum heap covers the §7.2 planning allowance. Text uses the same process-resource ceiling without
  inheriting Parquet's row-group gate. Production gives each lane
  `min(64, floor(256 / writers))` queue slots, preserving the 64-slot-per-lane compatibility
  behavior through four writers while bounding aggregate queued batches by 256 (192 at the
  default three writers). Increasing concurrency therefore does not multiply queued page batches.
  This is a memory bound, not a throughput promise: above four writers each lane gets fewer slots,
  so the sole sticky dispatcher can encounter head-of-line blocking sooner while another lane is
  idle. `submit_blocked_ms`, `head_of_line_blocked_ms`, and the lane queue peaks decide whether a
  higher count helped on a real run.
  Workers emit `PageBatch`es into the writer pool. **Sticky assignment:** all
  pages of a node go to writer `node_id % numWriters`, so a node's pages
  occupy a *contiguous* run of that writer's parts (which finalize in order)
  — this is what makes the `durable_cursor` advance (algorithms.md §4.5)
  sound. A part file holds pages from many nodes and a node's pages may span
  several parts; **there is no one-part-per-node rule.** Admission is closed
  atomically before shutdown queues each lane's poison sentinel. A submitter
  blocked by a full sticky lane waits only on that lane's bounded-space
  condition, never while holding the lifecycle admission barrier: close or abort can
  therefore revoke admission and wake it even if the lane's writer is wedged.
  On wake, the submitter rechecks the barrier before enqueueing, so no batch
  can land behind poison. Already-admitted batches drain before a graceful
  close publishes; a submitter still waiting for a full lane when close starts
  is rejected rather than becoming an implicit part of that successful close.
- Each writer rotates its open part by **target size** (default 256 MB), or,
  whichever fires first, by **time-open** or **row count** (`--part-rotation-
  interval` / `--part-rotation-max-rows`, default 30 s / 2M rows) —
  the same finalize/rotate path either way, so a lane that never fills up to
  256 MB still finalizes on a bounded cadence instead of leaving `COMPLETED`
  nodes with a `NULL durable_cursor` for the run's whole duration (the resume
  RPO gap). The time/row triggers never fire on an empty (0-row) open part, so
  an idle lane never produces empty parts; both are disabled (`0`) for a
  single-file `-o *.parquet` destination, which must stay exactly one part. On `close()` (footer
  fsynced), the lane commits its checkpoint callback first; one synchronous publication owner then
  adds the part to its monotone set and atomically replaces the manifest. It has no queue or thread
  of its own, so lane failures remain synchronous and shutdown has no second drain protocol.
  `durable_cursor` advances for every node whose pages the part held. The cadence
  is evaluated on the lane's **own writer thread** — both on write (inside
  `writeBatch`) and, when a rotation interval is set, on an **idle-timeout
  wakeup** (the thread polls its queue with the interval as the timeout, so
  an idle-but-non-empty lane still re-evaluates the trigger and finalizes
  its tail within ~one interval, with no separate sweeper thread and no
  cross-thread state). Worst case, an idle lane's tail is durable within one
  interval of going idle; with the interval disabled (`0`, incl. a
  single-file destination) the lane blocks until the next batch or `close()`. The CLI
  rejects a positive `--part-rotation-interval` below **100 ms** (arbitrarily
  small values, e.g. `PT0.000000001S`, would otherwise make this poll-and-
  timeout loop spin the lane thread with no work); `ParquetWriterPool` itself
  additionally clamps the poll *wait* (never the staleness check above, which
  always uses the true configured interval) to a 50 ms floor as defense-in-
  depth against a `ParquetSpec` constructed outside the CLI.
- **On-disk layout.** Every directory dataset has a `data/` subdirectory for
  its parts plus root-level `manifest.json`, `_SUCCESS`, the internal
  `.swath-state.json`, and `symlink.txt`. A Parquet dataset's `data/` is pure
  `*.parquet`; a TSV/JSONL dataset's `data/` contains only parts of that text
  format, optionally as complete gzip/Zstandard frames. There are no manifests
  or markers under `data/`, so a format-specific glob is safe by construction —
  DuckDB's directory glob (swath's own ingest) does **not** honor the Hadoop
  `_`-prefix skip rule.
- parquet-mr `_metadata` summary files are deprecated/off-by-default — swath
  writes its **own consumer `manifest.json`** at the dataset root in the
  **S3-Inventory schema plus additive sortedness fields**:
  `sourceBucket`, `version`, `creationTimestamp`, `fileFormat`,
  `fileSchema`, a top-level **`sorted`** boolean and **`sortKey`** (non-null
  iff `sorted`), and `files[]` of `{key: "data/<part>", size, MD5checksum,
  rowCount, minKey?, maxKey?}` — `rowCount` is present on **every** file
  (sorted or not); `minKey`/`maxKey` (plain UTF-8 key text, **not**
  base64/hex) are present only when `sorted`, and are each file's TRUE first/
  last key (never derived from Parquet footer min/max statistics, for the
  same truncation reason as `SortedFileIndex`) — the publisher verifies the corresponding retained
  raw-byte bounds before renaming, and a consumer holding valid UTF-8 keys can verify
  `files[i].maxKey < files[i+1].minKey` (unsigned byte, strict) across the
  whole dataset without opening a single Parquet file. Final rolling preserves that strictness by
  treating every equal-raw-key group as one indivisible file atom: once a part reaches its byte
  target, rotation waits for the next distinct key. A version-heavy key can therefore make one part
  exceed `final-file-bytes`; the merge remains streaming and retains only the previous key while it
  waits. Committed atomically exactly once at
  successful consumer publication (`manifest.json.tmp`, fsync, rename), after every writer lane
  has joined. A finalized part updates the publication owner's live counters for periodic JSON
  summaries but does not emit an incomplete consumer manifest. The retained file list and terminal
  manifest serialization are each `O(parts)` in memory/work; part-count metadata is therefore
  outside the active-buffer bound in I11, but cumulative manifest serialization is no longer
  `O(parts²)`. Counts above the measured envelope still emit an operator warning, and
  `part_digest_*` / `manifest_write_*` in `dataset_writer` distinguish per-part streamed digest work
  from the one terminal manifest attempt.
  The same publication owner writes the final state and symlink artifacts and `_SUCCESS` last,
  after every lane has joined; its terminal state rejects any later part publication.
  **Resume bookkeeping stays out of the consumer manifest**: finalized parts and per-node
  `durable_cursor` live in the checkpoint, while `args_hash` and
  the checkpoint `run_id` live in the internal `.swath-state.json` (same
  atomic write). For every directory format, swath creates and fsyncs that
  identity before the first part or staging segment; a fresh-run cleanup may
  treat reserved part filenames as owned only after this durable evidence
  exists. Part-looking names alone never establish ownership. Publication
  refreshes the identity, which the sorted publish-reentry check reads to
  distinguish
  "published by this run" from a stale/foreign dataset in the same dir. The
  whole-snapshot completion marker **`_SUCCESS`** (empty) is written **last**,
  after the manifest; `symlink.txt` lists the `data/<part>` paths for
  Hive/Athena/Trino auto-discovery. The finalized-part durability bookkeeping
  (`writer_id`/`finalized`/row counts) lives in the checkpoint `part_file`
  rows (§3), **not** the consumer manifest.
- **Sorted output (`--sort`, §6) gets the same consumer `manifest.json` +
  `.swath-state.json` + `_SUCCESS` at publish time** — the
  final **`part-NNNNN.parquet`** files (uniform naming, no
  `sorted-` prefix; `NNNNN` is a dense, zero-based sequence beginning at
  `00000`, `%05d` zero-padded, and lexical name order == key order) live
  under `data/` like any part — a `--sort` dataset and a plain dataset share
  the same `part-` prefix (never a `sorted-` prefix on either) but differ by
  the `w`-infix: plain (unsorted) parts are `part-w{worker}-{seq}.parquet`,
  where each writer's dense `seq` is independently zero-based,
  `--sort` finals are `part-{NNNNN}.parquet` with no `w`-infix — a consumer
  distinguishes them either way, but authoritatively via `manifest.json`'s
  `sorted` field; the manifest carries their
  `data/<part>` keys/sizes/MD5s/rowCounts/minKeys/maxKeys and the schema, and
  `.swath-state.json` records `args_hash` **and** the `run_id`, committed at
  §6's publish commit point (final files written `*.tmp`, renamed in name
  order under `data/`; then the manifest, the state file, `symlink.txt`, and
  finally `_SUCCESS`). Each final writer computes the byte-exact MD5 over the
  exact bytes it emits and retains its exact first key, last key, row count,
  and emitted byte count. Those immutable facts become publishable only after
  footer close + mandatory file fsync succeeds and the directory-fsync barrier
  completes (or the startup probe classifies that exact filesystem/provider as
  unsupported, as qualified below);
  the ordered merge result carries
  them through the rename into manifest assembly, so a newly produced final is
  not re-opened for either an MD5 pass or a projected-key bounds pass. A carried
  or third-party final without equivalent trustworthy close-gated metadata keeps
  the validating readback path (including true decoded bounds, never footer
  statistics), and any corrupt/truncated input aborts before `_SUCCESS`.
  Staging segments under the **visible** `_staging/` directory
  (not a hidden dot-dir — a mid-sort run must be observable with a
  plain `ls`, and distinguishable from a fresh/crashed-no-sort/complete
  dataset root purely from `(_SUCCESS, _staging/, manifest.sorted,
  data/part-w*)`) are internal working state and never appear in the
  manifest.
- v1.0 local Parquet durability is POSIX/Linux/macOS-oriented. File fsync is
  mandatory. Before workers start, swath probes the output root and each active
  `data/`/`_staging/` directory once per exact filesystem/provider identity and logs
  that identity when the operation is classified as unsupported. An open-time access
  denial remains fatal; a force-time failure degrades only on the explicit
  unsupported-filesystem allowlist. If filesystem discovery itself is unavailable,
  swath uses a path-scoped unknown identity and still attempts each directory's direct
  barrier rather than borrowing an ancestor's verdict. Permission failures, missing
  paths, and unexpected I/O failures on other filesystems remain fatal. A supported
  filesystem retains a directory barrier at every durable directory-entry or
  atomic-rename commit point.
- **On resume:** load finalized parts from checkpoint `part_file`, discard every non-finalized
  part, and never rewrite finalized parts. Neither a consumer manifest nor the periodic JSON
  summary is a resume input. Each node re-lists from its `durable_cursor` (the not-yet-durable
  tail), so finalized rows are neither lost nor duplicated ⇒ **exactly-once** (I6). A legacy
  incomplete manifest from an older writer remains non-authoritative without `_SUCCESS` and is
  atomically replaced by the completed snapshot when the resumed run succeeds. If every node is
  already output-complete but the prior process stopped during direct (unsorted) terminal
  publication, resume issues zero LIST requests and republishes directly from `part_file`; a direct
  terminal-publication I/O failure leaves this state resumable rather than marking it fatal. Sorted
  re-entry instead follows §6's checkpointed staging/merge state machine.

---

<a id="5-resume-args_hash-and-per-sink-guarantees"></a>

## 5. Resume, `args_hash`, and per-sink guarantees

> **v1.0 scope:** `--resume-output` (the `output_journal`) is **deferred to
> v1.1**. In v1.0, only a managed directory-dataset Parquet destination is
> resumable, and it resumes exactly-once (via `durable_cursor`, independent of
> any journal). **Stdout and FILE-kind destinations are non-resumable**;
> commit-before-emit still makes interrupted stdout/text at-most-once. The `--resume-output` material in this
> section specifies the v1.1 behavior and the dormant seams (`output_journal`,
> filters-excluded-from-`args_hash`) that keep it additive.

- **`args_hash` = SHA-256 of a canonical encoding of exactly the fields that
  change _what is listed_:** **store scheme + endpoint** (`s3`/`gs`/… +
  `--endpoint-url`), bucket, prefix, recursive flag, `--all-versions`, the
  **`--strategy` selector value** (the literal `auto`/`scan`/… — reserved for
  the target multi-engine build per §7.1, *not* the probe-resolved engine, so a
  bucket crossing the tiny threshold between runs still resumes), hints-file contents, inventory-manifest URI
  (+ delivery id). It **excludes** concurrency, output format, filters, log
  level, progress prefs. Resume refuses on mismatch; `--restart` discards.
  *(v1.0 wiring: only scheme/endpoint/bucket/prefix are driven by live flags; the
  remaining fields are dormant seams carrying their v1.0 defaults —
  `recursive=true`, `all_versions=false`, `strategy="auto"`, no hints, no
  inventory — each label-tagged in the canonical encoding so lighting one up later
  flips the hash deliberately.)*
- *(v1.1)* **The `output_journal` stores RAW, PRE-FILTER, byte-exact entries.**
  Filters/sort/format re-apply on replay, so changing a filter between runs
  is consistent (and is why filters aren't in `args_hash`).
  - **Why it's deferred:** it persists every raw page, so the journal is **≈
    the size of the listing itself** (tens of GB for a billion-object bucket) —
    the heaviest path in the system, for the least-common need. **Exactly-once
    Parquet does not use it** (the `durable_cursor` / part-discard model, §4.1,
    is fully independent), so v1.0 ships without it. The v1.1 storage vehicle is
    an open choice — the reserved `output_journal` table's BLOB, or append-only
    CBOR sidecar files (the latter avoids WAL/BLOB write-amplification); both
    satisfy the same replay contract.
- **Changing a filter/format on resume.** The already-written output of
  `COMPLETED` nodes reflects the *original* filter/format and can't be
  regenerated without the raw pages. So `run_meta` stores the original
  filter+format; on resume, if they differ, **v1.0 refuses** (points to
  `--restart`). *(v1.1, with `--resume-output`: the completed nodes' raw
  journal pages replay through the new filter/format — which is why filters are
  excluded from `args_hash`.)*
- **Resuming a `FAILED` run.** `run_meta.status` has a third terminal
  value distinct from `RUNNING`/`COMPLETED`: `FAILED`. Three DIFFERENT things
  set it, distinguished by the nullable `run_meta.fatal_error` column (above) —
  NOT by `status` alone:
  - A **broken-pipe truncation** (stdout closed downstream, e.g. `| head`)
    marks the current store's run `FAILED` with `fatal_error` left `NULL`/unset
    and exits cleanly. For the shipped stdout CLI path that store is ephemeral:
    no durable checkpoint remains, and `swath resume` cannot continue it. A
    **failed output publish** (the atomic rename of the completed text output,
    e.g. a full disk) records the same flag-unset `FAILED` before rethrowing:
    the cause is external and typically transient, so the run stays resumable
    rather than costing the operator every completed node.
  - A genuinely unrecoverable, deterministic in-process error (an
    unrecoverable `ListingException`/plain `OutputException`/`CheckpointException`,
    or a fatal seed-probe failure) escaping the CLI's engine dispatch marks
    `FAILED` **with `fatal_error=1`** (`markRunFatalUnlessFinished`). This is
    deliberately **not** the same as the `RUNNING` a raw SIGKILL or a graceful
    `--max-duration`/signal cancel leaves behind (both stay
    resumable-as-`RUNNING` on purpose) — nor the same as a plain broken-pipe
    `FAILED` above. The mark is a CAS on `RUNNING`, so it never overwrites a
    status the run already recorded for itself: not a durable `COMPLETED` (a
    failure in a post-completion step inside the same guarded region), and not
    a flag-unset `FAILED` a caller chose above — a publish failure rethrown
    into that guard stays resumable.
    Direct-dataset terminal publication is one explicit exception: it throws
    `PublicationPendingException`, leaves the run `RUNNING`, and can be retried from finalized
    checkpoint parts. Sorted publication has the same typed recovery only after its authority
    listener returns: `manifest.json`, `.swath-state.json`, `symlink.txt`, and last-written
    `_SUCCESS` already describe the valid dataset, so a subsequent sorter-local hook or staging
    cleanup failure latches `sort_phase=PUBLISHED`, throws `PublicationPendingException`, and leaves
    the run non-fatal for cleanup-only resume. The cleanup-only re-entry verifies that exact
    identity + `_SUCCESS` before creating or mutating staging, latches PUBLISHED before cleanup,
    and applies the same classification to every cleanup/reconciliation/sweep failure; any number
    of failed cleanup invocations therefore remain `RUNNING`/`PUBLISHED` with `fatal_error=0` until
    one succeeds and marks `COMPLETED`. A failure before the listener returns remains a plain
    `OutputException` and follows the fatal-error rule; it must re-enter neither PUBLISHED cleanup
    nor claim that publication committed.
  - A **protocol violation** (a response no conforming store may produce, e.g.
    `oversized_page`) is the one failure the run must never resume into, so it
    marks `FAILED` with `fatal_error=1` whatever ended the scan — including an
    otherwise-resumable cancellation or a user-correctable config/args error
    carrying the violation as a cause or a suppressed exception. Alone among
    the marks it overrides a flag-unset `FAILED` (`markRunUnresumable`): a
    resume admitted from such a row would walk straight back into the endpoint
    that violated the protocol. Only a durable `COMPLETED` is spared.

  At the checkpoint-store layer, resume refuses only a `FAILED` run whose
  `fatal_error` flag is set rather than silently re-attempting a deterministic
  re-failure; `--restart` discards it and starts fresh. A flag-unset `FAILED`
  row remains acceptable to that internal gate, but the shipped CLI exposes
  resume only for managed directory-dataset Parquet, so stdout broken pipe is
  not a public resume case. A plain `RUNNING` managed run (SIGKILL/interrupt,
  or an older checkpoint DB where the column is universally `NULL`) remains
  resumable.
  A user-correctable exit-2 config/args error (e.g. `--format parquet` with no
  `-o`) surfacing from the same engine-dispatch call site is explicitly
  EXCLUDED from `fatal_error` — it is not an unrecoverable in-process failure,
  and marking it fatal would wrongly refuse a later, corrected invocation
  (unless it carries a protocol violation, above).
- **Shipped CLI delivery and resume guarantees:**

| Destination | Interruption / delivery guarantee | Resume |
| --- | --- | --- |
| **stdout** | **At-most-once while the one-shot process runs:** commit-before-emit may leave a committed page absent if the process stops before emission. | **No.** `auto` and `none` are ephemeral; an explicit checkpoint path is refused. |
| **FILE-kind text** | **At-most-once while the one-shot process runs;** a successful publication atomically replaces the destination. Optional gzip/Zstandard streams are finished before publication. | **No.** FILE kind requires `--checkpoint none`. |
| **Directory-dataset TSV/JSONL** | Bounded parallel lanes write independent parts; successful completion publishes one atomic manifest and `_SUCCESS` last. A failed run has no `_SUCCESS` and may leave a manifest plus finalized parts from that attempt. | **No.** Text datasets require `--checkpoint none` in this release. |
| **FILE-kind Parquet** | Uses the Parquet writer path, but has no durable resume ledger. | **No.** FILE kind requires `--checkpoint none`. |
| **Managed directory-dataset Parquet** | **Exactly-once durable dataset** via the `durable_cursor` model (§4.1, I6): finalized parts are retained; an unfinalized tail is discarded and re-listed from `durable_cursor`. For direct/unsorted output, a failure after all parts finalize but before `_SUCCESS` leaves publication pending, not fatal. | **Yes.** `swath resume <dir>` opens the co-located checkpoint; a direct publication-only resume issues no LIST requests. |

  The deferred `--resume-output` journal describes a possible future text-replay
  contract; it does not make stdout, FILE-kind output, or directory text output resumable in the shipped CLI.

- **`swath resume` preserves a stored `--sort` run (`--format parquet`)** — the
  §6 checkpoint-tracked sorted-segment design means resume keeps durable
  segments and re-lists only the non-durable tail, then (re)runs the merge
  from staging; refused only on a `--sort`/`--no-sort` mismatch, like a
  format swap above. *(v1.1)* Only the deferred sorted **text**-sink path
  reinstates the old constraint — resuming a sorted text run is rejected unless
  `--resume-output` is set (sort then replays from the journal) — because a
  text sink still needs the raw journal to redo the sort on resume.
- **Restart-to-converge.** For a managed directory-dataset run, `swath resume`
  is the liveness-failure recovery layer (machine, JVM, algorithm, environment):
  liveness failures funnel into exit 75 (`STUCK`) without poisoning the durable
  checkpoint as a fatal exit-1 would (see "Resuming a `FAILED` run" above).
  Exit 75 carries no resume promise for ephemeral stdout/FILE-kind runs. A
  `STUCK` managed run may be auto-resumed by a supervisor and will **converge**
  to completion in a bounded number of cycles — *provided*:
  (1) each cycle commits **net progress** (the in-run shed provides this; a
  bounded wrapper retry backstops it); (2) the failure is **not deterministic
  at a fixed point** in the remaining work (needs the quarantine of a
  poisoned range, persisted in resume state — a deferred follow-up); and
  (3) the wrapper accounts for the fact that
  **restart preserves DATA state but discards CONTROL state** (a fresh
  process re-enters at pinned `Tmax` and re-storms — pair with the in-run
  shed and/or staggered restarts, never a naive tight restart loop, which
  re-establishes ~`T` TLS connections and amplifies a shared-load storm).
  This is a contract on the resume mechanism, not an implementation detail:
  a supervisor wrapping `swath` in restart-on-exit-75 is a *supported*
  recovery pattern, not a workaround.

---

## 6. Sorted staging and finalization

`--sort` applies only to Parquet output. The listing phase writes checkpoint-tracked `.pageseg`
files under the output directory's visible `_staging/` directory; only final output is Parquet. A
sorted text request is rejected with `InvalidArgsException`.

### Durable staging

The sort lane seals admitted pages at the heap-adaptive segment gate (§7). A sealed segment contains
a versioned header, CRC32C-framed page records carrying their key bounds, row count, codec, and
lengths, and a completeness trailer with exact segment bounds and record/entry totals. The full
envelope is specified in §6.1. The staging namespace has its own checkpoint `part_file` rows and
never contributes to the root output manifest.

Segments finalize strictly in seal order. Finalization writes the complete trailer, fsyncs the file
and parent directory, records `partFinalized`, and only then advances `durable_cursor`. An unsealed or
partially written segment is disposable; a sealed segment is the atomic durable unit reused by
resume. A page that is disordered only under the complete comparator may be repacked while keeping
its original last raw key as the durable maximum. Any raw-key regression is rejected before segment
fsync because repair could otherwise persist a key beyond the admission-time durable cursor.

Within a segment, pages sorted by first key must also have disjoint ranges:
`previous.maxKey < next.minKey` for `OBJECTS`. `VERSIONS` alone may use equality when one key spans
pages; `previous.maxKey > next.minKey` is always corruption. The writer enforces this before
checkpoint finalization and the reader enforces it again. The staging comparator is the in-memory
`ListEntryComparator`. Dormant `VERSIONS` plumbing orders `(key, version_id)` with null first and
then unsigned UTF-8 `version_id`; it remains unreachable until algorithms §9 resolves chronology and
the comparator, footer/manifest order, compatibility version, and independent tests change together.

### Finalization contract

Finalization converts the complete set of sealed segments into one sorted Parquet dataset. The
default `ranges` mechanism partitions raw-key space; the experimental `pipeline` mechanism is
selected with `--tune sort.finalization=pipeline` and routes page references into complete part
plans. Both mechanisms enforce the same result contract:

1. Rows are globally non-decreasing under the complete `ListEntryComparator`.
2. For every two non-empty adjacent parts, the first part's raw-byte maximum is strictly below the
   second part's raw-byte minimum.
3. A raw-key group is indivisible. `OBJECTS` rejects an adjacent equal raw key as
   `sort_duplicate_key`; dormant `VERSIONS` may keep equal raw keys together under the complete
   comparator. A soft part target never splits the group.
4. Final Parquet footers carry `swath.sort.order`, `swath.sort.mode`, and
   `swath.sort.format_version`. Every result also has dense one-based
   `swath.sort.file_index=1..N` and exactly one `swath.sort.file_final`, on part `N`. Filenames use
   the corresponding dense zero-based `part-00000.parquet` sequence.
5. Replacement parts remain temporary until every writer is footer-closed and fsynced and the
   mechanism's source proof, cross-part adjacency, and cardinality checks have passed. Nothing in
   the replacement is visible under a final part name before rename. Parts are renamed in ordinal
   order, and `data/` is fsynced before authority metadata is written.
6. The consumer manifest, `.swath-state.json`, and `symlink.txt` follow the part renames.
   `_SUCCESS` is written last and is the publication commit marker; it certifies complete
   publication, not snapshot semantics.

For `ranges`, validated page samples choose contiguous byte boundaries and type-2/type-3 indexes
provide seek seams. Each worker applies its exact `[lo, hi)` filter, and the coordinator verifies an
exact physical-zone proof from every segment header to `trailerStart` before accepting the parts.
For `pipeline`, header cursors prove the same frame tiling, order, and trailer totals without loading
bodies; the router consumes every reference once; encoders positionally read and CRC-check every
planned frame once; and source trailer rows, routed rows, emitted rows, and closed-writer rows must
agree. The pipeline does not use sparse indexes or seek-derived boundaries for correctness.

### Resource bounds

Let `B` be `merge-budget-bytes`, `K` the relevant segment count, `E` the largest persisted
page-record body, and `U` the largest validated raw-payload maximum (`0` when that metadata is
unavailable). The planner uses checked or saturating arithmetic so an overflow cannot become a small
admission. The normal range/serial per-stream price is

```text
Q = max(merge-per-stream-bytes, 2 * E + U + 256)
```

The `256` bytes reserve two 128-byte dictionary-coordinate owners. The runtime fan-in is bounded by
the configured fan-in, `floor(B / Q)`, and the process soft file-descriptor limit after 128 reserved
descriptors. If `K` exceeds that fan-in, `KWayMerge` writes page-run intermediates in bounded passes.
The pipeline reserves its requested output descriptors while choosing cascade width, then performs
its own encoder admission against the post-cascade catalog. The range mechanism admits `R > 1` only
when every range can open all survivors; otherwise the unchanged serial final merge cascades.

For `ranges`, parallel admission also requires at least `min-parallel-staged-bytes`. Boundary state
is capped at 16,384 retained candidate keys plus one segment's at-most-4,096-key validation sample.
The exact proof-spool slot is 6,212 bytes, so its logical extent is
`R * K * 6,212`. With `usableFds = softFdLimit - 128`, the initial range ceilings are:

```text
budget ranges = floor(B / ((Q + 6,212) * K))
fd ranges     = floor((usableFds - 1) / (K + 1))
```

The one subtracted descriptor is the shared proof spool; each range initially prices `K` readers
and one output. For a candidate `R`, proof bytes are removed from `B`, the remainder is divided
equally, and each range's fan-in is clamped by its budget share and its share of
`usableFds - R - 1`. `R` is decremented until that fan-in can open all `K` segments. The dynamic
open-output allowance then reserves `R * perRangeFanIn + 1` descriptors. Each range's page-aware
guard charges additional retained whole/overlap pages against its post-proof budget share. The
proof spool retains `O(R * K)` primitive/fixed-slot data and `O(R)` heap key material; its mapped
pages can contribute to RSS.

Range disk admission uses the exact original staged bytes `S` and proof extent `P(R)`. When staging
and output share a filesystem, required free space is `P(R) + max(3S, 1 GiB)`: one `S` each for
final temporary output, live cascade intermediates, and size/encoding safety. On separate
filesystems, staging requires the same amount and output requires `max(2S, 1 GiB)`. Admission lowers
`R` until the proof fits; if serial reserves do not fit, it refuses resumably before creating proof
or output state. The check is sampled again immediately before proof allocation.

For `pipeline`, let `N` be the candidate encoder count, `D = 2` both the per-segment header queue
depth and complete-plan queue depth per encoder, `J = max_s(min(1,024, E_s))` where `E_s` is segment
`s`'s trailer-declared maximum encoded record length, `H = 112 + 2J` bytes the retained price of one
reference and its two key arrays, `M = 16,384` the default hard references-per-plan cap, and `W = 8
MiB` the writer estimate. A key cannot exceed either the S3 key limit or the encoded record that
contains it, so `J` covers interior page bounds without scanning pages at admission. Let
`Ulegacy = min(256 MiB, B / 8)` for a segment without a validated raw maximum; `U` is the largest
current maximum or legacy ceiling in the post-cascade catalog. The conservative retained-page unit
is `P = 4 * max(E, U) + 23,304`. The additive term is derived from the format's five dictionary
columns, 64-value-per-column cap, maximum cache arrays/String headers, and coordinate owners; the
multiplier covers the record, decompression target, and variable dictionary character storage. With
`T` page records, initial logical part target `L`, and
`V = max(1, stagedBytes / T)`, one plan is priced for
`A = min(T, M, ceil(L / V))` references (`A = 0` for empty input). For each candidate `N`, admission
uses the largest `A' <= A` that fits, but does not reduce a non-empty large-catalog plan below 256
references. It reduces `N` only if that floor still does not fit. The reference charge is:

```text
refs(N)  = (K * (D + 2) + A' * (1 + (D + 1) * N)) * H
fixed(N) = refs(N) + N * (W + E)
```

`K * (D + 2)` covers each cursor's two queued references, one scanner-local reference blocked on
handoff, and the router frontier. `A * (1 + (D + 1) * N)` covers the router's current plan, all `2N`
complete plans in the shared work queue, and the up-to-`N` plans executing in encoder lanes. `N * E`
is the transient page term: each positional read materializes one bounded record body before
retained-page reservation, possibly while that encoder owns earlier cluster pages. Admission requires
`fixed(N) + N * P <= B`. Each admitted encoder receives
`C = floor((B - fixed(N)) / N)` retained bytes, with `C >= P`; its runtime guard uses that share for
both whole pages and incrementally admitted cluster pages. The admitted shape is therefore
`refs(N) + N * (W + E + C)`. Plans retain coordinates, never page bodies.

When admission did not reduce `A`, it remains the price for the initial reference wave rather than
the structural maximum after byte calibration: a later logical target may grow, but the router
still caps every plan at `M`. When admission did reduce `A`, that fitted `A'` becomes the runtime
plan cap. The exact runtime reference-count ceiling is therefore
`K * (D + 2) + M' * (1 + (D + 1) * N)`, where `M'` is `M` or the reduced `A'`. `H` and `W` are
conservative planning prices rather than byte-exact JVM accounting; the structural reference cap
and one-writer-per-encoder rule provide the I11 bound, while `C` is the enforced retained-byte
accounting guard for pages.

Pipeline descriptors are bounded by `K + N` after the same 128-descriptor process headroom. Encoder
admission has no range proof, boundary sample, seek, per-range stream, or staged-size-floor term. A
plan closes before its effective reference cap would be exceeded, even with an unbounded file-size
target. Heap admission may lower that cap from 16,384 to as few as 256 and records
`SORT.pipeline_plan_ref_capped`; this can create parts earlier than the calibrated
`final-file-bytes` target. If `usableFds <= K`, no output descriptor remains and admission refuses
before opening input channels or pipeline output, recording
`SORT.pipeline_encoders_fd_floor_exhausted`. If even `N = 1` at the plan floor cannot satisfy the
fixed-plus-retained-page heap formula, admission likewise refuses before opening pipeline output and
records `SORT.pipeline_encoder_heap_floor_exhausted`.
One transitive overlap component above that cap refuses resumably because it cannot be split without
risking cross-part interleaving.

The pipeline does not run the range mechanism's `MergeDiskPlan` free-space preflight. Its cascade and
final temporary writes remain ordinary checked I/O: a pre-publication space failure leaves the sealed
originals merge-pending for resume, but there is no pipeline-specific `3S` admission promise.

Pipeline part geometry uses uncompressed page-payload bytes. The first logical target assumes a 1:1
encoded/logical ratio and the router waits for that part's durable size before routing further plans;
later targets use the cumulative observed ratio. Parquet footer overhead makes small targets noisy.
Part boundaries remain soft and raw-key-atomic. A package-private fixed-row target exists only for
the benchmark harness.

### Failure, cancellation, and resume

`run_meta.sort_phase` advances through `LISTING`, `MERGING`, and `PUBLISHED`. During listing, only
sealed segments are durable; resume keeps them and re-lists at most the unsealed tail bounded by
`segment-bytes`/`segment-entries`. Once listing is complete, any pre-publication retry starts from the
checkpoint-owned sealed segments and performs zero new LIST requests.

Merge kickoff validates checkpoint ownership, physical segment format, and trailers before sweeping
disposable work. A `--sort`/`--no-sort` mismatch is refused. Explicit staging metadata other than the
current `page-run` format, or a recorded header/extension version that disagrees with the physical
file, is refused before cleanup as `page_run_format_mismatch`. Nullable legacy metadata remains
reader-authoritative. Current listing segments record format version 2 and extension type 3; type 1
minima and type 2 sparse indexes remain readable as described in §6.1.

A worker, cursor, router, encoder, proof, writer, cancellation, or pre-listener publication failure
first stops peer work, then joins it, closes readers/channels/writers, and sweeps only owned
range/pipeline/cascade/proof temporaries. Checkpoint-owned originals and any prior published finals
remain available. Memory or disk admission failures and an oversized pipeline overlap component are
merge-pending and resumable. A deterministic final order regression, duplicate-key rejection, or
source/output cardinality mismatch is classified and refuses publication; replaying the same inputs
is not presented as a repair.

Every merge attempt discards stale cascade intermediates and restarts a cascade at pass 0. Original
sealed segments are not deleted by cascade passes, while obsolete intermediates may be deleted after
a later pass folds them in. An original and its intermediate may coexist, giving the cascade its
approximately `2 * S` transient staged footprint within the disk policy above.

After all checks pass, stale finals are swept and replacement parts are renamed in dense ordinal
order. A crash during this loop can expose only a prefix of final filenames, never authority
metadata. Re-entry removes owned partial finals and temporaries and repeats the merge from the sealed
originals. Identity match without `_SUCCESS` is therefore `MERGING`, not published. A run is
recognized as its own committed publication only when `.swath-state.json` matches both `args_hash`
and checkpoint `run_id` and `_SUCCESS` exists.

The publication listener writes manifest, state, symlink, and `_SUCCESS` while original staging is
still intact. Once it returns, publication is committed and is not rolled back for cleanup failure.
Such a failure records `SORT.post_publish_cleanup_pending`, preserves the completed file/byte/row,
merge-pass, parallelism, and latency facts, and leaves the checkpoint `PUBLISHED`. Resume validates
identity plus `_SUCCESS` and retries only cleanup, again with zero LIST requests; success advances to
`COMPLETED`.

The default successful cleanup removes staging and the co-located checkpoint. Diagnostic
`sort.keep-staging=on` instead reconciles `_staging/` to exactly the checkpoint-finalized original
page-run segments and retains the checkpoint; cascade intermediates and all range, pipeline, proof,
and other temporary files remain disposable. Retention alone does not rerun finalization.

### 6.1 Page-run v2 envelope, disjoint pages, and trailer extensions

Each original listing-phase page-run segment embeds a bounded type-3 sparse page index in the
optional extension between `segMaxKey` and the fixed EOF tail. Type 1 is the legacy
minima-only block; type 2 is the prior sparse index without decoded-page metadata. Both remain
readable. Cascade intermediates and fixture
chunks are streamed after boundary selection or outside the structured live path and remain
extensionless. Page-run format 2 is a hard cut: an older header format fails the existing version
check, with no compatibility reader or migration path. The header is a bounded, versioned metadata
envelope. Unknown TLV field IDs are skipped by their declared length; ordering mode is required and
understood by this version. `trailerStart` still points at `segMinKey`:

```text
[magic u32][formatVersion u16 = 2][headerVersion u16 = 1][metadataLength u32]
metadataLength bytes of: [fieldId u16][fieldLength u16][fieldValue bytes]
  field 1, length 1: ordering mode (1 = OBJECTS, 2 = VERSIONS)
[headerCrc32c u32]  // over the header prefix and metadata

records*
segMinKey u16-len-prefixed
segMaxKey u16-len-prefixed
[extensionMagic u32][type u16][version u16][payloadLength u32][entryCount u32]

type 1 payload (legacy):
  entryCount * [keyLength u16][minKey]

type 2 payload (legacy sparse index):
  entryCount * [pageOrdinal u64]
               [fileOffset u64]
               [cumulativeEntries u64]
               [cumulativeFramedBytes u64]
               [minKeyLength u16][minKey]
               [prefixMaxLength u16][prefixMax]
  [finalPrefixMaxLength u16][finalPrefixMax]

type 3 payload (current):
  entryCount * [pageOrdinal u64]
               [fileOffset u64]
               [cumulativeEntries u64]
               [cumulativeFramedBytes u64]
               [minKeyLength u16][minKey]
               [prefixMaxLength u16][prefixMax]
  [finalPrefixMaxLength u16][finalPrefixMax]
  [maxRawPayloadLength u32]

[crc32c u32]
[trailerStart u64][totalRecords u32][totalEntries u64][maxRecordLen u32]
[maxRawPayloadLength u32][maxKeyLength u32]
[fixedTrailerCrc32c u32][magic u32]
```

The fixed-trailer CRC covers `trailerStart`, `totalRecords`, `totalEntries`, `maxRecordLen`,
`maxRawPayloadLength`, and `maxKeyLength`, so a torn or bit-flipped count cannot terminate a read
early while still appearing complete and kickoff admission needs no duplicate page-header scan.
The header and fixed-trailer CRCs, record CRCs, and extension CRC detect accidental
corruption, torn writes, and writer/reader bugs. They are integrity checks, not authentication and
do not defend against deliberate modification by an actor who can recompute them.

The u16 key-length fields preserve the extension envelope, but sparse listing indexes
accept at most the S3 key limit of 1,024 bytes for each minimum/prefix maximum. This supplies an
up-front extension-size ceiling of roughly 8 MiB at the 4,096-entry cap; a corrupt block cannot turn
the bounded boundary sample into hundreds of MiB of provisional key arrays.

`fileOffset` is the absolute offset of the sampled page's frame-length word. `cumulativeEntries`
and `cumulativeFramedBytes` describe pages before the sample; the byte value therefore equals
`fileOffset - HEADER_BYTES`. `prefixMax` is the unsigned maximum page maximum through and including
the sampled page, while `finalPrefixMax` covers all pages and equals `segMaxKey`. The writer records
these values while it writes each frame, with no page-body reread. For `P` listing pages it uses the
same `max(1, ceil(P / 4096))` stride and ordinals `0, stride, 2*stride, ...` as the type-1 sample, so
the page minima supplied to boundary selection are unchanged.

For non-empty segments, `segMinKey` is the unsigned minimum of all persisted page minima and
`segMaxKey` is the unsigned maximum of all persisted page maxima. Adjacent pages must satisfy the
mode-aware disjointness rule above. A CRC-valid hand-built `[a,z]`, `[b,c]` segment is rejected as
typed `page_run_page_overlap`; it is never repaired by merge fallback. Minima that go backwards are
still the more specific `page_run_min_regression` classification.

Every CRC-valid record body is structurally checked before its frontier is trusted: fixed fields,
dictionary counts and lengths, positive row count, codec, raw/stored payload lengths, no trailing
bytes, and `minKey <= maxKey` are bounded and validated before allocation. Every persisted Java
string (dictionary values, raw dictionary-column values, version IDs, and raw ETags) must be strict
UTF-8: overlong encodings, isolated continuations, surrogate encodings, truncation, and code points
above U+10FFFF are rejected as typed `page_run_body_corruption`; replacement decoding is forbidden.
Payload decoding uses
bounded non-negative int32 varints and checks every prefix, suffix, string, dictionary index,
boolean, and fixed-width field before allocation or access. The production writer enforces the
same 1,024-byte row/header key, u16 dictionary-value, and raw-payload
limits that the reader treats as format truth. When a persisted page is decoded, every adjacent row
must be non-decreasing under the complete `ListEntryComparator`; the decoded row count/payload
exhaustion and first/last raw keys are also checked against the header.
If a range cutoff or downstream close stops partway through a decoded page, the page-aware merger
drains every cursor it already owns solely to complete those checks; it emits none of the drained
rows and records no source-run, duplicate, engagement, or progress signal for them. It does not
decode untouched frontier pages, so cutoff validation work is bounded by the whole/overlap pages
already decoded for that range. Malformed bodies raise typed `page_run_body_corruption`; no
replacement output is published. An earlier read/consumer failure remains primary, with validation
and stream-close failures suppressed, and every opened frontier stream is still closed.
The read side owns one immutable CRC-validated record-body array for the required page lifetime and
parses its header exactly once into a stored-payload offset/length. A decoded `PageBlock` retains
that same body when the frontier advances or closes. Persisted dictionary headers are validated
without constructing Strings and retain only two five-int coordinate arrays into that body (40 raw
coordinate-data bytes per page, conservatively reserved as 128 heap bytes including array/object
headers). Referenced values are strict-decoded lazily into a cursor-local cache;
the page-aware reservation conservatively charges the complete possible cache before constructing
the cursor, including UTF-16 expansion and object/array overhead. `NONE` cursors read the slice directly;
compressed codecs decompress from the slice into only the decoded payload. No second
`storedPayloadLength` array is allocated, and serialization remains byte-exact.
Current type-3 metadata records the exact maximum raw payload across the segment. Parallel/full-index
preflight validates that field with the complete extension CRC and prices a normal stream as the
larger of the configured floor and two maximum encoded bodies plus that raw maximum and 256 bytes of
current/successor dictionary-coordinate heap: a decoded current page retains its body and one
128-byte conservative coordinate reservation while its successor frontier retains another.
Before codec allocation
every physical header on that path must stay at or below the validated claim; a CRC-valid underclaim
raises typed `page_run_decoded_page_limit` corruption. Serial/no-boundary preflight deliberately does
not read or trust the trailing field: it treats the maximum as unknown and prices/admit each actual
page header through the runtime residency guard before decompression. Legacy and extensionless input
use the same runtime path without an O(staging-bytes) kickoff scan.
At runtime each page-aware merger reserves the retained encoded body, the complete possible lazy
dictionary cache, and any separate compressed raw allocation for every whole/active page before
cursor construction/decompression, after reserving one frontier body
per open stream. Legal overlap clusters therefore cannot exceed the serial budget or a parallel
range's post-proof share; exhaustion is resumable as `sort_merge_memory_exhausted`.
PageRun format v2 persists ordering mode but no comparator identifier and is therefore explicitly fixed to
`ListEntryComparator`. Both `SortRun` and every `PageRunSegmentWriter` reject alternate comparator
implementations before merge or persistence; a future comparator requires a new identified format.
Each page's ordered flag records full-comparator order: comparator ties remain ordered, while a strict
regression clears the flag. The writer repacks only pages whose flag is false, and every codec
preserves the flag in the serialized header.

The block CRC covers its complete header and payload, excluding only the CRC field. Before retaining
a locator or publishing any provisional minimum, the sparse-index reader bounds all lengths and counts,
requires the exact systematic ordinals, strictly increasing in-file frame offsets, non-decreasing
minima/prefix maxima, strictly increasing cumulative entries and framed bytes after the first sample
with `cumulativeEntries >= pageOrdinal`,
`cumulativeFramedBytes == fileOffset - HEADER_BYTES`, an
exact first offset of `HEADER_BYTES`, a first minimum equal to `segMinKey`, and a final prefix maximum
equal to `segMaxKey`. These checks establish a bounded, self-consistent index representation; direct
positioning treats that representation only as an untrusted hint. The first frame after every seek
must match the selected offset/accounting/minimum, and the complete physical-zone proof below must
succeed before any range writer can be returned. Type 1 continues to receive its existing
length/count/order/bounds/CRC checks.
An absent, unknown, or structurally invalid extension falls back for that segment to the legacy
full-page boundary scan. Mixed absent/type-1/type-2/type-3 input retains the same boundary rule.

Both sides use fixed 64 KiB chunk buffers: the writer batches header, prefixes, and keys instead of
issuing per-key writes; the reader first streams CRC validation without allocating keys, then parses
the CRC-valid bounded payload transactionally. At a structured parallel kickoff, each segment's
trailer and extension are read during the descriptor's single preflight open. The reader validates
one segment's sample transactionally, then feeds its keys into the merge-wide capped candidate set
before closing that descriptor; descriptors retain only status, counts, primitive offsets, and a
page-index payload locator — never a sample-key collection. After boundaries are fixed and before any
worker starts, the planner streams each valid type-2/type-3 locator once into `O(segments × R)` primitive
seek seams. There is no worker positioning barrier and no retained per-descriptor sample list.
Serial/no-boundary kickoff reads only the fixed 16-byte optional-extension header to identify the
physical extension type; it does not CRC-walk, parse, or allocate keys from the sparse payload merely
to reach type 3's trailing decoded maximum. Its decoded maximum is consequently unknown and the
runtime page-header/residency guard admits each allocation safely. Arbitrary-sorted-run merges remain
outside boundary parsing. Reader peak boundary
state is the global candidate cap plus at most one segment's 4,096-key validation sample, one 64 KiB
scratch buffer, and the primitive seek seams, never `O(segments × samples)`.

Planning uses one shared 64 KiB cursor per segment because it consumes that segment's whole bounded
entry region. Worker target/sample verification uses exact positional entry reads instead: fixed
fields plus the two actual keys, with no adjacent-entry prefetch or 64 KiB per-seam buffer. Those
post-boundary metadata bytes are counted separately as `sort.merge_range_index_bytes` /
`swath.sort.merge.range.index.bytes`; under the explicit row-weighted boundary policy this total also
includes its one extra streamed entry-region read per indexed segment. The per-range log carries
worker-local `index_bytes_read`. The serial reader
uses a tracked primitive frame offset: after its one open-time channel positioning it performs no
per-page `FileChannel.position()` query, allocates no physical-position record/observer, and updates
no proof or index-byte accounting.

Across all segments, boundary selection deduplicates candidates and retains the deterministic
bottom-hash 16,384 keys (1,024 per range at the supported 16-range maximum). This whole-run cap makes
retained boundary state independent of segment
count and input order; it can change range balance only, never key inclusion or global ordering.

The parallel boundary policy is a resume-free run setting. `distinct` is the default and preserves
the existing evenly spaced split indices over that capped candidate set. The default-off `rows` arm
streams each validated type-2/type-3 entry region one segment at a time, assigns each interval's positive
`cumulativeEntries` delta to its retained predecessor candidate, and holds only one
`long[candidateCount]` histogram plus the cursor's current entry. It chooses strictly increasing
candidate indices whose prefix masses are nearest the global row quantiles, constraining later
indices so every requested boundary remains distinct. Thus peak policy state is `O(candidateCap)`,
not `O(segments × candidateCap)`, and bottom-hash candidates remain the sole retained key set.
These are approximate mass boundaries: a stride groups several physical pages at its sampled
minimum, and page ranges may overlap. Exact per-row range filtering still owns correctness and keeps
all rows with one raw key in one range. If any original is extensionless, type 1, invalid/unknown,
or mixed with another input kind, the whole `rows` arm falls back to the unchanged `distinct`
selector and emits one exact fallback classification; it never combines weighted and unweighted
denominators. No evidence in this change promotes `rows` to the default.

For segment `S`, let `start_r(S)` be range `r`'s planned page ordinal. Type-2 starts are sampled
ordinals selected by monotone `prefixMax`; absent/type-1/invalid indexes use ordinal 0 for every
range. The physical proof zones are `[start_r,start_(r+1))`, with the last ending at
`totalRecords`. Starts are non-decreasing; repeated starts are explicit empty zones. Before a range
stops at its exclusive high key it necessarily reads through the next range's start, so the owner of
each non-empty zone CRC-validates and structurally parses all of its pages without a separate pass.

Each range returns exactly one primitive topology summary plus a temporary exact-key proof spool.
Variable minima/maxima are never retained in a `segments × ranges` heap matrix: each range keeps
three reusable fixed key buffers (last minimum, zone maximum, and rolling sample prefix), and the
coordinator consumes one spooled segment/range summary at a time. Additional proof peak is therefore
`O(segments × R)` primitives plus `O(R)` heap key material, while all comparisons remain byte-exact
(no hash-only proof). The writer first materializes the fixed-slot extent sequentially and forces
its allocation, so insufficient disk is an ordinary constructor failure rather than a mapped-write
SIGBUS. The file is then mapped through a shared foreign-memory arena while
workers update disjoint absolute slots; source switches copy through the reusable range buffers
without positional channel calls or per-key buffer allocation. Every mapped field/key update and
read remains counted; this work can scale with page/source-switch count even though positional
syscalls do not. The arena is
closed deterministically before the read mapping and again before delete; offsets and mapping size
remain `long`; an actual sparse mapping above 2 GiB is touched at both ends, unmapped, and deleted in
the compatibility test. Mapped pages are file-backed but contribute to process RSS while resident:
the opt-in touched-mapping characterization and ordinary peak-RSS meter are the memory evidence, and
no memory-neutral claim follows from the `O(R)` heap-key bound. Preallocation polls cancellation and
marks progress per at-most-64-KiB chunk. Allocation/map failure records attempted work, emits the
stable `proof_spool_allocation_failed` classification, cleans the path, and stays a checked
`IOException`; a pre-latched interrupt or `ClosedByInterruptException` during write/force/map keeps
the interrupt, publishes attempted work, cleans the path, and translates to merge cancellation
without that failure reason. The coordinator reads and requires the fixed slot's reserved four-byte
field to remain zero, so its mapped-byte accounting covers all 56 fixed bytes. Spools use one shared
open descriptor for the whole range fleet. That descriptor is an explicit
one-FD reservation in both the effective-range clamp and the dynamic output-writer allowance, not
generic process headroom. Its exact `ranges × original segments × 6,212` extent is charged to the
configured merge budget before ranges are admitted, and the read-only coordinator requires that
exact file size before mapping. On proof failure the spool joins range/cascade temporaries in the
pre-publication cleanup. After successful proof the transform owns the verified spool until it
either deletes it on a later pre-commit failure or hands it to publisher-owned disposable state.
The authoritative dataset is then committed first and spool deletion occurs in post-publication
cleanup, so an unlink failure becomes cleanup-pending rather than forcing the merge to repeat.

Merge-start disk admission runs after the validated ownership scope has swept only canonical
disposable proof/range/cascade/output temporaries from an earlier attempt, but before it allocates
any new proof or output file. Let `S` be the exact current bytes of checkpoint-owned original
segments and `P(R)` the exact proof extent for candidate range count `R`. The existing 3×-staged
headroom is decomposed into named estimates of `S` for final output, `S` for live cascade
intermediates, and `S` for staged-size/encoding variance; these estimates are policy reserves, not
claims that final Parquet bytes are knowable exactly from compressed PageRuns. On one shared
filesystem the required free bytes are `P(R) + max(3S, 1 GiB)`. If staging and output resolve to
different `FileStore`s, each keeps its own safety allowance. Final Parquet temporaries are written
under staging before their move, so staging still requires `P(R) + max(3S, 1 GiB)` (final temporary,
cascade, and safety), while output requires `max(2S, 1 GiB)` (the copied/moved final plus safety).
The cross-store fallback is not an atomic rename; `_SUCCESS` remains the authority boundary and
pre-publication resume cleanup repairs any partial copy. An unknown usable-space query fails open,
preserving the earlier disk guard's behavior.

Filesystem admission is a separate pass after heap/FD/proof-budget planning. It decrements `R`
until the exact proof extent and policy reserves fit, recording `SORT.merge_range_disk_limited`; if
only `R=1` fits, the untouched serial merge runs without a proof spool. If the serial reserves do
not fit, the transform refuses before merge with `error_class=sort_disk_exhausted`, leaves the
checkpoint merge-pending and resumable, and allocates no proof/output state. Parallel execution
samples usable space again after actual boundaries and seek planning, immediately before proof
file creation/zero-fill; a changed filesystem that no longer admits that exact range count refuses
there. `sort.ignore-disk-check=on` is explicitly threaded into the core policy and bypasses both
samples, as well as the startup/listing guards.

The coordinator requires the independently planned range count even for an empty segment set and
rejects missing, extra, out-of-range, or duplicate range summaries. It chains zones from
`HEADER_BYTES` to `trailerStart`, checks claimed
cumulative seams against prior physical totals, checks cross-zone min monotonicity, verifies every
sampled ordinal/offset/cumulative/minimum/prefix maximum, and anchors total pages/entries/framed bytes
plus first minimum/global maximum to the fixed trailer. Because a structurally valid type-2/type-3 block
already requires `finalPrefixMax == segMaxKey`, that last body/trailer comparison also anchors the
final prefix maximum. A sample/seek disagreement is `page_run_index_mismatch`. A physical-zone seam
or tiling disagreement is index mismatch only when one of that zone's usable sparse-index seams
participated; extensionless, type-1, and rejected-index inputs remain
  `page_run_body_corruption`, as do body/trailer total or bound disagreements. An actual min
  regression remains `page_run_min_regression`; an ascending-min overlap is
  `page_run_page_overlap`. The same check is applied across indexed physical-zone seams.

The coordinator performs this proof before returning the ranges' still-open writers. Cancellation
is polled during planning and proof. Any worker or post-worker proof failure closes writers after
worker quiescence and sweeps range/cascade temporaries; no manifest, state, or success marker is
published. Independently, every final merge compares the sum of validated original trailer entries,
the rows drained by the merge, and the sum reported by all closed final writers before the first
stale-final sweep. The adjacent-row guard also rejects a comparator regression as fatal
`error_class=sort_output_order_regression`: replaying the same durable inputs cannot repair a
deterministic ordering invariant failure, so it is classified but deliberately not merge-pending.
Cardinality disagreement is likewise fatal/classified as
`error_class=sort_output_cardinality_mismatch` and emits
  `SORT.sort_output_cardinality_mismatch` once. After all parallel final parts close and before any
  stale-final sweep or rename, the publisher also requires each non-empty part's raw-byte maximum to
  be strictly below the next part's raw-byte minimum. These bounds are retained as `byte[]` alongside
  the manifest's UTF-8 display strings, so malformed fixture bytes cannot collapse through replacement
  decoding and evade the check; failure is typed `sort_output_order_regression`. Live `OBJECTS` final drains reject adjacent equal raw
keys (`sort_duplicate_key`); the dormant `VERSIONS` mode retains equal raw-key groups under the full
comparator. Any disagreement refuses publication. Successful parallel merges emit
`SORT.merge_zone_proof_complete` once plus exactly one
`SORT.merge_zone_proof_page_ranges_disjoint|overlap` classification derived from the bounds already
verified; per-range logs
carry `pages_seeked_over`, logical framed `bytes_read` (every page frame read by that range,
including cascade intermediates), and exact worker `index_bytes_read` alongside the existing page
counts. `summary.json.sort.merge_range_framed_bytes` is the cumulative run total of those frame
bytes; it is zero on serial merges.

---

## 7. Config defaults (single source of truth)

| Knob | Default | Notes |
| --- | --- | --- |
| `--concurrency` = `Tmax` | 64 | the AIMD **ceiling**; the live concurrency `T` ∈ [1, `Tmax`], starts at `min(4, Tmax)` (slow-start ramp) and is lowered/raised by AIMD (algorithms.md §5) |
| HTTP client `maxConnections` | `Tmax + 16` | built **once** from the configured `--concurrency` ceiling `Tmax` (not live AIMD `T`); **must exceed `T`** or it silently caps concurrency |
| `--object-listing-queue-size` | 50_000 **entries** | a `PageBatch` is admitted while in-flight channel weight < cap; ordinary batches weigh their entry count and zero-row node-completion control batches weigh one, so control traffic is bounded too. Budget ≈ cap × (max\_key\_len + ~200 B fixed per-entry overhead: etag, storageClass, versionId, owner, checksum strings + 11-field record header) × #queues; admission is at `PageBatch` granularity (≤ 1000 entries), so each queue may transiently overshoot the entry cap by up to one page |
| page batch size | one S3 page (≤1000) | pipeline granularity |
| seed delimiter levels | adaptive | a shallow `delimiter=/` seed that starts at the top level and **adaptively descends narrow sub-levels** while cut-point and probe budgets permit; a truncated flat-wide level is radix-banded rather than descended (algorithms.md §8) |
| steal probe | `max_keys=1` | one key per split attempt |
| AIMD | ×0.7 down on 503 / +1 up per clean 10 s | the 0.7/+1 numbers are DECIDED. The 10 s clean-window cool-down is re-armed **only by a REAL reduction** (`T` actually lowered); a **floor no-op** decrease (`floor(0.7·T) >= T`, i.e. `T` already at the floor) still casts its AIMD vote, still latches congestion, and still pauses stealing, but no longer resets the clean window. The re-arm write is **monotonic** (`max` with the prior timestamp): a concurrent, stale-timestamped decrease can never shorten an already-armed window. (algorithms.md §5) |
| Parquet writers | 3 (range 2–64; 5–64 heap-admitted) | decoupled from `T`; counts above the measured four-writer envelope must pass the §7.2 maximum-heap plan |
| Parquet part target size | 256 MB | rotate by size |
| `--text-writers` | 3 (range 2–64) | bounded TSV/JSONL directory writer lanes; decoupled from `T` and from Parquet's row-group memory policy |
| `--text-part-size` | 256 MB | per-lane TSV/JSONL part rotation target; zero is rejected |
| `--writeback-size` | disabled (`0`) | optional writeback-shaping cadence for physical bytes already emitted to an OPEN TSV/JSONL/direct-Parquet dataset part or sorted final Parquet file; positive values below 4 MB and unsupported output paths are rejected. A periodic data sync never closes/rotates the part, forces its directory, exposes digest metadata, advances a checkpoint, stamps a footer, or makes it publishable; **it does not shorten the crash-recovery window**. Final close retains the full file+parent durability boundary (I6). Physical post-compression bytes drive the cadence. Text adapters do not flush their codec; direct and sorted-final Parquet flush only their 4 KiB transport buffer and therefore can engage only after parquet-mr naturally emits a completed row group — they never flush a row group/page/column store. `--text-part-size`, `--parquet-part-size`, and sorted-final rolling retain their existing logical meanings. PageRun staging, cascade intermediates, single-file output, and merge concurrency/fan-in remain outside this policy. Disabling row/time rotation to obtain size-only direct Parquet parts can widen checkpoint RPO; writeback does not compensate for that. |
| `--compression` | `none` unless a text file suffix implies gzip/Zstandard | table/TSV/JSONL streams and TSV/JSONL dataset parts only; Parquet rejects it |
| `--part-rotation-interval` | 30 s | rotate a lane's open part by time too, even below the size target, so `durable_cursor` advances on a bounded cadence instead of only when a part happens to fill up; `0`/`none` disables; forced to `0` for a single-file `-o *.parquet` destination; a positive value below the 100 ms minimum is rejected (spin-storm guard — see below); `ParquetWriterPool` additionally floors its idle-lane poll wait at 50 ms as defense-in-depth |
| `--part-rotation-max-rows` | 2_000_000 | rotate by row count too, for bursts fast enough to write millions of small rows well inside the time interval; `0` disables; forced to `0` for a single-file `-o *.parquet` destination |
| `parquet.block.size` / `page.size` | 64 MB / 1 MB | **pinned**, measured in the PERF gate. block.size chosen so the §7.2 active-buffer Parquet heap budget holds for the current 100,000-key test at the four-writer release ceiling. This is not an N-independent whole-run heap claim: finalized-part metadata is `O(parts)`. Raising it toward 128 MB improves compression/scan but risks the measured budget — re-measure if you do. |
| `--request-rate` | unset | Bucket4j; cancellable acquire |
| SDK retry attempts / initial backoff (**internal constants — not CLI flags**) | 1 / 100 ms | `S3Config.DEFAULT_MAX_ATTEMPTS = 1` disables SDK-internal retry: swath's own gauge-aware fetch loop is the sole retrier, so the AIMD `ConcurrencyGauge` sees every real 503/5xx immediately instead of after the SDK silently absorbed several behind its own backoff. There is **no** `--aws-max-attempts` / `--initial-backoff-ms` Picocli option in v1.0 (the `S3Config.maxAttempts` plumbing exists but is not CLI-wired) — exposing them is a planned follow-up |
| page-timeout retry budget | per-fetch bounded retry, cap 8 (`MAX_TRANSIENT_RETRIES`), resets each fetch; disposition on exhaustion depends on `RetryPolicy` (see below) | `apiCallAttemptTimeout` is the per-attempt **timeout duration** (not a count; **10 s base for scan-class worker, seed, and delimiter/structure requests**, **3 s for point-class pivot/one-key probes** unless an escalation override raises it, with each class doubling from its own base per-fetch on consecutive attempt-timeouts of the SAME logical fetch — `apiCallAttemptTimeoutOverride` in §2 and [`probe-budgets.md`](probe-budgets.md)). Above the per-attempt budget the SDK client also enforces a **60 s overall `apiCallTimeout`** (`S3Config.DEFAULT_API_CALL_TIMEOUT`, the primary liveness ceiling on a wedged logical call). `WorkStealingScan.GaugedFetcher` (and, on the seed/sequential paths, `TransientRetryFetcher`) retries a **non-AIMD-voting** transient (`ThrottleException.Kind.ATTEMPT_TIMEOUT` / `NETWORK` — a client-side attempt-timeout or exhausted network fault, neither of which is genuine S3 backpressure; `NETWORK` also covers a client-local socket-closure / `IOException`-wrapper fault that escaped the SDK call as a non-`SdkException` `RuntimeException` such as `UncheckedIOException(SocketException("Socket closed"))`, reclassified transient rather than escaping raw as an exit-1 / `error_class=unknown` crash) up to `MAX_TRANSIENT_RETRIES = 8` times with jittered exponential backoff; the one seed-time exception is the explicitly seed-scoped decorator wired for a fresh run: if its cause chain contains `ConnectException` or `UnknownHostException`, it fails on its first swath-owned attempt because that invocation's endpoint configuration cannot work. This policy is not inferred from delimiter request fields, which mid-run structure probes also use. The counter is **per invocation of `fetchPage`** (i.e. per attempted page/probe fetch), not a cross-fetch/cross-node consecutive count (it does not persist across separate `fetchPage` calls the way a per-node counter would). **What happens once the cap is crossed depends on `RetryPolicy`, resolved once at CLI wiring time from whether a real `LivenessWatchdog` is armed** — the fix for the tail-stall that killed long-running large listings, where this cap (not the watchdog) was a second liveness policy that always won the race to end the run: under **`RIDE_OUT`** (a real watchdog is armed, the default) the cap **no longer cancels the run** — the fetch keeps retrying indefinitely (raised full-jitter backoff ceiling, 5 s→15 s, recording `TRANSIENT.storm_ride_out`) and the watchdog alone owns liveness death (crash-only, resumable exit-75); under **`BOUNDED`** (both watchdog windows disabled by flags — `LivenessWatchdog.arm()` returned its no-op, so nothing else could ever stop an unbounded retry) exhaustion keeps the legacy disposition: the fetch trips the run's cancellation with `StopReason.STUCK` (attributing `CancelSource.TRANSIENT_RETRY_CAP`, recording `TRANSIENT.retry_cap_stuck`) and aborts via `CancelledException` — the **resumable exit-75 (`EX_TEMPFAIL`) disposition**, the same code as a watchdog stop — so the checkpoint stays valid and `swath resume` can safely retry the bucket later, rather than escaping as a fatal `ListingException` (exit 1) that the CLI's guarded engine dispatch would mark `run_meta.fatal_error` and thereby **poison `swath resume`**; a fetch with **no `CancellationToken` wired** (degenerate/embedded use) is unaffected by `RetryPolicy` and stays count-bounded regardless, escaping as the fatal `ListingException` contract on exhaustion. The run records `stop_source`/`error_class` marker fields that this disposition drives. Either disposition aborts (or, under `RIDE_OUT`, never aborts) the whole run, never a selective "fail the node". The thief's structure/pivot **probe** fetches (`slotGated=false`) are exempt from this policy: they use a separate small fixed cap (`PROBE_TRANSIENT_RETRY_CAP = 1`), never cancel the run, and simply return the probe to its non-productive retry flow. A genuine AIMD-voting throttle (`SLOWDOWN` / `SERVER_5XX`, real 503/5xx) is retried **unbounded** by this counter regardless of `RetryPolicy` — AIMD's own multiplicative decrease paces it instead, bounded only by cancellation/`--max-duration` (the liveness contract) |
| `swath.sort.segment-bytes` | heap-adaptive: ≈8% of `Runtime.maxMemory()` estimated pre-encode bytes, floored at 64 MB | primary segment-flush gate (§6); ~160 MB at `-Xmx2g` ⇒ ~1.3M-row segments, ~5M-row at `-Xmx8g`; bigger heap ⇒ fewer, bigger segments ⇒ single-pass merge as the design point. Active segment buffers are a function of `-Xmx`; retained staging metadata is separately `O(segments)`. |
| `swath.sort.segment-entries` | secondary cap alongside `segment-bytes` | backstop entry-count cap on a sealed buffer |
| `swath.sort.heap-fraction` | `0.08` | the adaptive ratio `segment-bytes` derives from `Runtime.maxMemory()`; raise only after measurement, never unattended |
| `swath.sort.buffers` | 2 | in-flight sealed buffers (fill buffer while the sealed buffer encodes off-thread); **must be `>= 2`**: `SortLane` bounds live sealed buffers to exactly `buffers` (fill + `buffers - 1` off-thread); `buffers=1` would either deadlock (0 off-thread slots to hand a sealed buffer to) or, if floored instead, silently allow 2 live buffers while claiming a cap of 1 — `SortConfig` rejects `buffers < 2` outright (`IllegalArgumentException`), consistent with every other knob's validation in that immutable snapshot |
| `swath.sort.fan-in` | 10000 | merge fan-in `F` (§6); open page-run segment readers never exceed `F` per pass. The pass width actually used is clamped at runtime by (a) the **fd budget** — `min(fan-in, usable-fds)` derived from `ulimit -n` with headroom — and (b) the **per-open-stream capacity plan**, `effectiveFanIn = min(fan-in, max(2, merge-budget-bytes / merge-per-stream-bytes))`. `fan-in` alone is a correctness/fd ceiling, not a memory promise; raise `ulimit -n` (below) so the fd clamp does not force a cascade |
| `swath.sort.segment-codec` | `ZSTD1` | payload compression for page-run STAGING segments — `NONE` \| `LZ4` \| `ZSTD1`. Trades staging-disk ratio for pack/merge CPU: `LZ4` is faster; `ZSTD1` (default) is smaller on disk; `NONE` skips compression. Governs staging only, never the final Parquet output |
| `swath.sort.merge-per-stream-bytes` | ≈64 KiB configured floor (`DEFAULT_MERGE_PER_STREAM_BYTES`) | runtime planning price for one normal open page-run stream. For a fully validated parallel type-3 input the planner uses `max(configured floor, 2 × maxRecordLen + maxRawPayloadLength + 2 × persisted-dictionary-coordinate reserve)`: a decoded current page retains its encoded body, raw payload, and dictionary coordinates while its successor frontier retains another encoded body/coordinate set. Serial/no-boundary, legacy, and extensionless inputs have no trusted kickoff decoded maximum, so they keep the floor/encoded/coordinate price and the runtime guard checks actual header claims before allocation. Cascade intermediates are split to the admitted raw-page ceiling and their actual trailer `maxRecordLen` joins later-pass base reservation. |
| `swath.sort.final-file-bytes` | 1 GiB | soft roll target for multi-file sorted output — after a part reaches the target, rotation waits until the next distinct raw key so an equal-key/version group never straddles files. Parts are strictly key-disjoint and named in key order; one key with many versions can exceed the target by the size of that indivisible group. The experimental pipeline may also roll earlier when its heap-admitted reference cap binds. The wait is streaming/O(1) in rows, and each deferred group records `SORT.final_roll_equal_key_deferred` once |
| `swath.sort.final-row-group-bytes` | ≈4–8 MB | the served file's seek granularity (row-group size) |
| `swath.sort.final-page-rows` | 1024 | the served file's seek granularity WITHIN a row group: the cap on a data page's rows. A page is Parquet's smallest addressable unit — the page index prunes pages, never rows, and every encoding decodes strictly forward — so this is the floor on what a bounded key-range read decodes per column, however few rows it wanted. Parquet's own default caps a page at 20,000 rows and 1 MB, and the byte cap only binds on columns wide enough to reach it, so every narrow column sat at 20,000. Governs FINAL Parquet only; custom page-run staging has no Parquet pages or row groups. Not to be confused with the 1 MB data-page BYTE cap, which two independent gates (2026-07-04 P1/P4) measured dead in both directions |
| `swath.sort.merge-budget-bytes` | heap-adaptive: same shape as `segment-bytes` (≈8% of `Runtime.maxMemory()`, floored at 64 MB) | runtime page-run residency budget. For a candidate parallel range count, exact proof backing (`ranges × original segments × 6,212`) is charged first and the remainder prices normal streams. Before cursor construction/decompression, the page-aware merger reserves the current retained body, its conservative 128-byte dictionary-coordinate heap, the complete possible lazy dictionary cache, and any separate raw payload after the already-budgeted successor frontier/body coordinates were loaded; legal overlap clusters cannot grow beyond the serial budget or one range's post-proof share. The static config helper still exposes a two-stream floor, but runtime admission refuses resumably if the truthful minimum width cannot fit. Arbitrary non-page-frontier capture merges retain their existing entry-stream policy. |
| `swath.sort.merge-parallelism` | `max(1, min(8, availableProcessors / 2))` | configured maximum pipeline encoder count; `1` is the explicit single-encoder choice. Both the CLI tune and core configuration enforce `1..16`. The tune is resume-free because final parts remain disposable staging files until the complete manifest barrier. Admission reserves shared input channels and one writer descriptor per encoder, prices scanner/frontier and router/queued/executing-plan references at the catalog's exact key-length bound, one transient body read, one dictionary-safe retained page, and an 8 MiB writer per encoder, and divides its retained-byte cluster budget across admitted lanes. It may lower the runtime plan cap before lowering encoder count. Clamps record `SORT.pipeline_encoders_fd_clamped` or `SORT.pipeline_encoders_heap_clamped`; a missing writer floor records `SORT.pipeline_encoders_fd_floor_exhausted`. |
| `swath.sort.segment-format` | `page-run` | the staging-segment format string new `--sort` runs stage under and tag `part_file` rows with (`ListRunner.SORT_SEGMENT_FORMAT`), alongside the actual page-run header version and trailer-extension type; a resume refuses another format or explicit unknown page-run metadata while preserving pre-column `NULL` metadata (§6) — informational, not user-tunable |
| `ulimit -n` (OS, not a swath knob) | raise to ~65536 for single-pass | with fan-in 10000, a single merge pass opens up to ~`min(segments, fan-in)` page-run readers at once; a low `ulimit -n` forces the fd clamp to shrink `effectiveFanIn` and **degrade to a multi-pass cascade**. Raise the soft limit (`ulimit -n 65536`, or the launcher does it) for single-pass merges on large buckets |
| `--checkpoint` | `auto` (co-located at `<dir>/.swath/checkpoint.sqlite` for a managed Parquet directory; ephemeral for stdout), deleted on clean completion | FILE kind and TSV/JSONL directories accept only `none`; `none` ⇒ in-memory worklist and **no resume**. An explicit path is valid only with a Parquet directory, but the public `swath resume` command opens the managed co-located layout, not an arbitrary SQLite path. |

### 7.1 Router thresholds (the single-engine router)

> **Superseded by reality:** this
> subsection describes the **target** multi-engine design, not what ships.
> The current build has a single `WorkStealingScan` engine plus
> `EngineToggles` — there is no `--strategy` flag and no shape-probing router
> wired in. The historical table and prose below are retained deliberately
> (as the target design record) and are not being deleted or reworded.

The router (the `inspect` probe — one `ListObjectsV2` with `delimiter=/`,
`max-keys=1000`) selects exactly one engine; these are the authoritative
cutoffs (there is no "4-common-prefix" rule — that belonged to the retired
multi-strategy router):

| Condition | Engine |
| --- | --- |
| bucket is Express One Zone (`--x-s3`) and not `--allow-parallel-…` | `SEQUENTIAL` |
| **tiny** — `IsTruncated == false` **AND** `CommonPrefixes` is empty. (With a `delimiter=/` probe, `IsTruncated==false` only means the *top-level delimited view* fits one page — a bucket with 5 prefixes and 500 M objects returns 5 `CommonPrefixes` with `IsTruncated==false`; requiring `CommonPrefixes` empty ensures every returned entry is a top-level object, i.e. ≤ 1000 objects exist in total.) | `SEQUENTIAL` |
| otherwise | `WorkStealingScan` (seeded from the probe's common prefixes) |
| (an opaque-marker-only store) | `PREFIX`-partition (recursive); dormant in v1 (S3) |

`--strategy auto` applies this table; `--strategy scan|sequential`
pins the engine (and `scan → WorkStealingScan`).

### 7.2 Peak-heap budget per output format (PERF gate)

The Parquet release gate measures the **four-writer release envelope** (not the retired per-worker
model). The non-Parquet row describes its default configuration:

| Output | Budget (default config) | Composition |
| --- | --- | --- |
| stdout / TSV / JSONL / table | **< 256 MB** | default writer count, bounded queues + JVM baseline; expert text counts also add platform-thread stacks, compression state, parts, and manifest work and must be measured separately |
| Parquet | **< 1 GB** (the bound PERF-2 asserts at 100,000 keys, `ParquetPerf2Test`) | Four active writers × 64 MB row groups + parquet-mr overshoot + the production-derived queue share + baseline. parquet-mr buffers the **uncompressed** row group, so measure the actual peak. The implementation has no per-object heap accumulation (I11), but the public PERF-2 measurement covers 100,000 keys, not billion-object scale. |

The default is still three writers. Counts 2–4 are accepted as the measured compatibility
envelope. An expert request for 5–64 writers is admitted only when `Runtime.maxMemory()` covers the
planning floor `256 MiB + writers × 64 MiB × 4`; this conservative gate is reported in
`dataset_writer` and prevents a high count on a small heap, but it is not a promise that an admitted
workload will remain below that number. The process-resource ceiling of 64 also bounds platform
threads, open parts, and the saturated-path lane scan. The whole pool retains at most 256 queued
batches in production: counts 1–4 preserve the existing 64 slots per lane, while higher counts
divide the fixed ceiling and may leave a few slots unused. `Runtime.maxMemory()` is a heap ceiling,
not a process-RSS or CPU admission model; an explicit JVM heap must still fit inside the container or
host limit, and the operator must measure CPU, RSS, part count, queue blocking, and manifest time.

Lower the active-buffer budget by reducing `parquet.block.size`, writer count,
or `T`. I11 does not erase metadata growth: finalized parts contribute
`O(parts)`, and a sorted run contributes `O(segments)` staging metadata.

**Large Parquet listings:** the write path has no intentional per-object heap
accumulation — entries stream through the bounded object-listing queue, each
writer buffers one bounded row group, parts rotate and free their buffers, and
the split tree + cursors are SQLite-resident. That structure is the basis of I11;
the current public PERF-2 gate establishes the stated heap ceiling only for its
100,000-key workload. Re-measure at larger scales before claiming an identical
peak or prescribing `-Xmx` headroom for a billion-object run.

These exist so they are not re-litigated per PR; change with a benchmark, not
a guess.
