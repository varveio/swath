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
public record ObjectEntry(KeyBytes key, long size, long lastModifiedEpochMicros,
                          String etag, String storageClass, String versionId /*nullable*/,
                          boolean isLatest, String ownerId /*nullable*/,
                          String ownerDisplayName /*nullable*/,
                          String checksumAlgorithm /*nullable*/,
                          String checksumType /*nullable*/) implements ListEntry {}
public record CommonPrefixEntry(KeyBytes key) implements ListEntry {}   // key = the prefix
public record DeleteMarkerEntry(KeyBytes key, String versionId, boolean isLatest,
                                long lastModifiedEpochMicros, String ownerId) implements ListEntry {}
```

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
`entryCount()` is the queue-budget weight (I11). Channel close/failure is carried
by a sealed envelope, never by polluting `ListEntry`:

```java
public record PageBatch(long nodeId, long pageSeq, List<ListEntry> entries, PackedPage packed) {}
// exactly-one-of(entries, packed) enforced by the canonical constructor.
// Convenience: 3-arg PageBatch(nodeId, pageSeq, entries) (packed=null) and
// static PageBatch.ofPacked(nodeId, pageSeq, packed) (entries=null).

// PackedPage (swath-model seam): the drain-thread view of a packed page — entryCount,
// objectCount, commonPrefixCount, deleteMarkerCount, totalObjectSize — without decoding
// the payload; the sort package downcasts to recover the concrete packed block.
public interface PackedPage {
    long entryCount(); long objectCount(); long commonPrefixCount();
    long deleteMarkerCount(); long totalObjectSize();
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

> **Superseded by reality, like §7.1:** the **router**, the
> **`Store`** URI-scheme abstraction, and the **`Strategy`** engine abstraction
> described below as authoritative were part of the target multi-engine design
> and are **not built**. The current build has a single `WorkStealingScan` engine
> (plus `EngineToggles`); there is no router selecting an engine from declared
> capabilities, no `Store` interface, and no `Strategy` interface. `PageFetcher`,
> `StoreCapabilities`, `PageRequest`, `ListPage`, `CheckpointStore`, `Filter`,
> `EntryFormatter`, and `Scope` below are implemented internal seams consumed by
> the CLI. v0.1 is CLI-only: none is a supported Java API or SPI, and source or
> binary compatibility is not promised. `StoreCapabilities` is consumed today
> only by `RangeScanner` (for `maxKeysCap`) — not by any router.

```java
// INTERNAL SEAM — NOT sealed (sealing would drag the test mock into the prod permits list).
// Unsupported in v0.1; the CLI ships S3PageFetcher only (plus the test MockPageFetcher).
public interface PageFetcher {
    ListPage fetchPage(PageRequest req) throws ListingException, InterruptedException;
    StoreCapabilities capabilities();        // v0.1: read only by RangeScanner (maxKeysCap); no router
}
// StoreCapabilities is the declared-capability record. In the target design a router
// would pick the engine from it; in v0.1 there is no router and only maxKeysCap is read.
// v0.1 S3 declares supportsVersions=false until ListObjectVersions lands; the other
// S3 object-listing flags are true. StoreCapabilities.s3() = (true,true,true,false,true,1000,KEY).
public record StoreCapabilities(boolean supportsStartKey, boolean supportsRangeBounds,
                                boolean supportsDelimiter, boolean supportsVersions,
                                boolean guaranteesLexOrder, int maxKeysCap,
                                PaginationKind paginationKind /* KEY | OPAQUE_MARKER */) {}
// PLANNED / NOT BUILT: a `Store` URI-scheme → (PageFetcher + credentials, endpoint)
// abstraction (target design). v0.1 wires S3 directly; there is no Store interface.
public record PageRequest(ListingMode mode, int maxKeys, byte[] prefix, byte[] delimiter,
                          byte[] startAfter,        // OBJECTS pagination is purely startAfter=lastKey
                          byte[] endBefore,         // optional upper range bound for range-param stores (GCS startOffset/endOffset); null for S3
                          String continuationToken, // unused for OBJECTS (kept for marker/opaque stores)
                          byte[] keyMarker, String versionIdMarker,
                          int attemptTimeoutEscalationLevel) {}
                          // default 0 = the STORE's own base per-attempt budget for this request's
                          // call class (§7). Set by the retry loops (TransientRetryFetcher,
                          // WorkStealingScan.GaugedFetcher) on consecutive attempt-timeout faults
                          // of the SAME logical fetch, so a genuinely-slow tail page can complete
                          // instead of retrying forever at a budget it can never beat under
                          // maxAttempts=1. A LEVEL, deliberately not a Duration: retry POLICY (how
                          // many rungs, when to climb) is the engine's; what a rung is WORTH is the
                          // store's, because only the store knows each call class's base and call
                          // classes differ by more than a constant factor (a point lookup and a
                          // scan are not the same call). S3PageFetcher maps level to duration as
                          // base(callClass) * 2^level -- scan 10/20/40s, point 3/6/12s -- which is
                          // monotone by construction, so an escalation can never shrink a budget.
                          // Other stores may ignore it; does not touch args_hash or the checkpoint
                          // schema. Additive for serialization/checkpoint compatibility (a stored
                          // PageRequest predating this field still decodes, defaulting to 0) — NOT
                          // Java constructor binary compatibility: a record's canonical
                          // constructor's arity changes when this grows. That is acceptable for the
                          // unsupported v0.1 Java surface; out-of-tree callers have no guarantee.
public record ListPage(List<ListEntry> entries, List<KeyBytes> commonPrefixes, boolean truncated,
                       String nextContinuationToken, byte[] nextKeyMarker, String nextVersionIdMarker,
                       int httpStatus, java.time.Duration latency) {}

// PLANNED / NOT BUILT: a `Strategy` engine abstraction (target design). v1.0 runs a
// single WorkStealingScan engine directly; there is no Strategy interface.

// Pluggable: SqliteCheckpointStore now; a DynamoDbCheckpointStore later (multi-host).
// Sentinel: long SPLIT_ABORTED = -1L. All writes funnel to ONE checkpoint-writer thread
// (SQLite single-writer), so commitPage/splitNode/partFinalized serialize against each other.
public interface CheckpointStore extends AutoCloseable {
    RunMeta openRun(RunKey key, boolean resume, boolean restart)  // create or match by args_hash;
        throws CheckpointException, InvalidArgsException;         //   resume=true → load resumable nodes (error if no prior run);
                                                                  //   restart=true → discard prior run; mutual exclusion enforced here
    long insertNode(NodeSpec spec);              // returns node id
    List<Long> insertNodes(List<NodeSpec> specs); // ATOMIC all-or-nothing seed insert (I2): all nodes
                                                  //   of a seed set in one transaction or none. SeedMode NONE
                                                  //   skips this; SHALLOW/HINTS use it. Returns node ids.
    long countNodes(long runId);                 // total nodes in ANY state — the resume gate tells a
                                                  //   never-seeded run (0 nodes, I2 all-or-nothing) from a completed one
    List<Node> loadResumable(long runId, boolean fileSink);  // non-COMPLETED nodes; IN_PROGRESS->PENDING (keep cursor, I5).
                                                  //   fileSink=true (the Parquet exactly-once path): reset cursor:=durable_cursor and
                                                  //   reopen any COMPLETED-but-not-output-complete node (durable_cursor < cursor)
    void commitPage(PageCommit c);               // I1: cursor + status, one txn
    CompletableFuture<Void> commitPageAsync(PageCommit c);  // I1 async seam: awaited OUTSIDE the worker lock before emit
    long splitNode(SplitSpec s);                 // I4: narrow victim + insert child, one txn. SQL guard
                                                 // `(cursor IS NULL OR cursor < pivot) AND range_end IS oldHi AND status<>COMPLETED`
                                                 // (cursor IS NULL keeps a fresh root splittable; range_end IS is NULL-safe so
                                                 // the open frontier oldHi=NULL still matches). rowcount 0 → SPLIT_ABORTED → thief
                                                 // restores hi and retries (algorithms.md §4.3). The cursor clause can't fail by the
                                                 // cursor passing pivot single-process; the others reject a stale 2nd-thief split or
                                                 // an already-completed victim.
    void partFinalized(PartFinalize f);          // I6: insert finalized part_file row + advance each node's durable_cursor, one txn
    List<PartRef> finalizedParts(long runId);    // resume: seed manifest from finalized parts, discard the rest
    Set<String> finalizedPartFormats(long runId); // resume format-mismatch guard (§6): distinct staging formats of finalized parts
    void markOutputComplete(long runId);         // I6 latch: durable_cursor := cursor for COMPLETED nodes, only after pool.close()
    SortPhase sortPhase(long runId);             // --sort phase (LISTING/MERGING/PUBLISHED), or null for a non-sort run (§6)
    void setSortPhase(long runId, SortPhase phase);
    void markRunFinished(long runId, RunStatus status);  // terminal status (broken-pipe FAILED leaves fatal_error unset)
    void markRunFatalUnlessFinished(long runId);  // §5: CAS RUNNING→FAILED with fatal_error=1; never overwrites a run's own terminal status
    void markRunUnresumable(long runId);          // §5: →FAILED with fatal_error=1 from any state a resume admits; never downgrades COMPLETED
    void close();
}

public sealed interface Filter permits IncludeRegexFilter, ExcludeRegexFilter, SizeFilter,
                                       MtimeFilter, StorageClassFilter, ExpressionFilter {
    boolean matches(ListEntry e);
}
public sealed interface EntryFormatter permits ParquetFormatter, JsonlFormatter,
                                               TsvFormatter, AlignedFormatter {
    void writeHeader() throws IOException;
    void write(ListEntry e) throws IOException;
    void close() throws IOException;             // Parquet: writes footer ⇒ node becomes COMPLETE-able (I6)
}

// The in-house structured-concurrency helper — replaces StructuredTaskScope.
public final class Scope implements AutoCloseable { /* fork / joinAllOrThrow / close = shutdownNow+await */ }
```

### 2.1 The policy/executor split — view read timing, mutation timing, exactly-once engagements

`io.varve.swath.engine.policy` (`StealPolicy`/`ThiefPolicy`, `OwnerSplitPolicy`/`OwnerSplitGovernor`,
`FutilityPacingPolicy`/`IdleStealPacingPolicy`, `SeedPlanner`/`HybridSeedPlanner`) decides;
`io.varve.swath.engine` (`Thief`, `OwnerSelfSplit`, `WorkerState`, `IdleStealBackoff`, `SeedStep`) is
the only code that touches a lock, a clock, or the checkpoint CAS. The decision-trace goldens prove the two sides agree on *values*
(sequential replay, byte-identical). They say nothing about *when*, under real concurrency, a view's
fields were read relative to the lock scopes the pre-extraction monolith held them in — a policy
deciding on a stale or torn read costs a wasted probe or a wrong-but-recoverable counter (the CAS
re-validate in `commit()`/`maybeOwnerSelfSplit` still protects every tiling invariant, I1–I12), never
a gap/overlap. That failure mode has no other written home, which is itself the gap this section
closes.

**The source-agnostic rule (the one normative statement of it — every `io.varve.swath.engine.policy`
type's own javadoc cites this paragraph, not a copy of the rule).** Every view, decision, mutation,
and probe-outcome type in `io.varve.swath.engine.policy` is source-agnostic: no `WorkerState`
reference, no S3/protocol/wire type of any kind — keys as raw bytes, counts, booleans, and
policy-domain enums only. This is what lets the same shapes serve both the live engine and a future
discrete-event simulator without either one leaking into the other. Binding since before this
document's own §2.1 existed (the policy-package interfaces were designed to this rule from their
first commit); this paragraph is its one written, binding home in this repo — shipped javadoc must
not cite a rule a reader of the public repo cannot read, so no class below cites anything else for it.

**View construction — one pass, no lock, per view type:**

- **`VictimView`** (one per pool member, `Thief.steal`): `nodeId`/`lo`/`cursor`/`hi`/`keysEmitted`/
  `unsplittable`/`pacingSkipAvailable` are all read in a single unlocked pass over the pool, exactly
  as the pre-extraction monolith's selection loop read them — **before** `policy.selectVictim` runs
  over the resulting list.
- **`StealAttemptView`** (one per attempt, after `victim.snapshot()`): `cursor`/`hi` come from the
  snapshot (lock-guarded pair read, unchanged); `densityFraction`/`alphabetDigest`/
  `unchangedSinceNonProductiveSteal`/`keysEmitted`/`consecutiveZeroFanoutProbes`/
  `consecutiveTimedOutStructureProbes` are read once, up front, immediately after the snapshot —
  **before** `policy.beginAttempt` or any probe in this attempt has run.
- **`OwnerSplitView`** (one per self-split attempt, `OwnerSelfSplit.maybeOwnerSelfSplit`): every field
  is read once, entirely inside the caller's `ws.lock()` hold (`WorkStealingScan.runClaim` holds it
  across the whole method, unchanged from pre-extraction) — **before** `governor.decide` runs the
  whole gate chain.
- **Pacing has no `View` record at all — two mechanisms, two different reasons, two different
  shapes.** `WorkerState`'s per-victim futility counters (`consecutiveFutileSteals`/`futilityTrips`/
  `stealPacingSkips`) are lock-free `AtomicInteger`s two racing thieves can touch with no shared
  monitor at all (`recordFutileSteal`/`stealPaced`/`consumePacingSkip` are never called under
  `lock()`), so `FutilityPacingPolicy` reads and returns exactly **one** `int` at a time — the value
  each `AtomicInteger`'s own atomic op (`incrementAndGet`/`updateAndGet`) already produced — never a
  combined snapshot of the three. `IdleStealBackoff`'s fleet-wide pacing state, by contrast, is
  `synchronized` on every accessor, so its two former plain fields (`consecutiveNonProductive`,
  `nextAttemptNanos`) collapse safely into one immutable `IdleStealPacingState`, read and replaced as
  a whole inside the SAME `synchronized` method that used to touch both fields directly — the
  monitor, not field-level atomicity, supplies the guarantee there, so combining changes nothing.
  **The rule, restated: state whose transitions are independent per-field atomic read-modify-writes
  must not be collapsed into a single combined write-back; a combined read-only view is unaffected.**
  (An earlier version of this section stated the rule as "monitor-protected state may be combined;
  lock-free per-field atomics may not" — read literally, that phrasing would make `VictimView` itself
  non-compliant, since it combines four of `WorkerState`'s own lock-free per-field atomics
  (`cursor`/`hi`/`keysEmitted`/`unsplittable`) into one record; the phrasing is corrected here.)
  Checked against all four shapes this section describes: `VictimView` and `StealAttemptView` both
  only ever READ their lock-free source fields and never write a combined value back, so combining
  them into one record is safe by the restated rule; `OwnerSplitView` combines fields that are
  read-only from every OTHER thread's perspective (only the owning worker, under its own lock, ever
  writes them), so the same reasoning applies; the one field in the audit table below classified
  **WIDENED** (`pacingSkipAvailable`/`CONSUME_PACING_SKIP`) is exactly the read-modify-write case the
  restated rule forbids collapsing — pre-extraction, `stealPaced()` fused the check-and-decrement into
  one call; post-extraction, the check and the decrement are split across two separate points (view
  construction and post-selection mutation application), which is why that field, and only that
  field, earns WIDENED rather than unchanged.

  **A fifth mechanism, `ConcurrencyPolicy` — policy-owned and concurrent, not policy-owned because
  single-threaded.** Every justification above for the "no view, no mutations" bucket (the seed
  planner, below) rests on single-threadedness: seeding runs before any worker starts, so there is no
  other thread a wider read window could ever race against. `ConcurrencyPolicy` (the AIMD port,
  algorithms.md §5) is the opposite case: it is policy-owned (an interface in
  `io.varve.swath.engine.policy`, unwired to any production caller) **and** its real implementation
  (`ConcurrencyGauge`) is the single most contended, most timing-coupled mechanism in the engine —
  every method races under CAS against every worker's page fetches. Nothing in this section's audit
  table, and nothing `DecisionPathPurityTest` walks, checks a `ConcurrencyPolicy` implementation's
  concurrency discipline: the interface is never held as a field of any reachable policy type (its
  whole point is to be unwired), so the determinism-audit closure never reaches an implementation of
  it. A faithful port's purity/atomicity argument is the implementer's to make and have reviewed
  against algorithms.md §5, not something this contract or its enforcement test verifies.

  Neither policy reads `RunMetrics` or a lock directly. On ambient time specifically: `DecisionClock`
  is an executor-only seam — it is held as a field only by `IdleStealBackoff` (the executor), which
  reads it once per call site and passes the resulting `nowNanos` explicitly into
  `IdleStealPacingPolicy`'s methods. `IdleStealPacingPolicy` itself holds no `DecisionClock` reference
  and calls no clock at all; every one of its methods takes `nowNanos` as an ordinary `long` parameter
  (an earlier version of this section claimed `IdleStealPacingPolicy`'s ambient-time read "is injected
  via `DecisionClock`, mirrors `DecisionRng`" — neither half of that is accurate, and both are
  corrected here). `DecisionRng` is the one true parallel to `DecisionClock`: a policy-package class
  (`ThiefPolicy`) holds it as a field and calls it for a value. `IdleStealPacingPolicy` never holds or
  calls anything ambient — it already is the "instant-per-call" idiom, not a predecessor
  `ConcurrencyPolicy` improves on (`ConcurrencyPolicy.java`'s own javadoc claimed to push the
  discipline "one step further — an instant per call, not a clock to call", which is also corrected
  there). Three idioms, accurately: `DecisionRng` (an injected interface a policy calls for a value),
  `DecisionClock` (an injected interface only the executor calls, never a policy), and the
  `atNanos`/`nowNanos`-parameter idiom `IdleStealPacingPolicy` and `ConcurrencyPolicy` both already use
  (no interface to hold or call at all — the instant arrives with the signal).
- **The seed planner has no `View` at all — a third shape, for a third reason.** `SeedDescent`
  (`HybridSeedPlanner`, algorithms.md §8) reads no live executor-owned state through a view of any
  kind: unlike `WorkerState`/`IdleStealBackoff` — fields concurrent threads touch, which the policy
  must never read directly — the descent's frontier, cut set, and probe/sample budget are private
  state the `SeedDescent` instance owns outright from construction to its terminal `SeedPlan`.
  Nothing else ever reads or writes them, because seeding runs single-threaded, entirely before any
  worker starts — there is no live field to snapshot into a view in the first place, not merely a
  view this extraction chose to omit. The one thing the executor (`SeedStep`) does still own and
  translate is the page itself: it decodes each `ListPage` into a `SeedProbeOutcome` (source-agnostic
  facts only — no `ListPage`/`KeyBytes`/`ListEntry` crosses the seam) before handing it to
  `SeedDescent#onProbeResult`, exactly mirroring how `Thief` translates a probe response into a
  `ProbeOutcome` — translating a page is executor mechanics either way; the difference is only that
  the seed descent's OWN state, unlike a victim's, is never shared, so there is nothing on the far
  side of that translation for a view to snapshot.

  **The general rule the three shapes above illustrate: state the executor owns is snapshotted into
  views and mutated only via the mutations a decision returns; state the policy owns outright needs
  neither.** A future reader should not harmonize `VictimView`/`StealAttemptView`/`OwnerSplitView`,
  the two pacing policies' plain-`int`-or-combined-record split, and the seed planner's view-less
  shape into one uniform pattern — each is the correct shape for a different concurrency reality, and
  collapsing them would misrepresent at least one.

**Mutation timing.** A `VictimMutation`/`OwnerSplitMutation` is never applied by the policy itself —
only the executor mutates a live `WorkerState`, via `Thief.applyMutations`/`OwnerSelfSplit.applyMutations`,
and only in response to a `mutations()` list the policy returned alongside its decision:

- Selection-scoped `VictimMutation`s (`selection.mutations()`) are applied once, immediately after
  `policy.selectVictim` returns — i.e. after the *entire* pool has been scanned, not immediately after
  the one candidate that triggered the mutation was examined.
- Per-attempt `VictimMutation`s (`action.mutations()`) are applied once per `StealAction`, immediately
  before that action's own probe/commit/retry is dispatched — never re-applied, since the executor's
  `while (true)` loop discards each `action` after acting on it exactly once (this is what makes the
  exactly-once rule below hold across `Retry`/abort paths too).
- The split-defining mutations — `WorkerState#narrowHi`/`restoreHi` and the `CheckpointStore#splitNode`
  CAS itself — are **not** policy `VictimMutation`s at all; they are executor-only, always issued
  inside `victim.lock()`/`ws.lock()` in `commit()`/`maybeOwnerSelfSplit`. Every policy-driven
  `VictimMutation` that runs *outside* a lock is cheap, per-victim `AtomicInteger`/boolean bookkeeping
  (pacing skips, futility/no-pivot/structure-probe tallies) — never a field the split CAS's guard
  clause reads.
- `OwnerSplitMutation.CONSUME_CONFETTI_PROBE_SLOT` **is WIDENED — an earlier version of this section
  claimed it was the one exception with no widened window at all, which is wrong and was retracted.**
  `governor.decide(view)` and `applyMutations(...)` do run back-to-back inside the same `ws.lock()`
  hold, but `ws.lock()` is **per-worker** while the mutation's target, `ConfettiFeedbackGate`, is
  **run-scoped** — two different owners self-splitting concurrently hold two *different* worker locks,
  so nothing serializes their consults against each other the way one shared lock would.
  **Issue #31 found what that costs and is now fixed:** N owners sharing a `probeSeq` snapshot at a
  slot boundary all decided PROBE and all CARVED, multiplying exactly the confetti-sized carves the
  gate exists to suppress — not merely shifting which consult landed on the slot. A probe carve now
  carries `CLAIM_CONFETTI_PROBE_SLOT` instead, and the executor resolves it against the run-scoped
  gate before recording anything, admitting exactly one carve per slot and suppressing the rest exactly
  as the pre-#22 fused `incrementAndGet()` did. `ConfettiFeedbackGate#claimProbeSlot` is a
  **consume-and-claim**, not a bare CAS: a `compareAndSet` against the snapshotted value elects the one
  winner, and a caller that loses that CAS then advances the sequence with an `incrementAndGet` of its
  own — so the sequence advances once per over-threshold consult, winner or loser, which is what makes
  a loser *suppressed* rather than *dropped*. (An earlier version of this bullet called it "a single
  `compareAndSet` on the snapshotted value", which contradicted the winner-or-loser sentence that
  followed it — a bare CAS would advance the sequence only for the winner. Caught by CodeRabbit on
  PR #34; the method's own javadoc always stated both halves.)
  **This does not re-couple `decide()` to interleaving.** The decision stays a pure function of its
  view; the conditionality is explicit *in* the decision rather than an executor override — the same
  shape as the split CAS's own `SPLIT_ABORTED` path, where a decided carve can still be declined by an
  atomic the executor owns. The read window itself (the audit table's `ConfettiObservation` row below)
  remains WIDENED, and remains a heuristic input: which page-commit's snapshot the rate check sees can
  still vary, which changes probe *cadence*, never carve *count*.
- The per-victim futility-pacing counters and the fleet-wide idle-steal pacing state are mutated by
  the EXECUTOR objects that have always owned them (`WorkerState`, `IdleStealBackoff`), at the
  identical call sites and under the identical (lock-free / monitor) discipline as before this
  extraction — `FutilityPacingPolicy`/`IdleStealPacingPolicy` never touch a live field themselves,
  only compute the next value(s) the caller then writes.
- `SeedAction` (`RequestSeedProbe`/`SeedPlan`) carries no mutation list at all — the flip side of the
  seed planner having no `View` (above): with no live executor-owned state to read, there is
  symmetrically nothing for the executor to mutate on the descent's behalf afterward. `SeedStep`
  applies only `engagements()`, never a mutation, at every step of its request/response loop.

**Engagements are exactly-once by the same mechanism.** `applyEngagements` is called at the identical
points `applyMutations` is (selection-scoped once per `steal()` call; per-attempt once per
`action.engagements()`) — every `Engagement` a `Selection`/`StealAction`/`OwnerSplitDecision` carries is
recorded exactly once, because the object carrying it is visited exactly once. `ThiefPolicy.Attempt`'s
internal `pendingEngagements`/`pendingMutations` (drained via `List.copyOf(...)` + `.clear()` on each
`StealAction` it hands back) prevent the policy itself from double-emitting across a multi-probe
cascade; the executor's single-application-per-action loop prevents the executor from doing so either,
including on every `Retry`/abort path (a retried attempt is a brand-new `Thief.steal()` call with a
brand-new view — it never replays a discarded action's engagements). `HybridSeedPlanner`'s `Descent`
carries the identical `pending`/`drain()` shape (a plain `List<Engagement>` rather than a pair, since
there is no mutation list to drain alongside it) to the same effect: every classification mark the
descent's explicit phase state machine fires — mid-descent, inside a `SAMPLE_CHILD`/`WEIGHT_SAMPLE`
sub-loop, or only once the whole run's shape is known at `finalizePlan` — is queued and delivered
exactly once, on whichever `SeedAction` (`RequestSeedProbe` or the terminal `SeedPlan`) is returned
next; `SeedStep`'s own request/response loop applies each action's `engagements()` exactly once, the
same discipline as `Thief`'s.

**Field-by-field audit against `main` (pre-extraction) — every widened window, whether or not benign.**
The seed planner has no row here: a widened window is a concurrency-timing question (could a
DIFFERENT thread's write land between when a view reads a field and when the field's live value is
next observed?), and the seed descent has no concurrency dimension to widen — it runs single-threaded,
entirely before any worker starts (algorithms.md §8), so there is no "other thread" a wider read
window could ever race against.

| View | Field(s) | Pre-extraction read timing | Post-extraction read timing | Classification |
|---|---|---|---|---|
| `VictimView` | `nodeId`,`lo`,`cursor`,`hi`,`keysEmitted`,`unsplittable` | read inline in the selection loop, but ONLY for candidates that had already passed the `unsplittable` gate, and (for `cursor`/`hi`/`keysEmitted`, needed only by `estRemaining`) also the `stealPaced` gate — a candidate failing either gate never had these fields read at all | unlocked, single pool pass, inline in the view-construction loop: every field is read for EVERY candidate, before `selectVictim` applies any gate | **read set widened to every candidate; pure reads, discarded for skipped candidates, gate order verified preserved (checked against `main`'s pre-extraction selection loop, which `continue`d past `unsplittable`/`stealPaced` before reading `cursor`/`hi`/`keysEmitted`) — benign** |
| `VictimView` | `pacingSkipAvailable` / `CONSUME_PACING_SKIP` | `stealPaced()`: check-and-decrement in **one** call, inline per-candidate — no other candidate's work intervenes between check and consume | check (`pacingSkipAvailable()`) at view-construction time; consume (`consumePacingSkip()`) applied only after `selectVictim` has scanned the **whole** pool | **WIDENED** — see below |
| `StealAttemptView` | `victimNodeId`,`lo` | read once, at attempt start, executor-local (the chosen victim's identity/immutable bound; never contended) | same | unchanged |
| `StealAttemptView` | `cursor`,`hi` (via `victim.snapshot()`) | lock-guarded pair read, same call site | lock-guarded pair read, same call site | unchanged |
| `StealAttemptView` | `densityFraction`,`alphabetDigest` | read live **once**, at a later, checks-gated point in the cascade (`densityFraction` inside the far-ahead-fraction computation; `alphabetDigest` at the pivot interpolation) — `recordAlphabetEngagement` afterward reuses the already-computed fraction/pivot, it does not re-read either field | the same single read now happens earlier, at view construction, immediately after the snapshot | narrowed (the one read moved earlier; still exactly one read either way) — and, since issue #30 (below), `alphabetDigest` is a frozen `AlphabetDigest.Snapshot` taken at that construction point rather than a reference to the live, concurrently-mutated digest |
| `StealAttemptView` | `unchangedSinceNonProductiveSteal(snap)` | pure comparison, immediately after the snapshot | same | unchanged |
| `StealAttemptView` | `keysEmitted` | not read by the per-attempt cascade in either version (only `VictimView.keysEmitted`, a distinct field, feeds `estRemaining` at selection) | same | unchanged (dead field either way) |
| `StealAttemptView` | `consecutiveZeroFanoutStructureProbes`,`consecutiveTimedOutStructureProbes` | read live at the structure-probe-suppression check, **after** this attempt's own far-ahead/step-back key-probe round trip(s) had already completed | cached at view construction, **before** any probe in this attempt has run; the structure-probe-suppression check consults the same cached value later | **WIDENED** — see below |
| `OwnerSplitView` | `hi`,`lo`,`keysEmitted`,`densityFraction`,`observedDensityRatio`,`alphabetDigest` | read once, inside `ws.lock()`; written only by the owning worker's own listing progress (no thief mutates a `WorkerState` it doesn't own) | same | unchanged (corrected: the record component and this row's field are `hi`, not `H` — an earlier version of this row misnamed it) |
| `OwnerSplitView` | `cursorTo`,`committed`,`lastSelfSplitPage` | read once, inside `ws.lock()`; `cursorTo` is this page-commit's just-advanced cursor, `committed`/`lastSelfSplitPage` are the owner-split rate-limit's caller-owned bookkeeping (`selfSplit[0]`/`selfSplit[1]` in the executor) — plain counts, never shared, never touched by another thread | same | unchanged (executor-local; no other thread ever reads or writes these) |
| `OwnerSplitView` | `outstanding` | read only if the demand gate was actually reached — i.e. only after the remaining-est-floor and rate-limit gates had already passed | read unconditionally at view construction, for every self-split attempt, whether or not the demand gate will be consulted | **WIDENED** — already disclosed at `OwnerSelfSplit`'s `outstanding` field javadoc; a heuristic input, so this changes only which page-commit's snapshot the demand gate happens to see, never correctness |
| `OwnerSplitView` | `ConfettiObservation` (`taggedTotal`,`taggedConfetti`,`probeSeq`) | N/A — pre-#22-fix, the equivalent read was fused into `ConfettiFeedbackGate.decide()`'s own side effect, gated behind the remaining-work/rate/demand/floor checks already having passed (verified against `main`'s pre-extraction `maybeOwnerSelfSplit`: the confetti check ran only after those four gates) | read via `confettiFeedback.snapshot()` at method entry (`OwnerSelfSplit.java:170`), BEFORE the remaining-work, rate, demand, and floor gates run inside `governor.decide(view)` | **WIDENED** — a heuristic input (changes only which page-commit's snapshot the confetti-rate check sees, i.e. probe cadence). The paired mutation this same relaxation enabled could change carve *count* — issue **#31**, fixed by the probe-slot CLAIM discussed above; what remains widened here is only the rate read |
| N/A (no `View`) | `WorkerState` futility counters: `consecutiveFutileSteals`,`futilityTrips`,`stealPacingSkips` | lock-free, unlocked, one `AtomicInteger` op at a time, inline in `recordFutileSteal`/`markStolen`/`stealPaced`/`consumePacingSkip` | identical call sites, identical op sequence — `FutilityPacingPolicy` supplies only the trip-check/cooldown-formula/decay/reset VALUES each op already computed inline; no combined read of the three | unchanged |
| N/A (no `View`) | `IdleStealBackoff` pacing state: `consecutiveNonProductive`,`nextAttemptNanos` | plain (non-atomic) fields, read/written only inside this object's own `synchronized` methods | collapsed into one `IdleStealPacingState`, read-and-replaced as a whole, still only inside the same `synchronized` methods | unchanged — monitor-protected, so combining the two fields into one record here is safe (see the shape-asymmetry paragraph above); contrast the futility-counter row directly above it |
| N/A (no `View`, mutation-only) | `consecutiveZeroFanoutStructureProbes` increment/reset (`RECORD_ZERO_FANOUT_STRUCTURE_PROBE`/`RESET_ZERO_FANOUT_STRUCTURE_PROBES`) | applied synchronously and inline in `structurePivot`, immediately after the probe response's fan-out was counted (verified against `main`'s pre-extraction `Thief#structurePivot`) | queued as a mutation by `ThiefPolicy#pickStructureBoundary` (`ThiefPolicy.java:435-438`) on the `StealAction` it returns; not applied until `Thief`'s `while(true)` loop comes back around and calls `applyMutations` on the NEXT action (`Thief.java:238`) — i.e. one full loop iteration after the response was counted | **WIDENED** — a third `Thief`-side widening, judged benign on the same grounds as the two below (see the bullet list immediately following this table) |

**The three `Thief`-side widenings in the table above are judged benign, but the judgment is the
reviewer's to check, not the implementer's to make silently:**

- *Pacing-skip window.* If `markStolen()` (resets the cooldown to zero on a productive split) and then
  a fresh `recordFutileSteal()` (starts a **new** cooldown episode) both land on the same candidate
  during this window, the stale `consumePacingSkip()` still fires unconditionally and erroneously
  consumes one skip of the new, unrelated cooldown. Both `pacingSkipAvailable()`'s `> 0` and
  `stealPaced()`'s `<= 0` checks treat a negative `stealPacingSkips` identically to zero, so this
  cannot flip a "not paced" read into a "still paced" one or vice versa — the consequence is bounded
  to wasting one skip of an unrelated episode, never a stuck/starved victim.
- *Structure-probe-suppression window.* A **different**, concurrently-racing thief probing the same
  victim during this attempt's own key-probe round trip can move the cached counters in either
  direction relative to what a live read would show: upward across the suppression threshold (this
  attempt proceeds with a structure probe it should have suppressed — a wasted probe) or downward back
  under it (this attempt is routed through the 1-in-64 recovery gate when it should have proceeded
  unconditionally — a missed structure-probe opportunity). Both directions cost only a wasted or missed
  probe; neither can produce a gap, overlap, or duplicate split.
- *Structure-probe-streak-mutation window.* A different, concurrently-racing thief probing the same
  victim between when this attempt's structure-probe response was counted and when the NEXT loop
  iteration finally applies the queued `RECORD_ZERO_FANOUT_STRUCTURE_PROBE`/
  `RESET_ZERO_FANOUT_STRUCTURE_PROBES` mutation can read this same victim's streak one step stale — the
  identical direction/consequence shape as the structure-probe-suppression window immediately above
  (a wasted or a missed probe), never a gap, overlap, or duplicate split.

**Was a live reference, now a snapshot: `alphabetDigest` (issue #30, CLOSED).** Every claim elsewhere
in this section that a view is a coherent, immutable read the policy decides over assumes the view's
fields are values or copies. `StealAttemptView.alphabetDigest`/`OwnerSplitView.alphabetDigest` was the
one field for which that was false: it was the victim's own live `AlphabetDigest` instance, whose
backing arrays (`long[][] mask`, `boolean[] clean`) are mutated by concurrent page commits on the SAME
victim (`WorkerState#recordPage` → `AlphabetDigest#observe`) while the decision was still in progress —
`ThiefPolicy` dereferences the digest later in the cascade, so a page commit landing between view
construction and that dereference changed what the digest reported. It falsified both "state the
executor owns is snapshotted into views" (above) and the stronger claim that a policy's decision is a
deterministic function of its recorded view.

Both views now carry an immutable `AlphabetDigest.Snapshot` that the executor freezes at view
construction, so every field in this document is a value or a copy and a recorded `(view, decision)`
pair is reproducible from the golden alone. The whole digest is a fixed 8 positions × 2 words plus 8
clean flags, so freezing it is one `long[16]` and a packed `int` — one small allocation per steal
attempt and per owner-split consult. Consult semantics are unchanged byte-for-byte, including *which*
`ALPHABET.*` fallback fires: the live digest and its snapshot share a single implementation of the
consult, and the snapshot carries `clean` explicitly rather than inferring it from an all-zero mask
(a dirty position and a merely-unobserved one both have a zero mask but report
`fallback_out_of_window` and `window_gap` respectively). A torn read while snapshotting is still
possible and still benign — `ByteMidpoint` re-validates any chosen scalar for safety and strict
betweenness, so a torn word can only shift the pivot's balance inside valid bounds. What the fix
eliminated is not tearing but mutation *after* the view was recorded.

This was never a production-behavior defect (the pre-extraction code also read the digest live at
decision time — a faithfully-preserved pre-existing non-determinism) and no I1–I12 invariant was ever
at risk (the split CAS re-validates independently of what the policy decided). It is now enforced
mechanically by `DecisionPathPurityTest#viewsCarryNoLiveExecutorState`.

One counter-conservation test exercising this contention directly —
`ThiefStealReasonConservationTest` — reconciles `swath.steal_reason` totals against the number of
`steal()` calls made under genuine multi-threaded racing (conservation only, never a specific
interleaving; issue #18). Precisely what it reconciles, no more: the four terminal outcome buckets
(`NO_VICTIM`/`RETRY`/`UNSPLITTABLE`/`CHILD_CREATED`) against the call count, the `NO_VICTIM`
discriminator/aggregate pairing, and (added after an independent review found the first version of
this section overclaimed) the `ALPHABET` "verdict" engagement (`alphabet_chosen`/`alphabet_fallback` —
deliberately not the whole `ALPHABET` outcome, which also carries `AlphabetDigest#chooseScalar`'s own
variable, digest-state-dependent fallback marks) against the same terminal-outcome/discriminator
counters (the verdict has exactly one, unconditional call site, whose firing condition reduces to
already-independently-recorded totals — see the test's own class javadoc for the derivation).
`STRUCTURE`/`PIVOT` mid-cascade marks and the `STEAL` pacing engagement are **not** reconciled by this
test under contention — each is gated by a cascade branch that is itself schedule/topology-dependent,
not reducible to the call totals alone; their single-threaded shape is pinned by the decision-trace
goldens instead, which is a check on VALUES, not on conservation under a race.

**The determinism audit's enforcement (added 2026-07-26, issue #19's closing slice).** A policy is a
deterministic function of its view. Three of the four checks below target ambient state; issue #30's
shape — a view component holding shared mutable arrays, legal under all three — needed the fourth,
`viewsCarryNoLiveExecutorState`, which walks every policy-package record's component types
transitively and rejects any that exposes a mutator or hands out its own array. Read a green run as
"none of the four shapes below is present"; the residual blind spots (an injected implementation's
body; a mutator that returns a value; a value type aliasing an array its caller keeps writing to) are
disclosed in that test's javadoc. Everything that follows describes what IS mechanically enforced: no
ambient clock, no
ambient randomness, and — the clause the audit's original grep-shaped brief did not have, and so
missed two of the three leaks the campaign actually found (issues #19, #22) — no ambient
*collaborator* state either. Concretely, no type
reachable from `decide()`/`selectVictim()`/`beginAttempt()`/`onProbeResult()` — every class in
`io.varve.swath.engine.policy`, plus the transitive closure of every field-reachable
`io.varve.swath.*` type (so `AlphabetDigest.Snapshot`, reached only via
`StealAttemptView.alphabetDigest()`, and `RemainingWorkEstimator`, the injected position sensor
`ThiefPolicy`/`OwnerSplitGovernor` hold as a field — algorithms.md §3.2 — are in scope despite living
in `io.varve.swath.engine`) — may
**hold** a `RunMetrics`/`TraceSink` reference as a field, or **mutate**
`java.util.concurrent.atomic` state, or **call** an ambient clock/randomness API directly, or
**expose** a mutator on a value a policy decides over. `DecisionPathPurityTest` (`swath-core`)
enforces this mechanically: a field-type closure walk for the first two (the exact shape issues #19
and #22 took), a comment-stripped source scan of the same closure for the third (issue #20's shape),
and a record-component closure walk rejecting mutators and array-returning accessors for the fourth
(issue #30's shape). An injected pure-function seam is likewise not ambient: `RemainingWorkEstimator` (the
`--engine-toggle rate_anchored_sensing` position-sensor choice, resolved once per run by
`EngineToggles#remainingWorkEstimator` and shared by both policies) carries no clock, no randomness
and no collaborator state, and reports what it observed through the caller's own `Engagement`
collector rather than to `RunMetrics`. An explicit,
caller-supplied parameter is not "ambient" and stays legal either way — `EngineToggles#recordOffMarks`
takes a `RunMetrics` parameter but is called only from executor code, never a `decide()` path, and
every `Engagement`/`VictimMutation` collector already threaded through this interface is the same
shape. The two executor-side defaults that inject through `DecisionRng`/`DecisionClock` (`Thief`'s
`ThreadLocalRandom` lambda, `IdleStealBackoff`'s `System::nanoTime`) live in classes never reached as
a field of any policy-package type, so the closure never scans them — this is what keeps the
exception legal without special-casing it in the test.

**Per-slice verification bar (added 2026-07-26).** Alongside the decision-trace goldens
(byte-identical, no regeneration) and `check-instrumentation-drift.py`'s own `--self-test`,
`./gradlew spotlessCheck` — run across **every** module, not just `:swath-core` — belongs in each
extraction slice's own verification, not only in CI. An unused import (`AlphabetDigest`, landed with
issue #20's fix, six commits before the seed-planner slice) sat failing `spotlessCheck` through six
independent review rounds. `./gradlew build` remains the integration gate (AGENTS.md); what this
session did was scope *implementation sub-agents* to targeted tests for speed, deferring the full
build to the slice boundary — and that narrower loop was never paired with a substitute format/lint
gate, so no slice ran one locally. The rule is therefore additive, not a substitution:
`spotlessCheck` belongs in each slice's own verification **as well as** the integration gate, never
instead of it. It runs in single-digit seconds branch-wide — there is no speed argument for leaving
it out.

---

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
  rows INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0);

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

## 4. Parquet output schema — **canonical superset + ETag rule**

One `MessageType`, used by every parallel writer. Logical types per Parquet
spec; timestamps `INT64` `TIMESTAMP(MICROS, UTC)`.

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

### 4.1 Multi-writer + manifest — **own manifest, not parquet `_metadata`**

- **2–4 writers**, decoupled from listing concurrency (not one per worker).
  Workers emit `PageBatch`es into the writer pool. **Sticky assignment:** all
  pages of a node go to writer `node_id % numWriters`, so a node's pages
  occupy a *contiguous* run of that writer's parts (which finalize in order)
  — this is what makes the `durable_cursor` advance (algorithms.md §4.5)
  sound. A part file holds pages from many nodes and a node's pages may span
  several parts; **there is no one-part-per-node rule.**
- Each writer rotates its open part by **target size** (default 256 MB), or,
  whichever fires first, by **time-open** or **row count** (`--part-rotation-
  interval` / `--part-rotation-max-rows`, default 30 s / 2M rows) —
  the same finalize/rotate path either way, so a lane that never fills up to
  256 MB still finalizes on a bounded cadence instead of leaving `COMPLETED`
  nodes with a `NULL durable_cursor` for the run's whole duration (the resume
  RPO gap). The time/row triggers never fire on an empty (0-row) open part, so
  an idle lane never produces empty parts; both are disabled (`0`) for a
  single-file `-o *.parquet` destination, which must stay exactly one part. On `close()` (footer
  fsynced) the part is marked `finalized` and added to the manifest, and
  `durable_cursor` advances for every node whose pages it held. The cadence
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
- **On-disk layout.** The dataset root (the `-o` dir) holds
  a **pure-parquet `data/` subdirectory** for the part files, plus the
  root-level `manifest.json`, `_SUCCESS`, the internal `.swath-state.json`, and
  `symlink.txt`. `data/` contains **only** `*.parquet` (no manifest, no
  markers), so a `data/*` glob is safe by construction — DuckDB's directory
  glob (swath's own ingest) does **not** honor the Hadoop `_`-prefix skip rule.
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
  same truncation reason as `SortedFileIndex`) — a consumer can verify
  `files[i].maxKey < files[i+1].minKey` (unsigned byte, strict) across the
  whole dataset without opening a single Parquet file. Committed atomically
  (`manifest.json.tmp`, fsync, rename) on each finalize and at run end. The
  retained file list and each manifest rewrite are `O(parts)` in memory/work;
  rewriting the complete list at every finalize makes cumulative manifest
  serialization `O(parts²)` over the run. Part-count metadata is therefore
  outside the active-buffer bound in I11.
  **Resume bookkeeping stays out of the consumer manifest**: `args_hash` and
  the checkpoint `run_id` live in the internal `.swath-state.json` (same
  atomic write), which the sorted publish-reentry check reads to distinguish
  "published by this run" from a stale/foreign dataset in the same dir. The
  whole-snapshot completion marker **`_SUCCESS`** (empty) is written **last**,
  after the manifest; `symlink.txt` lists the `data/<part>` paths for
  Hive/Athena/Trino auto-discovery. The finalized-part durability bookkeeping
  (`writer_id`/`finalized`/row counts) lives in the checkpoint `part_file`
  rows (§3), **not** the consumer manifest.
- **Sorted output (`--sort`, §6) gets the same consumer `manifest.json` +
  `.swath-state.json` + `_SUCCESS` at publish time** — the
  final **`part-NNNNN.parquet`** files (uniform naming, no
  `sorted-` prefix; `%05d` zero-padded, lexical name order == key order) live
  under `data/` like any part — a `--sort` dataset and a plain dataset share
  the same `part-` prefix (never a `sorted-` prefix on either) but differ by
  the `w`-infix: plain (unsorted) parts are `part-w{worker}-{seq}.parquet`,
  `--sort` finals are `part-{NNNNN}.parquet` with no `w`-infix — a consumer
  distinguishes them either way, but authoritatively via `manifest.json`'s
  `sorted` field; the manifest carries their
  `data/<part>` keys/sizes/MD5s/rowCounts/minKeys/maxKeys and the schema, and
  `.swath-state.json` records `args_hash` **and** the `run_id`, committed at
  §6's publish commit point (final files written `*.tmp`, renamed in name
  order under `data/`; then the manifest, the state file, `symlink.txt`, and
  finally `_SUCCESS`). Staging segments under the **visible** `_staging/` directory
  (not a hidden dot-dir — a mid-sort run must be observable with a
  plain `ls`, and distinguishable from a fresh/crashed-no-sort/complete
  dataset root purely from `(_SUCCESS, _staging/, manifest.sorted,
  data/part-w*)`) are internal working state and never appear in the
  manifest.
- v1.0 local Parquet durability is POSIX/Linux/macOS-oriented. File fsync is
  mandatory; directory fsync is attempted for durable directory entries and
  atomic renames, but filesystems/OSes that do not support directory fsync
  degrade to a debug-logged no-op.
- **On resume:** discard every non-finalized part; finalized parts are never
  rewritten. Each node re-lists from its `durable_cursor` (the not-yet-durable
  tail), so finalized rows are neither lost nor duplicated ⇒ **exactly-once**
  (I6).

---

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
    unrecoverable `ListingException`/`OutputException`/`CheckpointException`,
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
| **FILE-kind text** | **At-most-once while the one-shot process runs;** a successful publication atomically replaces the destination. | **No.** FILE kind requires `--checkpoint none`. |
| **FILE-kind Parquet** | Uses the Parquet writer path, but has no durable resume ledger. | **No.** FILE kind requires `--checkpoint none`. |
| **Managed directory-dataset Parquet** | **Exactly-once durable dataset** via the `durable_cursor` model (§4.1, I6): finalized parts are retained; an unfinalized tail is discarded and re-listed from `durable_cursor`. | **Yes.** `swath resume <dir>` opens the co-located checkpoint. |

  The deferred `--resume-output` journal describes a possible future text-replay
  contract; it does not make stdout or FILE-kind output resumable in the shipped CLI.

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

## 6. Sort temp-run format

External merge sort, behind `--sort`. **v1 `--sort` applies to `--format
parquet` only** — only the **FINAL output** is Parquet; the intermediate sort
**STAGING segments are checkpoint-tracked page-run files** — a stream of
CRC32C-framed, min/max-headed, codec-compressed page records (§7
`segment-codec`), not Parquet. `--sort` with a text format is a v1
`InvalidArgsException` (a sorted text sink is deferred to `--resume-output`,
which owns the at-most-once-text durability questions it would reopen):

- **Spill vehicle: checkpoint-tracked page-run segments**, not a custom
  run-file format and not columnar Parquet either. The sort lane
  buffers admitted pages up to a heap-adaptive segment gate (§7), then flushes the
  sealed buffer as an internally-sorted page-run segment (a `.pageseg` file: header
  magic/version, one CRC32C-framed page record per page carrying `[minKey, maxKey,
  count, codec, len]`, then a completeness trailer with the exact `segMin`/`segMax`
  and record/entry counts) into a staging directory, the **visible** `_staging/`
  inside the output directory (a mid-sort run must be observable with a plain
  `ls`) (same
  filesystem as the final output, so the final rename is cheap; found by
  resume; the sorter only ever creates/deletes content it owns inside that
  directory). Segments are tracked like parts in
  the checkpoint schema (§3, `part_file` rows under a staging namespace so
  the root output dir's `manifest.json` is never polluted by staging) and
  **finalize strictly in seal order** (one ordered encoder, double-buffered
  fill vs. off-thread encode); `durable_cursor` advances on each segment
  finalize exactly as it does for parts (§4.1) — out-of-order finalize would
  let `durable_cursor` over-advance past keys still sitting in an earlier
  unfinalized buffer and silently lose them on resume.
- **Comparator** equals the in-memory comparator exactly, including the
  `(key, version_id)` tiebreak when `--all-versions` (else sort is
  non-deterministic across versions of one key).
- **Cascaded multi-pass merge:** with fan-in `F` (default 10000, §7), merge runs
  in passes so open file descriptors never exceed `F`. The effective width is
  further clamped at runtime by the fd budget (`ulimit -n` headroom) and by the
  per-open-stream memory bound (`merge-per-stream-bytes`, §7) — if that clamp
  forces the width below the segment count, the cascade engages. Segments are
  sized (heap-adaptive gate, §7) so a single pass is the design point at the
  heap you grant; the cascade remains only as a correctness backstop for
  absurd N-vs-heap ratios (or a too-low `ulimit -n`).
- **`--sort` forces single-file output (or a final cross-part merge)** — the
  design realizes this alternative literally: the merge k-way-merges the
  sealed segments straight into the final sorted output; parallel unsorted
  part files are not globally ordered.
- **Served-file footer stamp**: each published
  file carries static footer key-value metadata — `swath.sort.order` (the
  comparator order), `swath.sort.mode` (`objects` | `versions`),
  `swath.sort.format_version`, and, for a multi-file rolled output,
  `swath.sort.file_index`/`swath.sort.file_final` (1-based position and a
  last-file marker proving the resolved file set is complete) — static values
  only, no routing data; the replay server derives its own in-memory
  first-key index at startup instead of trusting an embedded index.
- **`swath resume` reattaches sorted runs.** `sort_enabled` is stored in `run_meta`
  (distinct from the served file's `swath.sort.mode` footer stamp) and a
  `--sort`/`--no-sort` mismatch on resume is **refused**, exactly like the
  existing filter/format-swap refusal (§5) — `args_hash` rightly excludes
  output flags, so without this a mismatched resume would interleave
  unsorted parts with orphaned staging.
- **Staging-format mismatch on resume is refused**: if a
  resuming `--sort` run's checkpoint carries staging `part_file` rows tagged
  with a format other than the current `SORT_SEGMENT_FORMAT` (`page-run`) — an
  older `parquet-segment` in-flight run, or any future format-version
  mismatch — the run **refuses cleanly** (`InvalidArgsException`, exit 2, same
  as the `--sort`/`--no-sort` refusal) naming the recorded vs expected format
  and pointing at `--restart`. Without this, the reattach path (which selects
  staging by `page-run`) would treat the un-recognized old-format finalized
  segments as non-finalized, sweep them, and silently re-list their data
  (dup/loss). *(The page-run `FORMAT_VERSION` is not yet recorded in the
  checkpoint, so a version bump is caught later/loudly by the segment reader
  rather than refused here — recording it is a noted future hardening.)*
- **Sort-phase state machine:** run phases **LISTING → MERGING → PUBLISHED**
  are recorded in `run_meta` (`sort_phase`). Resume dispatch: nodes incomplete ⇒
  resume listing (keep durable segments, re-list only the non-durable tail); nodes
  complete + not-yet-published ⇒ (re)run the merge from staging; **published by
  THIS run** ⇒ clean staging + stale `*.tmp` and exit success (idempotent
  re-entry). **Publish commit point = the empty `_SUCCESS` marker, written LAST**
  (§4.1): every final file is written as `*.tmp` and renamed in name order under
  `data/`, then the consumer `manifest.json`, then the internal
  `.swath-state.json` identity, then `symlink.txt`, and **finally `_SUCCESS`** —
  so `_SUCCESS`'s presence proves the whole publish committed. Re-entry treats
  `_SUCCESS` as the **authoritative** completion marker: "published by this run"
  requires BOTH the `.swath-state.json` identity (`args_hash` **and** the
  checkpoint `run_id`) to match this run AND `_SUCCESS` to exist. Identity match
  but `_SUCCESS` absent ⇒ our own publish crashed mid-commit ⇒ re-enter `MERGING`
  and re-run the merge idempotently (rewriting manifest/state/symlink/`_SUCCESS`);
  a crash between the manifest write and staging deletion leaves double data,
  which the same re-entry path cleans up.
- Staging dir cleaned on successful publish; **a crash mid-sort redoes only
  the sort (the LIST work is checkpointed).**
- **Cascade-scale resume semantics**:
  every `SortTransform.transform` (the merge-pending re-entry, whether from a
  fresh-listing hand-off or a `runSortMergeOnly` redo) unconditionally sweeps
  `merge-*` cascade intermediates from the staging dir at entry
  (`cleanStaleMergeIntermediates`, which clears **both** the active
  `merge-N.pageseg` page-run intermediates it writes today **and** legacy
  `merge-N.parquet` debris) **before** merging — so a **multi-pass
  cascade never resumes mid-cascade; it always restarts at pass 0** from the
  original checkpoint-tracked sealed segments, and any stale partial
  `merge-N.pageseg` left by a crashed prior cascade attempt is discarded,
  never read. This is safe and cheap to redo because `KWayMerge` never
  deletes an ORIGINAL input segment at any pass (only its own earlier-pass
  intermediates once a later pass has folded them in) — so every sealed
  segment the checkpoint named is still on disk for the redo to consume, at
  the accepted cost of a transient **~2× staging disk footprint** during an
  in-flight cascade (an original and the intermediate it folded into can
  briefly coexist). The durability boundary is therefore exactly the segment
  boundary: a page-run segment is the
  atomic durable unit. It is finalized (`fsync` file + parent dir, then
  checkpoint-`partFinalized`) only after its completeness trailer is written, so a
  half-written `.pageseg` from a crash has no valid trailing magic/trailer and is
  discarded **whole** on resume (never partially read). Thus a **sealed** segment
  is durable and always reused across any crash/resume; only an **unsealed**,
  still-filling buffer is lost and re-listed on resume (at most the one in-flight
  buffer/segment's worth of listing work, bounded by `segment-bytes`/
  `segment-entries`, §7) — a segment-granularity RPO. The cascade's own pass
  structure carries no
  separate resume state of its own, by design. A merge redo (`runSortMergeOnly`)
  re-runs entirely from durable staging with **zero new LIST fetches**
  (`SORT.merge_redone`), regardless of how many cascade passes the redo
  itself needs to run.

---

## 7. Config defaults (single source of truth)

| Knob | Default | Notes |
| --- | --- | --- |
| `--concurrency` = `Tmax` | 64 | the AIMD **ceiling**; the live concurrency `T` ∈ [1, `Tmax`], starts at `min(4, Tmax)` (slow-start ramp) and is lowered/raised by AIMD (algorithms.md §5) |
| HTTP client `maxConnections` | `Tmax + 16` | built **once** from the configured `--concurrency` ceiling `Tmax` (not live AIMD `T`); **must exceed `T`** or it silently caps concurrency |
| `--object-listing-queue-size` | 50_000 **entries** | a `PageBatch` is admitted while in-flight entry count < cap; budget ≈ cap × (max\_key\_len + ~200 B fixed per-entry overhead: etag, storageClass, versionId, owner, checksum strings + 11-field record header) × #queues; admission is at `PageBatch` granularity (≤ 1000 entries), so each queue may transiently overshoot the entry cap by up to one page |
| page batch size | one S3 page (≤1000) | pipeline granularity |
| seed delimiter levels | adaptive | a shallow `delimiter=/` seed that starts at the top level and **adaptively descends narrow sub-levels** while cut-point and probe budgets permit; a truncated flat-wide level is radix-banded rather than descended (algorithms.md §8) |
| steal probe | `max_keys=1` | one key per split attempt |
| AIMD | ×0.7 down on 503 / +1 up per clean 10 s | the 0.7/+1 numbers are DECIDED. The 10 s clean-window cool-down is re-armed **only by a REAL reduction** (`T` actually lowered); a **floor no-op** decrease (`floor(0.7·T) >= T`, i.e. `T` already at the floor) still casts its AIMD vote, still latches congestion, and still pauses stealing, but no longer resets the clean window. The re-arm write is **monotonic** (`max` with the prior timestamp): a concurrent, stale-timestamped decrease can never shorten an already-armed window. (algorithms.md §5) |
| Parquet writers | 3 | decoupled from `T` |
| Parquet part target size | 256 MB | rotate by size |
| `--part-rotation-interval` | 30 s | rotate a lane's open part by time too, even below the size target, so `durable_cursor` advances on a bounded cadence instead of only when a part happens to fill up; `0`/`none` disables; forced to `0` for a single-file `-o *.parquet` destination; a positive value below the 100 ms minimum is rejected (spin-storm guard — see below); `ParquetWriterPool` additionally floors its idle-lane poll wait at 50 ms as defense-in-depth |
| `--part-rotation-max-rows` | 2_000_000 | rotate by row count too, for bursts fast enough to write millions of small rows well inside the time interval; `0` disables; forced to `0` for a single-file `-o *.parquet` destination |
| `parquet.block.size` / `page.size` | 64 MB / 1 MB | **pinned**, measured in the PERF gate. block.size chosen so the §7.2 active-buffer Parquet heap budget holds for the current 100,000-key test (3 writers × 64 MB × parquet-mr uncompressed-row-group overshoot). This is not an N-independent whole-run heap claim: finalized-part metadata is `O(parts)`. Raising it toward 128 MB improves compression/scan but risks the measured budget — re-measure if you do. |
| `--request-rate` | unset | Bucket4j; cancellable acquire |
| SDK retry attempts / initial backoff (**internal constants — not CLI flags**) | 1 / 100 ms | `S3Config.DEFAULT_MAX_ATTEMPTS = 1` disables SDK-internal retry: swath's own gauge-aware fetch loop is the sole retrier, so the AIMD `ConcurrencyGauge` sees every real 503/5xx immediately instead of after the SDK silently absorbed several behind its own backoff. There is **no** `--aws-max-attempts` / `--initial-backoff-ms` Picocli option in v1.0 (the `S3Config.maxAttempts` plumbing exists but is not CLI-wired) — exposing them is a planned follow-up |
| page-timeout retry budget | per-fetch bounded retry, cap 8 (`MAX_TRANSIENT_RETRIES`), resets each fetch; disposition on exhaustion depends on `RetryPolicy` (see below) | `apiCallAttemptTimeout` is the per-attempt **timeout duration** (not a count; **10 s default for WORKER pages**, **3 s for pivot/structure PROBES** unless an escalation override raises it, escalated 10 s→20 s→40 s per-fetch on consecutive attempt-timeouts of the SAME logical fetch — `apiCallAttemptTimeoutOverride` in §2). Above the per-attempt budget the SDK client also enforces a **60 s overall `apiCallTimeout`** (`S3Config.DEFAULT_API_CALL_TIMEOUT`, the primary liveness ceiling on a wedged logical call). `WorkStealingScan.GaugedFetcher` (and, on the seed/sequential paths, `TransientRetryFetcher`) retries a **non-AIMD-voting** transient (`ThrottleException.Kind.ATTEMPT_TIMEOUT` / `NETWORK` — a client-side attempt-timeout or exhausted network fault, neither of which is genuine S3 backpressure; `NETWORK` also covers a client-local socket-closure / `IOException`-wrapper fault that escaped the SDK call as a non-`SdkException` `RuntimeException` such as `UncheckedIOException(SocketException("Socket closed"))`, reclassified transient rather than escaping raw as an exit-1 / `error_class=unknown` crash) up to `MAX_TRANSIENT_RETRIES = 8` times with jittered exponential backoff; the counter is **per invocation of `fetchPage`** (i.e. per attempted page/probe fetch), not a cross-fetch/cross-node consecutive count (it does not persist across separate `fetchPage` calls the way a per-node counter would). **What happens once the cap is crossed depends on `RetryPolicy`, resolved once at CLI wiring time from whether a real `LivenessWatchdog` is armed** — the fix for the tail-stall that killed long-running large listings, where this cap (not the watchdog) was a second liveness policy that always won the race to end the run: under **`RIDE_OUT`** (a real watchdog is armed, the default) the cap **no longer cancels the run** — the fetch keeps retrying indefinitely (raised full-jitter backoff ceiling, 5 s→15 s, recording `TRANSIENT.storm_ride_out`) and the watchdog alone owns liveness death (crash-only, resumable exit-75); under **`BOUNDED`** (both watchdog windows disabled by flags — `LivenessWatchdog.arm()` returned its no-op, so nothing else could ever stop an unbounded retry) exhaustion keeps the legacy disposition: the fetch trips the run's cancellation with `StopReason.STUCK` (attributing `CancelSource.TRANSIENT_RETRY_CAP`, recording `TRANSIENT.retry_cap_stuck`) and aborts via `CancelledException` — the **resumable exit-75 (`EX_TEMPFAIL`) disposition**, the same code as a watchdog stop — so the checkpoint stays valid and `swath resume` can safely retry the bucket later, rather than escaping as a fatal `ListingException` (exit 1) that the CLI's guarded engine dispatch would mark `run_meta.fatal_error` and thereby **poison `swath resume`**; a fetch with **no `CancellationToken` wired** (degenerate/embedded use) is unaffected by `RetryPolicy` and stays count-bounded regardless, escaping as the fatal `ListingException` contract on exhaustion. The run records `stop_source`/`error_class` marker fields that this disposition drives. Either disposition aborts (or, under `RIDE_OUT`, never aborts) the whole run, never a selective "fail the node". The thief's structure/pivot **probe** fetches (`slotGated=false`) are exempt from this policy: they use a separate small fixed cap (`PROBE_TRANSIENT_RETRY_CAP = 1`), never cancel the run, and simply return the probe to its non-productive retry flow. A genuine AIMD-voting throttle (`SLOWDOWN` / `SERVER_5XX`, real 503/5xx) is retried **unbounded** by this counter regardless of `RetryPolicy` — AIMD's own multiplicative decrease paces it instead, bounded only by cancellation/`--max-duration` (the liveness contract) |
| `swath.sort.segment-bytes` | heap-adaptive: ≈8% of `Runtime.maxMemory()` estimated pre-encode bytes, floored at 64 MB | primary segment-flush gate (§6); ~160 MB at `-Xmx2g` ⇒ ~1.3M-row segments, ~5M-row at `-Xmx8g`; bigger heap ⇒ fewer, bigger segments ⇒ single-pass merge as the design point. Active segment buffers are a function of `-Xmx`; retained staging metadata is separately `O(segments)`. |
| `swath.sort.segment-entries` | secondary cap alongside `segment-bytes` | backstop entry-count cap on a sealed buffer |
| `swath.sort.heap-fraction` | `0.08` | the adaptive ratio `segment-bytes` derives from `Runtime.maxMemory()`; raise only after measurement, never unattended |
| `swath.sort.buffers` | 2 | in-flight sealed buffers (fill buffer while the sealed buffer encodes off-thread); **must be `>= 2`**: `SortLane` bounds live sealed buffers to exactly `buffers` (fill + `buffers - 1` off-thread); `buffers=1` would either deadlock (0 off-thread slots to hand a sealed buffer to) or, if floored instead, silently allow 2 live buffers while claiming a cap of 1 — `SortConfig` rejects `buffers < 2` outright (`IllegalArgumentException`), consistent with every other knob's validation in that record |
| `swath.sort.fan-in` | 10000 | merge fan-in `F` (§6); open page-run segment readers never exceed `F` per pass. The pass width actually used is clamped at runtime by (a) the **fd budget** — `min(fan-in, usable-fds)` derived from `ulimit -n` with headroom — and (b) the **per-open-stream memory bound**, `effectiveFanIn = min(fan-in, max(2, merge-budget-bytes / merge-per-stream-bytes))`. `fan-in` alone is a correctness/fd ceiling, not a memory promise; raise `ulimit -n` (below) so the fd clamp does not force a cascade |
| `swath.sort.segment-codec` | `LZ4` | payload compression for page-run STAGING segments — `NONE` \| `LZ4` \| `ZSTD1`. Trades staging-disk ratio for pack/merge CPU: `LZ4` (default) is fast; `ZSTD1` is smaller-on-disk but slower; `NONE` skips compression. Governs staging only, never the final Parquet output |
| `swath.sort.merge-per-stream-bytes` | ≈64 KiB fixed per-open-stream estimate (`DEFAULT_MERGE_PER_STREAM_BYTES`, to be validated at the perf gate) | the per-open-page-run-stream memory a merge holds (≈ one decoded page's worth); `merge-budget-bytes / merge-per-stream-bytes` bounds `effectiveFanIn` (above) so realized merge peak memory stays within `merge-budget-bytes` regardless of segment count (I11). For page-run input the exact bound is read O(1) from each segment's trailer (`maxRecordLen`) |
| `swath.sort.final-file-bytes` | 1 GiB | roll threshold for multi-file sorted output — the final Parquet output rolls ~1 GiB parts by default; files are range-disjoint and named in key order |
| `swath.sort.final-row-group-bytes` | ≈4–8 MB | the served file's seek granularity (row-group size) |
| `swath.sort.segment-row-group-bytes` | 1 MB | governs ONLY the legacy columnar-Parquet staging path (the equivalence-fixture path); the default page-run staging is row-oriented and does not use it. (Columnar path: row-group size for internal Parquet staging segments — deliberately small, unlike `final-row-group-bytes` — because a `SegmentReader` preloads one full row group per open merge stream, so a bigger segment row group multiplied into merge-phase peak memory) |
| `swath.sort.merge-budget-bytes` | heap-adaptive: same shape as `segment-bytes` (≈8% of `Runtime.maxMemory()`, floored at 64 MB) | the merge-phase memory budget: caps `effectiveFanIn` (above) so realized merge-phase peak memory is `streams/pass × merge-per-stream-bytes ≤ merge-budget-bytes` for any budget `>= 2 × merge-per-stream-bytes` — a function of the budget knob, never of segment count (I11), even where `fan-in` alone would have allowed more streams open at once. **The `effectiveFanIn` floor of 2 streams is documented, not rejected**: a merge needs at least 2 streams to merge anything, so a budget below `2 × merge-per-stream-bytes` still realizes exactly the 2-stream floor (the minimum realizable merge memory) rather than a smaller peak |
| `swath.sort.segment-format` | `page-run` | the staging-segment format string new `--sort` runs stage under and tag `part_file` rows with (`ListRunner.SORT_SEGMENT_FORMAT`); a resume whose checkpoint records any other staging format is refused (§6) — informational, not user-tunable |
| `ulimit -n` (OS, not a swath knob) | raise to ~65536 for single-pass | with fan-in 10000, a single merge pass opens up to ~`min(segments, fan-in)` page-run readers at once; a low `ulimit -n` forces the fd clamp to shrink `effectiveFanIn` and **degrade to a multi-pass cascade**. Raise the soft limit (`ulimit -n 65536`, or the launcher does it) for single-pass merges on large buckets |
| `--checkpoint` | `auto` (co-located at `<dir>/.swath/checkpoint.sqlite` for a directory-dataset output; ephemeral for stdout), deleted on clean completion | FILE kind accepts only `none`; `none` ⇒ in-memory worklist and **no resume**. An explicit path is valid only with a directory dataset, but the public `swath resume` command opens the managed co-located layout, not an arbitrary SQLite path. |

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

The release gate asserts measured peak heap stays under these, sized to the
**2–4-writer** Parquet model (not the retired per-worker model):

| Output | Budget (default config) | Composition |
| --- | --- | --- |
| stdout / TSV / JSONL / table | **< 256 MB** | bounded queues + JVM baseline |
| Parquet | **< 1 GB** (the bound PERF-2 asserts at 100,000 keys, `ParquetPerf2Test`) | `Parquet writers (3) × block.size (64 MB) × parquet-mr overshoot` + bounded queues + baseline. parquet-mr buffers the **uncompressed** row group, so real overshoot runs hotter than the ~2–3× first modeled — measure it, don't model it. The implementation has no per-object heap accumulation (I11), but the public PERF-2 measurement covers 100,000 keys, not billion-object scale. |

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
