/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.Timer;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.error.ThrottleType;
import io.varve.swath.output.ControlCharEscaper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Per-run Micrometer wiring and concurrency-safe metric state. */
public final class RunMetrics {

    private static final int DISPLAY_LIMIT = 120;

    private final MeterRegistry registry;

    private final AtomicLong concurrencyTarget = new AtomicLong();
    // The MINIMUM effective-T the run ever reached (the shed floor / "ceiling hit"),
    // so a COMPLETED run's summary still reports how far the AIMD/shed brake had to pull T down.
    // 0 = never set (matches concurrencyTarget's default); updated to the running min on each
    // setConcurrencyTarget. Purely a low-water mirror — no gauge/control logic.
    private final AtomicLong concurrencyTargetLowWater = new AtomicLong();
    private final AtomicLong peakInFlight = new AtomicLong();
    private final AtomicLong pages = new AtomicLong();
    private final AtomicLong splits = new AtomicLong();
    // Liveness watchdog: a monotonic, phase-appropriate forward-progress tick. During LISTING the
    // watchdog reads page/object progress directly (see progressSignal); this counter carries the
    // progress that pages/objects DON'T reflect — the sort-merge/parquet-finalize tail, where no page
    // completes but real work advances (marking rows written / phase boundaries) — so the watchdog
    // does NOT false-trip a nearly-done sorted run in its final k-way merge. Bumped by markProgress().
    private final AtomicLong livenessProgress = new AtomicLong();
    // Liveness watchdog: plain monotonic AtomicLong tallies that mirror the Micrometer
    // counters progressSignal() folds in (progress-units, sort-segments, throttle-events). The
    // liveness signal MUST NOT read Counter.count() — under a DELTA/step OTLP registry that value
    // resets per step, making progressSignal non-monotonic (the watchdog would read a rollover as
    // "progress" and never trip, or false-trip), a fault no SimpleMeterRegistry test can see. These
    // tallies are incremented alongside the counters and summed by progressSignal() instead, so the
    // signal is monotonic regardless of any registry's export temporality. progressUnits already
    // advances in EVERY phase by construction (entries during listing/writing, rows merged during the
    // sort cascade) — folding its tally in means a long intermediate cascade-merge pass counts as
    // forward progress and cannot false-trip the watchdog.
    private final AtomicLong progressUnitsTally = new AtomicLong();
    private final AtomicLong sortSegmentsTally = new AtomicLong();
    private final AtomicLong throttleEventsTally = new AtomicLong();
    // STUCK/TRANSIENT_RETRY_CAP/CRASH error_class classification (the windowed throttle-event tallies
    // + first-writer-wins fatal/cap attributions) — extracted to StuckErrorClassifier. The
    // facade feeds it two booleans per throttle event and a snapshot on every real-progress advance.
    private final StuckErrorClassifier stuckClassifier = new StuckErrorClassifier();
    private final AtomicLong runId = new AtomicLong(-1L);
    private final AtomicLong runStartNanos = new AtomicLong();
    // The listing->merge boundary stamp: null until the boundary is crossed. ONE nullable atomic
    // rather than a 0 sentinel on a primitive, because nanoClock is a test seam a deterministic test
    // may legitimately start at 0 (the same reasoning spelled out for sessionClaimed below) -- a
    // CAS-from-0 would store 0, still read as "still listing", and hand the win to the first
    // NON-ZERO crossing. It is also ONE variable rather than a claim flag beside the timestamp: a
    // pair publishes in two steps, so a reader could see the claim before the stamp it guards and
    // compute a listing span against an unwritten boundary. A nullable reference closes both --
    // claiming the boundary and publishing its stamp are the same write. See markListingFinished().
    private final AtomicReference<Long> listingEndNanos = new AtomicReference<>();
    // The whole-invocation session clock's zero point: set exactly once, when the OUTERMOST
    // RunProgressReporter claims this run (the CLI's session-wide reporter, opened before seeding —
    // see RunProgressReporter's own javadoc) -- never by a nested/joined start. `sessionClaimed`
    // is a SEPARATE flag rather than a 0/negative sentinel on sessionStartNanos itself: nanoClock is
    // a test seam (RunMetrics(MeterRegistry, LongSupplier)) that a deterministic test may legitimately
    // start at 0, which a sentinel-on-the-timestamp scheme would misread as "never claimed" --
    // see sessionDuration(Duration).
    private final AtomicLong sessionStartNanos = new AtomicLong();
    private final AtomicBoolean sessionClaimed = new AtomicBoolean();
    private final AtomicLong firstStealNanos = new AtomicLong(-1L);
    private final AtomicLong peakInFlightNanos = new AtomicLong(-1L);
    // Time-weighted average in-flight gauge — peak_in_flight saturates/blinds at the concurrency
    // ceiling; avg_in_flight is the metric the density-reflected-pivot direction is expected to
    // move. Sample-on-change: every incrementInFlight/decrementInFlight transition (the same seam
    // peakInFlight already hooks) folds in value * elapsed-since-last-transition, so there is no
    // polling thread. Extracted to InFlightGauge; `nanoClock` is a seam (default
    // System.nanoTime) so a test can drive deterministic transitions on a fake clock — shared with
    // the gauge (peakInFlightNanos/markRunStarted/markFirstSteal read it too).
    private final LongSupplier nanoClock;
    private final InFlightGauge inFlightGauge;
    private final AtomicReference<byte[]> currentCursor = new AtomicReference<>();
    private final AtomicReference<String> currentPrefix = new AtomicReference<>("");
    // Where the terminal RunSummary goes beyond the log lines and the JSON report — NONE until a
    // presentation layer installs its own (see RunSummarySink / setSummarySink).
    private final AtomicReference<RunSummarySink> summarySink = new AtomicReference<>(RunSummarySink.NONE);
    // Where live progress goes, and the single session-wide reporter that feeds it (see
    // ProgressSink / RunProgressReporter). The reporter reference is what makes ONE progress
    // lifecycle out of the several nested scopes that each want one: the first start owns it, a
    // nested start joins it.
    private final AtomicReference<ProgressSink> progressSink = new AtomicReference<>(ProgressSink.LOG);
    private final AtomicReference<RunProgressReporter> progressReporter = new AtomicReference<>();
    // The one lock a tick and the terminal summary contend for (see emitProgress/finishProgress):
    // it is what makes "the summary is the run's last word" true for WHATEVER sink is installed,
    // rather than only for a sink that happens to share the CLI's stderr coordinator.
    private final Object progressLock = new Object();
    private boolean progressFinished;
    // Whether the provider's LIST pricing is knowable at all (false under --endpoint-url), so no
    // progress renderer can publish an AWS-priced figure for a run that went somewhere else.
    private volatile boolean listCostKnown = true;
    private final AtomicReference<String> strategy = new AtomicReference<>("unknown");
    private final AtomicReference<String> strategyWhy = new AtomicReference<>("unknown");
    // §3.3: at most one swath.disk.free_bytes gauge per run — registerDiskFreeGauge is called from
    // several ListCommand call sites (whichever output/staging directory becomes known first wins).
    private final AtomicBoolean diskGaugeRegistered = new AtomicBoolean();

    private final Counter entriesEmitted;
    /**
     * Keys durably committed AND kept post-filter while their node's upper bound was {@code null}
     * (issue #76) — the share of {@link #entriesEmitted} the owner-split governor's un-carvable
     * {@code OWNER_SPLIT.open_frontier} skip is structurally unable to peel off. Deliberately counted
     * at the SAME contract {@link #entriesEmitted} is: past the durable page commit (never bumped on
     * a commit that fails — a review caught an earlier draft that recorded before {@code
     * awaitCommit}), and on the post-filter kept count (never the pre-filter page size a review also
     * caught), so this counter can never read as a larger share than 1 of {@link #entriesEmitted}. So
     * a run whose mass sits past the last seed cut shows this counter move alongside the aggregate
     * {@link #entriesEmitted} it is a genuine subset of; an analyst divides the two for the share.
     */
    private final Counter openFrontierKeysEmitted;
    private final Counter bytesEstimated;
    private final Timer listObjectsLatency;
    private final Timer queueWait;
    private final Timer rateLimitWait;
    private final Timer apiRateLimitWait;
    private final ConcurrentMap<String, Counter> apiCalls = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> errors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> steals = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> stealReasonCounters = new ConcurrentHashMap<>();
    private final Counter probeFetches;
    private final Counter structureProbeFetches;
    private final Counter emptyUpperBisections;
    private final Counter unsplittableVictims;
    private final Counter splitGuardAborts;
    private final Counter rawPages;
    private final Counter rawPageKeys;
    private final Counter shortTruncatedPages;
    /**
     * THE typed throttle-events counter {@code swath.throttle.events{type=slowdown|server5xx|
     * attempt_timeout|network}}, pre-resolved at construction (one per {@link ThrottleType}) so the
     * recording site never does a per-increment tag lookup — see §3.1 of the OTLP metrics plan (this
     * is the OTLP-exported schema validated against Cloud Monitoring). It is the SINGLE unified
     * throttle-events series: every event {@code S3PageFetcher} classifies is recorded here exactly
     * once via {@link #recordThrottleEvent} at its classification point. Whether an event drove AIMD
     * down is the SEPARATE {@link #aimdVotes} question (only the voting {@link ThrottleType}s do); the
     * honest {@code throttle}/{@code transient} diagnostics split reads this same series by voting
     * class — see {@link #throttleEventsByVoting}.
     */
    private final Map<ThrottleType, Counter> throttleEvents;
    /** {@code swath.aimd.votes}: multiplicative-decrease votes the gauge actually received (real 503/5xx only). */
    private final Counter aimdVotes;
    private final Counter aimdTargetReductions;
    /** {@code swath.aimd.timeout_shed}: sustained-timeout-storm sheds (NOT real-503 votes — kept distinct). */
    private final Counter aimdTimeoutShed;
    /** {@code swath.aimd.latency_freeze}: successful-attempt latency-inflation +1-growth freezes. */
    private final Counter aimdLatencyFreeze;
    /** {@code swath.aimd.freeze_gate_checks}: successes that reached the growth-freeze gates at all. */
    private final Counter aimdFreezeGateChecks;
    /** {@code swath.aimd.growth_freeze}: transient-timeout +1-growth freezes (worker-storm-only). */
    private final Counter aimdGrowthFreeze;
    /** {@code swath.aimd.latency_baseline_ms}: the Vegas rolling-min healthy-latency baseline (ms). */
    private final AtomicLong latencyBaselineMillis = new AtomicLong();

    // Idle-backoff, checkpoint/resume, and Parquet
    // writer-pool instrumentation. Same unified idiom as the rest of this class: dedicated
    // Micrometer meters (lazily-tagged where the tag set is bounded), read back generically
    // via the meter registry into the JSON summary's `meters[]` (docs/metrics-and-observability.md §1).
    private final AtomicLong idleBackoffLevel = new AtomicLong();
    private final Counter idleBackoffResets;
    private final Counter idleBackoffSlotDenied;
    private final Timer idleBackoffParkTime;
    private final Timer checkpointCommitLatency;
    private final Timer checkpointQueueWait;
    private final Timer checkpointCommitWait;
    private final DistributionSummary checkpointCommitBatchSize;
    private final ConcurrentMap<String, Counter> parquetRotations = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> parquetParts = new ConcurrentHashMap<>();
    private final Timer parquetFinalizeLatency;
    private final Timer parquetWriteLatency;

    // Output-completeness meters — the Micrometer surface was blind to written output
    // beyond the Parquet main path (JSONL/TSV/TABLE/sort-final all passed 0 bytes / hardcoded
    // counts to `summary()`), and to wall-clock run duration/throughput + the text broken-pipe
    // outcome. Same lazy computeIfAbsent idiom as apiCalls/errors/steals/parquetParts above.
    private final ConcurrentMap<String, Counter> outputFiles = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> outputBytes = new ConcurrentHashMap<>();
    private final Counter outputBrokenPipe;
    private final Timer emitLatency;
    private final Timer runDuration;
    private final AtomicReference<Double> runThroughputKeysPerSec = new AtomicReference<>();

    // --sort first-class meters. Registered always but only touched on the sort
    // path, so a non-sort run leaves them all at zero (§1 idiom: zero hot-path cost off the path).
    private final Counter sortEntries;
    private final Counter sortSegmentsWritten;
    private final Counter sortSegmentBytes;
    private final Counter sortMergePasses;
    private final Timer sortMergeLatency;
    // Per-RANGE merge wall time on the parallel range-merge path (off-by-default,
    // swath.sort.merge-parallelism>1). Distinct from sortMergeLatency (the whole-run merge wall): this
    // records once per concurrent range so an A/B can see per-range cost with vs without row-group skip.
    private final Timer sortMergeRangeLatency;
    // The parallel path's SERIAL prologue: boundary sampling, once per run, before any range starts.
    private final Timer sortMergeBoundariesLatency;
    private final Counter sortMergeBoundaryEmbeddedEntries;
    private final Counter sortMergeBoundaryEmbeddedBytes;
    private final Counter sortMergeBoundaryScanBytes;
    private final Timer sortFinalizeCloseLatency;
    private final Timer sortFinalizeLatency;
    private final Timer sortPublicationLatency;
    private final Counter sortManifestMd5Bytes;
    private final Timer sortManifestMd5Latency;
    private final Counter sortManifestBoundsRows;
    private final Counter sortManifestBoundsBytes;
    private final Timer sortManifestBoundsLatency;
    private final AtomicLong sortFinalizeParallelism = new AtomicLong();
    private final Timer sortBackpressureWait;
    private final DistributionSummary sortPageRunsPerBuffer;

    // In-flight sort-staging legibility (instrumentation only — no admit/seal/backpressure
    // behavior change). Same CAS-max high-water-mark idiom as peakInFlight (getAndAccumulate(v,
    // Math::max)); a future billion-scale repro reads these peaks to adjudicate "unbounded
    // leak" (climbs without bound under a retry storm) vs "linear-in-T tuning" (tracks ~T ×
    // segmentBytes, off_thread never exceeds its configured buffers()-1 bound) — see
    // SortLaneMeters's javadoc for the full reading guide.
    private final AtomicLong sortStagingBytesPeak = new AtomicLong();
    private final AtomicLong sortHandoffQueueDepthPeak = new AtomicLong();
    private final AtomicLong sortOffThreadBuffersPeak = new AtomicLong();

    // §3.8: S3 connection-pool utilization gauges — fed by S3PoolMetricPublisher (swath-s3), which
    // stays SDK-free-boundary-safe by living outside this SDK-agnostic module. `-1` = unobserved
    // (no attempt has reported this metric yet); the gauge suppliers below map that to `NaN` via the
    // single shared `nanIfUnavailable` idiom (also used by `swath.process.*` and `swath.phase`) —
    // AtomicLong (not AtomicInteger) purely so every one of these "-1 unobserved" gauges shares that
    // one helper instead of a duplicated int-flavored copy.
    private final AtomicLong s3PoolLeased = new AtomicLong(-1);
    private final AtomicLong s3PoolIdleAvailable = new AtomicLong(-1);
    private final AtomicLong s3PoolPendingAcquisition = new AtomicLong(-1);
    private final AtomicLong s3PoolMax = new AtomicLong(-1);

    // Connection-churn visibility for the ATTEMPT_TIMEOUT self-amplification livelock — every
    // classified ATTEMPT_TIMEOUT/NETWORK fault destroys its Apache connection (SDK-source-confirmed:
    // ApiCallAttemptTimeoutException aborts the in-flight attempt, which walks
    // `abortable.abort() -> ConnectionHolder.cancel() -> abortConnection() -> managedConn.shutdown()` —
    // a hard socket destroy, never the reusable-release path), so under a storm the pool must open a
    // fresh TLS connection per retry at up to ~T/attemptTimeout handshakes/sec. `swath.s3.pool.*`
    // above only shows leased/idle/pending/max snapshots and cannot see this churn.
    private final Counter s3ConnectionAborted;
    private final Counter s3ConnectionHandshakes;

    // A client-local socket-closure / IOException-wrapper fault (e.g. a
    // `UncheckedIOException(SocketException("Socket closed"))` surfacing from a transient S3 500
    // burst) that escaped the SDK call as a non-`SdkException` RuntimeException, reclassified into
    // the transient NETWORK kind and ridden out instead of crashing the run at `error_class=unknown`.
    // A distinct series from the modeled `isNetworkExhaustion` `SdkClientException` NETWORK path (both
    // record `swath.throttle.events{type=network}`) so post-hoc analysis can tell the wrapper-escape
    // path apart and see how often it engaged.
    private final Counter s3SocketClosureRecovered;

    // §3.2: universal monotonic progress counter (THE stuck signal — advances in every phase by
    // construction, so `rate(progress.units)==0 ⇒ stuck` is unconditionally true with no phase
    // gating/boundary race) and the companion live `swath.phase` gauge (dashboard readability only,
    // NOT the correctness gate — see Phase's javadoc). `-1` = unset/no phase set yet, same
    // unobserved-until-first-value idiom as the §3.8 pool gauges above.
    private final Counter progressUnits;
    private final AtomicLong phaseCode = new AtomicLong(-1);
    // Live-progress state, all read once per progress tick and never on a hot path. The two
    // tallies are plain monotonic mirrors of counters whose totals would otherwise cost a full
    // registry walk per tick (counterTotal scans every meter and every measurement); same
    // dedicated-mirror idiom the liveness tallies above use, for the same reason.
    private final AtomicLong apiCallsTally = new AtomicLong();
    private final AtomicLong stealsTally = new AtomicLong();
    /** Objects a resume carried over from a previous attempt — session work is emitted minus this. */
    private final AtomicLong recoveredObjects = new AtomicLong();
    /**
     * When the current phase began, so a tick can show phase elapsed WITHOUT resetting session
     * elapsed. Seeded at construction rather than left at zero: before the first {@link #setPhase}
     * the run IS in {@link Phase#STARTING}, and that state began when these metrics did — a zero
     * here would report the phase clock as running since the {@link System#nanoTime()} origin.
     */
    private final AtomicLong phaseStartNanos = new AtomicLong(System.nanoTime());
    /** {@code progress.units} at the merge's start — the baseline merge WORK is counted from. */
    private final AtomicLong mergeProgressBaseline = new AtomicLong(-1);
    /** {@code progress.units} at the final merge pass's start — the baseline completion is counted from. */
    private final AtomicLong finalPassProgressBaseline = new AtomicLong(-1);
    /** Rows staged into the sort segments handed to the merge: the merge's exact denominator. */
    private final AtomicLong sortStagedRows = new AtomicLong();
    private final AtomicLong sortStagedSegments = new AtomicLong();
    /** Seed structure probes completed, their budget, and when the last one landed (liveness). */
    private final AtomicLong seedProbes = new AtomicLong();
    private final AtomicLong seedProbeBudget = new AtomicLong();
    private final AtomicLong lastSeedProbeNanos = new AtomicLong(-1);

    // Shape feature-vector accumulators (end-of-run classification signals; §5) — extracted to
    // ShapeAccumulator; its two dimension constants now live there too (relocated). Aliases
    // kept here for RunMetrics' own pinned-surface callers.
    /** Number of relative code-point positions in the run-level alphabet cardinality AGGREGATE. */
    static final int ALPHABET_POSITIONS = ShapeAccumulator.ALPHABET_POSITIONS;
    /** Number of divergence-depth histogram buckets; the last bucket is depth {@code >= 15}. */
    static final int DIVERGENCE_DEPTH_BUCKETS = ShapeAccumulator.DIVERGENCE_DEPTH_BUCKETS;
    private final ShapeAccumulator shapeAccumulator =
            new ShapeAccumulator(ShapeAccumulator.ALPHABET_POSITIONS, ShapeAccumulator.DIVERGENCE_DEPTH_BUCKETS);

    // Per-call-class latency-phase decomposition (worker page fetch vs the thief's 1-key pivot
    // probe vs its delimiter=/ structure probe) -- lazily-registered Timers, same computeIfAbsent idiom
    // as stealReasonCounters/apiCalls above. Bounded cardinality: 3 call classes x 5 phases = 15 series.
    /** {@code call_class} tag value: a worker's slot-gated range page fetch. */
    public static final String CALL_CLASS_WORKER_PAGE = "worker_page";
    /** {@code call_class} tag value: the thief's 1-key ({@code max_keys<=1}, no delimiter) pivot probe. */
    public static final String CALL_CLASS_PIVOT_PROBE = "pivot_probe";
    /** {@code call_class} tag value: the thief's {@code delimiter=/} structure probe. */
    public static final String CALL_CLASS_STRUCTURE_PROBE = "structure_probe";
    /** {@code phase} tag value: the Apache HTTP client's connection-pool checkout wait. */
    public static final String LATENCY_PHASE_CONNECT_ACQUIRE = "connect_acquire";
    /** {@code phase} tag value: SDK-reported time-to-first-byte (request start through first response byte). */
    public static final String LATENCY_PHASE_TTFB = "ttfb";
    /**
     * {@code phase} tag value: the SDK's own RESPONSE-HANDLING window — first response byte through
     * the SDK's protocol response handler returning — bridged in from the SDK's per-attempt metrics
     * by the store layer's metric publisher. For a sync S3 {@code ListObjectsV2} that is draining the
     * remaining response body off the socket plus the XML parse and POJO construction: the dominant
     * term of the {@link #LATENCY_PHASE_TOTAL}-minus-{@link #LATENCY_PHASE_TTFB} residual, which
     * before this phase existed was the only thing that made it visible at all.
     *
     * <p><b>Not a pure unmarshal span, and not pure client CPU.</b> The SDK's own
     * unmarshal-duration metric is not reported at all for this operation, so the window is DERIVED
     * from the SDK's time-to-last-byte and time-to-first-byte stamps (see the store layer's
     * publisher for the exact derivation and why it is a close upper bound rather than the true
     * boundary). It also EXCLUDES the SDK's response-interceptor chain (for S3, the percent-decode of
     * the {@code encoding-type=url} response the store layer itself requests, which rebuilds the
     * response object), everything request-side of the attempt's first byte, any retry backoff, and
     * {@link #LATENCY_PHASE_RESPONSE_PARSE}, which happens after the call returns. So this phase
     * narrows the residual, it does not close it.
     *
     * <p>Best-effort like {@link #LATENCY_PHASE_CONNECT_ACQUIRE}/{@link #LATENCY_PHASE_TTFB}: absent
     * on an attempt it could not be derived for, and then skipped rather than fabricated as {@code 0}.
     */
    public static final String LATENCY_PHASE_SDK_UNMARSHAL = "sdk_unmarshal";
    /** {@code phase} tag value: this fetcher's own measured wall-clock total. */
    public static final String LATENCY_PHASE_TOTAL = "total";
    /**
     * {@code phase} tag value: the client-side conversion of an already-received response into
     * swath's own page model (entries + common prefixes) — the one parse cost swath itself owns and
     * can time. NOT the SDK's own unmarshalling, which happens inside the call and is reported
     * separately as {@link #LATENCY_PHASE_SDK_UNMARSHAL}.
     */
    public static final String LATENCY_PHASE_RESPONSE_PARSE = "response_parse";
    private final ConcurrentMap<String, Timer> callClassLatencyTimers = new ConcurrentHashMap<>();

    /**
     * Distribution-statistic window for every {@link Timer}/{@link DistributionSummary} here: one
     * non-rotating bucket covering the whole run.
     *
     * <p>Micrometer's DEFAULT is a ROLLING window — {@code expiry=2m}, {@code bufferLength=3} — which
     * makes {@code max()} and every published percentile decay, while {@code count()} and {@code
     * totalTime()} stay cumulative. In a JSON run summary, which is explicitly a post-hoc forensics
     * artifact, that silently mixes two different time bases in one row. One run showed the mismatch
     * starkly: {@code swath.rate_limit.wait} reported {@code count=6819, total_ms=143045,
     * max_ms=0.001117} — 21 ms of average slot wait but a sub-microsecond max, because slot
     * contention stopped partway through and the rolling max had decayed to nothing by the time the
     * summary was written. Every percentile in {@code probe_latency[]} and {@code
     * shape.regime.api_latency_p*} had the same defect: they described only the run's last ~2 minutes
     * while being presented, and read, as run-level facts.
     *
     * <p>A single bucket that never rotates makes {@code max}/percentiles cover exactly what {@code
     * count}/{@code total} already cover — the run. Bounded in memory: expiry governs histogram
     * ROTATION, not bucket count.
     */
    private static final Duration DISTRIBUTION_WINDOW = Duration.ofDays(3650);

    /**
     * A {@link Timer} builder whose distribution statistics span the whole run rather than
     * Micrometer's rolling 2-minute default — see {@link #DISTRIBUTION_WINDOW} for why every timer
     * feeding the run summary must be built through this.
     */
    private static Timer.Builder runScopedTimer(String name) {
        return Timer.builder(name)
                .distributionStatisticExpiry(DISTRIBUTION_WINDOW)
                .distributionStatisticBufferLength(1);
    }

    /** The percentiles every distribution-reporting timer here publishes. */
    private static final double[] PUBLISHED_PERCENTILES = {0.5, 0.90, 0.99};

    /**
     * A {@link #runScopedTimer} that also publishes {@link #PUBLISHED_PERCENTILES} — the builder
     * every client-service-cost span goes through, so the whole decomposition reports the same
     * distribution statistics rather than a per-span mix of mean-only and percentile-bearing rows.
     */
    private static Timer.Builder clientCostSpanTimer(String name) {
        return runScopedTimer(name).publishPercentiles(PUBLISHED_PERCENTILES);
    }

    /** The {@link DistributionSummary} sibling of {@link #runScopedTimer}. */
    private static DistributionSummary.Builder runScopedSummary(String name) {
        return DistributionSummary.builder(name)
                .distributionStatisticExpiry(DISTRIBUTION_WINDOW)
                .distributionStatisticBufferLength(1);
    }

    // Demand-gate T-vs-Tmax visibility -- the last/min effective T observed at the INSTANT an
    // OWNER_SPLIT.demand_gated suppression fired, plus the run's Tmax, so a shed-shrunken demand gate is
    // readable from one artifact. `-1` = never fired (the nanIfUnavailable/atomicLongOrNan idiom, NaN
    // in the gauge, null in the JSON summary).
    private final AtomicLong demandGatedEvents = new AtomicLong();
    private final AtomicLong demandGatedLastT = new AtomicLong(-1);
    private final AtomicLong demandGatedMinT = new AtomicLong(-1);
    private final AtomicLong demandGatedTMax = new AtomicLong(-1);

    // Efficiency-ratio inputs that aren't already exposed as a standalone counter.
    /** The {@code max-keys} page-size configured for this run, captured off {@link #recordListingPageShape}. */
    private final AtomicLong configuredMaxKeys = new AtomicLong();
    /** The seed step's summary (mode/probes/cut_points/synthesized_cuts/ranges), null until seeded. */
    private final AtomicReference<RunSummary.SeedSummary> seedSummary = new AtomicReference<>();

    // Trajectory rollup: a fixed-size (TRAJECTORY_BINS) time-bin rollup of in-flight concurrency +
    // progress rate, folded on the SAME "sample on every transition" seam recordInFlightTransition
    // already uses (pure observation — no extra store/API call). Extracted to TrajectoryRollup;
    // its bin-count constant now lives there too (relocated). Alias kept here for RunMetrics' own
    // pinned-surface callers.
    static final int TRAJECTORY_BINS = TrajectoryRollup.TRAJECTORY_BINS;
    private final TrajectoryRollup trajectory = new TrajectoryRollup(TrajectoryRollup.TRAJECTORY_BINS);

    // Tail-occupancy sampler: bounded (DEFAULT_CAPACITY-slot) stride-gated samples of (keys
    // emitted, elapsed, in-flight), folded on the SAME already-serialized recordEntriesEmitted
    // seam (one consumer stage per run) -- see TailOccupancySampler's javadoc for why a whole-run
    // avg_in_flight cannot screen for a serial tail, and swath.tail_occupancy.{avg_in_flight,
    // wall_share}{pct=5|10} below for the reported gauges.
    private final TailOccupancySampler tailOccupancy = new TailOccupancySampler(TailOccupancySampler.DEFAULT_CAPACITY);

    // Slow-range dump: a supplier the engine (WorkStealingScan) registers once at construction
    // so a terminal/mid-run summary can read a point-in-time snapshot of the live worklist without
    // this observability-layer class depending on the engine's worklist type. `null` (the default,
    // e.g. most unit tests that never construct a WorkStealingScan against this RunMetrics) renders
    // an empty slow_ranges[] — never an error.
    private volatile Supplier<List<RangeSnapshot>> rangeSnapshotSource;

    // CPU baseline for cpu_seconds/cpu_efficiency. Defaults to construction time, but ListRunner
    // calls markRunStarted() at the same instant it starts the wall clock so the two share a zero
    // point (otherwise pre-run CPU — checkpoint open, resume load — is counted against a wall window
    // that excludes it, inflating cpu_efficiency).
    private volatile long baselineCpuNanos = ResourceMetrics.processCpuTimeNanos();
    private final AtomicReference<RateSample> lastRateSample = new AtomicReference<>();

    private record RateSample(long elapsedNanos, long keys) {
    }

    public RunMetrics(MeterRegistry registry) {
        this(registry, System::nanoTime);
    }

    /** {@code nanoClock} seam for deterministic avg-in-flight tests; production always uses {@link #RunMetrics(MeterRegistry)}. */
    public RunMetrics(MeterRegistry registry, LongSupplier nanoClock) {
        this.registry = registry;
        this.nanoClock = nanoClock;
        this.inFlightGauge = new InFlightGauge(nanoClock);
        entriesEmitted = Counter.builder("swath.entries.emitted").register(registry);
        openFrontierKeysEmitted = Counter.builder("swath.open_frontier.keys_emitted").register(registry);
        bytesEstimated = Counter.builder("swath.bytes.estimated").register(registry);
        progressUnits = Counter.builder("swath.progress.units").register(registry);
        probeFetches = Counter.builder("swath.probe.fetches").register(registry);
        structureProbeFetches = Counter.builder("swath.probe.structure_fetches").register(registry);
        emptyUpperBisections = Counter.builder("swath.probe.empty_upper_bisections").register(registry);
        unsplittableVictims = Counter.builder("swath.split.unsplittable_victims").register(registry);
        splitGuardAborts = Counter.builder("swath.split.guard_aborts").register(registry);
        rawPages = Counter.builder("swath.page.raw_count").register(registry);
        rawPageKeys = Counter.builder("swath.page.raw_keys").register(registry);
        shortTruncatedPages = Counter.builder("swath.page.short_truncated").register(registry);
        // §3.1: one pre-resolved Counter per ThrottleType — self-inflicted attempt_timeout is now
        // distinguishable from a real S3 slowdown/server5xx/network throttle on the dashboard.
        throttleEvents = new EnumMap<>(ThrottleType.class);
        // (see #runScopedTimer for why every Timer/DistributionSummary below is built through the
        // run-scoped helpers rather than Micrometer's rolling defaults)
        for (ThrottleType type : ThrottleType.values()) {
            throttleEvents.put(type,
                    Counter.builder("swath.throttle.events").tag("type", type.tag()).register(registry));
        }
        aimdVotes = Counter.builder("swath.aimd.votes").register(registry);
        aimdTargetReductions = Counter.builder("swath.aimd.target_reductions").register(registry);
        aimdTimeoutShed = Counter.builder("swath.aimd.timeout_shed").register(registry);
        aimdLatencyFreeze = Counter.builder("swath.aimd.latency_freeze").register(registry);
        aimdFreezeGateChecks = Counter.builder("swath.aimd.freeze_gate_checks").register(registry);
        aimdGrowthFreeze = Counter.builder("swath.aimd.growth_freeze").register(registry);
        Gauge.builder("swath.aimd.latency_baseline_ms", latencyBaselineMillis, AtomicLong::get).register(registry);
        // Publish client-side p50/p99 so the regime-confound RTT is real (the timer
        // otherwise exposes only mean/max) — read back into the shape block at end of run.
        listObjectsLatency = runScopedTimer("swath.api.latency").tag("op", "listObjectsV2")
                .publishPercentiles(PUBLISHED_PERCENTILES).register(registry);
        // A client-service-cost span (see #buildClientCostSummary): every span in that
        // decomposition publishes percentiles, because a per-page cost is only interpretable as a
        // DISTRIBUTION — a mean hides the contended-writer tail that separates "iid per-page cost"
        // from "queued behind a shared writer".
        queueWait = clientCostSpanTimer("swath.queue.wait").register(registry);
        rateLimitWait = runScopedTimer("swath.rate_limit.wait").register(registry);
        // Splits the reactive AIMD concurrency-slot wait (above) from the opt-in
        // `--rate-limit-api` proactive client-side cap, which accrues here
        // instead (see
        // docs/metrics-and-observability.md §1.1).
        apiRateLimitWait = runScopedTimer("swath.rate_limit.api_wait").register(registry);

        // Idle-backoff.
        idleBackoffResets = Counter.builder("swath.idle_backoff.resets").register(registry);
        idleBackoffSlotDenied = Counter.builder("swath.idle_backoff.slot_denied").register(registry);
        idleBackoffParkTime = runScopedTimer("swath.idle_backoff.park_time").register(registry);
        Gauge.builder("swath.idle_backoff.level", idleBackoffLevel, AtomicLong::get).register(registry);

        // Checkpoint/resume (the SqliteCheckpointStore single-writer path). The three timers are
        // client-service-cost spans (see #buildClientCostSummary).
        checkpointCommitLatency = clientCostSpanTimer("swath.checkpoint.commit.latency").register(registry);
        checkpointQueueWait = clientCostSpanTimer("swath.checkpoint.queue.wait").register(registry);
        checkpointCommitWait = clientCostSpanTimer("swath.checkpoint.commit.wait").register(registry);
        checkpointCommitBatchSize = runScopedSummary("swath.checkpoint.commit_batch_size").register(registry);

        // Parquet writer pool (rotation/finalize/discard, plus the lane threads' own encode/write
        // work — a client-service-cost span, see #buildClientCostSummary).
        parquetFinalizeLatency = runScopedTimer("swath.parquet.finalize.latency").register(registry);
        parquetWriteLatency = clientCostSpanTimer("swath.parquet.write.latency").register(registry);

        // Text-sink broken-pipe outcome + end-of-run duration/throughput aggregates.
        outputBrokenPipe = Counter.builder("swath.output.broken_pipe").register(registry);
        // The consumer stage's own per-page sink-write span (client service cost, see
        // #buildClientCostSummary).
        emitLatency = clientCostSpanTimer("swath.emit.latency").register(registry);
        runDuration = runScopedTimer("swath.run.duration").register(registry);
        Gauge.builder("swath.run.throughput", runThroughputKeysPerSec,
                        r -> r.get() == null ? Double.NaN : r.get())
                .register(registry);

        // --sort meters.
        sortEntries = Counter.builder("swath.sort.entries").register(registry);
        sortSegmentsWritten = Counter.builder("swath.sort.segments.written").register(registry);
        sortSegmentBytes = Counter.builder("swath.sort.segment.bytes").baseUnit("bytes").register(registry);
        sortMergePasses = Counter.builder("swath.sort.merge.passes").register(registry);
        sortMergeLatency = runScopedTimer("swath.sort.merge.latency").register(registry);
        sortMergeRangeLatency = runScopedTimer("swath.sort.merge.range.latency").register(registry);
        sortMergeBoundariesLatency =
                runScopedTimer("swath.sort.merge.boundaries.latency").register(registry);
        sortMergeBoundaryEmbeddedEntries =
                Counter.builder("swath.sort.merge.boundaries.embedded.entries").register(registry);
        sortMergeBoundaryEmbeddedBytes = Counter.builder("swath.sort.merge.boundaries.embedded.bytes")
                .baseUnit("bytes").register(registry);
        sortMergeBoundaryScanBytes = Counter.builder("swath.sort.merge.boundaries.scan.bytes")
                .baseUnit("bytes").register(registry);
        sortFinalizeCloseLatency = runScopedTimer("swath.sort.finalize.close.latency").register(registry);
        sortFinalizeLatency = runScopedTimer("swath.sort.finalize.latency").register(registry);
        sortPublicationLatency = runScopedTimer("swath.sort.publication.latency").register(registry);
        sortManifestMd5Bytes = Counter.builder("swath.sort.manifest.md5.bytes")
                .baseUnit("bytes").register(registry);
        sortManifestMd5Latency = runScopedTimer("swath.sort.manifest.md5.latency").register(registry);
        sortManifestBoundsRows = Counter.builder("swath.sort.manifest.bounds.rows").register(registry);
        sortManifestBoundsBytes = Counter.builder("swath.sort.manifest.bounds.bytes")
                .baseUnit("bytes").register(registry);
        sortManifestBoundsLatency = runScopedTimer("swath.sort.manifest.bounds.latency").register(registry);
        Gauge.builder("swath.sort.finalize.parallelism", sortFinalizeParallelism, AtomicLong::get)
                .register(registry);
        sortBackpressureWait = runScopedTimer("swath.sort.backpressure.wait").register(registry);
        sortPageRunsPerBuffer = runScopedSummary("swath.sort.page_runs_per_buffer").register(registry);
        // Peak in-flight staging bytes / handoff-queue depth / off-thread buffer count — see
        // the field javadoc above for the bounded-vs-unbounded reading guide.
        Gauge.builder("swath.sort.staging.bytes.peak", sortStagingBytesPeak, AtomicLong::get)
                .baseUnit("bytes").register(registry);
        Gauge.builder("swath.sort.handoff.queue.depth.peak", sortHandoffQueueDepthPeak, AtomicLong::get)
                .register(registry);
        Gauge.builder("swath.sort.off_thread.buffers.peak", sortOffThreadBuffersPeak, AtomicLong::get)
                .register(registry);

        // §3.8: S3 connection-pool utilization — see the field javadoc above for the `-1`/`NaN`
        // unobserved idiom. `pending_acquisition > 0` is acquisition-starvation; `leased ≪ max`
        // while in-flight is low is AIMD self-throttle — the two are indistinguishable from
        // `workers.active`/`in_flight.avg` alone.
        Gauge.builder("swath.s3.pool.leased", s3PoolLeased, RunMetrics::atomicLongOrNan).register(registry);
        Gauge.builder("swath.s3.pool.idle_available", s3PoolIdleAvailable, RunMetrics::atomicLongOrNan).register(registry);
        Gauge.builder("swath.s3.pool.pending_acquisition", s3PoolPendingAcquisition, RunMetrics::atomicLongOrNan).register(registry);
        Gauge.builder("swath.s3.pool.max", s3PoolMax, RunMetrics::atomicLongOrNan).register(registry);
        // Connection-churn counters, see the field javadoc above.
        s3ConnectionAborted = Counter.builder("swath.s3.pool.connection_aborted").register(registry);
        s3ConnectionHandshakes = Counter.builder("swath.s3.pool.handshakes").register(registry);
        // Socket-closure/IOException-wrapper recovery engagement counter (see field javadoc).
        s3SocketClosureRecovered = Counter.builder("swath.s3.socket_closure_recovered").register(registry);

        // §3.2: live phase gauge, NaN/absent until the first setPhase() call.
        Gauge.builder("swath.phase", phaseCode, RunMetrics::atomicLongOrNan).register(registry);

        Gauge.builder("swath.workers.active", concurrencyTarget, AtomicLong::get).register(registry);
        // The min effective-T reached (shed floor / ceiling hit), so the recovered-error
        // rollup and post-hoc analysis can see how far the brake pulled T down on a COMPLETED run.
        Gauge.builder("swath.aimd.target_low_water", concurrencyTargetLowWater, AtomicLong::get).register(registry);
        // Demand-gate T-vs-Tmax visibility -- see the fields' javadoc above.
        Gauge.builder("swath.owner_split.demand_gated_t", demandGatedLastT, RunMetrics::atomicLongOrNan).register(registry);
        Gauge.builder("swath.owner_split.demand_gated_t_min", demandGatedMinT, RunMetrics::atomicLongOrNan).register(registry);
        // Pull-based (evaluated on each read, same idiom as the swath.process.* gauges below) —
        // no background sampler thread; the accumulator is folded on every in-flight transition.
        Gauge.builder("swath.in_flight.avg", this, RunMetrics::avgInFlight).register(registry);
        // Tail-occupancy screen: last-N% window avg in-flight + wall-time share, tagged pct=5|10
        // rather than four separate meter names (see TailOccupancySampler's javadoc). Pull-based,
        // same idiom as the gauge above -- no background sampler thread.
        for (int pct : TailOccupancySampler.WINDOW_PERCENTS) {
            Gauge.builder("swath.tail_occupancy.avg_in_flight", this,
                            m -> m.tailOccupancyAvgInFlight(pct))
                    .tag("pct", Integer.toString(pct)).register(registry);
            Gauge.builder("swath.tail_occupancy.wall_share", this,
                            m -> m.tailOccupancyWallShare(pct))
                    .tag("pct", Integer.toString(pct)).register(registry);
        }

        Gauge.builder("swath.process.memory.rss.bytes", this, m -> nanIfUnavailable(ResourceMetrics.currentRssBytes()))
                .tag("kind", "current").baseUnit("bytes").register(registry);
        Gauge.builder("swath.process.memory.rss.bytes", this, m -> nanIfUnavailable(ResourceMetrics.peakRssBytes()))
                .tag("kind", "peak").baseUnit("bytes").register(registry);
        Gauge.builder("swath.process.memory.heap.bytes", this, m -> nanIfUnavailable(ResourceMetrics.currentHeapBytes()))
                .tag("kind", "current").baseUnit("bytes").register(registry);
        Gauge.builder("swath.process.memory.heap.bytes", this, m -> nanIfUnavailable(ResourceMetrics.peakHeapBytes()))
                .tag("kind", "peak").baseUnit("bytes").register(registry);
        // CPU time is cumulative/monotonic, so a FunctionCounter, not a gauge; a FunctionCounter can't
        // sanely report NaN, so only register it when the platform bean is available up front.
        if (ResourceMetrics.processCpuTimeNanos() >= 0) {
            FunctionCounter.builder("swath.process.cpu.time", this, m -> ResourceMetrics.processCpuTimeNanos() / 1_000_000_000.0)
                    .baseUnit("seconds").register(registry);
        }
    }

    /** Maps the {@code ResourceMetrics} {@code -1} "unavailable" sentinel to {@code NaN} for gauge suppliers. */
    static double nanIfUnavailable(long value) {
        return value < 0 ? Double.NaN : (double) value;
    }

    /**
     * {@link AtomicLong}-adapter for {@link #nanIfUnavailable} — the single shared {@code -1} →
     * {@code NaN} unobserved idiom, reused (not duplicated) for the §3.8 pool gauges and {@code
     * swath.phase}.
     */
    private static double atomicLongOrNan(AtomicLong value) {
        return nanIfUnavailable(value.get());
    }

    /**
     * §3.8: updates the S3 connection-pool utilization gauges from whatever a
     * {@code MetricPublisher} observed on one attempt — the SDK reports only a subset of
     * {@code leased}/{@code idleAvailable}/{@code pending}/{@code max} per attempt (a per-attempt
     * child collection, not every field on every call), so each argument is applied only when
     * non-null; the others are left at their last-observed value (or unobserved). Thread-safe:
     * each field is an independent atomic, so concurrent updates from different attempts never
     * tear a single field's value.
     */
    public void updateS3Pool(Integer leased, Integer idleAvailable, Integer pending, Integer max) {
        if (leased != null) {
            s3PoolLeased.set(leased);
        }
        if (idleAvailable != null) {
            s3PoolIdleAvailable.set(idleAvailable);
        }
        if (pending != null) {
            s3PoolPendingAcquisition.set(pending);
        }
        if (max != null) {
            s3PoolMax.set(max);
        }
    }

    /**
     * One Apache connection was forced-destroyed by a client-side abort — recorded once per
     * classified {@code ATTEMPT_TIMEOUT}/{@code NETWORK} fault ({@code S3PageFetcher}'s catch arms),
     * each of which is 1:1 with an aborted/shutdown connection (see the field javadoc above). A
     * distinct series from {@code swath.throttle.events{type}} (that is the fault-classification
     * count; this is the connection-churn count it drives) — never folded together.
     */
    public void recordConnectionAborted() {
        s3ConnectionAborted.increment();
    }

    /**
     * One new Apache HTTP connection completed its TLS handshake ({@code
     * S3HandshakeCountingSocketFactory.connectSocket}, swath-s3) — the rate this is observed to climb
     * to under a storm is the handshake-churn signal {@link #recordConnectionAborted()} predicts.
     */
    public void recordConnectionHandshake() {
        s3ConnectionHandshakes.increment();
    }

    /**
     * A socket-closure / IOException-wrapper fault (a non-{@link software.amazon.awssdk.core.exception.SdkException}
     * RuntimeException whose cause chain contains an {@link java.io.IOException}, e.g. {@code
     * UncheckedIOException(SocketException("Socket closed"))}) escaped the SDK call and was reclassified
     * into the transient {@code NETWORK} kind and ridden out ({@code S3PageFetcher}'s catch arm),
     * rather than crashing the run unclassified at {@code error_class=unknown}. Recorded once per such
     * recovery — a distinct series from the modeled {@code SdkClientException} NETWORK path so post-hoc
     * analysis can tell the wrapper-escape path engaged and how often (surfaced in the {@code
     * recovered_errors} rollup as {@code socket_closure}).
     */
    public void recordSocketClosureRecovered() {
        s3SocketClosureRecovered.increment();
    }

    /**
     * §3.3: registers the pull-based {@code swath.disk.free_bytes} gauge, sampling {@code
     * FileStore.getUsableSpace()} on {@code scratchDir}'s filesystem on every scrape — same idiom as
     * the {@code swath.process.memory.*} gauges above (no background sampler thread). {@code
     * scratchDir} should be the volume that actually takes the write load (sort-staging segments +
     * Parquet parts).
     *
     * <p>Idempotent and null-safe: a {@code null} path, or a second call after the gauge is already
     * registered, is a no-op — so the many unit tests that build a {@link RunMetrics} with no output
     * path never register anything, and production call sites that may resolve the directory more
     * than once (e.g. distinct {@code ListCommand} branches) can each call this unconditionally.
     */
    public void registerDiskFreeGauge(Path scratchDir) {
        if (scratchDir == null || !diskGaugeRegistered.compareAndSet(false, true)) {
            return;
        }
        try {
            // Same WeakReference hazard as registerCheckpointQueueDepthGauge above: scratchDir may
            // not be strongly retained anywhere else by the caller, so pin it via strongReference
            // to avoid the gauge going NaN after the first GC.
            Gauge.builder("swath.disk.free_bytes", scratchDir, RunMetrics::usableSpaceOrNaN)
                    .baseUnit("bytes").strongReference(true).register(registry);
        } catch (RuntimeException e) {
            // Registration failed -- don't permanently lose the gauge behind an already-flipped flag;
            // a later call (e.g. a retried ListCommand branch) can still register it.
            diskGaugeRegistered.set(false);
            throw e;
        }
    }

    /**
     * {@code Files.getFileStore(dir).getUsableSpace()}, or {@code NaN} on failure (emitted as a
     * {@code NaN} datapoint at export and dropped downstream by the collector/backend, not omitted
     * by Micrometer itself).
     */
    private static double usableSpaceOrNaN(Path dir) {
        try {
            return (double) Files.getFileStore(dir).getUsableSpace();
        } catch (IOException | RuntimeException e) {
            return Double.NaN;
        }
    }

    public MeterRegistry registry() {
        return registry;
    }

    public Timer.Sample startS3PageTimer() {
        return Timer.start(registry);
    }

    public void recordS3Latency(Timer.Sample sample) {
        sample.stop(listObjectsLatency);
    }

    public void recordS3Error(String code) {
        recordError(code);
    }

    public void recordS3Throttle() {
        recordError("throttle");
    }

    public Timer.Sample startQueueWaitTimer() {
        return Timer.start(registry);
    }

    public void recordQueueWait(Timer.Sample sample) {
        sample.stop(queueWait);
    }

    public Timer.Sample startRateLimitWaitTimer() {
        return Timer.start(registry);
    }

    public void recordRateLimitWait(Timer.Sample sample) {
        sample.stop(rateLimitWait);
    }

    /**
     * The {@code --rate-limit-api} proactive client-side cap's OWN timer, distinct from
     * {@link #startRateLimitWaitTimer()}/{@link #recordRateLimitWait} (which stays the AIMD
     * concurrency-slot wait's own meter, {@code swath.rate_limit.wait}). Only ever driven
     * by {@code RateLimitedPageFetcher}, which is only constructed when {@code --rate-limit-api}
     * is set — so {@code swath.rate_limit.api_wait} is genuinely zero when the flag is unset.
     */
    public Timer.Sample startApiRateLimitWaitTimer() {
        return Timer.start(registry);
    }

    /** Stops the timer started by {@link #startApiRateLimitWaitTimer()}. */
    public void recordApiRateLimitWait(Timer.Sample sample) {
        sample.stop(apiRateLimitWait);
    }

    public void recordApiCall() {
        String tag = normalizeTag(strategy.get());
        apiCalls.computeIfAbsent(tag,
                s -> Counter.builder("swath.api.calls").tag("strategy", s).register(registry)).increment();
        apiCallsTally.incrementAndGet();   // cheap mirror for the live progress tick
    }

    public void recordError(String type) {
        errors.computeIfAbsent(normalizeTag(type).toLowerCase(Locale.ROOT),
                t -> Counter.builder("swath.errors").tag("type", t).register(registry)).increment();
    }

    public void recordSteal(String outcome) {
        steals.computeIfAbsent(normalizeTag(outcome),
                o -> Counter.builder("swath.steals").tag("result", o).register(registry)).increment();
        stealsTally.incrementAndGet();   // cheap mirror for the live progress tick
    }

    public void recordSplit() {
        splits.incrementAndGet();
    }

    public void recordPage() {
        pages.incrementAndGet();
        stuckClassifier.snapshotAtProgress();
    }

    /**
     * Mark one unit of phase-appropriate forward progress that page/object counters do NOT
     * capture — the sort-merge / parquet-finalize tail (rows drained into the final sorted file,
     * phase-boundary handoffs). Cheap (one atomic increment); the liveness watchdog reads the result
     * via {@link #progressSignal()} to tell "the merge is still advancing" from "the pipeline is
     * wedged". Throttle the call granularity at the hook (e.g. every N rows) so it is never per-key.
     */
    public void markProgress() {
        livenessProgress.incrementAndGet();
        stuckClassifier.snapshotAtProgress();
    }

    /**
     * Liveness watchdog progress signal: a cheap, monotonic-nondecreasing snapshot that advances
     * on forward progress in EVERY phase, so the watchdog can distinguish a wedged run (this value
     * frozen for the whole stall window ⇒ abort) from a legitimately slow-but-progressing one. It is
     * PHASE-AWARE by construction: {@code pages}/{@code objects} advance during LISTING, sort staging
     * segments advance while the sort encoder drains, and {@link #markProgress()} ticks carry the
     * sort-merge/finalize tail where no page completes. The absolute value is meaningless — only its
     * change between two reads matters — so summing these disjoint monotonic quantities is safe.
     *
     * <p>Also folds in {@code swath.throttle.events} (every type —
     * {@code slowdown}/{@code server5xx}/{@code attempt_timeout}/{@code network}). A page under
     * sustained real-503/attempt-timeout backpressure retries for many multiples of the stall window
     * without ever COMPLETING — the plan's own "attempts finishing but failing" case, which is alive,
     * not wedged — so each classified transient/throttle event (itself only recorded when an attempt
     * actually returns, per {@code S3PageFetcher}) counts as forward progress here. A TRUE wedge (the
     * socket read never returns at all) produces zero new events AND zero completions, so it still
     * correctly freezes this signal and trips.
     *
     * <p><b>Backed by plain AtomicLongs only.</b> This reads {@code AtomicLong} tallies
     * ({@link #progressUnitsTally}/{@link #sortSegmentsTally}/{@link #throttleEventsTally}), NEVER a
     * Micrometer {@code Counter.count()} — under a DELTA/step OTLP registry {@code count()} resets per
     * step, which would make this signal non-monotonic in production while every
     * {@code SimpleMeterRegistry} test hid it. {@code progressUnits} (folded in via its tally) already
     * advances in every phase — entries during listing/writing, rows merged during the sort cascade —
     * so a long intermediate cascade-merge pass counts as forward progress here and cannot false-trip.
     */
    public long progressSignal() {
        return pages.get()
                + livenessProgress.get()
                + progressUnitsTally.get()
                + sortSegmentsTally.get()
                + throttleEventsTally.get();
    }

    /**
     * Real forward progress = useful work COMMITTED (pages/objects, sort segments, merge/finalize
     * {@link #markProgress()} ticks); deliberately EXCLUDES throttle/retry ACTIVITY so an
     * active-but-failing run (a 503/5xx or attempt-timeout storm that commits nothing) does not look
     * alive. This is exactly {@link #progressSignal()} MINUS {@link #throttleEventsTally}.
     *
     * <p>It advances in EVERY phase (entries during listing, {@link #markProgress()} during seed
     * probes and the sort/finalize tail), so a slow-but-progressing run — including the THR-1 503
     * grind, which really commits pages, just slowly — keeps it advancing and does not false-trip the
     * zero-real-progress backstop. Same discipline as {@link #progressSignal()}: reads plain
     * {@code AtomicLong} tallies only, so it stays monotonic under a DELTA/step OTLP registry.
     */
    public long realProgressSignal() {
        return pages.get()
                + livenessProgress.get()
                + progressUnitsTally.get()
                + sortSegmentsTally.get();
    }

    public void recordListingPageShape(long keysOnPage, boolean truncated, int maxKeys) {
        rawPages.increment();
        rawPageKeys.increment(Math.max(0L, keysOnPage));
        if (truncated && keysOnPage < maxKeys) {
            shortTruncatedPages.increment();
        }
        configuredMaxKeys.set(maxKeys);   // constant per run; captured here to feed pageFillRatio
    }

    public void recordStealReason(String outcome, String reason) {
        stealReasonCounter(outcome, reason).increment();
        if ("CHILD_CREATED".equals(outcome)) {
            markFirstSteal();
        } else if ("UNSPLITTABLE".equals(outcome)) {
            unsplittableVictims.increment();
        } else if ("RETRY".equals(outcome) && "split_aborted".equals(reason)) {
            splitGuardAborts.increment();
        }
    }

    /**
     * Lazily-registered {@code swath.steal_reason{outcome,reason}} Micrometer counter, following the
     * same {@code computeIfAbsent} idiom {@code apiCalls}/{@code errors}/{@code steals} use. The
     * {@code category.reason} namespace is a bounded enum (~30-50 values, see §5), so — unlike
     * {@code bucket}/run-id tags — it is safe as a Micrometer tag dimension.
     */
    private Counter stealReasonCounter(String outcome, String reason) {
        String outcomeTag = normalizeTag(outcome);
        String reasonTag = normalizeTag(reason);
        return stealReasonCounters.computeIfAbsent(outcomeTag + "." + reasonTag,
                ignored -> Counter.builder("swath.steal_reason")
                        .tag("outcome", outcomeTag)
                        .tag("reason", reasonTag)
                        .register(registry));
    }

    // ---- --sort first-class meters -------------------------

    /** Entries admitted into the sort lane's fill buffer ({@code swath.sort.entries}). */
    public void recordSortEntries(long entries) {
        sortEntries.increment(entries);
    }

    /**
     * One sorted staging segment was flushed + finalized ({@code swath.sort.segments.written},
     * {@code swath.sort.segment.bytes}); {@code pageRuns} feeds the {@code page_runs_per_buffer}
     * classification signal.
     */
    public void recordSortSegment(long bytes, int pageRuns) {
        sortSegmentsWritten.increment();
        sortSegmentsTally.incrementAndGet();   // monotonic mirror for progressSignal()
        sortSegmentBytes.increment(bytes);
        sortPageRunsPerBuffer.record(pageRuns);
        stuckClassifier.snapshotAtProgress();
    }

    /**
     * Merge-only {@code --sort --resume} backfill
     * — the listing/staging phase that would normally call {@link #recordSortSegment} once per
     * segment never runs in a merge-only resume process (the segments were finalized durably by an
     * EARLIER process; this one re-runs ONLY the k-way merge), so without this {@code
     * swath.sort.segments.written} — and the summary's {@code sort.segments} field, which reads it
     * straight off the registry — would under-report 0 even though the merge published the full,
     * correct segment set. Bumps ONLY the segment-COUNT counter (+ its {@code progressSignal} tally,
     * mirroring {@link #recordSortSegment}'s own bookkeeping) by the durable count read back from the
     * checkpoint. Deliberately does NOT touch {@code segment_bytes}/{@code page_runs_per_buffer} (the
     * per-segment page-run count is not preserved on the durable {@code PartRef} — recording a
     * fabricated {@code 0} would skew the distribution rather than honestly leave it at its
     * already-zero starting point) and does NOT touch {@code entries}/{@code progress.units} (the
     * merge already fed those correctly via {@link #recordProgress} during the k-way merge itself).
     */
    public void recordRecoveredSortSegments(long count) {
        if (count > 0) {
            sortSegmentsWritten.increment(count);
            sortSegmentsTally.addAndGet(count);
        }
    }

    /**
     * Merge-only {@code --sort --resume} row attribution — the rows that resume's merge re-published
     * from durable segments an EARLIER process listed. Attributes them as RECOVERED work and nothing
     * else, so the per-second/per-API-call figures ({@code keys_per_sec}, {@code
     * api_calls_per_1k_objects}) come out at zero for a process that issued no LIST call at all,
     * instead of crediting a whole bucket to the merge's wall clock (see {@link
     * #sessionObjects(long)}).
     *
     * <p>Deliberately NOT {@link #recordRecoveredObjects}, which is the OTHER resume shape's seam:
     * that one also backfills {@code swath.entries.emitted}, because a reattach resume's own {@code
     * objects} field is read off that counter and would otherwise under-report the pre-crash rows. A
     * merge-only resume takes {@code objects} from {@code summary(..., objectsOverride)} instead, so
     * the counter needs no backfill here — and this path must leave {@code entries.emitted}/{@code
     * progress.units} untouched (the merge already fed {@code progress.units} row-by-row via {@link
     * #recordProgress}, and this process listed nothing to attribute entries to). Nothing is folded
     * into the {@link #tailOccupancy} baseline either, for the same reason: that baseline offsets
     * {@code entries.emitted}, which stays at zero on this path, and no sample is ever taken.
     */
    public void recordRecoveredSortRows(long rows) {
        if (rows > 0) {
            recoveredObjects.addAndGet(rows);
        }
    }

    /**
     * Reattach/partial-relist {@code --sort --resume} backfill
     * — sibling of {@link #recordRecoveredSortSegments} for the OTHER
     * resume shape. On a reattach resume, {@code ListRunner} re-lists only the non-durable TAIL:
     * the pre-crash segments' rows were emitted by an earlier, now-dead process, so THIS fresh
     * {@code RunMetrics} instance's {@code swath.entries.emitted} counter — and the summary's
     * {@code objects} field, which reads it straight off the registry — under-reports by the
     * pre-crash amount even though the published output (durable pre-crash segments + freshly
     * relisted tail) is complete. Bumps ONLY the {@code entries.emitted} counter by the durable
     * row count read back from the checkpoint's pre-crash segments. Deliberately does NOT touch
     * {@code swath.progress.units} — unlike {@link #recordEntriesEmitted}, which bumps both: the
     * listing of the freshly relisted tail already fed {@code progress.units} correctly via its
     * own {@code recordEntriesEmitted} calls in this same process, so replaying it here for the
     * pre-crash rows would double-count. Never route this backfill through {@code
     * recordEntriesEmitted} itself for that reason.
     *
     * <p>The same rows are tallied separately as RECOVERED work so live progress can label them as
     * such: they land in one lump when the merge is already done, and a display that folded them
     * into this session's emitted count would sit at zero and then jump by the whole pre-crash
     * total (see {@link ProgressEvent.Listing}).
     *
     * <p>Also folds {@code rows} into {@link #tailOccupancy}'s baseline ({@link
     * TailOccupancySampler#recordBaseline}) so the backfilled jump never pollutes the tail-occupancy
     * windows: without it, this bump would land BEFORE any real sample from this process's relisted
     * tail, making {@code totalEmitted} already near the checkpoint's whole-bucket total the instant
     * the first sample lands — collapsing both {@code pct} windows onto this process's entire
     * relisted span (see {@code TailOccupancySampler}'s resume-semantics note).
     */
    public void recordRecoveredObjects(long rows) {
        if (rows > 0) {
            entriesEmitted.increment(rows);
            recoveredObjects.addAndGet(rows);
            tailOccupancy.recordBaseline(rows);
        }
    }

    /** Time the listing (drain) thread blocked handing a sealed buffer to the encoder (backpressure). */
    public void recordSortBackpressureWait(long nanos) {
        sortBackpressureWait.record(nanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Fold a live in-flight staging-bytes reading (the {@link io.varve.swath.sort.SortLane}
     * fill buffer plus every sealed-but-unfinalized buffer, {@code SortLaneMeters#stagingBytesLive})
     * into the run's high-water mark ({@code swath.sort.staging.bytes.peak}). Same CAS-max idiom as
     * {@link #incrementInFlight()}'s {@code peakInFlight}.
     */
    public void recordSortStagingBytesLive(long liveBytes) {
        sortStagingBytesPeak.getAndAccumulate(liveBytes, Math::max);
    }

    /**
     * Fold a live {@link io.varve.swath.sort.SortLane} handoff-queue depth reading into the
     * run's high-water mark ({@code swath.sort.handoff.queue.depth.peak}). An unbounded queue is the
     * prime leak suspect — this should never exceed the configured {@code buffers()-1}
     * bound (the off-thread semaphore already gates entry to the queue).
     */
    public void recordSortHandoffQueueDepth(int depth) {
        sortHandoffQueueDepthPeak.getAndAccumulate(depth, Math::max);
    }

    /**
     * Fold a live concurrently-live off-thread (queued + encoding) sealed-buffer count into
     * the run's high-water mark ({@code swath.sort.off_thread.buffers.peak}) — lets a future repro
     * see whether the {@code buffers()-1} semaphore bound is actually holding.
     */
    public void recordSortOffThreadBuffersLive(int live) {
        sortOffThreadBuffersPeak.getAndAccumulate(live, Math::max);
    }

    /** Merge passes executed by the cascaded k-way merge ({@code swath.sort.merge.passes}). */
    public void recordSortMergePasses(long passes) {
        sortMergePasses.increment(passes);
    }

    /** Start the per-run merge-wall timer ({@code swath.sort.merge.latency}). */
    public Timer.Sample startSortMergeTimer() {
        return Timer.start(registry);
    }

    /** Stop the merge-wall timer started by {@link #startSortMergeTimer()}. */
    public void recordSortMerge(Timer.Sample sample) {
        sample.stop(sortMergeLatency);
    }

    /**
     * Record one concurrent range's merge wall time ({@code swath.sort.merge.range.latency}) for
     * row-group-skip. Called once per range from a range thread (Micrometer {@link Timer} is
     * thread-safe); {@code nanos} is measured in {@code ParallelRangeMerge} so no {@link Timer.Sample}
     * crosses the sort-package seam.
     */
    public void recordSortMergeRange(long nanos) {
        sortMergeRangeLatency.record(Duration.ofNanos(nanos));
    }

    /**
     * Record the parallel merge's boundary-sampling prologue ({@code
     * swath.sort.merge.boundaries.latency}) — the one phase of that path that does NOT parallelise,
     * timed once per run before any range starts. Surfaced as {@code sort.merge_boundaries_ms} so an
     * A/B can subtract it from {@code merge_ms} and see the ranges' own scaling.
     */
    public void recordSortMergeBoundaries(long nanos) {
        sortMergeBoundariesLatency.record(Duration.ofNanos(nanos));
    }

    /** Boundary metadata/page-scan volume for the persisted-sample A/B. */
    public void recordSortMergeBoundaryIo(long embeddedEntries, long embeddedBytes, long scanBytes) {
        sortMergeBoundaryEmbeddedEntries.increment(embeddedEntries);
        sortMergeBoundaryEmbeddedBytes.increment(embeddedBytes);
        sortMergeBoundaryScanBytes.increment(scanBytes);
    }

    /** One final part's footer-write + fsync durability span. */
    public void recordSortFinalizeClose(long nanos) {
        long nonNegative = Math.max(0L, nanos);
        sortFinalizeCloseLatency.record(nonNegative, TimeUnit.NANOSECONDS);
    }

    /** Incremental exact-byte digest work performed by a final writer (or safe readback fallback). */
    public void recordSortManifestMd5(long bytes, long nanos) {
        sortManifestMd5Bytes.increment(Math.max(0L, bytes));
        sortManifestMd5Latency.record(Math.max(0L, nanos), TimeUnit.NANOSECONDS);
    }

    /** Exact first/last/row observation work; inline writers record zero post-close scan latency. */
    public void recordSortManifestBounds(long rows, long bytes, long nanos) {
        sortManifestBoundsRows.increment(Math.max(0L, rows));
        sortManifestBoundsBytes.increment(Math.max(0L, bytes));
        sortManifestBoundsLatency.record(Math.max(0L, nanos), TimeUnit.NANOSECONDS);
    }

    /** Manifest/state/symlink/_SUCCESS publication after every final part is durably closed. */
    public void recordSortPublication(long nanos) {
        long nonNegative = Math.max(0L, nanos);
        sortPublicationLatency.record(nonNegative, TimeUnit.NANOSECONDS);
    }

    /** Metadata assembly/validation plus local publication after all final writers closed. */
    public void recordSortFinalizeTail(long nanos) {
        sortFinalizeLatency.record(Math.max(0L, nanos), TimeUnit.NANOSECONDS);
    }

    /** Effective number of independently-writing final ranges (1 on the serial path). */
    public void recordSortFinalizeParallelism(int parallelism) {
        sortFinalizeParallelism.set(Math.max(1, parallelism));
    }

    public void recordProbeFetch() {
        probeFetches.increment();
    }

    /**
     * A {@code delimiter=/} structure-probe LIST fetch. Distinct from
     * {@link #recordProbeFetch} (the 1-key {@code start_after} probe) so post-hoc analysis can
     * attribute structure-probe I/O — folded into {@code wasted_probe_ratio}'s denominator,
     * distinct from {@code delimiter_fanout} — as its own
     * waste class (§5). One increment per probe fetch.
     */
    public void recordStructureProbeFetch() {
        structureProbeFetches.increment();
    }

    public void recordEmptyUpperBisection() {
        emptyUpperBisections.increment();
    }

    /**
     * One call-class/phase latency observation ({@code swath.fetch.latency.phase{call_class,
     * phase}}) -- {@code callClass} is one of {@link #CALL_CLASS_WORKER_PAGE}/{@link
     * #CALL_CLASS_PIVOT_PROBE}/{@link #CALL_CLASS_STRUCTURE_PROBE}, {@code phase} one of {@link
     * #LATENCY_PHASE_CONNECT_ACQUIRE}/{@link #LATENCY_PHASE_TTFB}/{@link
     * #LATENCY_PHASE_SDK_UNMARSHAL}/{@link #LATENCY_PHASE_TOTAL}/{@link
     * #LATENCY_PHASE_RESPONSE_PARSE}. {@code nanos < 0} is the SDK-didn't-report-this-phase sentinel
     * (a best-effort SDK metric publisher observation, not guaranteed present on every attempt) and
     * is silently skipped -- never fabricates a 0 sample. Bounded cardinality (15 series max), lazily
     * registered the same {@code computeIfAbsent} idiom as {@link #stealReasonCounter}.
     */
    public void recordCallClassLatency(String callClass, String phase, long nanos) {
        if (nanos < 0) {
            return;
        }
        String key = normalizeTag(callClass) + "." + normalizeTag(phase);
        callClassLatencyTimers.computeIfAbsent(key, ignored -> runScopedTimer("swath.fetch.latency.phase")
                        .tag("call_class", callClass)
                        .tag("phase", phase)
                        .publishPercentiles(PUBLISHED_PERCENTILES)
                        .register(registry))
                .record(nanos, TimeUnit.NANOSECONDS);
    }

    /**
     * At the instant an {@code OWNER_SPLIT.demand_gated} suppression fires, record the current
     * effective {@code T} and the run's {@code Tmax} -- so a shed-shrunken {@code T} closing the demand
     * gate is visible from the {@code swath.owner_split.demand_gated_t}/{@code _t_min} gauges (or the
     * JSON summary's {@code demand_gate} block) without cross-referencing {@code swath.workers.active}'s
     * history against this event's log timestamp. Observation only -- never influences the gate itself.
     */
    public void recordDemandGatedConcurrency(int currentT, int tMax) {
        demandGatedEvents.incrementAndGet();
        demandGatedLastT.set(currentT);
        demandGatedTMax.set(tMax);
        demandGatedMinT.accumulateAndGet(currentT, (prev, next) -> prev < 0 ? next : Math.min(prev, next));
    }

    /**
     * At {@code ConcurrencyGauge}'s sustained-timeout SHED fire, record the worker-vs-probe
     * call-class mix that fed the shed window -- {@code SHED.timeout_storm_worker_fed}/{@code
     * _probe_fed}, magnitude-incremented by the observed counts (the {@link #recordSeedBands}
     * non-1-magnitude idiom), so post-hoc analysis can tell whether a shed was driven by worker page
     * fetches or probe fetches timing out — a client-vs-server falsifier signal.
     */
    public void recordShedCallClassMix(int workerTimeouts, int probeTimeouts) {
        if (workerTimeouts > 0) {
            stealReasonCounter("SHED", "timeout_storm_worker_fed").increment(workerTimeouts);
        }
        if (probeTimeouts > 0) {
            stealReasonCounter("SHED", "timeout_storm_probe_fed").increment(probeTimeouts);
        }
    }

    /**
     * Dead-zone diagnostic classification signal (§5): when a split commits, classify the pivot's
     * DIVERGENCE byte — the first byte where {@code pivot} differs from the range's cursor/lo
     * {@code reference} (the byte the split actually turns on) — into the lowercase-hex byte regions.
     * Surfaced as {@code PIVOT_BYTE.{hex_digit,dead_zone,hex_alpha,other}} so post-hoc analysis can
     * tell whether owner/thief pivots on a hex/UUID keyspace land in the {@code 0x3A-0x60} DEAD ZONE
     * between the hex digits ({@code 0x30-0x39}) and hex letters ({@code 0x61-0x66}) — a dead-zone
     * pivot leaves one child empty and hands the other the whole mass (a zero-transfer split). Cheap:
     * one classification per commit, never per key.
     */
    public void recordPivotByteRegion(byte[] pivot, byte[] reference) {
        if (pivot == null || pivot.length == 0) {
            return;
        }
        byte[] ref = reference == null ? EMPTY_BYTES : reference;
        int i = 0;
        int n = Math.min(pivot.length, ref.length);
        while (i < n && pivot[i] == ref[i]) {
            i++;
        }
        if (i >= pivot.length) {
            return;   // pivot is a prefix of the reference — no divergence byte to classify
        }
        // The LCP depth `i` — the byte position where this split's pivot diverges from
        // its range's cursor — into the run-level divergence-depth histogram (cheap: `i` is already
        // computed for the byte-region classification below; one bump per split commit, never per key).
        shapeAccumulator.recordDivergenceDepth(i);
        int b = pivot[i] & 0xFF;
        String region;
        if (b >= 0x30 && b <= 0x39) {
            region = "hex_digit";
        } else if (b >= 0x3A && b <= 0x60) {
            region = "dead_zone";
        } else if (b >= 0x61 && b <= 0x66) {
            region = "hex_alpha";
        } else {
            region = "other";
        }
        recordStealReason("PIVOT_BYTE", region);
    }

    /**
     * Per-child emitted-mass classification signal (§5): when a claimed node (a split child, or a
     * seed) completes its drain, bucket how many keys it emitted. Surfaced as
     * {@code CHILD_MASS.{empty,tiny,small,large}} so post-hoc analysis can see whether the emitted
     * mass is BIMODAL (mostly {@code empty} + a few {@code large}) — the fingerprint of zero-transfer
     * splits, where each split hands the whole tail to one side. One classification per completed
     * node, never per key.
     */
    public void recordChildMass(long keysEmitted) {
        String bucket;
        if (keysEmitted <= 0L) {
            bucket = "empty";
        } else if (keysEmitted <= 100L) {
            bucket = "tiny";
        } else if (keysEmitted <= 10_000L) {
            bucket = "small";
        } else {
            bucket = "large";
        }
        recordStealReason("CHILD_MASS", bucket);
    }

    /**
     * Fold one completed node's per-position printable-ASCII presence masks (2 words per
     * relative position, from {@code AlphabetDigest.maskWords()}) into the run-level cardinality
     * AGGREGATE by OR-ing the observed-scalar sets. Called once per completed node (never per key); the
     * per-position union popcount is read out in {@link #summary} as {@code alphabet_cardinality}.
     */
    public void recordAlphabetObservation(long[] maskWords) {
        shapeAccumulator.recordAlphabet(maskWords);
    }

    /**
     * Record the delimiter=/ fan-out (distinct child directories) observed at one
     * structure/seed probe. Tracks the widest fan-out seen, the running total, and the probe count so
     * post-hoc analysis reads a bucket's branching factor from the metrics alone. One update per probe.
     */
    public void recordDelimiterFanout(int childCount) {
        shapeAccumulator.recordDelimiterFanout(childCount);
    }

    /**
     * Steal-attempt engagement counter (§5): one increment per real thief steal attempt (after the
     * idle-backoff slot is acquired), regardless of outcome. Surfaced as {@code STEAL.attempted} so
     * post-hoc analysis reads the attempt DENOMINATOR directly (attempts vs {@code CHILD_CREATED}
     * successes and {@code NO_VICTIM.no_splittable_victim} misses) without summing the per-outcome
     * {@code swath.steals} breakdown — distinguishing "thieves stopped attempting" from "thieves
     * attempt but find nothing eligible" (steal suppression).
     */
    public void recordStealAttempt() {
        recordStealReason("STEAL", "attempted");
    }

    private static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * Seed-time radix-band classification signal (§5): the number of leading-byte bands the dense
     * flat regions were pre-cut into. Surfaced in {@code list_run_diagnostics} steal_reasons as
     * {@code SEED.radix_bands} so post-hoc analysis can read the band count, not just that banding fired.
     */
    public void recordSeedBands(long bands) {
        if (bands > 0) {
            stealReasonCounter("SEED", "radix_bands").increment(bands);
        }
    }

    /**
     * Promotes {@code SeedStep}'s already-computed {@code mode/probes/cut_points/synthesized_cuts/
     * ranges} (§5) into the JSON
     * run-summary's {@code seed} block. Called once, from {@code ListCommand.seedFreshRun}, before
     * {@code markRunStarted()} resets the run-scoped counters — the seed fields are themselves
     * unaffected by that reset (seeding happens exactly once, before the engine starts), so they
     * survive to {@link #summary}. Never called on a resumed run (seeding never re-runs), so {@link
     * #summary} renders {@code seed: null} there.
     */
    public void recordSeedSummary(String mode, long probes, long cutPoints, long synthesizedCuts, long ranges) {
        recordSeedSummary(mode, probes, cutPoints, synthesizedCuts, ranges, List.of());
    }

    /**
     * As the 5-arg {@link #recordSeedSummary}, plus the per-probed-level seed decision trace
     * ({@code decisions}, bounded by {@code SeedStep}'s own probe cap ≤ ~256) promoted into the
     * {@code seed.decisions[]} block. {@code decisions} carries raw byte-key prefixes (never a
     * display string) so the ONE shared {@link #display(byte[])} escape/truncate pass happens here,
     * at JSON-model-build time — the same idiom every other byte-key rendering in this class uses.
     */
    public void recordSeedSummary(String mode, long probes, long cutPoints, long synthesizedCuts, long ranges,
            List<SeedProbeDecision> decisions) {
        List<RunSummary.SeedSummary.SeedDecision> rendered = decisions.stream()
                .map(d -> new RunSummary.SeedSummary.SeedDecision(display(d.prefix()), d.fanout(), d.truncated(),
                        d.classification(), d.cutsKept(), d.cutsDiscarded()))
                .toList();
        seedSummary.set(new RunSummary.SeedSummary(mode, probes, cutPoints, synthesizedCuts, ranges, rendered));
    }

    /**
     * Transfer record: one {@code SeedStep} probed level, in raw-bytes form, handed to {@link
     * #recordSeedSummary(String, long, long, long, long, List)} for the single shared display-escape
     * pass. See {@code RunSummary.SeedSummary.SeedDecision} for what each field means.
     */
    public record SeedProbeDecision(byte[] prefix, int fanout, boolean truncated, String classification,
                                     int cutsKept, int cutsDiscarded) {
    }

    /**
     * Records one classified throttle/transient event on the unified {@code swath.throttle.events{type}}
     * series. This is the SINGLE recording site's increment, called once per event
     * from {@code S3PageFetcher} at its classification point ({@code slowdown}/{@code server5xx}/{@code
     * attempt_timeout}/{@code network}). It is pure OBSERVABILITY of the event; whether it also drove
     * AIMD down is the separate {@link #recordAimdVote()} question (only the voting types vote).
     */
    public void recordThrottleEvent(ThrottleType type) {
        throttleEvents.get(type).increment();
        throttleEventsTally.incrementAndGet();   // monotonic mirror for progressSignal()
        // Mirror the two classifier inputs (attempt-timeout, AIMD-voting) into the stuck classifier.
        stuckClassifier.recordThrottleEvent(type == ThrottleType.ATTEMPT_TIMEOUT,
                ThrottleException.Kind.valueOf(type.name()).votesAimdDown());
    }

    /**
     * Sum of all 4 typed {@code swath.throttle.events{type}} counters — the same total the
     * single untyped {@code swath.throttle.events} counter reported before §3.1 typed it, so the
     * JSON summary's throttle total is numerically unchanged by the retag. Cheap (bounded to the ~4
     * {@link ThrottleType} values, read off the pre-resolved EnumMap, no registry scan) so {@link
     * #progressSignal()} stays cheap enough for the watchdog's frequent poll.
     */
    public double throttleEventsTotal() {
        double total = 0.0;
        for (Counter c : throttleEvents.values()) {
            total += c.count();
        }
        return total;
    }

    /**
     * Classify the PROXIMATE cause of a run-wide {@code StopReason.STUCK} freeze into a distinct
     * {@code error_class} token ({@code stuck_api_timeouts}/{@code stuck_throttle}/{@code
     * stuck_unknown}). Only valid for the {@code CancelSource.LIVENESS_WATCHDOG} terminal — see
     * {@link StuckErrorClassifier#classifyStuck()} for the windowed dominance rule.
     */
    public String classifyStuckErrorClass() {
        return stuckClassifier.classifyStuck();
    }

    /**
     * Called by a transient-retry loop AT THE INSTANT its own local cap is exhausted under {@code
     * RetryPolicy.BOUNDED} (the {@code CancelSource.TRANSIENT_RETRY_CAP} terminal). See {@link
     * StuckErrorClassifier#recordTransientRetryCapExhaustion} for the first-writer-wins discipline.
     */
    public void recordTransientRetryCapExhaustion(long attemptTimeoutFaults, long votingFaults) {
        stuckClassifier.recordTransientRetryCapExhaustion(attemptTimeoutFaults, votingFaults);
    }

    /**
     * The pre-derived {@code error_class} for a {@code CancelSource.TRANSIENT_RETRY_CAP} terminal, as
     * recorded by the WINNING cap-tripping fetch via {@link #recordTransientRetryCapExhaustion}.
     */
    public String transientRetryCapErrorClass() {
        return stuckClassifier.transientRetryCapErrorClass();
    }

    /**
     * The ONE source-routed {@code error_class} derivation for a {@code StopReason.STUCK} terminal,
     * shared by {@code ListCommand}'s {@code list_stuck_stop} marker AND the JSON run-summary sidecar
     * so the two observability surfaces can never disagree. Takes the raw {@code stop_source} TAG
     * STRING (never {@code CancelSource} — {@code observability} must not depend on {@code runtime}).
     */
    public String stuckErrorClass(String stopSourceTag) {
        return stuckClassifier.stuckErrorClass(stopSourceTag);
    }

    /**
     * Record the {@code error_class} of a classified FATAL failure that is unwinding the run, so the
     * {@code StopReason.CRASH} terminal summary can name it instead of {@code error_class:null}.
     * First-writer-wins (see {@link StuckErrorClassifier#recordFatalErrorClass}); a {@code null} class
     * is ignored, so a caller can pass a cause-chain-walk result unconditionally.
     */
    public void recordFatalErrorClass(String errorClass) {
        stuckClassifier.recordFatalErrorClass(errorClass);
    }

    /**
     * The {@code error_class} of the classified fatal failure that is unwinding this run, or {@code
     * null} if the crash was never classified. Read by {@code ListRunner}'s {@code StopReason.CRASH}
     * terminal status.
     */
    public String fatalErrorClass() {
        return stuckClassifier.fatalErrorClass();
    }

    /**
     * One AIMD multiplicative-decrease vote reached the gauge ({@code swath.aimd.votes}). Only
     * genuine store backpressure (503 SlowDown / 5xx) votes — client attempt-timeouts
     * and network faults are retried without a vote — so this counter is the honest denominator for
     * "how often did the store actually push back", distinct from {@code swath.throttle.events{kind}}.
     */
    public void recordAimdVote() {
        aimdVotes.increment();
    }

    public void recordAimdTargetReduction() {
        aimdTargetReductions.increment();
    }

    /**
     * One sustained-attempt-timeout-storm SHED engaged on the {@link
     * io.varve.swath.engine.ConcurrencyGauge} ({@code swath.aimd.timeout_shed}). Deliberately
     * DISTINCT from {@link #recordAimdVote()}: an attempt-timeout storm is not store backpressure, so
     * it must never inflate the honest real-503 down-vote denominator (guarded by
     * {@code AimdAttemptTimeoutSignalContractTest}). Counts engagements — a shed that fires while already
     * at the floor of 1 (no reduction possible) still increments here, so post-hoc analysis can see
     * the path engaged even when it couldn't cut further.
     */
    public void recordTimeoutShed() {
        aimdTimeoutShed.increment();
    }

    /**
     * The successful-attempt latency-inflation rung froze a would-be +1 growth step on the
     * {@link io.varve.swath.engine.ConcurrencyGauge} ({@code swath.aimd.latency_freeze}). This is a
     * GROWTH GATE ONLY — it never decreases {@code T} (the shed owns all decreases). Counts every
     * suppressed +1 opportunity, so post-hoc analysis can see the rung engage relative to the churn /
     * handshake meters even on a run whose target was already pinned.
     */
    public void recordLatencyFreeze() {
        aimdLatencyFreeze.increment();
    }

    /**
     * A successful attempt on the {@link io.varve.swath.engine.ConcurrencyGauge} reached the
     * growth-freeze gates ({@code swath.aimd.freeze_gate_checks}) — the DENOMINATOR for {@link
     * #recordLatencyFreeze()} and {@link #recordGrowthFreeze()}. A success that returns earlier (at
     * {@code Tmax}, or inside a throttle cool-down) could never have frozen, so raw freeze counts
     * alone are incomparable across runs at different saturation: a healthy run pinned at {@code
     * Tmax} reads zero freezes by construction, not by health. {@code latency_freeze /
     * freeze_gate_checks} is the comparable rate.
     */
    public void recordFreezeGateCheck() {
        aimdFreezeGateChecks.increment();
    }

    /**
     * The transient-timeout rung froze a would-be +1 growth step on the {@link
     * io.varve.swath.engine.ConcurrencyGauge} ({@code swath.aimd.growth_freeze}). Distinct from {@link
     * #recordLatencyFreeze()} (a separate freeze rung) so post-hoc analysis can tell which rung
     * suppressed a given growth step. This rung is fed ONLY by WORKER-timeout storms (probe
     * timeouts are fully excluded -- see {@link io.varve.swath.engine.ConcurrencyGauge#onTransientTimeout
     * (boolean)}). GROWTH GATE ONLY -- it never decreases {@code T} (the shed owns all decreases).
     */
    public void recordGrowthFreeze() {
        aimdGrowthFreeze.increment();
    }

    /**
     * Publish the gauge's current Vegas rolling-minimum healthy-latency baseline, in
     * milliseconds, as {@code swath.aimd.latency_baseline_ms}. Lets post-hoc analysis see WHERE the
     * latency-freeze rung engaged relative to the observed baseline.
     */
    public void setLatencyBaselineMillis(long value) {
        latencyBaselineMillis.set(value);
    }

    // ---- Idle-backoff ---------------------

    /** Current shared backoff level ({@code consecutiveNonProductive}); live, not a per-run max. */
    public void setIdleBackoffLevel(long level) {
        idleBackoffLevel.set(level);
    }

    /** A worker denied an idle-steal attempt slot by the backoff guard (not the AIMD gauge). */
    public void recordIdleBackoffSlotDenied() {
        idleBackoffSlotDenied.increment();
    }

    /** The backoff recovered from a non-productive streak (level was {@code >0} before reset). */
    public void recordIdleBackoffReset() {
        idleBackoffResets.increment();
    }

    public Timer.Sample startIdleBackoffParkTimer() {
        return Timer.start(registry);
    }

    /** Actual time a worker spent parked on the idle-wait ({@code work.await}), not the requested budget. */
    public void recordIdleBackoffPark(Timer.Sample sample) {
        sample.stop(idleBackoffParkTime);
    }

    // ---- Checkpoint/resume (SqliteCheckpointStore) -----------------

    public Timer.Sample startCheckpointCommitTimer() {
        return Timer.start(registry);
    }

    /** The writer-thread batch's op-execution + {@code conn.commit()} (the I1 WAL-fsync critical path). */
    public void recordCheckpointCommit(Timer.Sample sample, int batchSize) {
        sample.stop(checkpointCommitLatency);
        checkpointCommitBatchSize.record(batchSize);
    }

    /** Time a checkpoint task waited on the writer queue before its batch was drained. */
    public void recordCheckpointQueueWait(long waitNanos) {
        checkpointQueueWait.record(Math.max(0L, waitNanos), TimeUnit.NANOSECONDS);
    }

    /**
     * The FETCH WORKER's own blocking wait for its page commit to become durable (the I1
     * commit-before-emit await), one observation per committed page. Distinct from the two
     * writer-thread meters above, which decompose the SAME work as the single-writer thread sees it
     * (per-task queue wait + per-BATCH commit): this is what one page actually paid, so a per-page
     * client-service-cost model reads it directly instead of re-deriving it from a batch mean.
     * Recorded only when the await returns normally — a failed/interrupted commit is a terminal
     * state, not a representative sample (same discipline as {@link #recordCheckpointCommit}).
     */
    public void recordCheckpointCommitWait(long waitNanos) {
        checkpointCommitWait.record(Math.max(0L, waitNanos), TimeUnit.NANOSECONDS);
    }

    /** Live depth of the checkpoint writer's task queue. */
    public void registerCheckpointQueueDepthGauge(IntSupplier depthSupplier) {
        // Micrometer gauges hold their state object via WeakReference by default; the caller's
        // supplier (often a bare method-ref lambda, e.g. queue::size) has no other strong
        // referent, so the first GC would collect it and the gauge would read NaN forever.
        Gauge.builder("swath.checkpoint.queue.depth", depthSupplier, IntSupplier::getAsInt)
                .strongReference(true).register(registry);
    }

    /**
     * Resume-engagement counters (§5 {@code recordStealReason} idiom, category {@code RESUME}):
     * fire only on a genuine resume (self-gating — a fresh run's freshly-seeded nodes never match
     * the "was previously touched" predicates these are computed from, see {@code
     * SqliteCheckpointStore.doLoadResumable}).
     */
    public void recordResumeNodesReopened(long count) {
        if (count > 0) {
            stealReasonCounter("RESUME", "nodes_reopened").increment(count);
        }
    }

    /**
     * The RPO bound, as a cheaply-available node-count proxy: how many of the reopened nodes
     * had a non-durable tail (durable_cursor lagging cursor) that a file-sink resume re-lists. NOT
     * an exact key/page count — that would need per-node durable-vs-total page bookkeeping (hot-path
     * plumbing), out of scope for additive instrumentation.
     */
    public void recordResumeDurableCursorLag(long count) {
        if (count > 0) {
            stealReasonCounter("RESUME", "durable_cursor_lag").increment(count);
        }
    }

    /** A {@code --resume} refused because {@code args_hash} changed since the checkpointed run. */
    public void recordResumeArgsHashRefused() {
        stealReasonCounter("RESUME", "args_hash_refused").increment();
    }

    // ---- Parquet writer pool ----------------------------------------

    /** A lane rotation actually fired ({@code trigger} one of {@code size|rows|time}). */
    public void recordParquetRotation(String trigger) {
        parquetRotations.computeIfAbsent(normalizeTag(trigger),
                t -> Counter.builder("swath.parquet.rotation").tag("trigger", t).register(registry)).increment();
    }

    /** A lane's open part reached a terminal outcome ({@code outcome} one of {@code finalized|discarded|finalize_failed}). */
    public void recordParquetPart(String outcome) {
        parquetParts.computeIfAbsent(normalizeTag(outcome),
                o -> Counter.builder("swath.parquet.parts").tag("outcome", o).register(registry)).increment();
    }

    public Timer.Sample startParquetFinalizeTimer() {
        return Timer.start(registry);
    }

    /** Footer-write + fsync latency ({@code PartWriter.close()} — the I6 durability point). */
    public void recordParquetFinalizeLatency(Timer.Sample sample) {
        sample.stop(parquetFinalizeLatency);
    }

    /**
     * One stretch of writer-LANE work: the encode+write of a batch's rows into the open part, plus
     * whatever part finalize (footer fsync, part MD5, manifest rewrite) or drain-time discard that
     * stretch performed — measured on the lane's own thread, between two waits on its queue, so
     * summing this span over a run accounts for the pool's CPU (an aborted run's lanes drain their
     * queued batches without writing them, and those record nothing). A client-service-cost span
     * (see {@link #buildClientCostSummary}),
     * but the ONE that is not on the page's critical path: the lanes run concurrently with fetch and
     * {@link #recordEmit emit} (for Parquet, {@code emit} is the pool DISPATCH only), so this span
     * overlaps them in wall-clock and must never be added to a page's serial cost. It also strictly
     * CONTAINS {@code swath.parquet.finalize.latency} whenever a rotation fired inside the stretch.
     */
    public void recordParquetWrite(long nanos) {
        parquetWriteLatency.record(Math.max(0L, nanos), TimeUnit.NANOSECONDS);
    }

    // ---- Output-completeness + run-level aggregate meters ----------------

    /**
     * A sink completed a write of {@code files} output file(s)/part(s) totalling {@code bytes}
     * ({@code format} one of {@code jsonl|tsv|table|parquet}; {@code outcome} e.g. {@code
     * written}). Covers every sink (text JSONL/TSV/TABLE, Parquet, and {@code --sort}'s final
     * merged output, which is itself Parquet) with the SAME counts already passed to {@link
     * #summary}, so {@code swath.output.files}/{@code swath.output.bytes} and the JSON summary's
     * {@code output_files}/{@code compressed_size_bytes} never disagree. {@code files == 0} is a
     * no-op (a zero-row run still calls this once with {@code files=1} for a text sink that opened
     * a stream, so the zero-guard here only matters for a genuinely absent write).
     */
    public void recordOutput(String format, String outcome, long files, long bytes) {
        String formatTag = normalizeTag(format);
        if (files > 0) {
            outputFiles.computeIfAbsent(formatTag + "." + normalizeTag(outcome),
                    ignored -> Counter.builder("swath.output.files")
                            .tag("format", formatTag).tag("outcome", normalizeTag(outcome)).register(registry))
                    .increment(files);
        }
        if (bytes > 0) {
            outputBytes.computeIfAbsent(formatTag,
                    f -> Counter.builder("swath.output.bytes").tag("format", f).register(registry))
                    .increment(bytes);
        }
    }

    /** A text sink (stdout or a file) was truncated by a downstream reader closing the pipe (INT-12). */
    public void recordOutputBrokenPipe() {
        outputBrokenPipe.increment();
    }

    /**
     * End-of-run aggregate wall-clock duration + throughput, recorded exactly ONCE per completed
     * run — every {@code ListRunner} terminal path calls this alongside {@code
     * setPhase(Phase.COMPLETE)}, not from inside {@link #summary}, because {@code summary()} is
     * also invoked periodically for the live JSON-sidecar snapshot (and would otherwise record one
     * Timer sample / clobber the throughput gauge per flush interval). The LIFETIME
     * {@code keys_per_sec} average is misleading as a live/continuously
     * scraped signal — this meter is an end-of-run aggregate for post-hoc analysis only, distinct
     * from the windowed live throughput already surfaced via {@code ProgressSnapshot}/{@code
     * swath.in_flight.avg}-style gauges (unchanged by this method).
     */
    public void recordRunCompletion(Duration duration, double keysPerSecond) {
        runDuration.record(duration);
        runThroughputKeysPerSec.set(keysPerSecond);
    }

    /**
     * The consumer stage's per-page sink-write span: how long ONE {@code PageBatch} took to go
     * through the installed output (format + write for the text sinks, pool dispatch for Parquet,
     * lane admission for {@code --sort}), including that stage's own row tally. Exactly one consumer
     * stage runs per run, so this stays a single untagged series — which output produced it is
     * already in the summary's {@code config.format}. Recorded only when the write returned normally
     * (a broken pipe truncates the page mid-write and is not a representative sample).
     */
    public void recordEmit(long nanos) {
        emitLatency.record(Math.max(0L, nanos), TimeUnit.NANOSECONDS);
    }

    /**
     * The per-page entries-emitted bump: {@code swath.entries.emitted} plus universal progress
     * (§3.2). Called from exactly ONE of {@link io.varve.swath.output.OutputStage} ("the single
     * output stage" — its own class javadoc), {@link
     * io.varve.swath.output.dataset.DatasetOutputStage} or {@code SortOutputStage} per run — the
     * three are mutually-exclusive {@code Pipeline.Consumer<PageBatch>} implementations, and a run
     * wires up exactly one depending on the sink, never more than one concurrently. That makes this
     * call site THE already-serialized point for this run: no extra synchronization is needed for a
     * caller (like the tail-occupancy sampler below) to treat "cumulative keys emitted" and "current
     * in-flight" as observed together, consistently, once per page.
     */
    public void recordEntriesEmitted(long keyCount) {
        if (keyCount > 0) {
            entriesEmitted.increment(keyCount);
            // §3.2: entries emitted during listing/writing are also universal progress — covers
            // those two phases by construction. Merge progress (rows merged) is recorded
            // separately via recordProgress(), from the merge path, so the two never double-count
            // (listing emits entries; merge emits merged rows — distinct phases).
            progressUnits.increment(keyCount);
            progressUnitsTally.addAndGet(keyCount);   // monotonic mirror for progressSignal()
            stuckClassifier.snapshotAtProgress();
            // Tail-occupancy sample: this call is THE already-serialized point (see this method's
            // own javadoc), so no extra synchronization is needed to know "cumulative keys emitted"
            // and "current in-flight" were observed together, consistently, once per page.
            tailOccupancy.record(Math.round(entriesEmitted.count()),
                    nanoClock.getAsLong() - runStartNanos.get(), (int) currentInFlight());
        }
    }

    /**
     * A page-commit's keys were durably committed and kept post-filter while their node's {@code hi}
     * was {@code null} (issue #76) — called from {@code WorkStealingScan}'s page-commit callback
     * AFTER {@code awaitCommit} returns normally (never on a commit that fails) and on {@code
     * kept.size()}, the same post-{@code FilterChain} count the page's downstream {@code PageBatch}
     * carries — never the pre-filter page size. Both {@code hi} and the kept count are values the
     * caller already has for its own bookkeeping at that point, so this is still O(1): no extra read,
     * no per-key work. See {@link #openFrontierKeysEmitted}'s javadoc for why this contract matters
     * (it is what keeps this counter a genuine subset of {@link #entriesEmitted}).
     */
    public void recordOpenFrontierKeysEmitted(long keyCount) {
        if (keyCount > 0) {
            openFrontierKeysEmitted.increment(keyCount);
        }
    }

    /**
     * §3.2: THE stuck signal — one monotonic {@code swath.progress.units} counter that advances in
     * EVERY phase by construction (entries during listing/writing via {@link #recordEntriesEmitted},
     * rows merged during the sort merge via this method), so {@code rate(progress.units)==0 ⇒ stuck}
     * is unconditionally true regardless of phase, with no phase-gating logic and no boundary race.
     */
    public void recordProgress(long units) {
        if (units > 0) {
            progressUnits.increment(units);
            progressUnitsTally.addAndGet(units);   // monotonic mirror for progressSignal()
            stuckClassifier.snapshotAtProgress();
        }
    }

    /**
     * §3.2: live {@code swath.phase} gauge — dashboard readability only (see {@link Phase}'s
     * javadoc for why this is never the stuck-detection gate). Also starts the phase clock live
     * progress reports alongside (never instead of) session elapsed, and — entering {@link
     * Phase#MERGING} — pins the {@code progress.units} baseline the merge's own row count is
     * measured from, so a merge reports the rows IT moved rather than the run's emitted objects
     * (which stay flat throughout a merge, and are zero outright on a merge-only resume). The final
     * merge pass's own baseline — the one a completion fraction is measured from — is pinned by
     * {@link #startFinalMergePass(boolean)}, not here, because only the merge knows whether the work
     * from that point on is one pass over the staged rows.
     *
     * <p>The phase clock reads {@link System#nanoTime()} directly, NOT the injectable {@code
     * nanoClock}: that seam exists so the in-flight gauge and the {@code time_to_*} summary fields
     * are deterministic under a fake clock, and every extra read off it shifts those pinned values.
     * A display clock has no such contract — it measures wall time for a human, nothing more.
     */
    public void setPhase(Phase phase) {
        phaseCode.set(phase.code());
        phaseStartNanos.set(System.nanoTime());
        if (phase == Phase.MERGING) {
            mergeProgressBaseline.compareAndSet(-1L, progressUnitsTally.get());
        }
    }

    /**
     * The merge has stopped folding intermediates and started writing the output it will publish
     * ({@link io.varve.swath.sort.FinalPassListener}): advance {@code swath.phase} to
     * {@link Phase#WRITING} and, when that remaining work is exactly ONE pass over the staged rows,
     * pin the baseline its completion fraction is measured from. A merge that still cascades from
     * here — the parallel range merge, whose ranges each fold their own intermediates — pins
     * nothing, so it reports rows merged and no percentage ({@link #completionOf}) rather than a
     * figure that runs past 100% and shows a finished merge before any output exists.
     *
     * <p>The baseline is pinned BEFORE the phase flips, so no tick can observe {@code WRITING}
     * with a denominator that is about to be withdrawn.
     */
    public void startFinalMergePass(boolean stagedRowsAreTheDenominator) {
        if (stagedRowsAreTheDenominator) {
            finalPassProgressBaseline.compareAndSet(-1L, progressUnitsTally.get());
        }
        setPhase(Phase.WRITING);
    }

    /**
     * One completed seed structure probe. The count and the age of the last one are the only live
     * evidence a seeding run is alive: it emits no entries, fetches no pages and holds no workers,
     * so every listing-shaped field reads zero for its whole duration.
     */
    public void recordSeedProbe() {
        seedProbes.incrementAndGet();
        lastSeedProbeNanos.set(System.nanoTime());
    }

    /** The seed step's probe budget — the exact denominator seed-phase completion is measured against. */
    public void recordSeedProbeBudget(long budget) {
        seedProbeBudget.set(Math.max(0L, budget));
    }

    /**
     * The staged sort segments handed to the k-way merge, and their exact row total — the merge's
     * documented denominator (see {@link ProgressEvent.Merging}). Recorded at merge kickoff, on
     * both the listing-completion and the merge-only-resume path.
     */
    public void recordSortStaged(long segments, long rows) {
        sortStagedSegments.set(Math.max(0L, segments));
        sortStagedRows.set(Math.max(0L, rows));
    }

    public void recordEstimatedBytes(long bytes) {
        if (bytes > 0) {
            bytesEstimated.increment(bytes);
        }
    }

    public long incrementInFlight() {
        long value = recordInFlightTransition(1L);
        long previousPeak = peakInFlight.getAndAccumulate(value, Math::max);
        if (value > previousPeak) {
            // Benign TOCTOU: between the accumulate above and this set, a concurrent increment may
            // reach an even higher peak and also set its timestamp; last writer wins, so
            // time_to_peak_in_flight_ms can be off by one concurrency window. Acceptable for a
            // diagnostic (it is not used for any control decision).
            peakInFlightNanos.set(nanoClock.getAsLong());
        }
        return value;
    }

    public long decrementInFlight() {
        return recordInFlightTransition(-1L);
    }

    /**
     * The current in-flight count — {@link InFlightGauge}'s CAS'd value is the single source of truth
     * (no separate {@code AtomicLong} shadow copy, which would be a stale-overwrite hazard for a reader
     * like {@link #snapshot}).
     */
    public long currentInFlight() {
        return inFlightGauge.current();
    }

    /**
     * Delegate the CAS'd in-flight transition to {@link InFlightGauge}, then fold the winning
     * transition's {@code (value HELD for window)} into the bounded {@link TrajectoryRollup} — reusing
     * the exact {@code (now, window, valueDuringWindow)} tuple the gauge's area integral already
     * computed, never a second measurement. The peak high-water mark ({@link #incrementInFlight}) and
     * the run clock ({@code runStartNanos}) stay facade-owned.
     */
    private long recordInFlightTransition(long delta) {
        InFlightGauge.Transition t = inFlightGauge.recordTransition(delta);
        trajectory.record(t.nowNanos() - runStartNanos.get(), t.windowNanos(), t.valueDuringWindow(),
                Math.round(entriesEmitted.count()));
        return t.value();
    }

    /**
     * Registers the live-worklist snapshot source (the engine calls this once at construction —
     * see {@code WorkStealingScan}) so {@link #summary} can dump the top-{@code N} slowest/remaining
     * live ranges without this observability-layer class depending on the engine's worklist type.
     */
    public void registerRangeSnapshotSource(Supplier<List<RangeSnapshot>> source) {
        this.rangeSnapshotSource = source;
    }

    /**
     * Transfer record: one live range's point-in-time state, in raw-bytes form, from the
     * engine's live worklist — {@link #buildSlowRanges} does the single shared display-escape pass
     * and the top-{@code N} selection. See {@code RunSummary.SlowRange} for what each field means.
     */
    public record RangeSnapshot(byte[] lo, byte[] hi, byte[] cursor, double estRemaining, double drainRate,
                                 long cursorPassedPivot, long noPivot, long structureSuppressed,
                                 long demandGated) {
    }

    /** Top-{@code N} slow_ranges dump size. */
    private static final int SLOW_RANGES_LIMIT = 10;

    /**
     * The top-{@link #SLOW_RANGES_LIMIT} live ranges by estimated remaining span, at THIS
     * instant — a terminal (or mid-run periodic) snapshot of exactly which ranges are dragging.
     * Empty when no source is registered (most unit tests) or the source itself throws (best-effort:
     * this is a diagnostic dump, never worth failing the whole summary over) or the worklist is
     * already empty (a fully-quiesced COMPLETED run has nothing left to report — expected, not a bug).
     */
    private List<RunSummary.SlowRange> buildSlowRanges() {
        Supplier<List<RangeSnapshot>> source = rangeSnapshotSource;
        if (source == null) {
            return List.of();
        }
        List<RangeSnapshot> live;
        try {
            live = source.get();
        } catch (RuntimeException e) {
            return List.of();
        }
        if (live == null || live.isEmpty()) {
            return List.of();
        }
        return live.stream()
                .sorted(Comparator.comparingDouble(RangeSnapshot::estRemaining).reversed())
                .limit(SLOW_RANGES_LIMIT)
                .map(r -> new RunSummary.SlowRange(display(r.lo()), display(r.hi()), display(r.cursor()),
                        r.estRemaining(), r.drainRate(), r.cursorPassedPivot(), r.noPivot(),
                        r.structureSuppressed(), r.demandGated()))
                .toList();
    }

    /**
     * The time-weighted average in-flight listing count since {@link #markRunStarted()} — the
     * running area integral (folded on every transition above) plus the tail since the gauge's last
     * transition, divided by elapsed wall time since run start. {@code 0.0} before the run starts
     * (zero-elapsed guard, same idiom as the other ratio getters).
     */
    public double avgInFlight() {
        return inFlightGauge.average(runStartNanos.get());
    }

    /**
     * The {@code pct}% tail-occupancy avg-in-flight gauge supplier — reads the CURRENT cumulative
     * emitted-keys total (live, mid-run values are a valid — if still-growing — approximation, same
     * as every other pull-based gauge here) and asks {@link #tailOccupancy} to derive the
     * last-{@code pct}% window's mean in-flight over its bounded sample buffer.
     *
     * <p>The elapsed span handed to the sampler is LISTING-scoped ({@link #listingElapsedNanos}):
     * still growing during listing, frozen at the listing&rarr;merge boundary afterwards. The
     * sampler only ever samples listing-time page emits, so a whole-run elapsed would open a window
     * (and, for the wall-share sibling, a denominator) that runs past the last sample and swallows a
     * sorted run's whole merge/publish tail — reporting the merge instead of the serial listing tail
     * these gauges exist to screen for.
     */
    private double tailOccupancyAvgInFlight(int pct) {
        return tailOccupancy.avgInFlightForWindow(pct, Math.round(entriesEmitted.count()),
                listingElapsedNanos());
    }

    /** Sibling of {@link #tailOccupancyAvgInFlight} for the window's wall-time SHARE. */
    private double tailOccupancyWallShare(int pct) {
        return tailOccupancy.wallShareForWindow(pct, Math.round(entriesEmitted.count()),
                listingElapsedNanos());
    }

    /** Nanos of LISTING elapsed: live until {@link #markListingFinished()}, frozen at it after. */
    private long listingElapsedNanos() {
        Long end = listingEndNanos.get();
        return (end != null ? end : nanoClock.getAsLong()) - runStartNanos.get();
    }

    public void setConcurrencyTarget(long value) {
        concurrencyTarget.set(value);
        // Ignore the uninitialized 0; otherwise track the running minimum.
        if (value > 0) {
            concurrencyTargetLowWater.accumulateAndGet(value,
                    (cur, v) -> cur == 0 ? v : Math.min(cur, v));
        }
    }

    public void setRunId(long value) {
        runId.set(value);
    }

    /**
     * Install the run's terminal-summary sink (see {@link RunSummarySink}) — a per-run setter
     * alongside {@link #setRunId}/{@link #setStrategy}, so a presentation layer can be wired in
     * without threading a parameter through every {@code ListRunner.run*} entry point. The sink
     * itself stays encapsulated: written here, read only by {@link #emitSummary}.
     */
    public void setSummarySink(RunSummarySink sink) {
        summarySink.set(sink == null ? RunSummarySink.NONE : sink);
    }

    /**
     * Hand the terminal summary to the installed {@link RunSummarySink}. The terminal summary is
     * the run's last word, so live progress ends first and permanently ({@link #finishProgress()})
     * — for whichever sink is installed, not merely for one that shares the summary's fd.
     */
    public void emitSummary(RunSummary summary, RunDiagnostics diagnostics,
            JsonRunSummaryWriter.TerminalStatus status) {
        finishProgress();
        summarySink.get().accept(summary, diagnostics, status);
    }

    public void setStrategy(String value) {
        strategy.set(normalizeTag(value));
    }

    public void setStrategyWhy(String value) {
        strategyWhy.set(normalizeTag(value));
    }

    public void setPrefix(byte[] prefix) {
        currentPrefix.set(display(prefix));
    }

    public void setCursor(byte[] cursor) {
        currentCursor.set(cursor);
    }

    public long peakInFlight() {
        return peakInFlight.get();
    }

    /**
     * A live read of objects emitted so far (the same counter {@link #summary}'s
     * {@code objects} field reports), cheap enough for a diagnostic line without building a full
     * {@link RunSummary} snapshot.
     */
    public long objectsEmitted() {
        return Math.round(counterTotal("swath.entries.emitted"));
    }

    /**
     * As {@link #objectsEmitted()}, minus whatever a resume backfilled from a previous attempt — the
     * read a "did this process make ANY headway" judgement needs (zero-progress {@code
     * --max-duration} vs. a legit timeboxed partial): a resume that recovered a prior attempt's rows
     * and then listed nothing at all made no headway, whatever the run total says.
     */
    public long sessionObjectsEmitted() {
        return sessionObjects(objectsEmitted());
    }

    /**
     * A live read of total bytes flushed to {@code --sort} staging segments so far
     * ({@code swath.sort.segment.bytes}), cheap enough for {@link io.varve.swath.sort.SortDiskGuard}
     * to poll periodically during the listing phase without building a full snapshot.
     */
    public long sortSegmentBytesWritten() {
        return (long) sortSegmentBytes.count();
    }

    /**
     * A live read of {@code swath.progress.units} (the SAME monotonic tally
     * {@link #progressSignal()}/{@link #realProgressSignal()} fold in) so an external merge-phase
     * heartbeat can show genuine forward progress mid-merge without building a full {@link
     * RunSummary} snapshot — mirrors {@link #sortSegmentBytesWritten()}'s cheap-live-read idiom.
     */
    public long progressUnits() {
        return progressUnitsTally.get();
    }

    /**
     * A live read of completed k-way merge passes ({@code swath.sort.merge.passes})
     * for the same merge-phase heartbeat. Populated exactly once, after the whole merge finishes
     * ({@link #recordSortMergePasses}), so this reads 0 for the whole merge and only becomes
     * non-zero at the very end — expected, and echoed in the heartbeat line for context alongside
     * the genuinely-advancing {@link #progressUnits()}.
     */
    public long sortMergePassesCount() {
        return (long) sortMergePasses.count();
    }

    /**
     * Install the run's live-progress sink (see {@link ProgressSink}) — the per-run setter
     * alongside {@link #setSummarySink}, so a presentation layer can own the progress channel
     * without threading a parameter through every {@code ListRunner.run*} entry point. The
     * installed sink REPLACES {@link ProgressSink#LOG}: one tick renders once. Ignored after
     * {@link #finishProgress()}: progress ends once, permanently.
     */
    public void setProgressSink(ProgressSink sink) {
        synchronized (progressLock) {
            if (!progressFinished) {
                progressSink.set(sink == null ? ProgressSink.NONE : sink);
            }
        }
    }

    /**
     * End live progress for good, whichever sink is installed. This is where the "no progress after
     * the run's last word" rule lives, because this is the layer that knows WHICH sink is installed
     * — a presentation layer can only silence its own channel, and the structured {@link
     * ProgressSink#LOG} record is not on it. Called by {@link #emitSummary} before the terminal
     * block reaches the summary sink, and by the CLI when a run unwinds without one.
     *
     * <p>The lock is what makes the ordering real rather than probabilistic: a tick that is already
     * rendering completes BEFORE this returns, and one that has not started sees {@link
     * ProgressSink#NONE} and never builds an event. Idempotent.
     */
    public void finishProgress() {
        synchronized (progressLock) {
            progressFinished = true;
            progressSink.set(ProgressSink.NONE);
        }
    }

    /**
     * Render one tick to the installed sink — the whole of {@link RunProgressReporter}'s tick body,
     * here rather than there so the enabled check, the event build and the render happen under the
     * one lock {@link #finishProgress()} takes.
     */
    void emitProgress(Duration sessionElapsed) {
        ProgressSink sink = progressSink.get();
        if (!sink.isEnabled()) {
            return;   // nothing renders this tick: build no event at all
        }
        synchronized (progressLock) {
            if (progressSink.get() != sink) {
                return;   // progress ended while this tick was waiting: its frame is dropped
            }
            sink.accept(progressEvent(sessionElapsed));
        }
    }

    /**
     * Whether the provider's LIST pricing is knowable — {@code false} under {@code --endpoint-url},
     * where the AWS reference rate ({@link #LIST_COST_PER_1K_USD}) describes a different provider's
     * bill. Recorded on the run rather than in one renderer's preferences so EVERY progress surface
     * withholds the figure by construction (see {@link ProgressEvent#estimatedCostUsd()}).
     */
    public void setListCostKnown(boolean known) {
        listCostKnown = known;
    }

    /**
     * The session-wide progress reporter, or {@code null} when none is running. Owned by {@link
     * RunProgressReporter#start}: CAS'd in by the first (outermost) start and cleared by that same
     * reporter's {@code close()}; a nested start joins the winner instead of scheduling a second
     * ticker with its own clock and its own windowed-rate baseline.
     */
    boolean claimProgressReporter(RunProgressReporter reporter) {
        boolean claimed = progressReporter.compareAndSet(null, reporter);
        if (claimed) {
            // This IS session start: the first (outermost) reporter opens before a fresh run's seed
            // step, so its claim instant is the same session zero point its own elapsed-since-start
            // ticks already measure -- see sessionDuration(Duration).
            sessionStartNanos.set(nanoClock.getAsLong());
            sessionClaimed.set(true);
        }
        return claimed;
    }

    /** Releases the reporter installed by {@link #claimProgressReporter}; a no-op for any other. */
    void releaseProgressReporter(RunProgressReporter reporter) {
        progressReporter.compareAndSet(reporter, null);
    }

    /**
     * ONE immutable sample of the run's live state for {@code sessionElapsed}, built once per tick
     * and fanned out (see {@link ProgressEvent}). Cheap by construction: every field is an atomic
     * read or one Micrometer counter read — no registry walk, no resource probe — and the windowed
     * rate advances its baseline exactly once per tick, because exactly one reporter builds this.
     */
    public ProgressEvent progressEvent(Duration sessionElapsed) {
        long apiCallCount = apiCallsTally.get();
        Phase phase = phaseOf(phaseCode.get());
        long now = System.nanoTime();
        return new ProgressEvent(
                phase,
                runId.get(),
                strategy.get(),
                sessionElapsed,
                Duration.ofNanos(Math.max(0L, now - phaseStartNanos.get())),
                apiCallCount,
                listCostKnown ? estimatedListCost(apiCallCount) : null,
                throttleEventsTally.get(),
                completionOf(phase),
                phase == Phase.SEEDING ? seedingProgress(now) : null,
                phase == Phase.LISTING ? listingProgress(sessionElapsed) : null,
                phase == Phase.MERGING || phase == Phase.WRITING ? mergingProgress() : null);
    }

    /**
     * {@code swath.phase}'s current value as a {@link Phase}; {@link Phase#STARTING} before it is
     * set — the honest answer for a run still opening its checkpoint, rather than a fabricated
     * LISTING whose phase clock has not started.
     */
    private static Phase phaseOf(long code) {
        for (Phase phase : Phase.values()) {
            if (phase.code() == code) {
                return phase;
            }
        }
        return Phase.STARTING;
    }

    /**
     * The phase's completion figure, or {@code null} where no honest denominator exists — which is
     * the LISTING case and the reason there is no bar, no percentage and no ETA there: an unsorted
     * scan does not know its object total until it ends.
     *
     * <p>Seeding has one (probes against a bounded budget). The merge has one only for its FINAL
     * pass ({@link Phase#WRITING}), measured from the baseline pinned when that pass began: a
     * cascading merge rewrites every staged row once per pass and does not know its pass count in
     * advance, so cumulative merge work over staged rows would pass 100% mid-cascade and report a
     * finished merge before any final output was written. The cascade therefore reports work
     * ({@link ProgressEvent.Merging#sessionRowsMerged()}) and no percentage.
     */
    private ProgressEvent.Completion completionOf(Phase phase) {
        if (phase == Phase.SEEDING && seedProbeBudget.get() > 0) {
            return new ProgressEvent.Completion(seedProbes.get(), seedProbeBudget.get(),
                    ProgressEvent.Unit.PROBES);
        }
        if (phase == Phase.WRITING && sortStagedRows.get() > 0 && finalPassProgressBaseline.get() >= 0) {
            return new ProgressEvent.Completion(rowsSince(finalPassProgressBaseline), sortStagedRows.get(),
                    ProgressEvent.Unit.ROWS);
        }
        return null;
    }

    private ProgressEvent.Seeding seedingProgress(long now) {
        long last = lastSeedProbeNanos.get();
        return new ProgressEvent.Seeding(seedProbes.get(), seedProbeBudget.get(),
                Duration.ofNanos(last < 0 ? Math.max(0L, now - phaseStartNanos.get())
                        : Math.max(0L, now - last)));
    }

    private ProgressEvent.Listing listingProgress(Duration sessionElapsed) {
        // Every rate here is SESSION work over SESSION time: the elapsed clock started with this
        // process, so dividing the whole run's objects (a resume's recovered rows included) by it
        // would report a resumed run's pre-crash billions as this second's throughput.
        long sessionKeys = sessionObjects(Math.round(entriesEmitted.count()));
        double seconds = Math.max(0.001, sessionElapsed.toNanos() / 1_000_000_000.0);
        double avgRate = sessionKeys / seconds;
        return new ProgressEvent.Listing(
                sessionKeys,
                recoveredObjects.get(),
                windowedRate(sessionElapsed.toNanos(), sessionKeys, avgRate),
                avgRate,
                pages.get(),
                currentInFlight(),
                concurrencyTarget.get(),
                stealsTally.get(),
                splits.get());
    }

    /**
     * The objects THIS process listed out of {@code totalObjects}: the run total minus whatever a
     * resume backfilled from a previous attempt ({@link #recordRecoveredObjects}). Object COUNTS
     * describe the dataset and so use the total; anything divided by this session's elapsed time, or
     * measured against this session's API calls, uses this instead — the pre-crash rows cost this
     * process neither a second nor a LIST call.
     */
    private long sessionObjects(long totalObjects) {
        return Math.max(0L, totalObjects - recoveredObjects.get());
    }

    private ProgressEvent.Merging mergingProgress() {
        return new ProgressEvent.Merging(rowsSince(mergeProgressBaseline), sortStagedRows.get(),
                sortStagedSegments.get(), (long) sortMergePasses.count());
    }

    /** Rows of merge work since a pinned {@code progress.units} baseline; {@code 0} while unpinned. */
    private long rowsSince(AtomicLong baseline) {
        long pinned = baseline.get();
        return pinned < 0 ? 0L : Math.max(0L, progressUnitsTally.get() - pinned);
    }

    public RunSummary summary(Duration duration, String strategy, long outputFiles, long compressedBytes) {
        return summary(duration, strategy, outputFiles, compressedBytes, null);
    }

    /**
     * As the 4-arg {@link #summary}, but with an explicit {@code objectsOverride} for a caller that
     * knows the true object count from a source OTHER than {@code swath.entries.emitted} — namely
     * {@link io.varve.swath.runtime.ListRunner#runSortMergeOnly}'s merge-only {@code --sort --resume}:
     * the listing/staging phase that normally drives {@code recordEntriesEmitted} never runs
     * in that process (only the k-way merge re-runs, over already-durable segments), so the counter
     * stays 0 and the summary would under-report {@code objects:0} despite publishing the full,
     * correct output. The override is NOT threaded through {@code recordEntriesEmitted} itself
     * because that call also bumps {@code swath.progress.units} (§3.2) — the merge path already
     * feeds that counter correctly, row-by-row, via {@link #recordProgress} during the k-way merge;
     * re-deriving it from {@code recordEntriesEmitted} here would double-count. {@code null} (the
     * 4-arg overload) preserves the original counter-derived behavior for every other caller.
     */
    public RunSummary summary(Duration duration, String strategy, long outputFiles, long compressedBytes,
            Long objectsOverride) {
        setStrategy(strategy);
        long keyCount = objectsOverride != null
                ? objectsOverride
                : Math.round(counterTotal("swath.entries.emitted"));
        // objects/keys describe the DATASET (recovered rows included); every figure below that is
        // per-second or per-API-call describes this PROCESS's work, so it divides the session's own
        // objects instead — see sessionObjects(long).
        long sessionKeyCount = sessionObjects(keyCount);
        long apiCallCount = Math.round(counterTotal("swath.api.calls"));
        double seconds = Math.max(0.001, duration.toNanos() / 1_000_000_000.0);
        double cpuSec = cpuSeconds();   // sample CPU once so cpu_efficiency == cpu_seconds / wall

        // Efficiency ratios: derived once, here, from counters already maintained elsewhere —
        // no new hot-path work (see RunSummary's javadoc for numerator/denominator + the classifier
        // rationale).
        long rawKeyCount = Math.round(rawPageKeys.count());
        long rawPageCount = Math.round(rawPages.count());
        long totalSteals = Math.round(counterTotal("swath.steals"));
        long childCreated = Math.round(stealsCount("CHILD_CREATED"));
        long unsplittable = Math.round(unsplittableVictims.count());
        long probeFetchCount = Math.round(probeFetches.count());
        // Fold structure-probe fetches into the wasted_probe_ratio denominator — they are
        // probes too (a delimiter=/ LIST fetch), and excluding them undercounted total probe waste.
        long totalProbeFetchCount = probeFetchCount + Math.round(structureProbeFetches.count());
        long emptyUpperCount = Math.round(emptyUpperBisections.count());
        double meanKeysPerPage = rawPageCount > 0 ? (double) rawKeyCount / rawPageCount : 0.0;
        // Raw (S3-reported, uncompressed) estimated bytes / actually-written output
        // bytes — the Parquet writer pool's committedBytes() is already the compressedBytes param.
        // For a text sink, compressedBytes is the real CountingWriter-measured byte count,
        // not 0; the ratio only renders 0.0 via the zero-denominator guard on a genuinely
        // empty run (compressedBytes == 0).
        long rawBytesEstimated = Math.round(bytesEstimated.count());
        // The listing-only clock: `duration` runs to summary time, so on a --sort run it already
        // includes the merge/publish tail. Before the boundary is claimed -- an unsorted run, or a
        // mid-listing snapshot -- the two are the same span by construction.
        Long listingEnd = listingEndNanos.get();
        Duration listingDuration = listingEnd != null
                ? Duration.ofNanos(Math.max(0L, listingEnd - runStartNanos.get()))
                : duration;

        return new RunSummary(
                runId.get(),
                keyCount,
                recoveredObjects.get(),
                duration,
                sessionDuration(duration),
                listingDuration,
                strategy,
                apiCallCount,
                estimatedListCost(apiCallCount),
                outputFiles,
                compressedBytes,
                keyCount,
                pages.get(),
                peakInFlight.get(),
                avgInFlight(),
                elapsedMillis(runStartNanos.get(), firstStealNanos.get()),
                elapsedMillis(runStartNanos.get(), peakInFlightNanos.get()),
                totalSteals,
                splits.get(),
                Math.round(counterTotal("swath.errors")),
                sessionKeyCount / seconds,
                apiCallsPer1kObjects(apiCallCount, sessionKeyCount),
                ResourceMetrics.peakRssBytes(),
                ResourceMetrics.peakHeapBytes(),
                cpuSec,
                cpuEfficiency(cpuSec, seconds),
                ratio(rawKeyCount, sessionKeyCount),
                ratio(meanKeysPerPage, configuredMaxKeys.get()),
                ratio(unsplittable, totalSteals),
                ratio(emptyUpperCount, totalProbeFetchCount),
                ratio(childCreated, totalSteals),
                ratio(rawBytesEstimated, compressedBytes),
                seedSummary.get(),
                // Only carry the `shape` feature-vector when the listing engine actually
                // fetched at least one page (rawPageCount > 0). The pre-run early-exit summaries
                // (seed_failure / resume_refused / no-op completed) never run RangeScanner, so they
                // would otherwise emit an all-zero `shape` — contrary to the metrics docs, which
                // omit the block when no shape was computed. The writer drops a null shape.
                rawPageCount > 0
                        ? shapeAccumulator.buildSummary(
                                this::massSkewGini,
                                () -> latencyPercentileMs(0.5),
                                () -> latencyPercentileMs(0.99))
                        : null,
                // Same rawPageCount>0 guard as shape — a pre-run early exit never fetched a
                // page, so it never transitioned in-flight either; the writer drops a null trajectory.
                rawPageCount > 0 ? trajectory.buildSummary(() -> (int) peakInFlight()) : null,
                // Cheap regardless of rawPageCount (bounded by the live-worklist size); never null.
                buildSlowRanges(),
                // Per-call-class latency-phase percentiles -- cheap regardless of rawPageCount
                // (an empty list when no fetch of any class has completed); never null.
                buildCallClassLatencySummary(),
                // Per-page client-service-cost spans -- same shape and same cheapness (an empty
                // list when no page was ever serviced); never null.
                buildClientCostSummary(),
                // Demand-gate T-vs-Tmax visibility -- null when OWNER_SPLIT.demand_gated never
                // fired this run (the writer omits the whole block, same idiom as seed/shape/trajectory).
                demandGatedEvents.get() > 0
                        ? new RunSummary.DemandGateSummary(demandGatedEvents.get(),
                                (int) demandGatedLastT.get(), (int) demandGatedMinT.get(), (int) demandGatedTMax.get())
                        : null);
    }

    /** The CHILD_MASS 4-bucket reason tags, and coarse representative masses (geometric bucket centers). */
    private static final String[] CHILD_MASS_BUCKETS = {"empty", "tiny", "small", "large"};
    private static final double[] CHILD_MASS_REPRESENTATIVE = {0.0, 10.0, 1005.0, 100_000.0};

    /**
     * Mass-skew Gini over the already-instrumented {@code CHILD_MASS.{empty,tiny,small,
     * large}} distribution. A coarse 4-bucket approximation (the raw per-child masses are never
     * retained — that would be unbounded memory), computed from the bucket counts and representative
     * masses. {@code 0.0} when no node has completed (no basis for inequality).
     */
    private double massSkewGini() {
        long[] counts = new long[CHILD_MASS_BUCKETS.length];
        for (int i = 0; i < counts.length; i++) {
            Counter c = stealReasonCounters.get("CHILD_MASS." + CHILD_MASS_BUCKETS[i]);
            counts[i] = c == null ? 0L : Math.round(c.count());
        }
        return giniFromGroups(counts, CHILD_MASS_REPRESENTATIVE);
    }

    /**
     * Gini coefficient for grouped data: {@code (Σi Σj n_i n_j |v_i − v_j|) / (2 · N · Σ)} where
     * {@code N} is the total count and {@code Σ} the total mass. {@code 0.0} on an empty/zero-mass set.
     */
    static double giniFromGroups(long[] counts, double[] values) {
        long n = 0L;
        double sum = 0.0;
        for (int i = 0; i < counts.length; i++) {
            n += counts[i];
            sum += counts[i] * values[i];
        }
        if (n == 0L || sum <= 0.0) {
            return 0.0;
        }
        double meanAbsDiff = 0.0;
        for (int i = 0; i < counts.length; i++) {
            for (int j = 0; j < counts.length; j++) {
                meanAbsDiff += (double) counts[i] * counts[j] * Math.abs(values[i] - values[j]);
            }
        }
        return meanAbsDiff / (2.0 * n * sum);
    }

    /**
     * The client-side {@code p50}/{@code p99} of {@code swath.api.latency} in
     * milliseconds (enabled via {@code publishPercentiles} on that timer), or {@code null} when no
     * call has been timed yet (so the shape block reads {@code null} rather than a fabricated {@code 0}).
     */
    private Double latencyPercentileMs(double percentile) {
        if (listObjectsLatency.count() == 0L) {
            return null;
        }
        for (var vp : listObjectsLatency.takeSnapshot().percentileValues()) {
            if (Math.abs(vp.percentile() - percentile) < 1e-9) {
                return vp.value(TimeUnit.MILLISECONDS);
            }
        }
        return null;
    }

    /** All 3 {@code call_class} tag values, in a fixed, stable iteration order for the summary. */
    private static final List<String> CALL_CLASSES =
            List.of(CALL_CLASS_WORKER_PAGE, CALL_CLASS_PIVOT_PROBE, CALL_CLASS_STRUCTURE_PROBE);
    /**
     * All 5 {@code phase} tag values, in a fixed, stable iteration order for the summary — roughly
     * chronological within one fetch: pool checkout, first byte, the SDK's response handling, the
     * whole call, then swath's own post-return parse.
     */
    private static final List<String> LATENCY_PHASES = List.of(LATENCY_PHASE_CONNECT_ACQUIRE,
            LATENCY_PHASE_TTFB, LATENCY_PHASE_SDK_UNMARSHAL, LATENCY_PHASE_TOTAL,
            LATENCY_PHASE_RESPONSE_PARSE);

    /**
     * Read back every populated {@code call_class}/{@code phase} Timer's p50/p90/p99/max/count
     * into the JSON summary's {@code probe_latency[]} -- the generic {@code meters[]} readback (§1)
     * carries a Timer's count/total_ms/max_ms only, not its percentiles, so this dedicated readback is
     * necessary (the same reason {@code shape.regime.api_latency_p50_ms}/{@code _p99_ms} needed one).
     * Omits any {@code call_class}/{@code phase} pair with zero observations -- never a fabricated
     * all-zero row.
     */
    private List<RunSummary.CallClassLatencySummary> buildCallClassLatencySummary() {
        List<RunSummary.CallClassLatencySummary> out = new ArrayList<>();
        for (String callClass : CALL_CLASSES) {
            for (String phase : LATENCY_PHASES) {
                Timer t = callClassLatencyTimers.get(normalizeTag(callClass) + "." + normalizeTag(phase));
                if (t == null || t.count() == 0L) {
                    continue;
                }
                out.add(new RunSummary.CallClassLatencySummary(callClass, phase, t.count(),
                        timerPercentileMs(t, 0.5), timerPercentileMs(t, 0.90), timerPercentileMs(t, 0.99),
                        t.max(TimeUnit.MILLISECONDS)));
            }
        }
        return out;
    }

    /** {@code span} name: the fetch worker's blocking wait for its page commit to become durable. */
    public static final String CLIENT_COST_SPAN_CHECKPOINT_COMMIT_WAIT = "checkpoint_commit_wait";
    /** {@code span} name: a checkpoint task's wait on the single-writer queue before its batch drained. */
    public static final String CLIENT_COST_SPAN_CHECKPOINT_QUEUE_WAIT = "checkpoint_queue_wait";
    /** {@code span} name: the checkpoint writer thread's own batch op-execution + {@code conn.commit()}. */
    public static final String CLIENT_COST_SPAN_CHECKPOINT_COMMIT = "checkpoint_commit";
    /** {@code span} name: the consumer stage's per-page sink write. */
    public static final String CLIENT_COST_SPAN_EMIT = "emit";
    /** {@code span} name: the fetch worker's blocked-on-a-full-channel wait handing the page downstream. */
    public static final String CLIENT_COST_SPAN_WRITER_BACKPRESSURE = "writer_backpressure";
    /** {@code span} name: a Parquet writer lane's own encode/write stretch, off the page's critical path. */
    public static final String CLIENT_COST_SPAN_PARQUET_WRITE = "parquet_write";

    /**
     * Read back every client-service-cost span's p50/p90/p99/max/count into the JSON summary's
     * {@code client_cost[]} — the per-page cost of SERVICING a page once the store has answered,
     * decomposed into the spans that can contend independently, so a replay/perf analysis can tell an
     * iid per-page cost from a queue behind a shared writer (the latter grows with worker count; the
     * former does not). Same dedicated-readback reason as {@link #buildCallClassLatencySummary}: the
     * generic {@code meters[]} readback (§1) carries a Timer's count/total_ms/max_ms only, never its
     * percentiles.
     *
     * <p>The two response-side spans of the decomposition — the SDK's response handling and swath's
     * own response PARSE — are deliberately not here: both are attributable per call class, so they
     * live in {@code probe_latency[]} as {@code phase=}{@value #LATENCY_PHASE_SDK_UNMARSHAL} and
     * {@code phase=}{@value #LATENCY_PHASE_RESPONSE_PARSE} rather than being flattened into
     * call-class-blind rows.
     *
     * <p>{@value #CLIENT_COST_SPAN_PARQUET_WRITE} is the one member measured OFF the page's critical
     * path — a Parquet run's sink work is done by the writer-pool lanes, so {@code emit} sees only
     * the dispatch and the real encode/write cost is only visible here. It is what makes a Parquet
     * run's client cost measurable rather than a lower bound; see {@link #recordParquetWrite} for
     * the overlap that follows from it running on its own threads.
     *
     * <p>Omits any span with zero observations — never a fabricated all-zero row.
     */
    private List<RunSummary.ClientCostSpan> buildClientCostSummary() {
        List<RunSummary.ClientCostSpan> out = new ArrayList<>();
        addClientCostSpan(out, CLIENT_COST_SPAN_CHECKPOINT_COMMIT_WAIT, checkpointCommitWait);
        addClientCostSpan(out, CLIENT_COST_SPAN_CHECKPOINT_QUEUE_WAIT, checkpointQueueWait);
        addClientCostSpan(out, CLIENT_COST_SPAN_CHECKPOINT_COMMIT, checkpointCommitLatency);
        addClientCostSpan(out, CLIENT_COST_SPAN_EMIT, emitLatency);
        addClientCostSpan(out, CLIENT_COST_SPAN_WRITER_BACKPRESSURE, queueWait);
        addClientCostSpan(out, CLIENT_COST_SPAN_PARQUET_WRITE, parquetWriteLatency);
        return out;
    }

    /** Appends one populated {@link #buildClientCostSummary} row; a never-observed span contributes none. */
    private static void addClientCostSpan(List<RunSummary.ClientCostSpan> out, String span, Timer timer) {
        if (timer.count() == 0L) {
            return;
        }
        out.add(new RunSummary.ClientCostSpan(span, timer.count(),
                timerPercentileMs(timer, 0.5), timerPercentileMs(timer, 0.90), timerPercentileMs(timer, 0.99),
                timer.max(TimeUnit.MILLISECONDS)));
    }

    /** As {@link #latencyPercentileMs}, generalized to any Timer with {@code publishPercentiles} enabled. */
    private static Double timerPercentileMs(Timer t, double percentile) {
        for (var vp : t.takeSnapshot().percentileValues()) {
            if (Math.abs(vp.percentile() - percentile) < 1e-9) {
                return vp.value(TimeUnit.MILLISECONDS);
            }
        }
        return null;
    }

    /** The registered {@code swath.steals{result=<outcome>}} counter's count, or {@code 0} if never fired. */
    private double stealsCount(String outcome) {
        Counter c = steals.get(normalizeTag(outcome));
        return c == null ? 0.0 : c.count();
    }

    /** {@code numerator / denominator}, guarded to {@code 0.0} on a zero/negative denominator. */
    private static double ratio(double numerator, double denominator) {
        return denominator > 0.0 ? numerator / denominator : 0.0;
    }

    public RunDiagnostics diagnostics(Duration duration) {
        long pageCount = pages.get();
        long keyCount = Math.round(counterTotal("swath.entries.emitted"));
        long rawPageCount = Math.round(rawPages.count());
        long rawKeyCount = Math.round(rawPageKeys.count());
        long pageShapePages = rawPageCount > 0 ? rawPageCount : pageCount;
        long pageShapeKeys = rawPageCount > 0 ? rawKeyCount : keyCount;
        double meanKeysPerPage = pageShapePages > 0 ? (double) pageShapeKeys / pageShapePages : 0.0;
        long started = runStartNanos.get();
        return new RunDiagnostics(
                runId.get(),
                duration.toMillis(),
                strategy.get(),
                strategyWhy.get(),
                collectStealReasons(),
                Math.round(probeFetches.count()),
                Math.round(emptyUpperBisections.count()),
                splits.get(),
                Math.round(unsplittableVictims.count()),
                Math.round(splitGuardAborts.count()),
                peakInFlight.get(),
                elapsedMillis(started, firstStealNanos.get()),
                elapsedMillis(started, peakInFlightNanos.get()),
                pageShapePages,
                pageShapeKeys,
                meanKeysPerPage,
                Math.round(shortTruncatedPages.count()),
                Math.round(throttleEventsByVoting(true)),
                Math.round(throttleEventsByVoting(false)),
                Math.round(aimdVotes.count()),
                Math.round(aimdTargetReductions.count()));
    }

    /**
     * Rebuilds the {@code Outcome.reason -> count} map ({@code steal_reasons}) by reading the
     * {@code swath.steal_reason} Micrometer meters back from the registry — same key spelling
     * ({@code normalizeTag(outcome) + "." + normalizeTag(reason)}) the hand-rolled map used to produce,
     * so {@code list_run_diagnostics}/JSON consumers see identical entries.
     */
    private Map<String, Long> collectStealReasons() {
        return Map.copyOf(registry.find("swath.steal_reason").counters().stream()
                .collect(Collectors.toMap(
                        c -> normalizeTag(c.getId().getTag("outcome")) + "." + normalizeTag(c.getId().getTag("reason")),
                        c -> Math.round(c.count()))));
    }

    /**
     * Live keys/sec over the window since the previous snapshot — far more
     * useful than cumulative average on long runs (a stall shows immediately).
     * The first snapshot (no prior sample) and any non-advancing window fall
     * back to the cumulative average so the field is never spuriously zero.
     */
    private double windowedRate(long elapsedNanos, long keys, double cumulativeRate) {
        RateSample prev = lastRateSample.getAndSet(new RateSample(elapsedNanos, keys));
        if (prev == null) {
            return cumulativeRate;
        }
        long deltaNanos = elapsedNanos - prev.elapsedNanos();
        if (deltaNanos <= 0) {
            return cumulativeRate;
        }
        long deltaKeys = Math.max(0L, keys - prev.keys());
        return deltaKeys / (deltaNanos / 1_000_000_000.0);
    }

    private static double apiCallsPer1kObjects(long apiCalls, long objects) {
        return objects > 0 ? apiCalls * 1_000.0 / objects : 0.0;
    }

    /**
     * Reset the CPU baseline to the wall-clock start so cpu_seconds/cpu_efficiency share a zero
     * point. Also resets the avg-in-flight gauge's integration window to start here (whatever is
     * already in flight carries forward as the gauge's starting value, not reset to zero — a resumed
     * run's warm-start reopen work is real, not a gap).
     */
    public void markRunStarted() {
        long now = nanoClock.getAsLong();
        runStartNanos.set(now);
        listingEndNanos.set(null);
        firstStealNanos.set(-1L);
        peakInFlightNanos.set(-1L);
        inFlightGauge.reset(now);
        baselineCpuNanos = ResourceMetrics.processCpuTimeNanos();
    }

    /**
     * Stamp the LISTING&rarr;merge boundary — the instant this run stopped listing and handed off to
     * the post-listing merge/publish tail. One CAS on {@link #listingEndNanos} both claims the
     * crossing and publishes its stamp, so the first crossing wins even when the clock reads 0
     * there, and no reader can observe a claimed boundary without the stamp it is claimed with (see
     * the field's comment) — there is no publication window for a concurrent gauge scrape to fall
     * into, so no test can exercise one. It scopes the tail-occupancy gauges (see {@link
     * #tailOccupancyAvgInFlight}) and {@code listing_duration_ms}; a run that never merges never
     * calls it, and both fall back to the live run clock.
     *
     * <p>Reads the injectable {@code nanoClock}, NOT {@link System#nanoTime()}: this stamp feeds
     * summary and gauge values, so it must share {@link #runStartNanos}'s zero point and stay
     * deterministic under a fake clock. It is deliberately a separate call from {@code
     * setPhase(Phase.MERGING)} for that reason — that method's clock is a display clock, and must
     * not add reads off the injectable seam (see {@link #setPhase}). The CAS argument is evaluated
     * first, so a repeat call reads the clock and discards it; the boundary call sites are mutually
     * exclusive per run, so that is at most one wasted read on no real path.
     */
    public void markListingFinished() {
        listingEndNanos.compareAndSet(null, nanoClock.getAsLong());
    }

    private void markFirstSteal() {
        firstStealNanos.compareAndSet(-1L, nanoClock.getAsLong());
    }

    private static long elapsedMillis(long started, long event) {
        if (started <= 0L || event < 0L) {
            return -1L;
        }
        return Math.max(0L, Duration.ofNanos(event - started).toMillis());
    }

    /**
     * The whole-invocation session clock -- {@code runDuration} (the post-seed {@code duration}
     * param every caller of {@link #summary} passes) with seeding folded back in, when there was a
     * session-wide reporter around to measure it. Falls back to {@code runDuration} itself (the two
     * collapse to one figure, never a garbage delta) when {@link #sessionStartNanos} was never
     * claimed -- a pre-seed early exit, or a caller (most unit tests) that builds a summary without
     * ever starting a {@link RunProgressReporter}.
     */
    private Duration sessionDuration(Duration runDuration) {
        if (!sessionClaimed.get()) {
            return runDuration;
        }
        return Duration.ofNanos(Math.max(0L, nanoClock.getAsLong() - sessionStartNanos.get()));
    }

    /** CPU seconds consumed by this run (delta from run start), or {@code -1} if unavailable. */
    private double cpuSeconds() {
        long now = ResourceMetrics.processCpuTimeNanos();
        if (now < 0 || baselineCpuNanos < 0) {
            return -1.0;
        }
        return Math.max(0L, now - baselineCpuNanos) / 1_000_000_000.0;
    }

    /** cpu_time / wall_time — mean core-utilization (effective parallelism), or {@code -1}. */
    private static double cpuEfficiency(double cpu, double wallSeconds) {
        return cpu < 0 ? -1.0 : cpu / wallSeconds;
    }

    /**
     * {@code throttleEvents}/{@code transientEvents}/{@code aimdVotes}: three
     * DISTINCT counts, deliberately not folded into one another so no field lies about what actually
     * happened.
     * <ul>
     * <li>{@code throttleEvents} — genuine store backpressure only ({@code ThrottleException.Kind}
     * {@code SLOWDOWN}/{@code SERVER_5XX}, real 503/5xx). Do not fold client attempt-timeouts/network
     * faults into this field — a hung-read storm with zero real 503s would then misleadingly read as
     * sustained S3 throttling, conflating a client-side hang with real store backpressure.
     * <li>{@code transientEvents} — client-side transients that are retried but never voted AIMD down
     * ({@code ATTEMPT_TIMEOUT}/{@code NETWORK}): "is the underlying hang still happening" even though
     * it no longer strangles concurrency.
     * <li>{@code aimdVotes} — the honest count of multiplicative-decrease votes the gauge actually
     * received ({@code swath.aimd.votes}); should track {@code throttleEvents} closely (both driven
     * by the same voting kinds) but is recorded independently in {@code ConcurrencyGauge}, so the two
     * numbers double-check each other.
     * </ul>
     */
    public record RunDiagnostics(
            long runId,
            long durationMs,
            String strategy,
            String strategyWhy,
            Map<String, Long> stealReasons,
            long probeFetches,
            long emptyUpperBisections,
            long splitsCommitted,
            long unsplittableVictims,
            long splitGuardAborts,
            long peakInFlight,
            long timeToFirstStealMs,
            long timeToPeakInFlightMs,
            long pages,
            long totalKeys,
            double meanKeysPerPage,
            long shortTruncatedPages,
            long throttleEvents,
            long transientEvents,
            long aimdVotes,
            long aimdTargetReductions) {
    }

    /**
     * Sum the unified {@code swath.throttle.events{type}} series split by whether
     * the {@link ThrottleType} is a genuine AIMD-voting backpressure signal ({@code slowdown}/{@code
     * server5xx}, {@code voting=true}) or a non-voting client-side transient ({@code attempt_timeout}/
     * {@code network}, {@code voting=false}). Voting class is read off the behavioral
     * {@link ThrottleException.Kind#votesAimdDown()} (the single source of truth, 1:1 name-mapped from
     * {@link ThrottleType}), so this diagnostics split can never drift from the retry wrapper's
     * actual voting decision. Keeps {@code RunDiagnostics.throttleEvents} honest — it must never again
     * silently include attempt-timeouts.
     */
    private double throttleEventsByVoting(boolean voting) {
        double total = 0.0;
        for (Map.Entry<ThrottleType, Counter> e : throttleEvents.entrySet()) {
            if (ThrottleException.Kind.valueOf(e.getKey().name()).votesAimdDown() == voting) {
                total += e.getValue().count();
            }
        }
        return total;
    }

    private double counterTotal(String name) {
        switch (name) {
            case "swath.entries.emitted":
                return entriesEmitted.count();
            case "swath.bytes.estimated":
                return bytesEstimated.count();
            default:
                break;
        }
        double total = 0.0;
        for (Meter meter : registry.getMeters()) {
            if (!meter.getId().getName().equals(name)) {
                continue;
            }
            for (var measurement : meter.measure()) {
                if (measurement.getStatistic() == Statistic.COUNT) {
                    total += measurement.getValue();
                }
            }
        }
        return total;
    }

    private static String normalizeTag(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }

    /**
     * The USD-per-1000-LIST-requests rate every cost figure swath reports is derived from — the
     * published AWS reference rate for LIST requests. It is a single-region constant and LIST
     * pricing is not uniform across regions or providers, so every surface that renders a dollar
     * figure must state this rate alongside it (and withhold the figure entirely when the provider
     * is unknown, i.e. under {@code --endpoint-url}) rather than implying a precise bill.
     */
    public static final double LIST_COST_PER_1K_USD = 0.005;

    /** The tag identifying {@link #LIST_COST_PER_1K_USD}'s provenance in the JSON run report. */
    public static final String LIST_COST_SOURCE = "aws-list-reference-rate";

    private static double estimatedListCost(long apiCalls) {
        return apiCalls * LIST_COST_PER_1K_USD / 1_000.0;
    }

    private static String display(byte[] bytes) {
        if (bytes == null) {
            return "<none>";
        }
        String text = ControlCharEscaper.escape(new String(bytes, StandardCharsets.UTF_8));
        if (text.length() <= DISPLAY_LIMIT) {
            return text;
        }
        return text.substring(0, DISPLAY_LIMIT) + "...";
    }
}
