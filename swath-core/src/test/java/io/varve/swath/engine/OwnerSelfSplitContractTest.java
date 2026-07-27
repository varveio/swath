/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SplitSpec;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.CheckpointException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.AbortingCheckpointStore;
import io.varve.swath.testkit.ApiCallBudget;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.RangePartition;
import io.varve.swath.testkit.RecordingSplitStore;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adversarial guard for owner-side proactive self-split at page-commit
 * ({@code OwnerSelfSplit.maybeOwnerSelfSplit}). Owner-split introduces a <b>new source of
 * split + {@code outstanding} accounting</b> (the draining owner carves its own far-ahead tail),
 * so the risk is a completeness / termination regression: an {@code outstanding} over-count hangs
 * the run, an under-count declares quiescence early and drops the tail, and a mis-tiled owner
 * pivot double-carves or leaves a gap. These tests prove none of that happens.
 *
 * <p>This guard is independent of the light {@code OwnerSelfSplitTriggerTest} (which only
 * checks the trigger conditions). Everything here is deterministic: an in-memory
 * {@link MockPageFetcher}, no
 * real S3, no wall-clock dependence (byte-exact set equality is invariant under thread scheduling;
 * termination is guarded with a preemptive timeout that fails — never hangs — on a regression).
 *
 * <p>The engine runs on real virtual-thread workers, so its interleavings are <i>not</i>
 * bit-deterministic. That is fine: the properties asserted (set equality, disjoint-cover tiling,
 * probe-boundedness) hold for <b>every</b> schedule, and the concurrency stress is applied by
 * raising the worker count and slowing the drainer so owner-splits and thief-steals genuinely
 * race on the same dense range (see {@link #ownerSplitsRaceThiefStealsAndStayByteExact}).
 */
// Tagged at METHOD granularity, NOT class-level: the CONC/PROP adversarial methods here (termination +
// byte-exact completeness, negative triggers, no-gap/no-overlap tiling, owner/thief race byte-exact,
// stale-snapshot revalidation, abort byte-exact) are FAST and deterministic (set equality is
// schedule-invariant), so they MUST stay per-commit. Only the schedule-sensitive probe-budget method
// (`ownerSplitAddsNoProbesAndStaysWithinTheApiBudget`) is `@Tag("deep")` — see its own comment.
final class OwnerSelfSplitContractTest {

    private static final String OWNER_SPLIT_KEY = "OWNER_SPLIT.self_published";
    private static final String THIEF_SPLIT_KEY = "CHILD_CREATED.split_committed";

    // Tight window around the dense flat cluster. A ⊥ lower bound would spread the estRemaining
    // window over empty low space and collapse the density estimate (owner-split would never fire),
    // so the dense seed is bounded snugly: every key is strictly inside (LO, HI].
    private static final byte[] LO = "d/00".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HI = "d/05".getBytes(StandardCharsets.UTF_8);

    /** A dense flat directory {@code d/000000..} of {@code n} uniform keys, all inside {@code (LO, HI]}. */
    private static List<byte[]> denseFlat(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(String.format("d/%06d", i).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    private static RunKey key(String label) {
        return new RunKey("s3", null, "bucket", new byte[0], label,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static NodeSpec range(long runId, byte[] lo, byte[] hi) {
        // A fresh bounded seed resumes from its lower bound: cursor == lo (exactly like SeedStep.seedTile
        // and a split child whose cursor begins at the pivot). A null cursor would restart the scan at ⊥.
        return new NodeSpec(runId, null, NodeKind.RANGE, lo, hi, lo, null);
    }

    /** One engine run's observable outcome: emitted keys, recorded splits + their seed frame, metrics. */
    private record Run(List<byte[]> emitted,
                       List<RangePartition.Split> splits,
                       Map<Long, RangePartition.Interval> seedLive,
                       Map<String, Long> stealReasons,
                       long probeFetches,
                       long peakInFlight,
                       long apiCalls,
                       int structureProbes) {

        long ownerSplits() {
            return stealReasons.getOrDefault(OWNER_SPLIT_KEY, 0L);
        }

        long thiefSplits() {
            return stealReasons.getOrDefault(THIEF_SPLIT_KEY, 0L);
        }
    }

    /**
     * Drive the full {@link WorkStealingScan} over {@code keyspace} with the given {@code seeds}
     * (each a half-open {@code (lo, hi]}; {@code null} = ⊥/⊤), recording every committed split so
     * both the emitted-key set and the durable range-set tiling can be checked. A structure-probe
     * counter (delimiter listing with a non-null {@code start_after}) proves owner-split adds zero probes.
     */
    private Run run(Path dir, String label, List<byte[]> keyspace, List<RangePartition.Interval> seeds,
                    int workers, int maxKeys, Duration dataLatency) throws Exception {
        return run(dir, label, keyspace, seeds, workers, maxKeys, dataLatency, EngineToggles.DEFAULT);
    }

    private Run run(Path dir, String label, List<byte[]> keyspace, List<RangePartition.Interval> seeds,
                    int workers, int maxKeys, Duration dataLatency, EngineToggles toggles) throws Exception {
        return run(dir, label, keyspace, seeds, workers, maxKeys, dataLatency, toggles, metrics -> { });
    }

    /**
     * As above, plus {@code onDataPage}: a hook invoked on the fetching worker's own thread for every
     * WORKER listing page (not probes), before the page is returned. Blocking in it parks that worker
     * inside its fetch — which is outside the victim lock, so a thief can still probe and carve the
     * very range whose owner is parked. That is what lets a test SCHEDULE the owner/thief race instead
     * of waiting for it to happen by luck.
     */
    private Run run(Path dir, String label, List<byte[]> keyspace, List<RangePartition.Interval> seeds,
                    int workers, int maxKeys, Duration dataLatency, EngineToggles toggles,
                    Consumer<RunMetrics> onDataPage) throws Exception {
        Path ckptDir = dir.resolve(label);
        Files.createDirectories(ckptDir);

        AtomicInteger structureProbes = new AtomicInteger();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(keyspace)
                .latency(req -> req.maxKeys() > 1 && req.delimiter() == null ? dataLatency : Duration.ZERO)
                .interceptor((PageRequest req, int idx, ListPage page) -> {
                    if (req.delimiter() != null && req.startAfter() != null) {
                        structureProbes.incrementAndGet();
                    } else if (req.maxKeys() > 1 && req.delimiter() == null) {
                        onDataPage.accept(metrics);
                    }
                    return page;
                })
                .build();

        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        Map<Long, RangePartition.Interval> seedLive = new HashMap<>();

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(key(label), false, false);
            for (RangePartition.Interval seed : seeds) {
                long id = store.insertNode(range(run.id(), seed.lo(), seed.hi()));
                seedLive.put(id, seed);
            }
            List<Node> resumable = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withToggles(toggles),
                    mock, store, workers, maxKeys, resumable, FilterChain.EMPTY);

            RunContext ctx = RunContext.create();
            PipelineDrain.collectKeys(5000, ctx, engine, emitted);

            RunMetrics.RunDiagnostics diag = metrics.diagnostics(Duration.ZERO);
            return new Run(emitted, store.splits(), seedLive, diag.stealReasons(),
                    diag.probeFetches(), diag.peakInFlight(), mock.apiCalls(), structureProbes.get());
        }
    }

    // -------------------------------------------------------------------------
    // (1) THE critical guard: termination + byte-exact completeness under many owner-splits.
    // -------------------------------------------------------------------------

    @Test
    void denseMegaDayTerminatesAndIsByteExactUnderManyOwnerSplits(@TempDir Path dir) throws Exception {
        // ~40k uniform keys / 100 per page ≈ 400 pages of remaining work ≫ the 4×maxKeys (=400 keys)
        // trigger, so the draining owner (and every bounded child it carves) self-splits MANY times.
        // If owner-split's outstanding accounting over-counts, quiescence is never reached and the run HANGS —
        // the preemptive timeout below fails the test rather than hanging the suite. If it under-counts,
        // quiescence fires early and the tail is dropped — the set equality below fails.
        List<byte[]> keyspace = denseFlat(40_000);
        int maxKeys = 100;
        int pages = (int) Math.ceil((double) keyspace.size() / maxKeys);

        Run run = assertTimeoutPreemptively(Duration.ofSeconds(60), () ->
                run(dir, "megaday", keyspace,
                        List.of(new RangePartition.Interval(LO, HI)), 8, maxKeys, Duration.ZERO));

        EngineHarness.assertExactlyOnce(run.emitted(), keyspace);

        // (2) The mechanism engaged multiple times...
        assertThat(run.ownerSplits())
                .as("owner-side self-split fired multiple times on the dense mega-day")
                .isGreaterThanOrEqualTo(2L);
        // ...and is rate-limited — never one-per-page confetti (consistent with the 32-page gate).
        assertThat(run.ownerSplits())
                .as("owner-splits are rate-limited well below one per page (%d pages)", pages)
                .isLessThan((long) pages);
    }

    // -------------------------------------------------------------------------
    // (2) Negative triggers: no self-split on a tiny bounded range or the open frontier.
    // -------------------------------------------------------------------------

    @Test
    @Timeout(60)
    void smallBoundedAndOpenFrontierNeverSelfSplit(@TempDir Path dir) throws Exception {
        // 50 keys < the 4×maxKeys (=400) estRemaining floor ⇒ the bounded owner never self-splits.
        Run small = run(dir, "small", denseFlat(50),
                List.of(new RangePartition.Interval(LO, HI)), 4, 100, Duration.ZERO);
        EngineHarness.assertExactlyOnce(small.emitted(), denseFlat(50));
        assertThat(small.ownerSplits()).as("tiny bounded range must not self-split").isZero();

        // Open frontier (hi == null), single worker so no thief ever bounds it: owner-split keeps the
        // density-extrapolation path and must NOT self-split, no matter how dense.
        List<byte[]> dense = denseFlat(40_000);
        Run frontier = run(dir, "frontier", dense,
                List.of(new RangePartition.Interval(null, null)), 1, 100, Duration.ZERO);
        EngineHarness.assertExactlyOnce(frontier.emitted(), dense);
        assertThat(frontier.ownerSplits()).as("open frontier must never self-split").isZero();
    }

    // -------------------------------------------------------------------------
    // (3) Partition invariant under owner-split (I2/I3): the disjoint cover holds throughout.
    // -------------------------------------------------------------------------

    @Test
    @Timeout(60)
    void ownerSplitsTileTheKeyspaceWithNoGapNoOverlap(@TempDir Path dir) throws Exception {
        // Three seeds tile (⊥, ⊤]: an empty (⊥, LO], the dense (LO, HI] that self-splits, and an
        // empty (HI, ⊤]. Replaying every recorded split (owner AND thief) from this seed frame must
        // reconstruct a gap-free, overlap-free tiling of (⊥, ⊤] — the structural I2/I3 proof under
        // an owner-split-heavy run, reusing the PROP-1 RangePartition machinery (boundary belongs LEFT).
        List<byte[]> keyspace = denseFlat(40_000);
        List<RangePartition.Interval> seeds = List.of(
                new RangePartition.Interval(null, LO),
                new RangePartition.Interval(LO, HI),
                new RangePartition.Interval(HI, null));

        Run run = run(dir, "tiling", keyspace, seeds, 8, 100, Duration.ZERO);

        EngineHarness.assertExactlyOnce(run.emitted(), keyspace);
        assertThat(run.ownerSplits()).as("owner-splits fired in the tiling run").isGreaterThanOrEqualTo(2L);
        assertTilesFromSeeds(run.seedLive(), run.splits());
    }

    // -------------------------------------------------------------------------
    // (4) INT-8: owner-split adds ZERO probes and does not inflate the API budget.
    // -------------------------------------------------------------------------

    // `deep` tier (not per-commit): schedule-sensitive — its owner-split probe guard is a tight
    // ablation-relative bound (`< control + slack`, floor 12) with no O(pages) backstop, so it flakes
    // under per-commit parallel CI load. Runs SERIALLY in the deep tier (main-merge + nightly),
    // where the sensitive bound holds. The GROSS regression (owner-split ballooning structure probes)
    // stays guarded per-commit by StealStructureProbeTest's O(pages) bound.
    @Tag("deep")
    @Test
    @Timeout(120)
    void ownerSplitAddsNoProbesAndStaysWithinTheApiBudget(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = denseFlat(40_000);
        int maxKeys = 100;
        int workers = 8;
        long pages = (long) Math.ceil((double) keyspace.size() / maxKeys);

        // PAIRED arms on the same machine (platform-robust): the shipped defaults vs the SAME
        // fixture with owner_split=off. Do not compare structureProbes against ownerSplits as an
        // absolute bound: fewer, larger splits (~23-35 here) leave only ~1.1x margin between them —
        // a scheduling coin-flip on a slow runner. The intent ("owner-split adds zero probes") is
        // instead re-expressed against the ablation control below.
        Run run = run(dir, "int8", keyspace,
                List.of(new RangePartition.Interval(LO, HI)), workers, maxKeys, Duration.ZERO);
        Run noOwnerSplit = run(dir, "int8-off", keyspace,
                List.of(new RangePartition.Interval(LO, HI)), workers, maxKeys, Duration.ZERO,
                EngineToggles.DEFAULT.withOwnerSplit(false));

        EngineHarness.assertExactlyOnce(run.emitted(), keyspace);
        EngineHarness.assertExactlyOnce(noOwnerSplit.emitted(), keyspace);
        assertThat(run.ownerSplits()).as("owner-splits fired").isGreaterThanOrEqualTo(2L);
        assertThat(noOwnerSplit.ownerSplits()).as("the control arm's ablation engaged").isZero();

        // Owner-split uses interpolate() (pure math) — it issues no LIST at all. Total API stays O(pages+workers).
        // (This absolute budget held across every observed schedule, including CI — kept as-is.)
        ApiCallBudget.assertWithinInt8Budget(run.apiCalls(), pages, workers,
                "owner-split adds zero API calls: total LIST ≤ 4·pages + 4·workers + 64");
        // Owner-split issues NO probes: the only structure probes in EITHER arm are the sporadic
        // flat-tail ones the thief issues anyway, so the default arm must not probe meaningfully more
        // than the no-owner-split world. The two arms' baselines are statistically equal (measured
        // idle and under 8-core saturation: default 18-26 vs control 17-27, diff spread -9..+6), so a
        // raw <= would itself be a coin-flip; the additive slack absorbs that scheduling noise while
        // still failing a REAL regression, which adds ~1 probe per owner-split (diff ≈ ownerSplits ≈
        // 23-35). The slack has a CONSTANT floor rather than scaling with ownerSplits alone: a starved
        // scheduler both suppresses owner-splits (demand-gated) AND widens probe noise, so a purely
        // proportional slack shrinks exactly when the noise grows. O(pages) stays asserted absolutely.
        // This ablation-relative comparison is this test's SOLE regression guard (the `<= pages`
        // co-bound is ~8x too loose to backstop the diff ≈ ownerSplits ≈ 23-35 signal). Widening the
        // slack floor to absorb CI contention would blunt exactly that signal (a starved scheduler
        // drives ownerSplits toward its floor, so the diff shrinks below a wide floor and the guard
        // goes vacuous). So we DON'T widen it — the floor stays at 12 (2x the idle noise tail); see
        // the method comment above for why this test instead runs @Tag("deep").
        assertThat((long) run.structureProbes())
                .as("structure probes (%d, %d owner-splits) do not exceed the owner_split=off "
                                + "control's (%d) beyond scheduling noise — owner-split adds zero probes",
                        run.structureProbes(), run.ownerSplits(), noOwnerSplit.structureProbes())
                .isLessThan(noOwnerSplit.structureProbes() + Math.max(12L, run.ownerSplits() / 2))
                .isLessThanOrEqualTo(pages);
    }

    // -------------------------------------------------------------------------
    // (5) Concurrency: owner-splits and thief-steals race on the same dense range, still byte-exact.
    // -------------------------------------------------------------------------

    /**
     * Both split sources must be live on the SAME range at once, and the run must still be
     * byte-exact under that double carve.
     *
     * <p><b>The race is scheduled, not hoped for.</b> The earlier shape raised the worker count and
     * slowed the drainer so that idle workers would probe-and-steal a range while its owner was
     * self-splitting, then asserted {@code thiefSplits >= 1} — an assertion about what the scheduler
     * happened to do. Whether any worker is ever idle at the same moment a victim is stealable is a
     * timing outcome: the owner's own carving keeps refilling the ready queue, so workers claim
     * instead of stealing, and on a contended runner a run can legitimately finish with zero thief
     * steals. It read {@code thiefSplits = 0} on CI while passing on the same commit minutes earlier.
     *
     * <p>The measured bottleneck is the engine's fleet-wide SINGLE in-flight steal slot: idle workers
     * were denied it 2721 times in one instrumented run, so whether a steal attempt actually completes
     * before the run drains is a race against the run's own length, not a property of the shape.
     *
     * <p>{@link #thiefCarveWindow()} closes that: exactly one worker listing page parks inside its
     * fetch until a thief carve commits. A parked fetch does NOT hold the victim lock, so that range
     * stays fat, far-ahead and stealable — with its cursor already advanced — while the thieves, whose
     * 1-key probes and structure probes are deliberately NOT gated, work on it. The other 15 workers
     * keep draining, so the pool still reaches idleness and produces the steal attempt. So the two
     * sources are not merely both present in the final tally: the thief's carve provably lands while
     * that range's owner is parked mid-drain.
     *
     * <p>The wait is bounded ({@link #THIEF_CARVE_TIMEOUT}) and falls through rather than hanging, so
     * a genuine regression that stops thieves from stealing still fails at the assertion below —
     * naming the count — instead of timing out the suite.
     */
    @Test
    void ownerSplitsRaceThiefStealsAndStayByteExact(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = denseFlat(40_000);
        int workers = 16;

        Run run = assertTimeoutPreemptively(Duration.ofSeconds(90), () ->
                run(dir, "race", keyspace,
                        List.of(new RangePartition.Interval(LO, HI)), workers, 100, Duration.ofMillis(1),
                        EngineToggles.DEFAULT, thiefCarveWindow()));

        EngineHarness.assertExactlyOnce(run.emitted(), keyspace);

        // Owner-splits fired...
        assertThat(run.ownerSplits()).as("owner-splits fired under the race").isGreaterThanOrEqualTo(2L);
        // ...and thief-steals carved the same range while its owner was parked mid-drain: both split
        // sources are proven to co-occur here — the double-carve stress.
        assertThat(run.thiefSplits())
                .as("thief-steals also fired — both split sources raced the same range")
                .isGreaterThanOrEqualTo(1L);
        // Parallelism actually happened (not a serial baton).
        assertThat(run.peakInFlight()).as("the dense range parallelized").isGreaterThanOrEqualTo(2L);
    }

    /** How long the single parked page waits for a thief to carve before falling through. */
    private static final Duration THIEF_CARVE_TIMEOUT = Duration.ofSeconds(10);

    /** Poll interval for that wait — short enough not to distort the race, long enough not to spin. */
    private static final Duration THIEF_CARVE_POLL = Duration.ofMillis(2);

    /** Pages the owner commits before the gate arms, so the victim is fat and far-ahead when it parks. */
    private static final int PAGES_BEFORE_GATE = 2;

    /**
     * The ONE-SHOT data-page gate described on {@link #ownerSplitsRaceThiefStealsAndStayByteExact}:
     * exactly one worker listing page — the {@value #PAGES_BEFORE_GATE}nd, by which point its range
     * has committed pages and is far-ahead enough to be worth stealing — parks until a thief carve
     * commits.
     *
     * <p>One-shot is the load-bearing part. An earlier version of this gate parked EVERY worker page
     * until a thief carved, which starves the thieves it is waiting for: once enough workers own
     * carved children, all of them park inside their fetches, none is idle, and no steal attempt is
     * ever made — measured, that self-starvation burned the full timeout on 2 of 4 parks. Parking a
     * single page leaves the other 15 workers running, so the pool still drains toward idleness while
     * one fat victim sits stealable with its lock free.
     */
    private Consumer<RunMetrics> thiefCarveWindow() {
        AtomicInteger dataPages = new AtomicInteger();
        AtomicBoolean parked = new AtomicBoolean();
        return metrics -> {
            if (dataPages.incrementAndGet() < PAGES_BEFORE_GATE
                    || stealReason(metrics, THIEF_SPLIT_KEY) > 0L
                    || !parked.compareAndSet(false, true)) {
                return;
            }
            long deadline = System.nanoTime() + THIEF_CARVE_TIMEOUT.toNanos();
            while (stealReason(metrics, THIEF_SPLIT_KEY) == 0L && System.nanoTime() < deadline) {
                try {
                    Thread.sleep(THIEF_CARVE_POLL);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        };
    }

    /** Live read of one {@code swath.steal_reason{outcome,reason}} counter mid-run. */
    private static long stealReason(RunMetrics metrics, String key) {
        int dot = key.indexOf('.');
        Counter counter = metrics.registry().find("swath.steal_reason")
                .tag("outcome", key.substring(0, dot))
                .tag("reason", key.substring(dot + 1))
                .counter();
        return counter == null ? 0L : (long) counter.count();
    }

    // -------------------------------------------------------------------------
    // (T1) A thief carrying a STALE (cursor, H) snapshot across an OWNER self-split of the SAME node
    //      cannot reopen the child's (m, H] range.
    // -------------------------------------------------------------------------

    @Test
    @Timeout(60)
    void thiefStaleSnapshotCannotReopenRangeAcrossOwnerSelfSplit(@TempDir Path dir) throws Exception {
        // Deterministic force (a probe-time gate, not many-workers-and-luck): the thief snapshots the
        // victim's (cursor=C, hi=HI) and parks inside its 1-key probe; while it is parked THERE, the
        // OWNER commits a page and self-splits THIS SAME victim to pivot m (narrow hi=m in memory +
        // durable child (m, HI] committed). The thief then resumes its lock-guarded revalidation with
        // the now-stale (C, HI). The I4 CAS `range_end IS oldHi` guard — and the thief's early-loser
        // `hi != H` check it backs — must stop the stale snapshot from reopening (m, HI]:
        //   * RETRY (not CHILD_CREATED),
        //   * hi is NOT clobbered back to HI (no re-opened overlap with the child),
        //   * exactly ONE durable split (the owner's) and the range set tiles with no gap/overlap,
        //   * the remaining tail (C, HI] resumes byte-exact.
        // Capable of FAILING if the guard were removed: a stale narrow+split would abort on the durable
        // CAS (range_end is now m, not HI) → restoreHi(HI) → hi clobbered to HI (the assertion below).
        Path ckptDir = dir.resolve("t1-stale");
        Files.createDirectories(ckptDir);
        List<byte[]> keyspace = megaDay();
        byte[] lo = "2022/03/05/10000000".getBytes(StandardCharsets.UTF_8);
        byte[] c = "2022/03/05/50000000".getBytes(StandardCharsets.UTF_8);
        byte[] hi = "2022/03/05/99999999".getBytes(StandardCharsets.UTF_8);
        byte[] m = ByteMidpoint.between(c, hi);   // the owner's self-split pivot, strictly in (C, HI)
        assertThat(m).isNotNull();
        assertThat(Arrays.compareUnsigned(c, m)).as("C < m").isLessThan(0);
        assertThat(Arrays.compareUnsigned(m, hi)).as("m < HI").isLessThan(0);

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(key("t1"), false, false);
            long victimId = store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, lo, hi, c, null));

            WorkerState victim = WorkerStates.of(victimId, lo, c, hi);
            victim.addKeysEmitted(500);   // consumed span + emitted keys ⇒ estRemaining > 0 (a selectable victim)

            AtomicBoolean ownerSplit = new AtomicBoolean();
            MockPageFetcher mock = MockPageFetcher.builder()
                    .keys(keyspace)
                    .interceptor((PageRequest req, int idx, ListPage page) -> {
                        // The thief's ONLY maxKeys==1 call is its speculative probe. While it is in
                        // flight, the OWNER self-splits this victim to m: narrow hi + durable child.
                        if (req.maxKeys() == 1 && ownerSplit.compareAndSet(false, true)) {
                            victim.lock().lock();
                            try {
                                victim.narrowHi(m);
                                long child = store.splitNode(new SplitSpec(run.id(), victimId, m, hi));
                                assertThat(child).as("owner self-split committed durably")
                                        .isNotEqualTo(CheckpointStore.SPLIT_ABORTED);
                            } catch (CheckpointException e) {
                                throw new IllegalStateException("owner self-split failed", e);
                            } finally {
                                victim.lock().unlock();
                            }
                        }
                        return page;
                    })
                    .build();

            Thief thief = Thiefs.of(store, mock, run.id(), new byte[0], ListingMode.OBJECTS,
                    (id, childLo, childHi) -> { }, metrics);

            Thief.Outcome outcome = thief.steal(List.of(victim));

            // The stale (C, HI) snapshot loses the early-loser race and touches nothing.
            assertThat(outcome).as("stale snapshot must not reopen the child's range").isEqualTo(Thief.Outcome.RETRY);
            assertThat(metrics.diagnostics(Duration.ZERO).stealReasons().getOrDefault("RETRY.bound_moved", 0L))
                    .as("detected the moved bound (early-loser), not a durable clobber").isEqualTo(1L);
            assertThat(victim.hi()).as("owner's narrow to m preserved — NOT clobbered back to HI").isEqualTo(m);

            // Exactly ONE durable split (the owner's); the thief committed none. The durable range set
            // tiles (⊥, ⊤] with no overlap: owner keeps (LO, m], the child owns (m, HI].
            List<RangePartition.Split> splits = store.splits();
            assertThat(splits).as("only the owner's split is durable").hasSize(1);
            RangePartition.Split s = splits.get(0);
            assertThat(s.victimId()).isEqualTo(victimId);
            assertThat(s.pivot()).isEqualTo(m);
            assertThat(s.oldHi()).isEqualTo(hi);
            RangePartition.assertTiles(List.of(
                    new RangePartition.Interval(null, lo),
                    new RangePartition.Interval(lo, m),     // owner keeps (LO, m]
                    new RangePartition.Interval(m, hi),     // child owns (m, HI]
                    new RangePartition.Interval(hi, null)));

            // Byte-exact tail: resume the two durable nodes and emit exactly the keys in (C, HI] once
            // (no key dropped at the m boundary, no key double-emitted across it).
            List<Node> resumable = store.loadResumable(run.id(), false);
            assertThat(resumable).as("owner + child both resume").hasSize(2);
            List<byte[]> emitted = drainNodes(store, run.id(), keyspace, resumable);
            List<byte[]> tail = keyspace.stream()
                    .filter(k -> Arrays.compareUnsigned(k, c) > 0)
                    .toList();
            EngineHarness.assertExactlyOnce(emitted, tail);
        }
    }

    // -------------------------------------------------------------------------
    // (T2) The owner self-split ABORT path (SPLIT_ABORTED) is byte-exact.
    // -------------------------------------------------------------------------

    @Test
    @Timeout(60)
    void ownerSelfSplitAbortPathIsByteExact(@TempDir Path dir) throws Exception {
        // Force the owner self-split late-loser branch: a single worker (no thief) whose EVERY split
        // attempt is rejected by the store with SPLIT_ABORTED. An aborted owner split must leave the
        // parent range intact — hi restored to H, no child enqueued, cursor unchanged — so the lone
        // owner drains the whole (LO, HI] byte-exact, and the OWNER_SPLIT.self_aborted counter fires.
        Path ckptDir = dir.resolve("t2-abort");
        Files.createDirectories(ckptDir);
        List<byte[]> keyspace = denseFlat(40_000);
        int maxKeys = 100;

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        RecordingSplitStore recording;
        AbortingCheckpointStore aborting;
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            recording = new RecordingSplitStore(sqlite);
            // Abort EVERY split attempt: with one worker these are all owner self-splits (§3 late loser).
            aborting = new AbortingCheckpointStore(recording, (idx, spec) -> true);
            RunMeta run = aborting.openRun(key("t2"), false, false);
            aborting.insertNode(range(run.id(), LO, HI));
            List<Node> resumable = aborting.loadResumable(run.id(), false);

            MockPageFetcher mock = MockPageFetcher.builder().keys(keyspace).build();
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics),
                    mock, aborting, 1, maxKeys, resumable, FilterChain.EMPTY);

            RunContext ctx = RunContext.create();
            PipelineDrain.collectKeys(5000, ctx, engine, emitted);
        }

        EngineHarness.assertExactlyOnce(emitted, keyspace);
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("OWNER_SPLIT.self_aborted", 0L))
                .as("the owner-split abort branch fired").isPositive();
        assertThat(reasons.getOrDefault("OWNER_SPLIT.self_published", 0L))
                .as("no owner split was ever published (all aborted)").isZero();
        assertThat(aborting.aborts()).as("splits were forced to abort").isPositive();
        assertThat(recording.splits()).as("an aborted split enqueues no durable child").isEmpty();
    }

    /** The dense mega-day key list ({@code 2022/03/05/<v>}) straddling the whole (C, HI] used by T1. */
    private static List<byte[]> megaDay() {
        List<byte[]> keys = new ArrayList<>();
        for (int v = 10_000_000; v <= 99_000_000; v += 1_000_000) {
            keys.add("2022/03/05/%08d".formatted(v).getBytes(StandardCharsets.UTF_8));
        }
        keys.add("2022/03/05/99999999".getBytes(StandardCharsets.UTF_8));   // a key exactly at HI
        return keys;
    }

    /** Drive a fresh {@link WorkStealingScan} over the resumed nodes, collecting every emitted key. */
    private static List<byte[]> drainNodes(RecordingSplitStore store, long runId, List<byte[]> keyspace,
                                           List<Node> resumable) throws Exception {
        MockPageFetcher mock = MockPageFetcher.builder().keys(keyspace).build();
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        WorkStealingScan engine = new WorkStealingScan(
                EngineContexts.of(runId, new byte[0], ListingMode.OBJECTS, new RunMetrics(new SimpleMeterRegistry())),
                mock, store, 4, 100, resumable, FilterChain.EMPTY);
        RunContext ctx = RunContext.create();
        PipelineDrain.collectKeys(5000, ctx, engine, emitted);
        return emitted;
    }

    // -------------------------------------------------------------------------
    // Assertion helpers.
    // -------------------------------------------------------------------------

    /**
     * Replay {@code splits} (owner + thief) from the initial seed frame and assert the result tiles
     * {@code (⊥, ⊤]} — same validation as {@link RangePartition#replay} (oldHi == victim hi; pivot
     * strictly between lo and oldHi, boundary belongs LEFT) but seeded from multiple live ranges.
     */
    private static void assertTilesFromSeeds(Map<Long, RangePartition.Interval> seedLive,
                                             List<RangePartition.Split> splits) {
        Map<Long, RangePartition.Interval> live = new HashMap<>(seedLive);
        for (RangePartition.Split s : splits) {
            RangePartition.Interval v = live.get(s.victimId());
            assertThat(v).as("split victim %d must be a live range", s.victimId()).isNotNull();
            assertThat(Arrays.equals(v.hi(), s.oldHi()))
                    .as("split oldHi must equal the victim's current hi").isTrue();
            if (v.lo() != null) {
                assertThat(Arrays.compareUnsigned(v.lo(), s.pivot())).as("lo < pivot").isLessThan(0);
            }
            if (s.oldHi() != null) {
                assertThat(Arrays.compareUnsigned(s.pivot(), s.oldHi())).as("pivot < oldHi").isLessThan(0);
            }
            live.put(s.victimId(), new RangePartition.Interval(v.lo(), s.pivot()));
            live.put(s.childId(), new RangePartition.Interval(s.pivot(), s.oldHi()));
        }
        RangePartition.assertTiles(new ArrayList<>(live.values()));
    }
}
