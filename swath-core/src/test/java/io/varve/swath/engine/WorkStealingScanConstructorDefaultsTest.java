/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Characterizes the {@link WorkStealingScan} construction path's default-fallback plumbing. The engine
 * exposes a single public constructor taking an {@link EngineContext}; the context's compact constructor
 * coalesces each omitted policy seam to its documented default — engine toggles at
 * {@link EngineToggles#DEFAULT}, the no-op {@link TraceSink#NONE} tracer, and {@link RetryConfig#DEFAULT}
 * (a {@link RetryPolicy#BOUNDED} retry policy, never an owner-less unbounded ride-out) — while the engine
 * constructor body clamps a non-positive worker count up to a single worker. The one default deliberately
 * left unpinned is the {@code null} gauge clock (production concurrency-gauge windows): the clock is
 * consumed into the gauge at construction and never stored, and a differently-windowed clock still drains
 * exactly-once, so there is no field seam and no cheap behavioural observable to assert against.
 *
 * <p>These are ordinary wiring guards, not the cross-cutting PROP/RES/CONC interleavings. They pin
 * the exact default each omitted seam lands on so a future change cannot silently re-route one of them
 * (e.g. flip the owner-split default, drop the trace wiring, or remove the worker-count clamp). Where a
 * default's only behavioural difference shows up on a path too expensive to exercise cheaply (the
 * bounded-vs-ride-out retry policy diverges only under a sustained throttling storm), the default is
 * pinned by identity on the constructed context rather than by a live run.
 */
final class WorkStealingScanConstructorDefaultsTest {

    private static final int WORKERS = 4;
    private static final int MAX_KEYS = 100;
    private static final byte[] LO = "d/00".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HI = "d/02".getBytes(StandardCharsets.UTF_8);

    /** How a test assembles the engine; the constant scalars are supplied by each lambda. */
    @FunctionalInterface
    private interface EngineFactory {
        WorkStealingScan create(PageFetcher fetcher, CheckpointStore store, long runId, byte[] prefix,
                                ListingMode mode, List<Node> seeds, RunMetrics metrics);
    }

    private record ScanResult(List<byte[]> emitted, Map<String, Long> stealReasons) {
    }

    private static RunKey key(String label) {
        return new RunKey("s3", null, "bucket", new byte[0], label,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static NodeSpec seed(long runId, byte[] lo, byte[] hi) {
        return new NodeSpec(runId, null, NodeKind.RANGE, lo, hi, null, null);
    }

    /** A dense flat keyspace {@code d/000000..d/00NNNN} inside {@code (LO, HI]} — the shape that drives owner self-split. */
    private static List<byte[]> denseFlat(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(String.format("d/%06d", i).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /** Seed a single bounded range, build the engine via {@code factory}, drain it, and return keys + steal reasons. */
    private ScanResult run(Path dir, String label, List<byte[]> keyspace, EngineFactory factory) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + ".sqlite"))) {
            RunMeta runMeta = store.openRun(key(label), false, false);
            store.insertNode(seed(runMeta.id(), LO, HI));
            List<Node> seeds = store.loadResumable(runMeta.id(), false);
            WorkStealingScan engine =
                    factory.create(fetcher, store, runMeta.id(), new byte[0], ListingMode.OBJECTS, seeds, metrics);
            PipelineDrain.collectKeys(2000, engine, emitted);
        }
        return new ScanResult(emitted, metrics.diagnostics(Duration.ZERO).stealReasons());
    }

    /** Build the engine via {@code factory} without running it — for reading a default off its fields. */
    private WorkStealingScan construct(Path dir, String label, List<byte[]> keyspace, EngineFactory factory)
            throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + ".sqlite"))) {
            RunMeta runMeta = store.openRun(key(label), false, false);
            store.insertNode(seed(runMeta.id(), LO, HI));
            List<Node> seeds = store.loadResumable(runMeta.id(), false);
            return factory.create(fetcher, store, runMeta.id(), new byte[0], ListingMode.OBJECTS, seeds, metrics);
        }
    }

    private static Object field(WorkStealingScan engine, String name) throws Exception {
        Field f = WorkStealingScan.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(engine);
    }

    private static long mark(ScanResult result, String reason) {
        return result.stealReasons().getOrDefault(reason, 0L);
    }

    private static void assertExactlyOnce(List<byte[]> emitted, List<byte[]> keyspace) {
        TreeSet<byte[]> distinctKeyspace = new TreeSet<>(Arrays::compareUnsigned);
        distinctKeyspace.addAll(keyspace);
        TreeSet<byte[]> distinctEmitted = new TreeSet<>(Arrays::compareUnsigned);
        distinctEmitted.addAll(emitted);
        assertThat(distinctEmitted).as("full byte-exact coverage, no duplicates").isEqualTo(distinctKeyspace);
    }

    // =========================================================================
    // EngineContext compact ctor: null policy seams coalesce to their canonical defaults
    // =========================================================================

    /**
     * The single construction path funnels every run through {@link EngineContext}, whose compact
     * constructor coalesces each {@code null} policy seam to its canonical default — the plumbing every
     * shorter overload used to supply. Pinned by identity: {@link EngineToggles#DEFAULT}, the no-op
     * {@link TraceSink#NONE} singleton, and {@link RetryConfig#DEFAULT} (whose policy is
     * {@link RetryPolicy#BOUNDED}, never an owner-less unbounded ride-out). A non-null seam must pass
     * through untouched — the compact constructor defaults only nulls.
     */
    @Test
    void engineContextCoalescesNullPolicySeamsToTheirCanonicalDefaults() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        EngineContext defaults = new EngineContext(1L, new byte[0], ListingMode.OBJECTS, metrics, null, null, null);
        assertThat(defaults.toggles())
                .as("null toggles coalesce to EngineToggles.DEFAULT").isSameAs(EngineToggles.DEFAULT);
        assertThat(defaults.trace())
                .as("null trace coalesces to the no-op TraceSink.NONE singleton").isSameAs(TraceSink.NONE);
        assertThat(defaults.retryConfig())
                .as("null retryConfig coalesces to RetryConfig.DEFAULT").isSameAs(RetryConfig.DEFAULT);
        assertThat(defaults.retryConfig().policy())
                .as("the implicit retry default is BOUNDED, not an owner-less unbounded ride-out")
                .isEqualTo(RetryPolicy.BOUNDED);
        assertThat(defaults.decisionRngSeed())
                .as("the 7-arg compatibility constructor leaves decisionRngSeed null (the ambient "
                        + "DecisionRng default), not some other coalesced value")
                .isNull();

        EngineToggles ownerOff = EngineToggles.DEFAULT.withOwnerSplit(false);
        TraceSink sink = new RecordingTraceSink();
        RetryConfig ride = new RetryConfig(RetryPolicy.RIDE_OUT, ms -> { });
        EngineContext explicit =
                new EngineContext(1L, new byte[0], ListingMode.OBJECTS, metrics, ownerOff, sink, ride);
        assertThat(explicit.toggles()).as("a non-null toggles seam is left untouched").isSameAs(ownerOff);
        assertThat(explicit.trace()).as("a non-null trace seam is left untouched").isSameAs(sink);
        assertThat(explicit.retryConfig()).as("a non-null retry seam is left untouched").isSameAs(ride);
    }

    // =========================================================================
    // decisionRngSeed: opt-in wiring into Thief's ThiefPolicy
    // =========================================================================

    /**
     * {@link EngineContext#decisionRngSeed()} governs which of {@link Thief}'s two constructors
     * {@link WorkStealingScan} calls — the null-check ternary itself has no OTHER observable (both
     * branches build an otherwise-identical engine), so this reads the constructed {@code Thief}'s
     * {@code ThiefPolicy}'s own {@code rng} field back via reflection — the sole {@link
     * io.varve.swath.engine.policy.DecisionRng} consumer (contracts.md §2.1). A seed set on the
     * context must land on a live {@link SeededDecisionRng}, not the ambient lambda default.
     */
    @Test
    void decisionRngSeedWiresASeededDecisionRngIntoTheThiefsPolicy(@TempDir Path dir) throws Exception {
        WorkStealingScan engine = construct(dir, "seeded-rng", denseFlat(10),
                (f, s, id, p, m, seeds, metrics) -> new WorkStealingScan(
                        new EngineContext(id, p, m, metrics, null, null, null).withDecisionRngSeed(42L),
                        f, s, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY));
        assertThat(thiefPolicyRng(engine))
                .as("a non-null decisionRngSeed must wire a live SeededDecisionRng into ThiefPolicy")
                .isInstanceOf(SeededDecisionRng.class);
    }

    /**
     * The complementary default: an unset {@code decisionRngSeed} must leave {@code ThiefPolicy} on
     * {@link Thief}'s own ambient default (NOT a {@link SeededDecisionRng}) — byte-identical to every
     * run before this seam existed.
     */
    @Test
    void unsetDecisionRngSeedLeavesTheThiefsPolicyOnTheAmbientDefault(@TempDir Path dir) throws Exception {
        WorkStealingScan engine = construct(dir, "unseeded-rng", denseFlat(10),
                (f, s, id, p, m, seeds, metrics) -> new WorkStealingScan(
                        new EngineContext(id, p, m, metrics, null, null, null),
                        f, s, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY));
        assertThat(thiefPolicyRng(engine))
                .as("an unset decisionRngSeed must NOT wire a SeededDecisionRng — Thief's own ambient "
                        + "ThreadLocalRandom default stays live, unchanged from before this seam existed")
                .isNotInstanceOf(SeededDecisionRng.class);
    }

    /**
     * The construction branch above is also a router branch, so it carries an engagement mark:
     * {@code TOGGLE.decision_rng_seeded}, fired once when — and only when — the seeded stream is
     * selected. It is the only thing that distinguishes the two modes after the fact. Both branches
     * make the SAME draws at the same decision points and emit identical decision bytes (that
     * byte-identity is the seam's whole promise), so without the mark a summary or golden could not
     * tell a replay-reproducible run from an ambient one.
     */
    @Test
    void aSeededDecisionRngMarksItsEngagementExactlyOnce(@TempDir Path dir) throws Exception {
        ScanResult seeded = run(dir, "seeded-rng-mark", denseFlat(200),
                (f, s, id, p, m, seeds, metrics) -> new WorkStealingScan(
                        new EngineContext(id, p, m, metrics, null, null, null).withDecisionRngSeed(42L),
                        f, s, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY));

        assertThat(mark(seeded, "TOGGLE.decision_rng_seeded"))
                .as("a seeded run marks the opt-in branch exactly once, at engine construction")
                .isEqualTo(1L);
    }

    /**
     * The complementary polarity: the ambient default stays unmarked. Marking it would put a counter
     * on every run that has ever existed — including the pre-seam baselines this seam promises to
     * leave byte-identical — which is the same reason {@code readahead_on} only marks its opt-in side.
     */
    @Test
    void theAmbientDecisionRngIsNotMarked(@TempDir Path dir) throws Exception {
        ScanResult ambient = run(dir, "unseeded-rng-mark", denseFlat(200),
                (f, s, id, p, m, seeds, metrics) -> new WorkStealingScan(
                        new EngineContext(id, p, m, metrics, null, null, null),
                        f, s, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY));

        assertThat(ambient.stealReasons()).doesNotContainKey("TOGGLE.decision_rng_seeded");
    }

    /** Drills {@code engine.thief.policy.rng} via reflection — the one field a {@link Thief}
     *  constructor-overload choice actually changes. */
    private static Object thiefPolicyRng(WorkStealingScan engine) throws Exception {
        Object thief = field(engine, "thief");
        Field policyField = Thief.class.getDeclaredField("policy");
        policyField.setAccessible(true);
        Object policy = policyField.get(thief);
        Field rngField = policy.getClass().getDeclaredField("rng");
        rngField.setAccessible(true);
        return rngField.get(policy);
    }

    // =========================================================================
    // default context: owner-split on, every ablation toggle at its default
    // =========================================================================

    /**
     * A context built with a {@code null} toggles seam must land on owner-split enabled with
     * {@link EngineToggles#DEFAULT}: no {@code TOGGLE.*_off} ablation mark fires, and a large dense drain
     * still publishes owner self-splits — the whole keyspace emitted exactly once.
     */
    @Test
    @Timeout(90)
    void defaultContextKeepsOwnerSplitAndEveryToggleAtDefault(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = denseFlat(20_000);
        ScanResult r = run(dir, "default-context", keyspace,
                (f, s, id, p, m, seeds, metrics) -> new WorkStealingScan(
                        new EngineContext(id, p, m, metrics, null, null, null),
                        f, s, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY));

        assertExactlyOnce(r.emitted(), keyspace);
        for (String name : EngineToggles.NAMES) {
            assertThat(mark(r, "TOGGLE." + name + "_off"))
                    .as("a null toggles seam defaults to EngineToggles.DEFAULT: no ablation mark for %s", name)
                    .isZero();
        }
        assertThat(mark(r, "OWNER_SPLIT.self_published"))
                .as("owner-split defaults ON, so this dense drain publishes self-splits")
                .isGreaterThanOrEqualTo(1L);
    }

    // =========================================================================
    // owner-split-off toggle suppresses every owner self-split
    // =========================================================================

    /**
     * A context carrying {@code EngineToggles.DEFAULT.withOwnerSplit(false)} — the ablation the
     * {@code --no-owner-split} alias resolves to — must fire exactly one {@code TOGGLE.owner_split_off}
     * mark and publish zero owner self-splits, with the emitted set unchanged; the default context over
     * the same keyspace fires no such mark.
     */
    @Test
    @Timeout(150)
    void ownerSplitOffToggleSuppressesEveryOwnerSelfSplit(@TempDir Path dir) throws Exception {
        List<byte[]> dense = denseFlat(20_000);

        ScanResult off = run(dir, "owner-off", dense,
                (f, s, id, p, m, seeds, metrics) -> new WorkStealingScan(
                        new EngineContext(id, p, m, metrics, EngineToggles.DEFAULT.withOwnerSplit(false), null, null),
                        f, s, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY));

        assertExactlyOnce(off.emitted(), dense);
        assertThat(mark(off, "TOGGLE.owner_split_off"))
                .as("owner-split-off fires the ablation mark exactly once").isEqualTo(1L);
        assertThat(mark(off, "OWNER_SPLIT.self_published"))
                .as("owner-split-off suppresses every owner self-split").isZero();

        ScanResult on = run(dir, "owner-on", denseFlat(200),
                (f, s, id, p, m, seeds, metrics) -> new WorkStealingScan(
                        new EngineContext(id, p, m, metrics, null, null, null),
                        f, s, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY));
        assertThat(mark(on, "TOGGLE.owner_split_off"))
                .as("the default context keeps owner-split on: no ablation mark").isZero();
    }

    // =========================================================================
    // trace: the context's tracer reaches the engine; a null seam defaults to NONE
    // =========================================================================

    /**
     * A {@link TraceSink} carried on the context must actually reach the engine — a recording sink
     * observes the seed range and its completion — and a {@code null} trace seam must leave the engine
     * on the no-op {@link TraceSink#NONE}, read off the constructed engine's field ({@code NONE} has no
     * positive behavioural observable — it is the deliberate absence of trace events).
     */
    @Test
    @Timeout(60)
    void contextTraceSinkReachesTheEngineAndNullDefaultsToNoOp(@TempDir Path dir) throws Exception {
        RecordingTraceSink sink = new RecordingTraceSink();
        ScanResult traced = run(dir, "trace-wired", denseFlat(500),
                (f, s, id, p, m, seeds, metrics) -> new WorkStealingScan(
                        new EngineContext(id, p, m, metrics, null, sink, null),
                        f, s, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY));
        assertExactlyOnce(traced.emitted(), denseFlat(500));
        assertThat(sink.seeded.get())
                .as("the context trace slot is live: the single seed range is recorded exactly once")
                .isEqualTo(1);
        assertThat(sink.completed.get())
                .as("the context trace slot is live: range completions are recorded")
                .isGreaterThanOrEqualTo(1);

        WorkStealingScan viaDefaultContext = construct(dir, "trace-default", denseFlat(10),
                (f, s, id, p, m, seeds, metrics) -> new WorkStealingScan(
                        new EngineContext(id, p, m, metrics, null, null, null),
                        f, s, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY));
        assertThat(field(viaDefaultContext, "trace"))
                .as("a null trace seam leaves the engine on the no-op TraceSink.NONE")
                .isSameAs(TraceSink.NONE);
    }

    // =========================================================================
    // constructor body: non-positive worker count clamps up to one
    // =========================================================================

    /**
     * The engine constructor body clamps {@code workerCount} up to at least one ({@code Math.max(1, …)}).
     * A non-positive count must therefore still fork one worker and drain the whole keyspace exactly
     * once; without the clamp a zero/negative count forks no workers and the run never reaches quiescence.
     */
    @Test
    @Timeout(30)
    void workerCountBelowOneIsClampedToASingleWorker(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = denseFlat(300);
        for (int workerCount : new int[]{0, -3}) {
            String label = "clamp-" + (workerCount < 0 ? "neg" + (-workerCount) : workerCount);

            WorkStealingScan engine = construct(dir, label, keyspace,
                    (f, s, id, p, m, seeds, metrics) -> new WorkStealingScan(
                            new EngineContext(id, p, m, metrics, null, null, null),
                            f, s, workerCount, MAX_KEYS, seeds, FilterChain.EMPTY));
            assertThat(field(engine, "workerCount"))
                    .as("worker count %d clamps up to one", workerCount)
                    .isEqualTo(1);

            List<byte[]> emitted = new ArrayList<>(keyspace.size());
            MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
            try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + "-run.sqlite"))) {
                RunMeta runMeta = store.openRun(key(label + "-run"), false, false);
                store.insertNode(seed(runMeta.id(), LO, HI));
                List<Node> seeds = store.loadResumable(runMeta.id(), false);
                WorkStealingScan run = new WorkStealingScan(
                        new EngineContext(runMeta.id(), new byte[0], ListingMode.OBJECTS,
                                new RunMetrics(new SimpleMeterRegistry()), null, null, null),
                        fetcher, store, workerCount, MAX_KEYS, seeds, FilterChain.EMPTY);
                PipelineDrain.collectKeys(2000, run, emitted);
            }
            assertExactlyOnce(emitted, keyspace);
        }
    }

    /** A trace sink that counts the lifecycle events it observes; every method is thread-safe (workers are concurrent). */
    private static final class RecordingTraceSink implements TraceSink {
        private final AtomicInteger seeded = new AtomicInteger();
        private final AtomicInteger completed = new AtomicInteger();

        @Override
        public boolean enabled() {
            return true;
        }

        @Override
        public void seeded(long nodeId, byte[] lo, byte[] hi) {
            seeded.incrementAndGet();
        }

        @Override
        public void claimed(long workerId, long nodeId, byte[] lo, byte[] cursor, byte[] hi) {
        }

        @Override
        public void pageCommitted(long workerId, long nodeId, int keysEmitted, byte[] cursor, boolean completed) {
        }

        @Override
        public void stealAttempt(long workerId, String outcome, String reason) {
        }

        @Override
        public void ownerSplitDecision(long workerId, long nodeId, String reason, double est,
                long pagesSinceLastSelfSplit, long outstanding, int workerCount, double farAheadFraction,
                double densityRatio, long keysEmitted) {
            // Constructor-default tests only observe seeded/completed lifecycle events.
        }

        @Override
        public void victimScan(long workerId, int seen, int skippedUnsplittable, int skippedPaced,
                int skippedNoSpan, long chosenNodeId, double bestEst, String reason) {
            // Constructor-default tests only observe seeded/completed lifecycle events.
        }

        @Override
        public void split(long workerId, long parentNodeId, long childNodeId, String mechanism, byte[] pivot, byte[] hi) {
        }

        @Override
        public void ownerSplit(long workerId, long parentNodeId, long childNodeId, String mechanism, byte[] pivot, byte[] hi) {
        }

        @Override
        public void completed(long workerId, long nodeId) {
            completed.incrementAndGet();
        }

        @Override
        public void failed(long workerId, long nodeId, String reason) {
        }

        @Override
        public void close() {
        }
    }
}
