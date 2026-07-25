/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.PageBatch;
import io.varve.swath.output.JsonlFormatter;
import io.varve.swath.output.OutputStage;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * CONC guard at engine scale: across a live work-stealing scan, the number of <b>concurrent</b>
 * speculative probe fetches never exceeds one — {@link IdleStealBackoff}'s fleet-wide bound as
 * observed at the store, not as claimed by the class javadoc.
 *
 * <p>This is the shape the leak needed and the existing suite could not see. {@code
 * NarrowTailProbeVsWorkerAttributionTest} asserts probe <i>pressure</i> under a storm where
 * {@code eligibleVictims()} is empty or probes fail fast; it never measures overlap, so a slot that
 * {@code reset()} handed away mid-attempt went unnoticed. Here every worker commits pages
 * continuously — each commit is a {@code reset()} from a worker that does not own the slot — while
 * probes are held open long enough that any handover shows up as two probes in flight at once.
 *
 * <p>The multi-victim method here answers the separate head-of-line question #3 left open — see its
 * own javadoc.
 *
 * <p>Both latency-injecting methods are {@code deep} (probes sleep to open the overlap window, and
 * they race real worker threads); the bound's contract line is pinned per-commit by the
 * deterministic {@link IdleStealSlotOwnershipTest} (TESTING.md § tag convention). The
 * release-on-throw method injects no latency and aborts at the first probe, so it stays per-commit.
 */
final class IdleStealProbeConcurrencyTest {

    private static final int WORKERS = 16;
    private static final int MAX_KEYS = 4;
    private static final int KEYS = 4_000;
    /** Long enough that a handed-away slot produces an overlap, short enough to keep the run brisk. */
    private static final long PROBE_HOLD_MILLIS = 25L;
    private static final String INJECTED_FAULT = "injected unchecked fault inside the steal attempt";
    /** A field probe's multi-second cost, compressed: long enough that serialization is measurable. */
    private static final long SLOW_PROBE_HOLD_MILLIS = 50L;
    /**
     * The head-of-line fixture's own scale. Each victim must stay alive long enough for the
     * SERIALIZED thief fleet to get tens of attempts against it — at {@code HOL_KEYS / 4} keys per
     * victim, {@code MAX_KEYS} per page and {@code HOL_PAGE_MILLIS} per page, a victim lives about
     * 3.75 s while a probe costs 50 ms, so ~75 attempt slots fit into the run. Sized off the
     * mechanism, not tuned until it passed.
     */
    private static final int HOL_KEYS = 20_000;
    private static final long HOL_PAGE_MILLIS = 3L;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * A seed tile shaped exactly as {@code SeedStep} emits one — note {@code cursor = lo}, not
     * {@code null}: the scan starts at the claim's CURSOR, so a tile seeded at ⊥ would re-list the
     * whole keyspace and only its upper bound would differ.
     */
    private static NodeSpec seed(long runId, byte[] lo, byte[] hi) {
        return new NodeSpec(runId, null, NodeKind.RANGE, lo, hi, lo, null);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "probe-concurrency-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    /** The thief's speculative calls: a {@code max_keys<=1} pivot probe or a delimited structure probe. */
    private static boolean isProbe(PageRequest req) {
        return req.maxKeys() <= 1 || (req.delimiter() != null && req.delimiter().length > 0);
    }

    @Test
    @Tag("deep")
    @Timeout(120)
    void stealProbeFetchesNeverOverlapUnderCommitDrivenResets(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < KEYS; i++) {
            keyspace.add(("k%05d".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        AtomicInteger inFlightProbes = new AtomicInteger();
        AtomicInteger maxConcurrentProbes = new AtomicInteger();
        AtomicInteger totalProbes = new AtomicInteger();

        RunContext ctx = RunContext.create();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keyspace)
                .interceptor((req, idx, page) -> {
                    if (!isProbe(req)) {
                        Thread.sleep(1);   // keep workers committing pages (each commit is a reset)
                        return page;
                    }
                    totalProbes.incrementAndGet();
                    int now = inFlightProbes.incrementAndGet();
                    maxConcurrentProbes.accumulateAndGet(now, Math::max);
                    try {
                        Thread.sleep(PROBE_HOLD_MILLIS);
                    } finally {
                        inFlightProbes.decrementAndGet();
                    }
                    return page;
                })
                .build();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("probe-concurrency.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, ctx.metrics()),
                    fetcher, store, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY);

            StringWriter out = new StringWriter();
            OutputStage output = new OutputStage(new JsonlFormatter(out));
            new Pipeline<PageBatch>(1000).run(ctx, engine, output);

            assertThat(out.toString().lines().count())
                    .as("exactly-once emission is unaffected by the slot policy")
                    .isEqualTo(keyspace.size());
        }

        assertThat(totalProbes.get())
                .as("the fixture must actually exercise the steal path, or the bound is untested")
                .isGreaterThan(0);
        assertThat(maxConcurrentProbes.get())
                .as("at most one speculative steal attempt in flight fleet-wide (%d probes seen)",
                        totalProbes.get())
                .isEqualTo(1);
    }

    /**
     * The head-of-line question issue #3 left open: with the bound held strictly, does one slow
     * probe block the whole fleet from stealing, and does having <b>several</b> live victims buy
     * anything back? Four seed ranges give four concurrent victims; every probe is held open, so
     * the fleet's steal attempts can only proceed one at a time regardless of how many victims are
     * available to probe.
     *
     * <p>The answer this pins is: <b>yes, global serialization is real and it is by construction —
     * fleet-wide probe throughput is capped at {@code 1 / probe duration} no matter how many
     * victims exist — but it starves nothing.</b> Splits still commit and the scan still completes
     * exactly-once. That is the baseline a future N-permit policy (with per-victim reservations,
     * per #3) would have to beat, and the reason the measured A/B came out FASTER strictly paced
     * despite this cap: on the real bucket the thieves were losing the argmax CAS to each other,
     * so the concurrency they gave up was not buying attempts.
     *
     * <p>Measured at this fixture's scale: <b>30 probes over 2109 ms with 12 children committed</b>
     * — ~14 attempts/s against a 50 ms probe (the ~20/s ceiling the serialization implies), four
     * victims notwithstanding, and stealing plainly not starved.
     *
     * <p>The serialization assertion is an upper bound the mechanism cannot violate, not a timing
     * guess, so it cannot flake: N non-overlapping probes each held for
     * {@code SLOW_PROBE_HOLD_MILLIS} cannot fit into fewer than
     * {@code N x SLOW_PROBE_HOLD_MILLIS} of wall clock.
     */
    @Test
    @Tag("deep")
    @Timeout(180)
    void oneSlowProbeSerializesTheFleetAcrossManyVictimsWithoutStarvingThem(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < HOL_KEYS; i++) {
            keyspace.add(("k%05d".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        AtomicInteger inFlightProbes = new AtomicInteger();
        AtomicInteger maxConcurrentProbes = new AtomicInteger();
        AtomicInteger totalProbes = new AtomicInteger();

        RunContext ctx = RunContext.create();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keyspace)
                .interceptor((req, idx, page) -> {
                    if (!isProbe(req)) {
                        Thread.sleep(HOL_PAGE_MILLIS);   // keep all four victims alive and eligible
                        return page;
                    }
                    totalProbes.incrementAndGet();
                    maxConcurrentProbes.accumulateAndGet(inFlightProbes.incrementAndGet(), Math::max);
                    try {
                        Thread.sleep(SLOW_PROBE_HOLD_MILLIS);
                    } finally {
                        inFlightProbes.decrementAndGet();
                    }
                    return page;
                })
                .build();

        long startNanos = System.nanoTime();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("head-of-line.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            // Four victims, not one: if the bound blocked per-victim rather than globally, three of
            // them would still be probeable while the fourth's probe is held.
            store.insertNode(seed(run.id(), null, bytes("k05000")));
            store.insertNode(seed(run.id(), bytes("k05000"), bytes("k10000")));
            store.insertNode(seed(run.id(), bytes("k10000"), bytes("k15000")));
            store.insertNode(seed(run.id(), bytes("k15000"), null));
            List<Node> seeds = store.loadResumable(run.id(), false);
            assertThat(seeds).as("the fixture needs four concurrent victims").hasSize(4);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, ctx.metrics()),
                    fetcher, store, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY);

            StringWriter out = new StringWriter();
            OutputStage output = new OutputStage(new JsonlFormatter(out));
            new Pipeline<PageBatch>(1000).run(ctx, engine, output);

            assertThat(out.toString().lines().count())
                    .as("a serialized steal fleet still lists the keyspace exactly once")
                    .isEqualTo(keyspace.size());
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        assertThat(totalProbes.get()).as("the fixture must reach the steal path").isGreaterThan(0);
        assertThat(maxConcurrentProbes.get())
                .as("the bound holds with four victims available, not just one (%d probes seen)",
                        totalProbes.get())
                .isEqualTo(1);
        assertThat((long) totalProbes.get() * SLOW_PROBE_HOLD_MILLIS)
                .as("probe throughput is capped fleet-wide at 1/probe-duration, victim count "
                        + "notwithstanding — this IS the head-of-line cost, stated as the bound it is")
                .isLessThanOrEqualTo(elapsedMillis);
        assertThat(ctx.metrics().diagnostics(Duration.ZERO).stealReasons()
                        .getOrDefault("CHILD_CREATED.split_committed", 0L))
                .as("serialized is not starved: steals still commit children across the victims")
                .isGreaterThanOrEqualTo(1L);
    }

    /**
     * The release is unconditional. An <b>unchecked</b> throw from inside the acquired region —
     * modelled here at the probe fetch, but equally reachable from the metrics, logging,
     * {@code eligibleVictims()} or the child enqueue that share that region — must still hand the
     * slot back, or stealing is dead for the rest of the run.
     *
     * <p>Fails against a release wired only to the productive {@code CHILD_CREATED} path (the shape
     * the prototype for #3 had): there the throw skips the release entirely and the slot stays
     * owned by a worker that no longer exists.
     *
     * <p>Per-commit, not {@code deep}: it injects no latency and aborts at the first probe. Reaching
     * the steal path is not left to the scheduler — a latch holds the seed owner after its first
     * (eligibility-granting) page until a thief has reached the probe, so the interleaving is
     * established rather than hoped for.
     */
    @Test
    @Timeout(60)
    void anUncheckedThrowInsideTheAcquiredRegionStillReleasesTheSlot(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < KEYS; i++) {
            keyspace.add(("k%05d".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        AtomicInteger probesThrown = new AtomicInteger();
        // The barrier that makes this deterministic rather than merely likely: the seed owner is
        // let through exactly one page — enough to become steal-eligible — and is then held until a
        // thief has reached the probe, so no legal schedule can drain the keyspace before the fault
        // is injected. Its timeout is a fixture backstop; reaching it fails the run below.
        CountDownLatch probeReached = new CountDownLatch(1);
        AtomicInteger pagesServed = new AtomicInteger();
        RunContext ctx = RunContext.create();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keyspace)
                .interceptor((req, idx, page) -> {
                    if (isProbe(req)) {
                        probesThrown.incrementAndGet();
                        probeReached.countDown();
                        throw new IllegalStateException(INJECTED_FAULT);
                    }
                    if (pagesServed.incrementAndGet() > 1) {
                        probeReached.await(30, TimeUnit.SECONDS);
                    }
                    return page;
                })
                .build();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("steal-throw.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, ctx.metrics()),
                    fetcher, store, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY);

            StringWriter out = new StringWriter();
            OutputStage output = new OutputStage(new JsonlFormatter(out));
            assertThatThrownBy(() -> new Pipeline<PageBatch>(1000).run(ctx, engine, output))
                    .as("the abort is THIS fault surfacing, not any failure that happens to end the run")
                    .hasStackTraceContaining(INJECTED_FAULT);

            assertThat(probesThrown.get())
                    .as("the fixture must reach the steal path, or nothing was injected into the region")
                    .isGreaterThan(0);
            // Every worker has exited by the time run() returns; a slot still held means a path out
            // of the acquired region skipped its finally.
            assertThat(engine.stealAttemptInFlight())
                    .as("the attempt slot is released even when the acquired region throws")
                    .isFalse();
        }
    }
}
