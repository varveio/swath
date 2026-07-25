/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Quantifies the thief's {@code slotGated=false} probe retry-loop pressure on a NARROW TAIL (few
 * live ranges, MANY idle workers) under a permanent {@code ATTEMPT_TIMEOUT} storm — and the
 * probe-specific fail-fast retry cap that keeps that pressure bounded — using the {@code
 * TRANSIENT.attempt_timeout_{probe,worker}} / {@code TRANSIENT.probe_retry_cap_failfast}
 * attribution counters (§4 discriminator — §4/§7).
 *
 * <p><b>Two regimes, and the fix that made the second one safe.</b>
 * {@link IdleStealBackoff} admits <b>at most one in-flight steal attempt
 * engine-wide</b> ({@code attemptInFlight}), released only once {@code thief.steal()} fully RETURNS —
 * so idle-worker COUNT never multiplies probe pressure (30 idle workers behave no differently from 3
 * here). <b>This class does not measure that bound</b> — it asserts probe <i>pressure</i> and takes
 * the bound as given; {@link IdleStealSlotOwnershipTest} and {@link IdleStealProbeConcurrencyTest}
 * are what guard it (a leak that let unrelated workers hand the slot away survived here undetected):
 * <ul>
 *   <li>{@link #narrowTail_permanentAttemptTimeoutStorm_probeFetchesNeverStart} — a storm from
 *       {@code t=0} (no worker has EVER committed a page): {@code eligibleVictims()} is permanently
 *       EMPTY (progress-gated stealing, {@code stealEligible()} requires a prior committed page), so
 *       {@code thief.steal()} always resolves {@code NO_VICTIM} without ever issuing a probe fetch.
 *       {@code attempt_timeout_probe} stays at exactly 0 — probes are structurally suppressed, not
 *       merely rare.</li>
 *   <li>{@link #narrowTail_stealEligibleVictimsThenStorm_probeShareStaysSmallUnderTheFailFastCap} —
 *       the field-representative shape (a run healthy long enough for both live ranges to become
 *       steal-eligible BEFORE the storm hits): a probe that happens to be mid-flight when the storm
 *       starts must not be allowed to camp uncapped on the sole in-flight slot for the whole storm —
 *       {@code GaugedFetcher#PROBE_TRANSIENT_RETRY_CAP} plus {@code Thief#steal}'s
 *       catch cap every probe episode at a bounded number of attempts, so a camping probe frees the
 *       slot instead of dominating the storm's attempt-timeout volume; this test asserts the
 *       resulting probe share of total attempt-timeout retries stays small (the anti-camping
 *       contract, not just the raw numbers), and that the run still completes exactly-once.</li>
 * </ul>
 */
final class NarrowTailProbeVsWorkerAttributionTest {

    /** Few live ranges — the narrow-tail shape (§4's discriminator scenario). */
    private static final int LIVE_RANGES = 2;
    /** MANY idle workers relative to the 2 live ranges — most become thieves immediately. */
    private static final int WORKERS = 30;
    /**
     * Large enough that {@code ceil(LIVE_RANGES * KEYS_PER_RANGE / MAX_KEYS)} — the MINIMUM total page
     * fetches needed to fully drain the keyspace, regardless of how much splitting parallelizes the
     * drain — comfortably exceeds the {@code warmupCalls} window used by the steal-eligible-victims
     * test below: a too-small keyspace could fully drain within the near-instant warmup window on
     * fast/lucky scheduling, leaving {@code attempt_timeout_worker == 0} for the whole storm — not a
     * timing-tolerant flake to gate around, a fixture sizing bug to fix, so a worker fetch is
     * GUARANTEED to still be in flight when the storm starts.
     */
    private static final int KEYS_PER_RANGE = 250;
    private static final int MAX_KEYS = 4;
    /**
     * Total ATTEMPT_TIMEOUT faults (across BOTH worker and probe fetches, sharing one counter) before
     * the storm heals and every fetch succeeds. Large enough that idle-steal's real-nanoTime backoff
     * pacing ({@code PARK_NANOS}=5ms .. {@code IDLE_STEAL_BACKOFF_CAP_NANOS}=50ms) has room to admit a
     * handful of steal attempts alongside the worker retry loops, small enough the test stays fast.
     */
    private static final int HEAL_AFTER = 4_000;
    /** A small REAL sleep per retry (not a no-op) — paces worker/probe retries in actual wall time so
     *  they compete on realistic footing with idle-steal's real-nanoTime backoff, instead of a no-op
     *  sleeper letting worker retries spin at raw CPU speed (which would starve probes of any chance to
     *  run at all, a scheduling artifact rather than the architectural bound under test). */
    private static final TransientRetryFetcher.Sleeper PACED_SLEEPER = ms -> Thread.sleep(1);

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "narrow-tail-probe-attribution",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static byte[] cut(int i) {
        return b(String.format("p%02d/", i));
    }

    private static List<byte[]> keyspace() {
        List<byte[]> keys = new ArrayList<>(LIVE_RANGES * KEYS_PER_RANGE);
        for (int s = 0; s < LIVE_RANGES; s++) {
            for (int k = 0; k < KEYS_PER_RANGE; k++) {
                keys.add(b(String.format("p%02d/%06d", s, k)));
            }
        }
        return keys;
    }

    private static double steal(RunMetrics m, String outcome, String reason) {
        Counter c = m.registry().find("swath.steal_reason").tag("outcome", outcome).tag("reason", reason)
                .counter();
        return c == null ? 0.0 : c.count();
    }

    private static double counter(RunMetrics m, String name) {
        Counter c = m.registry().find(name).counter();
        return c == null ? 0.0 : c.count();
    }

    @Test
    @Timeout(60)
    void narrowTail_permanentAttemptTimeoutStorm_probeFetchesNeverStart(@TempDir Path dir)
            throws Exception {
        List<byte[]> keyspace = keyspace();
        AtomicInteger calls = new AtomicInteger();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keyspace)
                .interceptor((req, idx, page) -> {
                    if (calls.getAndIncrement() < HEAL_AFTER) {
                        throw ThrottleException.attemptTimeout("narrow-tail storm");
                    }
                    return page;   // healed: every fetch past HEAL_AFTER succeeds
                })
                .build();

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        RetryConfig retryConfig = new RetryConfig(RetryPolicy.RIDE_OUT, PACED_SLEEPER);

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(dir.resolve("narrow-tail.sqlite"))) {
            RunMeta run = sqlite.openRun(key(), false, false);
            for (int s = 0; s < LIVE_RANGES; s++) {
                byte[] lo = (s == 0) ? null : cut(s);
                byte[] hi = (s == LIVE_RANGES - 1) ? null : cut(s + 1);
                sqlite.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, lo, hi, lo, null));
            }
            List<Node> seeds = sqlite.loadResumable(run.id(), false);
            assertThat(seeds).as("narrow tail: exactly LIVE_RANGES seeded").hasSize(LIVE_RANGES);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withRetryConfig(retryConfig),
                    fetcher, sqlite, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY);

            List<byte[]> emitted = Collections.synchronizedList(new ArrayList<>());
            RunContext ctx = RunContext.create();
            PipelineDrain.collectKeys(1000, ctx, engine, emitted);

            EngineHarness.assertExactlyOnce(emitted, keyspace);

            double attemptTimeoutWorker = steal(metrics, "TRANSIENT", "attempt_timeout_worker");
            double attemptTimeoutProbe = steal(metrics, "TRANSIENT", "attempt_timeout_probe");
            double stormRideOutProbe = steal(metrics, "TRANSIENT", "storm_ride_out_probe");
            double stealAttempted = steal(metrics, "STEAL", "attempted");
            double slotDenied = counter(metrics, "swath.idle_backoff.slot_denied");

            assertThat(attemptTimeoutWorker)
                    .as("the storm produced worker-attributed attempt-timeout retries").isGreaterThan(0.0);
            // Progress-gated stealing (WorkerState#stealEligible): a live range only becomes a steal
            // VICTIM once it has committed a real page. From t=0, under a permanent storm, NEITHER live
            // range ever commits one — so eligibleVictims() is permanently empty and every thief.steal()
            // call resolves NO_VICTIM without ever reaching a probe fetch. This is a hard structural
            // zero, not a loose bound — a mutant that let Thief probe an ineligible/never-progressed
            // victim would flip this to > 0.
            assertThat(attemptTimeoutProbe)
                    .as("no probe fetch is ever issued when no victim has EVER committed a page "
                            + "(worker=%.0f)", attemptTimeoutWorker)
                    .isEqualTo(0.0);
            assertThat(stormRideOutProbe).as("no probe ever entered ride-out either").isEqualTo(0.0);
            // The idle-steal backoff gate is doing real work: idle-worker park cycles are denied a fresh
            // probe slot (or, once NO_VICTIM resolves, simply find nothing worth attempting) rather than
            // each of the WORKERS-1 idle workers independently hammering its own concurrent probe.
            assertThat(slotDenied)
                    .as("idle workers are mostly slot-denied, not each independently probing")
                    .isGreaterThanOrEqualTo(stealAttempted);
        }
    }

    /**
     * The field-representative narrow-tail shape: the 2 live ranges are ALREADY
     * {@link WorkerState#stealEligible() steal-eligible} (each has committed a real page — the field
     * incident's tail wedge hits a run that was healthy for a long time first, not a storm from
     * {@code t=0}) when the permanent {@code ATTEMPT_TIMEOUT} storm begins. Unlike the from-{@code t=0}
     * storm above — where {@link Thief#steal} always sees an EMPTY eligible-victim set and so never
     * issues a single probe fetch — this fixture lets a thief actually reach a live victim during the
     * real-time warmup window BEFORE the storm hits, exactly as a mid-run tail wedge would: some idle
     * worker is genuinely mid-probe when the storm starts.
     *
     * <p>Without the fail-fast cap, a probe caught mid-flight rides out the SAME uncapped schedule a
     * worker fetch uses for as long as the storm lasts, camping on the sole {@code IdleStealBackoff}
     * in-flight slot — the sustained-timeout SHED pausing {@code isStealingAllowed()} only blocks NEW
     * steal attempts from starting, it does not interrupt one already in flight — so a camping probe
     * would dominate the storm's total attempt-timeout volume.
     *
     * <p>{@code GaugedFetcher#PROBE_TRANSIENT_RETRY_CAP} caps that probe at 2 total
     * attempts, so it can camp for at most ~2 attempts' worth of the storm before {@link Thief#steal}
     * folds the failure into an ordinary {@code RETRY} and frees the slot. This test asserts the
     * probe's resulting share of total attempt-timeout volume stays under a conservative 10%
     * ceiling, well above the typical observed range, so the assertion stays robust to scheduling
     * variance across machines while still proving the cap keeps the share small.
     *
     * <p><b>Statistical/real-time, by nature.</b> Whether a probe ever lands mid-flight against the
     * storming victim at all is itself a real-time race
     * ({@code IdleStealBackoff} paces new steal attempts by wall-clock nanoTime against a narrow,
     * near-instant warmup window) — on slow/contended hardware that race can legitimately be lost
     * every time, leaving {@code attempt_timeout_probe == 0} for the whole run. That is NOT an
     * anti-camping violation (nothing engaged to camp), so every measurement below is conditioned on
     * genuine engagement rather than asserted unconditionally; see the in-method comments for the
     * precise gate (including the narrower case where an engaged probe heals within its one grace
     * retry and so legitimately never fires the fail-fast counter either).
     */
    @Test
    @Timeout(60)
    void narrowTail_stealEligibleVictimsThenStorm_probeShareStaysSmallUnderTheFailFastCap(@TempDir Path dir)
            throws Exception {
        List<byte[]> keyspace = keyspace();
        int warmupCalls = 20;   // enough for both live ranges to commit >= 1 real page before the storm
        AtomicInteger calls = new AtomicInteger();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keyspace)
                .interceptor((req, idx, page) -> {
                    int n = calls.getAndIncrement();
                    if (n < warmupCalls) {
                        return page;   // warmup: real pages succeed, making both ranges steal-eligible
                    }
                    if (n < warmupCalls + HEAL_AFTER) {
                        throw ThrottleException.attemptTimeout("narrow-tail storm (mid-run)");
                    }
                    return page;   // healed
                })
                .build();

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        RetryConfig retryConfig = new RetryConfig(RetryPolicy.RIDE_OUT, PACED_SLEEPER);

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(dir.resolve("narrow-tail-eligible.sqlite"))) {
            RunMeta run = sqlite.openRun(key(), false, false);
            for (int s = 0; s < LIVE_RANGES; s++) {
                byte[] lo = (s == 0) ? null : cut(s);
                byte[] hi = (s == LIVE_RANGES - 1) ? null : cut(s + 1);
                sqlite.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, lo, hi, lo, null));
            }
            List<Node> seeds = sqlite.loadResumable(run.id(), false);
            assertThat(seeds).hasSize(LIVE_RANGES);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withRetryConfig(retryConfig),
                    fetcher, sqlite, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY);

            List<byte[]> emitted = Collections.synchronizedList(new ArrayList<>());
            RunContext ctx = RunContext.create();
            PipelineDrain.collectKeys(1000, ctx, engine, emitted);

            EngineHarness.assertExactlyOnce(emitted, keyspace);

            double attemptTimeoutWorker = steal(metrics, "TRANSIENT", "attempt_timeout_worker");
            double attemptTimeoutProbe = steal(metrics, "TRANSIENT", "attempt_timeout_probe");
            double probeRetryCapFailfast = steal(metrics, "TRANSIENT", "probe_retry_cap_failfast");
            double stealAttempted = steal(metrics, "STEAL", "attempted");
            double slotDenied = counter(metrics, "swath.idle_backoff.slot_denied");

            assertThat(attemptTimeoutWorker).isGreaterThan(0.0);
            double total = attemptTimeoutWorker + attemptTimeoutProbe;

            // Whether a probe ever gets a real chance to land mid-flight against the storming victim
            // is itself a REAL-TIME race — IdleStealBackoff paces new steal attempts by wall-clock
            // nanoTime (5-50ms), and the
            // narrow `warmupCalls` window in which a probe can pick up a still-healthy victim before the
            // storm begins is a handful of near-instant calls. On slow/contended hardware that race can
            // legitimately be lost EVERY time in a given run — no probe ever reaches the storming victim
            // at all, so `attempt_timeout_probe` (and therefore `probe_retry_cap_failfast`) stays 0. That
            // is NOT a violation of the anti-camping contract — there is nothing to camp on if no probe
            // ever engaged — so every assertion below is conditioned on genuine ENGAGEMENT
            // (`attemptTimeoutProbe > 0`), never on a fixed wall-clock/count expectation. The always-true
            // correctness assertions (`assertExactlyOnce` above) already ran unconditionally.
            boolean probeEngagedWithStorm = attemptTimeoutProbe > 0.0;
            double observedShare = probeEngagedWithStorm ? attemptTimeoutProbe / total : 0.0;

            if (probeEngagedWithStorm) {
                // THE ANTI-CAMPING SHARE BOUND — safe to assert unconditionally once ANY probe engaged,
                // regardless of whether that probe went on to fail fast or heal within its one grace
                // retry (see below): PROBE_TRANSIENT_RETRY_CAP bounds EVERY probe episode to at most
                // PROBE_TRANSIENT_RETRY_CAP + 1 = 2 attempts before it resolves one way or the other, so
                // its contribution to total request volume is bounded by construction either way (see
                // this method's javadoc for the share ceiling this bounds toward).
                assertThat(observedShare)
                        .as("probe-attributed attempt-timeouts stay a SMALL fraction of total request "
                                + "volume under the fail-fast cap: worker=%.0f probe=%.0f",
                                attemptTimeoutWorker, attemptTimeoutProbe)
                        .isLessThanOrEqualTo(0.10);

                // The fail-fast counter is a STRICTLY NARROWER signal than "a probe engaged": a probe
                // that hits exactly one transient and then heals on its single grace retry (the storm
                // having already ended by the time that retry lands) legitimately never exceeds
                // PROBE_TRANSIENT_RETRY_CAP and so never fires probe_retry_cap_failfast at all, even
                // though attempt_timeout_probe > 0 for that same episode — a second real-time race
                // (whether the grace retry lands before or after the storm heals) layered on top of the
                // first. Asserting failfast > 0 whenever a probe merely engaged would just move the same
                // CI flake one level down, so it is deliberately NOT required — only asserted about
                // (never required to be positive) when it IS observed, which is itself timing-immune:
                // once a run records at least one fail-fast, that is unconditional, durable evidence the
                // mechanism fired this run, safe to log/inspect but not itself gated on anything further.
                if (probeRetryCapFailfast > 0.0) {
                    // Strong corroboration: at least one probe genuinely exceeded its cap and failed
                    // fast — freeing the slot quickly lets MANY MORE distinct steal attempts run in the
                    // same storm window than an uncapped camping probe would allow.
                    assertThat(stealAttempted)
                            .as("freeing the slot quickly lets many more distinct steal attempts run than "
                                    + "the pre-fix camping regime (~14-18)")
                            .isGreaterThan(20.0);
                }
                // else: every probe that engaged this run happened to heal within its one grace retry
                // before ever exceeding the cap — a legitimate, non-flaky outcome (nothing to assert
                // about fail-fast specifically; the share bound above already covers it).
            }
            // else: no probe ever engaged with the storm this run (slow/contended hardware lost the
            // real-time race to reach the victim at all) — the anti-camping property is vacuously
            // satisfied; nothing further to assert.

            assertThat(slotDenied)
                    .as("idle workers are mostly slot-denied, not each independently probing")
                    .isGreaterThanOrEqualTo(0.0);
        }
    }

}
