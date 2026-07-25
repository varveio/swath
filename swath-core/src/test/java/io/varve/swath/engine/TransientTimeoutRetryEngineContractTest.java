/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.CancelledException;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.StopReason;
import io.varve.swath.runtime.CancelSource;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.RecordingSplitStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end coverage through the REAL {@link WorkStealingScan} / {@code GaugedFetcher} path with a
 * {@link MockPageFetcher} injecting the two throttle shapes as thrown exceptions (the production
 * shape: a transient surfaces after the SDK's own retries, as an exception, not a page carrying
 * {@code httpStatus}).
 *
 * <p>Complements {@link Thr1SustainedThrottleTest} (which guards the 503 side). The headline here: a
 * stream of {@link ThrottleException.Kind#ATTEMPT_TIMEOUT} does NOT touch the AIMD signal ({@code
 * swath.aimd.votes} stays 0), while it IS retried; and an over-cap attempt-timeout storm RIDES OUT
 * (never cancels the run — a never-healing storm is the watchdog's job) and, once it clears,
 * completes exactly once — whereas a real 503 storm past that same threshold keeps retrying
 * (unbounded, AIMD-paced) and still completes.
 */
// Tagged at METHOD granularity, NOT class-level: every method here is latency-injecting (the
// GaugedFetcher retries transients through the shared exponential backoff — real sleeps) OR runs the
// retry loop to the transient cap (8 real backoffs), so all three are slow by construction and belong
// in the deep tier (main-merge + nightly, serial). Their per-commit backstops live in
// EngineThrottleRetrySmokeTest (fast attempt-timeout wiring: retried, no AIMD vote, full concurrency;
// and cap-exhaustion→resumable-STUCK) and ConcurrencyGaugeTest (the AIMD no-vote-on-timeout policy).
final class TransientTimeoutRetryEngineContractTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "transient-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static double aimdVotes(RunMetrics metrics) {
        return metrics.registry().get("swath.aimd.votes").counter().count();
    }

    private static double aimdTimeoutShed(RunMetrics metrics) {
        return metrics.registry().get("swath.aimd.timeout_shed").counter().count();
    }

    // ---- A integration: the two-dimensional shed invariant ---------------------------------------
    // The attempt-timeout regime is guarded on BOTH axes, by TWO methods below:
    //   (1) a STARVED BURST of timeouts (no coexisting progress) sheds T via the shed meter — never a
    //       vote (the shed is the adaptive path down);
    //   (2) a timeout TAIL that COEXISTS WITH real page progress clears the starvation gate and NEVER
    //       sheds — T stays at Tmax.
    // Both share the invariant that an attempt-timeout NEVER casts an AIMD vote (swath.aimd.votes==0).

    /**
     * STARVED BURST: the first {@code N} fetches throw an ATTEMPT_TIMEOUT throttle before ANY page
     * commits, then the store serves cleanly. That is a timeout storm with ~zero coexisting progress,
     * so the sustained-timeout SHED legitimately fires:
     * {@code timeouts >= max(3, ceil(0.3*T)) AND successes <= max(1, floor(T/32))} →
     * {@code T := max(1, floor(0.5*T))}, recorded on {@code swath.aimd.timeout_shed}. The reduction goes
     * through the SHED, NEVER a vote: the run completes exactly once (the timed-out fetch is retried,
     * not lost), {@code swath.aimd.votes} stays ZERO (an attempt-timeout is not store
     * backpressure), while {@code swath.aimd.timeout_shed > 0} and {@code effectiveT < Tmax}. A
     * mutant that cast a vote on the timeout FAILS the {@code votes==0} assertion; a
     * mutant that deleted the shed FAILS the {@code timeout_shed>0}/{@code effectiveT<Tmax} pair.
     */
    @Tag("deep")   // latency-injecting: retries an attempt-timeout tail through the real backoff
    @Test
    @Timeout(90)
    void attemptTimeoutStarvedBurst_isRetried_castsNoAimdVote_shedsViaShedNotVote(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = keyspace(1500);
        int workers = 6;

        RunResult r = runEngine(keyspace, workers, 24, dir.resolve("timeout.sqlite"),
                new KindThrowingInterceptor(5, ThrottleException.Kind.ATTEMPT_TIMEOUT), false);

        EngineHarness.assertExactlyOnce(r.emitted(), keyspace);
        assertThat(aimdVotes(r.metrics()))
                .as("attempt-timeouts must cast ZERO aimd votes")
                .isEqualTo(0.0);
        assertThat(aimdTimeoutShed(r.metrics()))
                .as("a starved timeout burst sheds T via the SHED meter, not a vote")
                .isGreaterThan(0.0);
        assertThat(r.gauge().effectiveT())
                .as("the shed reduced T below Tmax — the adaptive path down, via the shed not a vote")
                .isLessThan(workers);
    }

    /**
     * TIMEOUT TAIL COEXISTING WITH PROGRESS — the OTHER axis of the two-dimensional invariant. A
     * sparse ATTEMPT_TIMEOUT is interleaved among clean fetches (every {@code period}-th call), so
     * real pages keep committing and the shed's STARVATION gate ({@code successes <=
     * max(1, floor(T/32))}) is never satisfied — the timeout storm coexists with progress. This must
     * NEVER shed: the run completes exactly once, {@code swath.aimd.votes == 0}, {@code
     * swath.aimd.timeout_shed == 0} (the shed never engaged — progress cleared the gate), and
     * {@code effectiveT} stays at Tmax.
     */
    @Tag("deep")   // latency-injecting: retries an interleaved attempt-timeout tail through the real backoff
    @Test
    @Timeout(90)
    void attemptTimeoutTailCoexistingWithProgress_neverSheds_keepsFullConcurrency(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = keyspace(1500);
        int workers = 6;

        RunResult r = runEngine(keyspace, workers, 24, dir.resolve("interleaved.sqlite"),
                new InterleavedTimeoutInterceptor(8), false);

        EngineHarness.assertExactlyOnce(r.emitted(), keyspace);
        assertThat(aimdVotes(r.metrics()))
                .as("attempt-timeouts must cast ZERO aimd votes")
                .isEqualTo(0.0);
        assertThat(aimdTimeoutShed(r.metrics()))
                .as("a timeout tail coexisting with real progress clears the starvation gate — NEVER sheds")
                .isEqualTo(0.0);
        // Slow-start: T starts below Tmax and ramps up from SLOW_START_INITIAL_T. The invariant here
        // is that coexisting progress NEVER SHEDS (asserted above) — so T only ever grows and stays
        // at/above the slow-start seed, up to Tmax. The exact end value is timing-dependent on how far
        // the paced doubling ramped within the run, so it is a range, not == Tmax.
        assertThat(r.gauge().effectiveT())
                .as("coexisting progress never sheds — T stays between the slow-start seed and Tmax")
                .isBetween(ConcurrencyGauge.SLOW_START_INITIAL_T, workers);
    }

    /**
     * Contrast, the non-bound for real 503s: a 503 storm of {@code 12} (> the transient cap of 8)
     * then clean must still COMPLETE — real backpressure is retried UNBOUNDED (AIMD paces it,
     * cancellation bounds it), unlike attempt-timeouts. And it DOES vote T down. A mutant that
     * count-bounded 503s like the transients would abort here → FAIL.
     */
    @Tag("deep")   // latency-injecting: a 12-deep 503 storm retried (unbounded) through real backoff
    @Test
    @Timeout(90)
    void realSlowdownStormPastTheTransientCap_stillCompletes_andVotesTdown(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = keyspace(1200);
        int workers = 6;

        RunResult r = runEngine(keyspace, workers, 24, dir.resolve("slowdown.sqlite"),
                new KindThrowingInterceptor(12, ThrottleException.Kind.SLOWDOWN), false);

        EngineHarness.assertExactlyOnce(r.emitted(), keyspace);
        assertThat(aimdVotes(r.metrics()))
                .as("a real 503 storm votes AIMD down (unlike attempt-timeouts)")
                .isGreaterThan(0.0);
        assertThat(r.gauge().effectiveT())
                .as("real 503s collapse T below Tmax (12 throttles >> the 3 steps to the floor)")
                .isLessThan(workers);
    }

    // ---- C: an over-cap attempt-timeout storm RIDES OUT and completes, never cancelled -----------

    /**
     * Storm ride-out: an over-cap attempt-timeout storm on one fetch crosses {@code
     * MAX_TRANSIENT_RETRIES} but, with a token wired, is NOT cancelled — a never-healing storm's
     * death is owned SOLELY by the {@code LivenessWatchdog} (guarded by the storm/watchdog guard, not
     * here). Crossing the cap only engages ride-out (a raised backoff ceiling + a {@code
     * storm_ride_out} engagement counter); this bounded storm then clears and the run COMPLETES
     * exactly once, never cancelled, casting no aimd vote. A mutant that re-introduced a cap→cancel
     * would FAIL the not-cancelled / completes assertions.
     */
    @Tag("deep")   // latency-injecting: rides the real backoff past the cap (raised 15 s ceiling once)
    @Test
    @Timeout(90)
    void overCapAttemptTimeout_ridesOut_completesNotCancelled_castsNoAimdVote(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = keyspace(800);
        int workers = 4;
        Path ckpt = dir.resolve("ride-out.sqlite");

        // Throw 10 times (> the cap of 8) on the leading fetches, crossing into ride-out, then serve.
        // Ride-out is the behavior UNDER TEST here, so thread an EXPLICIT RIDE_OUT config (with the
        // real backoff sleeper this @deep test deliberately rides) — the implicit RetryConfig.DEFAULT is
        // BOUNDED (never an owner-less infinite ride-out), which would cap→cancel instead.
        RunResult r = runEngine(keyspace, workers, 24, ckpt,
                new KindThrowingInterceptor(10, ThrottleException.Kind.ATTEMPT_TIMEOUT), false,
                new RetryConfig(RetryPolicy.RIDE_OUT, TransientRetryFetcher.DEFAULT_SLEEPER));

        EngineHarness.assertExactlyOnce(r.emitted(), keyspace);
        assertThat(r.token().isCancelled())
                .as("ride-out never cancels the run (the watchdog owns a never-healing storm's death)")
                .isFalse();
        assertThat(r.metrics().registry().find("swath.steal_reason")
                .tag("outcome", "TRANSIENT").tag("reason", "storm_ride_out").counter())
                .as("the over-cap retries recorded a storm_ride_out engagement counter")
                .isNotNull();
        assertThat(aimdVotes(r.metrics()))
                .as("attempt-timeouts cast NO aimd vote even when they ride out past the cap")
                .isEqualTo(0.0);
    }

    // ---- C2: no watchdog armed → the BOUNDED cap still aborts RESUMABLY-STUCK ----------------------

    /**
     * The never-heals guard under {@link RetryPolicy#BOUNDED}, reachable only when NO watchdog is
     * armed to own storm death: every fetch persistently throws ATTEMPT_TIMEOUT and the engine's
     * {@code GaugedFetcher} runs BOUNDED, so cap exhaustion must NOT ride out forever (there is no
     * backstop) — it cancels the run resumably {@code STUCK}, attributing {@link
     * CancelSource#TRANSIENT_RETRY_CAP}, and unwinds as a {@link CancelledException} (never a fatal
     * {@link ListingException} that would poison {@code --resume}), leaving a resumable {@code
     * RUNNING} checkpoint. Casts no aimd vote. A no-op sleeper keeps it fast. A mutant that ignored
     * the BOUNDED policy (rode out unconditionally) would never finish → the {@code @Timeout} FAILS
     * it; one that let the cap escape fatally would FAIL the Cancelled/STUCK assertions.
     */
    @Test
    @Timeout(60)
    void persistentAttemptTimeout_boundedPolicy_abortsResumablyStuck_castsNoAimdVote(@TempDir Path dir)
            throws Exception {
        List<byte[]> keyspace = keyspace(800);
        int workers = 4;
        Path ckpt = dir.resolve("bounded-stuck.sqlite");

        RunResult r = runEngine(keyspace, workers, 24, ckpt,
                new KindThrowingInterceptor(Integer.MAX_VALUE, ThrottleException.Kind.ATTEMPT_TIMEOUT), true,
                new RetryConfig(RetryPolicy.BOUNDED, ms -> { }));

        assertThat(r.error())
                .as("BOUNDED cap exhaustion aborts the run (bounded), never an infinite ride-out")
                .isNotNull();
        assertThat(hasCause(r.error(), CancelledException.class))
                .as("cap exhaustion unwinds as a resumable CancelledException, not a fatal error")
                .isTrue();
        assertThat(hasCause(r.error(), ListingException.class))
                .as("it must NOT propagate as the fatal ListingException contract (poison)")
                .isFalse();
        assertThat(r.token().stopReason())
                .as("cap exhaustion attributes the STUCK (exit-75/tempfail) disposition")
                .isEqualTo(StopReason.STUCK);
        assertThat(r.token().source())
                .as("the retry cap is named as the cancel source")
                .isEqualTo(CancelSource.TRANSIENT_RETRY_CAP);
        assertThat(aimdVotes(r.metrics()))
                .as("attempt-timeouts cast NO aimd vote even when the BOUNDED cap fires")
                .isEqualTo(0.0);

        // The checkpoint left behind is VALID and RESUMABLE — reopening resumes a plain RUNNING run.
        try (SqliteCheckpointStore reopened = SqliteCheckpointStore.open(ckpt)) {
            RunMeta resumed = reopened.openRun(key(), true, false);
            assertThat(resumed.status())
                    .as("a BOUNDED stuck cap-exhaustion leaves the run RUNNING, not FAILED")
                    .isEqualTo(RunStatus.RUNNING);
            assertThat(resumed.fatalError())
                    .as("never the fatal_error flag that refuses --resume").isFalse();
        }
    }

    // ---- harness ---------------------------------------------------------------------------------

    private record RunResult(List<byte[]> emitted, ConcurrencyGauge gauge, RunMetrics metrics,
                             Throwable error, CancellationToken token) {
    }

    private static List<byte[]> keyspace(int n) {
        List<byte[]> keyspace = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keyspace.add(b(String.format("data/%05d", i)));
        }
        return keyspace;
    }

    /**
     * Run one full {@link WorkStealingScan} on a separate thread, awaited with a BOUNDED Awaitility
     * timeout so a livelock/deadlock fails the test at the bound rather than hanging the suite.
     * When {@code expectFailure} is true a producer error is captured and returned (not re-raised).
     */
    private static RunResult runEngine(List<byte[]> keyspace, int workers, int maxKeys, Path ckpt,
                                       MockPageFetcher.PageInterceptor interceptor, boolean expectFailure)
            throws Exception {
        // RetryConfig.DEFAULT is BOUNDED (never an owner-less infinite ride-out). The tests using this
        // short overload inject only FINITE transient bursts (resolved within the cap) or 503 storms
        // (retried unbounded regardless of policy), so BOUNDED vs RIDE_OUT is immaterial to them; the
        // one over-cap ride-out test threads an explicit RIDE_OUT config.
        return runEngine(keyspace, workers, maxKeys, ckpt, interceptor, expectFailure, RetryConfig.DEFAULT);
    }

    /** Overload that threads a {@link RetryConfig} (BOUNDED policy + no-op sleeper) into the engine. */
    private static RunResult runEngine(List<byte[]> keyspace, int workers, int maxKeys, Path ckpt,
                                       MockPageFetcher.PageInterceptor interceptor, boolean expectFailure,
                                       RetryConfig retryConfig)
            throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).interceptor(interceptor).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(ckpt)) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withRetryConfig(retryConfig),
                    fetcher, store, workers, maxKeys, seeds, FilterChain.EMPTY);

            List<byte[]> emitted = Collections.synchronizedList(new ArrayList<>());
            RunContext ctx = RunContext.create();
            Runnable pipeline = () -> {
                try {
                    PipelineDrain.collectKeys(1000, ctx, engine, emitted);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };

            ExecutorService exec = Executors.newSingleThreadExecutor();
            AtomicReference<Throwable> err = new AtomicReference<>();
            try {
                Future<?> f = exec.submit(() -> {
                    try {
                        pipeline.run();
                    } catch (Throwable t) {
                        err.set(t);
                    }
                });
                await().atMost(Duration.ofSeconds(60)).until(f::isDone);
            } finally {
                exec.shutdownNow();
            }
            if (!expectFailure && err.get() != null) {
                throw new AssertionError("engine run failed unexpectedly", err.get());
            }
            return new RunResult(emitted, engine.gauge(), metrics, err.get(), ctx.cancellation());
        }
    }

    private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (type.isInstance(c)) {
                return true;
            }
            if (c.getCause() == c) {
                break;
            }
        }
        return false;
    }

    /**
     * Throws a {@link ThrottleException} of the given {@code kind} (the real production shape — a
     * transient surfaced after the SDK's retries, delivering NO keys) for the first {@code throwCalls}
     * fetches, then passes pages through cleanly. {@code throwCalls == Integer.MAX_VALUE} models a
     * persistently-wedged endpoint.
     */
    private record KindThrowingInterceptor(int throwCalls, ThrottleException.Kind kind)
            implements MockPageFetcher.PageInterceptor {
        @Override
        public ListPage intercept(PageRequest req, int callIndex, ListPage computed) throws ThrottleException {
            if (callIndex < throwCalls) {
                throw ThrottleException.classifiedTransient("injected " + kind + " (call " + callIndex + ")", kind);
            }
            return computed;
        }
    }

    /**
     * Throws an ATTEMPT_TIMEOUT on every {@code period}-th fetch (callIndex % period == period-1) and
     * passes every other page cleanly — a SPARSE timeout tail that COEXISTS WITH real page progress.
     * Unlike {@link KindThrowingInterceptor} (which throws a leading BURST before any success), this
     * interleaves timeouts among successes so the shed's starvation gate stays cleared and T holds at
     * Tmax. Each timed-out fetch is retried on the next global {@code callIndex}, which is almost always
     * a clean one, so the run still completes (retries do not exhaust the transient cap).
     */
    private record InterleavedTimeoutInterceptor(int period) implements MockPageFetcher.PageInterceptor {
        @Override
        public ListPage intercept(PageRequest req, int callIndex, ListPage computed) throws ThrottleException {
            if (callIndex % period == period - 1) {
                throw ThrottleException.attemptTimeout("injected interleaved ATTEMPT_TIMEOUT (call " + callIndex + ")");
            }
            return computed;
        }
    }
}
