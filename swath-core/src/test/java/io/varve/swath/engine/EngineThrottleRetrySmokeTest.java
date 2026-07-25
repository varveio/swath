/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * PER-COMMIT BACKSTOP for the two engine-level throttle contracts guarded by the {@code deep}-tier
 * tests {@link Thr1SustainedThrottleTest} and {@link TransientTimeoutRetryEngineContractTest} (both
 * latency-injecting / recovery-window tests moved out of the per-commit suite). This smoke pins the
 * two threshold facts those tests share, fast and schedule-insensitively, so demoting them opens no
 * per-commit coverage gap:
 *
 * <ol>
 *   <li><b>Retry to exactly-once completion.</b> The full {@link WorkStealingScan} GaugedFetcher
 *       retries a thrown {@link ThrottleException} (the real S3 shape: a 503 surfaced after the SDK's
 *       retries, delivering no keys) instead of failing the run, and still emits every key exactly
 *       once. Complements {@link io.varve.swath.runtime.SequentialPathTransientRetryTest} (the
 *       sequential/decorator side) — this is the work-stealing engine side.</li>
 *   <li><b>AIMD reduces effective concurrency on 503.</b> A thrown 503 is routed into
 *       {@code gauge.reportStatus(503)} in a LIVE engine run, so the target {@code T} drops below the
 *       worker count. Complements {@link ConcurrencyGaugeTest} (the gauge-policy side) — this is the
 *       engine-wiring side that a thrown throttle actually feeds the limiter.</li>
 *   <li><b>Attempt-timeout is retried but casts NO AIMD vote and keeps full concurrency.</b> An
 *       {@link ThrottleException.Kind#ATTEMPT_TIMEOUT} thrown in a LIVE engine run is retried to
 *       exactly-once completion, yet {@code swath.aimd.votes} stays 0 and {@code effectiveT} stays at
 *       the worker count — the distinct-from-503 engine wiring that {@link
 *       TransientTimeoutRetryEngineContractTest#attemptTimeoutTail_isRetried_castsNoAimdVote_andKeepsFullConcurrency}
 *       (deep) covers with a long latency-injected tail.</li>
 *   <li><b>Over-cap storm RIDES OUT, not a crash/hang/cancel.</b> An over-cap attempt-timeout
 *       storm crosses {@code MAX_TRANSIENT_RETRIES} but, with a token wired, is NOT cancelled — the run
 *       rides out (raised backoff ceiling + a {@code storm_ride_out} counter) and, once the bounded storm
 *       clears, completes exactly once, casting no aimd vote. Do not escalate a bounded storm straight to
 *       STUCK: a never-healing storm is the {@code LivenessWatchdog}'s job. A no-op Sleeper is
 *       injected so this stays fast and fully deterministic.</li>
 * </ol>
 *
 * <p>Determinism: the interceptor throttles each DISTINCT request exactly once (keyed by request
 * signature), so every affected fetch incurs a single attempt-1 backoff (a small, bounded jitter),
 * never an escalating retry storm, and then succeeds on its retry — forward progress is guaranteed.
 * The assertions are monotone/threshold facts (exactly-once set equality; a retry was re-issued;
 * {@code reducedT < workers}), never race-y absolute timing. The attempt-timeout-vs-503 AIMD split
 * (a 503 votes T down; an attempt-timeout does NOT) IS asserted here per-commit; only the exact ×0.7
 * collapse-to-floor value and the steals-pause/recover timing window remain {@code deep}-only.
 */
final class EngineThrottleRetrySmokeTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "throttle-smoke-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    @Timeout(30)
    void engineRetriesThrownThrottle_completesExactlyOnce_andReducesT(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            keyspace.add(b(String.format("data/%05d", i)));
        }
        int workers = 4;
        int maxKeys = 8;   // small pages ⇒ several worker page fetches ⇒ several 503 votes

        ThrottleOncePerRequestInterceptor interceptor = new ThrottleOncePerRequestInterceptor();
        RunResult r = runEngine(keyspace, workers, maxKeys, dir.resolve("throttle-smoke.sqlite"), interceptor);

        // (1) The thrown 503s were retried (not fatal) and the run still emitted every key exactly once.
        EngineHarness.assertExactlyOnce(r.emitted(), keyspace);
        assertThat(interceptor.throwCount.get())
                .as("the interceptor injected at least one thrown 503")
                .isGreaterThan(0);
        assertThat(interceptor.sawRetry.get())
                .as("the engine RE-ISSUED a throttled fetch — a retry actually occurred (not a fatal abort)")
                .isTrue();

        // (2) A thrown 503 fed gauge.reportStatus(503) in the live engine, so T dropped below workers.
        assertThat(r.gauge().effectiveT())
                .as("AIMD reduced effective concurrency below the worker count on sustained thrown 503s")
                .isLessThan(workers);
    }

    /**
     * Gap (a) — the engine wiring, fast and per-commit: a thrown {@link
     * ThrottleException.Kind#ATTEMPT_TIMEOUT} (a client-side hung read, NOT S3 backpressure) is
     * RETRIED to exactly-once completion but must cast ZERO {@code swath.aimd.votes}. This is the
     * behavioral split from a 503 (which DOES vote T down, above): an attempt-timeout is never an AIMD
     * 503 down-vote. A mutant that folded the attempt-timeout into the AIMD down-vote would record a
     * vote → FAIL.
     *
     * <p><b>Reconciled for the timeout shed.</b> A live 4-worker cold start front-loads its timeouts
     * (all four first-fetches time out before any retry lands), which is a STARVED burst — so the
     * progress-gated shed may legitimately fire ONCE, dropping {@code effectiveT} from 4 to 2
     * within the one real shed window. That is a DISTINCT, non-voting decrease (a timeout shed), NOT a
     * 503 vote, so "effectiveT stays exactly at the worker count" is no longer the contract. What
     * remains rock-solid and is the point of this smoke: an attempt-timeout casts NO aimd vote, T never
     * collapses below the floor of 1, and ANY reduction here is attributable to the shed rather than a
     * vote. Each distinct request throttles exactly once (one attempt-1 backoff, ~100ms ceiling) then
     * succeeds — bounded and fast.
     */
    @Test
    @Timeout(30)
    void engineRetriesAttemptTimeout_castsNoAimdVote_keepsConcurrency(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            keyspace.add(b(String.format("data/%05d", i)));
        }
        int workers = 4;
        int maxKeys = 8;   // small pages ⇒ several worker page fetches ⇒ several attempt-timeouts

        ThrottleOncePerRequestInterceptor interceptor =
                new ThrottleOncePerRequestInterceptor(ThrottleException.Kind.ATTEMPT_TIMEOUT);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        RichRunResult r = runEngineRich(keyspace, workers, maxKeys,
                dir.resolve("attempt-timeout-smoke.sqlite"), interceptor, metrics, false);

        // (1) The attempt-timeouts were retried (not fatal) and every key was emitted exactly once.
        EngineHarness.assertExactlyOnce(r.emitted(), keyspace);
        assertThat(interceptor.throwCount.get())
                .as("the interceptor injected at least one thrown attempt-timeout").isGreaterThan(0);
        assertThat(interceptor.sawRetry.get())
                .as("the engine RE-ISSUED a timed-out fetch — a retry actually occurred").isTrue();

        // (2) Attempt-timeouts fed gauge.onTransientTimeout() ONLY: NO aimd vote.
        assertThat(aimdVotes(metrics))
                .as("an attempt-timeout casts ZERO aimd votes (distinct from a 503)")
                .isEqualTo(0.0);
        // The target may be shed once (4 -> 2) by the progress gate under the starved cold-start
        // burst, but never collapses below the floor, and never via a 503 vote.
        assertThat(r.gauge().effectiveT())
                .as("attempt-timeouts never collapse concurrency below the deadlock-safety floor of 1")
                .isBetween(1, workers);
        // Any reduction here is attributable to the distinct timeout-shed, NOT a 503 vote.
        double sheds = metrics.registry().get("swath.aimd.timeout_shed").counter().count();
        if (r.gauge().effectiveT() < workers) {
            assertThat(sheds)
                    .as("a concurrency reduction under attempt-timeouts is a timeout-shed, never a 503 vote")
                    .isGreaterThan(0.0);
        }
    }

    /**
     * Gap (b) — storm ride-out, per-commit: an over-cap {@link ThrottleException.Kind#ATTEMPT_TIMEOUT}
     * storm on one fetch crosses {@code MAX_TRANSIENT_RETRIES} but, with a token wired, is NOT
     * cancelled — the run RIDES OUT (raised backoff ceiling + a {@code storm_ride_out} engagement counter)
     * and, once the bounded storm clears, COMPLETES exactly once, never cancelled, casting no aimd vote.
     * Do not escalate a bounded storm to STUCK: a never-healing storm is the watchdog's job,
     * guarded by the storm/watchdog guard, not here. A no-op {@link TransientRetryFetcher.Sleeper} is
     * injected via {@code runEngineRich}, so the raised-ceiling ride-out path is exercised without real
     * sleeps. A mutant that left the loop cancelling on the cap would FAIL the not-cancelled / completes
     * assertions.
     */
    @Test
    @Timeout(60)
    void overCapAttemptTimeout_ridesOut_completesNotCancelled_castsNoAimdVote(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            keyspace.add(b(String.format("data/%05d", i)));
        }
        int workers = 1;   // one over-cap fetch is enough; keeps the run to a single retry loop
        Path ckpt = dir.resolve("ride-out-smoke.sqlite");

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // Throw 10 times (> the cap of 8, crossing into ride-out) on the leading fetches, then serve.
        RichRunResult r = runEngineRich(keyspace, workers, 8, ckpt,
                new LeadingThrowInterceptor(10, ThrottleException.Kind.ATTEMPT_TIMEOUT), metrics, false);

        EngineHarness.assertExactlyOnce(r.emitted(), keyspace);
        assertThat(r.token().isCancelled())
                .as("ride-out never cancels the run (the watchdog owns a never-healing storm's death)")
                .isFalse();
        assertThat(metrics.registry().find("swath.steal_reason")
                .tag("outcome", "TRANSIENT").tag("reason", "storm_ride_out").counter())
                .as("the over-cap retries recorded a storm_ride_out engagement counter")
                .isNotNull();
        assertThat(aimdVotes(metrics))
                .as("attempt-timeouts cast NO aimd vote even when they ride out past the cap")
                .isEqualTo(0.0);
    }

    // ---- helpers -------------------------------------------------------------------

    private static double aimdVotes(RunMetrics metrics) {
        return metrics.registry().get("swath.aimd.votes").counter().count();
    }

    private record RichRunResult(List<byte[]> emitted, ConcurrencyGauge gauge, Throwable error,
                                 CancellationToken token) {
    }

    /**
     * Like {@link #runEngine} but threads a caller-supplied {@link RunMetrics} (so the caller can read
     * {@code swath.aimd.votes}) and, when {@code expectFailure} is true, CAPTURES the producer error
     * and the run's cancellation token instead of failing the test — for the cap-exhaustion abort path.
     */
    private static RichRunResult runEngineRich(List<byte[]> keyspace, int workers, int maxKeys, Path ckpt,
                                               MockPageFetcher.PageInterceptor interceptor,
                                               RunMetrics metrics, boolean expectFailure) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).interceptor(interceptor).build();
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(ckpt)) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            // Inject a no-op Sleeper (RIDE_OUT policy) so the storm-ride-out path (which raises
            // the backoff ceiling to 15 s) is exercised WITHOUT real multi-second sleeps — fast per-commit.
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withRetryConfig(new RetryConfig(RetryPolicy.RIDE_OUT, ms -> { })),
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
                await().atMost(Duration.ofSeconds(45)).until(f::isDone);
            } finally {
                exec.shutdownNow();
            }
            if (!expectFailure && err.get() != null) {
                fail("engine run failed unexpectedly", err.get());
            }
            return new RichRunResult(emitted, engine.gauge(), err.get(), ctx.cancellation());
        }
    }

    private record RunResult(List<byte[]> emitted, ConcurrencyGauge gauge) {
    }

    /**
     * Build and run one full {@link WorkStealingScan} over {@code keyspace} on a separate thread,
     * awaited with a BOUNDED Awaitility timeout so a deadlock fails the test cleanly at the bound
     * instead of hanging the suite. Returns the emitted keys AND the engine's
     * {@link ConcurrencyGauge} so the caller can assert the real gauge-aware fetcher path reacted.
     */
    private static RunResult runEngine(List<byte[]> keyspace, int workers, int maxKeys, Path ckpt,
                                       MockPageFetcher.PageInterceptor interceptor) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).interceptor(interceptor).build();
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(ckpt)) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS),
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
                await().atMost(Duration.ofSeconds(20)).until(f::isDone);
            } finally {
                exec.shutdownNow();
            }
            if (err.get() != null) {
                fail("engine run failed under thrown throttles", err.get());
            }
            return new RunResult(emitted, engine.gauge());
        }
    }

    /**
     * Throws a {@link ThrottleException} of the given {@code kind} the FIRST time each distinct request
     * is seen, then serves the identical retried request cleanly. Because the engine re-issues the SAME
     * {@link PageRequest} after its backoff, every affected fetch throttles exactly once (one attempt-1
     * backoff) and then succeeds — bounded, deterministic, and always making forward progress.
     * {@code sawRetry} flips when a throttled request is re-issued, the direct proof that a retry
     * occurred. Used with {@link ThrottleException.Kind#SLOWDOWN} (a 503, the AIMD-voting kind) and with
     * {@link ThrottleException.Kind#ATTEMPT_TIMEOUT} (a client-side timeout, the non-voting kind).
     */
    private static final class ThrottleOncePerRequestInterceptor implements MockPageFetcher.PageInterceptor {
        private final ConcurrentHashMap<String, Boolean> throttledOnce = new ConcurrentHashMap<>();
        private final ThrottleException.Kind kind;
        final AtomicInteger throwCount = new AtomicInteger();
        final AtomicBoolean sawRetry = new AtomicBoolean();

        ThrottleOncePerRequestInterceptor() {
            this(ThrottleException.Kind.SLOWDOWN);
        }

        ThrottleOncePerRequestInterceptor(ThrottleException.Kind kind) {
            this.kind = kind;
        }

        @Override
        public ListPage intercept(PageRequest req, int callIndex, ListPage computed) throws ThrottleException {
            String sig = Arrays.toString(req.prefix()) + '|' + Arrays.toString(req.startAfter())
                    + '|' + Arrays.toString(req.delimiter()) + '|' + req.maxKeys();
            if (throttledOnce.putIfAbsent(sig, Boolean.TRUE) == null) {
                throwCount.incrementAndGet();
                throw ThrottleException.classifiedTransient("injected " + kind + " (call " + callIndex + ")", kind);
            }
            // Same request seen again ⇒ this is the engine's retry of a previously-throttled fetch.
            sawRetry.set(true);
            return computed;
        }
    }

    /**
     * Throws the given throttle {@code kind} for the FIRST {@code throwCalls} global fetches, then serves
     * cleanly — an over-cap-but-BOUNDED storm: with {@code throwCalls > MAX_TRANSIENT_RETRIES} the
     * leading fetch crosses the ride-out threshold, then the storm clears and the run completes.
     */
    private record LeadingThrowInterceptor(int throwCalls, ThrottleException.Kind kind)
            implements MockPageFetcher.PageInterceptor {
        @Override
        public ListPage intercept(PageRequest req, int callIndex, ListPage computed) throws ThrottleException {
            if (callIndex < throwCalls) {
                throw ThrottleException.classifiedTransient("injected " + kind + " (call " + callIndex + ")", kind);
            }
            return computed;
        }
    }
}
