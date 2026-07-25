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
 * Adversarial guard (§3): {@code GaugedFetcher.fetchPage}'s {@code transientRetries} counter (bounding NON-voting
 * transients — {@link ThrottleException.Kind#votesAimdDown()} false, e.g. {@code ATTEMPT_TIMEOUT}) MUST be
 * reset whenever a VOTING throttle ({@code votesAimdDown()} true, e.g. a real 503 {@code SLOWDOWN}) is
 * handled. The voting path is deliberately UNBOUNDED (THR-1: AIMD paces it, cancellation/max-duration
 * bounds it) — so a bucket that rides sustained 503 backpressure while occasionally drizzling non-
 * consecutive attempt-timeouts must NOT accumulate {@code transientRetries} across those non-consecutive
 * timeouts and needlessly engage {@code storm_ride_out}; that threshold exists only to shape backoff for
 * a RUN of CONSECUTIVE client-side transients (a genuinely wedged read).
 *
 * <p>Crossing the threshold does not cancel the run — it engages storm ride-out; the watchdog owns
 * a never-healing storm's death. The second method here guards ride-out completion, not a cap-STUCK.
 *
 */
final class MixedFaultTransientResetTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "mixed-fault-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static List<byte[]> keyspace(int n) {
        List<byte[]> keyspace = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keyspace.add(b(String.format("data/%05d", i)));
        }
        return keyspace;
    }

    /**
     * Guards the exact boundary of the reset: for the SAME retried request, the script is 8 CONSECUTIVE
     * {@code ATTEMPT_TIMEOUT}s (the non-voting cap is {@code MAX_TRANSIENT_RETRIES == 8}; 8 must NOT
     * trip it — the check is {@code > 8}), then ONE voting {@code SLOWDOWN}, then ONE more
     * {@code ATTEMPT_TIMEOUT}, then success. Without the reset, {@code transientRetries} would carry
     * across the intervening 503 and reach 8+1=9 on the final timeout, needlessly crossing the ride-out
     * threshold. With the reset, the 503 resets the counter to 0, so the final timeout only reaches 1
     * and the fetch retries through to success — the run completes and every key is emitted.
     */
    @Tag("deep")   // latency-injecting: rides the real backoff through 10 retries on one request
    @Test
    @Timeout(60)
    void interveningVotingThrottle_resetsNonVotingCounter_soRunCompletes(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = keyspace(200);
        int workers = 1;

        MixedScriptInterceptor interceptor = new MixedScriptInterceptor(
                ThrottleException.Kind.ATTEMPT_TIMEOUT, TransientRetryFetcher.MAX_TRANSIENT_RETRIES,
                ThrottleException.Kind.SLOWDOWN, 1,
                ThrottleException.Kind.ATTEMPT_TIMEOUT, 1);

        RunResult r = runEngine(keyspace, workers, 24, dir.resolve("mixed-fault.sqlite"), interceptor, false);

        assertThat(r.error())
                .as("a voting 503 between non-consecutive attempt-timeouts must reset the "
                        + "non-voting cap, so this mixed-fault sequence must NOT trip STUCK")
                .isNull();
        EngineHarness.assertExactlyOnce(r.emitted(), keyspace);
    }

    /**
     * Storm ride-out: {@code MAX_TRANSIENT_RETRIES + 1} CONSECUTIVE non-voting {@code ATTEMPT_TIMEOUT}s
     * on one fetch cross the cap, but with a token wired the run is NOT cancelled — a never-healing
     * storm's death is owned by the {@code LivenessWatchdog}, not this loop. Crossing the cap only
     * engages ride-out (a raised backoff ceiling + a {@code storm_ride_out} engagement counter); this
     * bounded storm then clears and the run COMPLETES exactly once, never cancelled. (The never-heals
     * → watchdog-STUCK disposition is guarded by the storm/watchdog guard, not here.)
     */
    @Tag("deep")   // latency-injecting: rides the real backoff through MAX_TRANSIENT_RETRIES+1 retries
    @Test
    @Timeout(60)
    void consecutiveNonVotingTransients_pastTheCap_rideOut_completeNotCancelled(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = keyspace(200);
        int workers = 1;

        MixedScriptInterceptor interceptor = new MixedScriptInterceptor(
                ThrottleException.Kind.ATTEMPT_TIMEOUT, TransientRetryFetcher.MAX_TRANSIENT_RETRIES + 1);

        RunResult r = runEngine(keyspace, workers, 24, dir.resolve("regression-consecutive.sqlite"),
                interceptor, false);

        assertThat(r.error())
                .as("crossing the cap rides out (run not cancelled); the bounded storm clears and completes")
                .isNull();
        EngineHarness.assertExactlyOnce(r.emitted(), keyspace);
        assertThat(r.token().isCancelled())
                .as("ride-out never cancels the run — the watchdog owns a never-healing storm's death")
                .isFalse();
        assertThat(r.metrics().registry().find("swath.steal_reason")
                .tag("outcome", "TRANSIENT").tag("reason", "storm_ride_out").counter())
                .as("the over-cap retry recorded a storm_ride_out engagement counter")
                .isNotNull();
    }

    // ---- harness ---------------------------------------------------------------------------------

    private record RunResult(List<byte[]> emitted, Throwable error,
                             CancellationToken token, RunMetrics metrics) {
    }

    private static RunResult runEngine(List<byte[]> keyspace, int workers, int maxKeys, Path ckpt,
                                       MockPageFetcher.PageInterceptor interceptor, boolean expectFailure)
            throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).interceptor(interceptor).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(ckpt)) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            // RetryConfig.DEFAULT is BOUNDED (never an owner-less infinite ride-out). Both tests
            // here assert RIDE_OUT behavior (one rides out past the cap; the other's reset keeps it under
            // the cap either way), so this threads an EXPLICIT RIDE_OUT config with the real backoff
            // sleeper these @deep tests deliberately ride.
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withRetryConfig(new RetryConfig(RetryPolicy.RIDE_OUT, TransientRetryFetcher.DEFAULT_SLEEPER)),
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
                await().atMost(Duration.ofSeconds(50)).until(f::isDone);
            } finally {
                exec.shutdownNow();
            }
            if (!expectFailure && err.get() != null) {
                throw new AssertionError("engine run failed unexpectedly", err.get());
            }
            return new RunResult(emitted, err.get(), ctx.cancellation(), metrics);
        }
    }

    /**
     * Throws a fixed script of {@code (kind, count)} pairs, in order, keyed by the fetcher's GLOBAL
     * {@code callIndex} — the first {@code script.size()} calls to the fetcher (across the whole run)
     * follow the script; every call after that succeeds (returns the computed page). With a single
     * worker (no concurrent thief probes racing the sole in-flight request — the worker thread is
     * either fetching or stealing, never both at once) this lands on the SAME retried request for the
     * whole script, exactly modeling one fetch's {@code GaugedFetcher} retry loop end to end (mirrors
     * {@code KindThrowingInterceptor} in {@link TransientTimeoutRetryEngineContractTest}, generalized from a
     * single kind to a mixed script).
     */
    private static final class MixedScriptInterceptor implements MockPageFetcher.PageInterceptor {
        private final List<ThrottleException.Kind> script;

        /** Varargs of alternating (Kind, count) pairs, e.g. (TIMEOUT, 8, SLOWDOWN, 1, TIMEOUT, 1). */
        MixedScriptInterceptor(Object... kindCountPairs) {
            List<ThrottleException.Kind> flattened = new ArrayList<>();
            for (int i = 0; i < kindCountPairs.length; i += 2) {
                ThrottleException.Kind kind = (ThrottleException.Kind) kindCountPairs[i];
                int count = (Integer) kindCountPairs[i + 1];
                for (int j = 0; j < count; j++) {
                    flattened.add(kind);
                }
            }
            this.script = List.copyOf(flattened);
        }

        @Override
        public ListPage intercept(PageRequest req, int callIndex, ListPage computed) throws ThrottleException {
            if (callIndex < script.size()) {
                ThrottleException.Kind kind = script.get(callIndex);
                throw ThrottleException.classifiedTransient(
                        "injected scripted " + kind + " (call " + callIndex + ")", kind);
            }
            return computed;
        }
    }
}
