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
import io.varve.swath.runtime.RunContext;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.ConcurrencyGauges;
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
 * THR-1: under SUSTAINED {@code 503 SlowDown} the AIMD {@link ConcurrencyGauge}
 * multiplicatively decreases the concurrency target {@code T} (×0.7, floored at 1),
 * pauses new steals, recovers {@code +1} on a clean window, and — the headline liveness
 * guarantee — the full {@link WorkStealingScan} run STILL COMPLETES and lists the whole
 * keyspace exactly once (no deadlock, no livelock, no dropped keys).
 *
 * <p>This guards the engine's THROTTLE WIRING, not the gauge's policy in isolation (that
 * lives in {@code ConcurrencyGaugeTest}). Real S3 surfaces a
 * sustained {@code 503 SlowDown} as an {@code SdkException} <b>after</b> the SDK's own
 * retries — i.e. as a {@link ThrottleException} reaching the fetcher, NOT as a successful
 * page carrying {@code httpStatus=503}. So the injection here is the REAL production shape:
 * the {@link MockPageFetcher} interceptor THROWS a {@link ThrottleException} for the first
 * {@code N} calls. The engine's gauge-aware fetcher must catch it, feed
 * {@code reportStatus(503)} into AIMD (collapse {@code T}, pause steals), back off, and retry
 * the SAME fetch so no key is lost. An engine that turned the throttle into a fatal
 * {@code ListingException}, or that only fed the gauge from successful-page status, would
 * fail here (the run aborts, or {@code T} never drops).
 *
 * <p><b>Metrics timers (§6 "three timers" note).</b> This test asserts
 * THR-1's behavioral contract: reduce {@code T}, pause steals, retry, and complete. The
 * network / queue-wait / rate-limit timer assertions belong to the observability slice
 * where those timers and the {@code MeterRegistry} exist; the timer requirement itself is
 * unchanged.
 */
// Tagged at METHOD granularity, NOT class-level. The gauge-level assertion
// (sustainedThrottle_pausesStealing_thenRecovers) is FAST and deterministic (pure ConcurrencyGauge
// calls via the forceCleanWindow seam — no real sleep), so it stays per-commit. Only the full-engine
// method (fullEngine_realThrownThrottle_reducesT_pausesSteals_completes) is `@Tag("deep")` — it
// injects sustained thrown 503s and drives the real GaugedFetcher backoff. Its engine-wiring
// (thrown 503 → AIMD T-drop, retried to exactly-once completion) is pinned per-commit by
// EngineThrottleRetrySmokeTest.
final class Thr1SustainedThrottleTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "thr1-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    // ---- 1. steals pause while throttled, resume on a clean window ------------------

    /**
     * The exact AIMD calls the engine's gauge-aware fetcher issues — {@code reportStatus(503)}
     * on a thrown throttle, {@code reportStatus(200)} on a healthy page — must pause stealing on
     * the first 503 and, after a clean window, recover {@code T} by +1 and re-enable stealing.
     *
     * <p>Adversarial: a gauge that kept {@code isStealingAllowed()==true} under throttle (so the
     * engine keeps spawning probes into a backing-off store) fails; one that never re-enabled
     * stealing / never recovered {@code T} fails the recovery half.
     */
    @Test
    @Timeout(20)
    void sustainedThrottle_pausesStealing_thenRecovers() {
        ConcurrencyGauge gauge = ConcurrencyGauges.of(8);
        assertThat(gauge.isStealingAllowed()).as("stealing on before any throttle").isTrue();

        // Sustained throttle window: every thrown throttle is reported as a 503 → stealing paused.
        for (int i = 0; i < 4; i++) {
            gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);
            assertThat(gauge.isStealingAllowed())
                    .as("stealing stays paused for the whole sustained-503 window").isFalse();
        }
        int tFloor = gauge.effectiveT();
        assertThat(tFloor).isEqualTo(1);

        // Clean window opens, store recovers: the next healthy page is a +1 recovery step and
        // re-enables stealing. forceCleanWindow() is the gauge's own test seam for the 10 s
        // cool-down (waiting it out would violate THR-1's bounded/fast constraint).
        gauge.forceCleanWindow();
        gauge.reportStatus(200);

        assertThat(gauge.effectiveT()).as("+1 recovery on a clean window").isEqualTo(tFloor + 1);
        assertThat(gauge.isStealingAllowed()).as("stealing re-enabled on recovery").isTrue();
    }

    // ---- 2. THE HEADLINE: real thrown-throttle shape drives AIMD; run completes -------

    /**
     * Run the full engine over a modest keyspace where the fetcher THROWS a
     * {@link ThrottleException} for its first {@code N} calls (the real production shape: a 503
     * surfaced after the SDK's retries), then serves cleanly. The run must:
     * <ul>
     *   <li>terminate within a bounded timeout (liveness — the floor {@code T≥1} keeps one
     *       worker draining even while steals are paused);</li>
     *   <li>emit every key exactly once (no dropped / duplicated key — the throttled fetch is
     *       retried, not lost);</li>
     *   <li>have collapsed {@code T} to (and stayed near) the floor via the REAL gauge-aware
     *       fetcher path (proving the thrown throttle fed {@code reportStatus(503)}).</li>
     * </ul>
     * The steals-paused-under-throttle half of THR-1 is pinned deterministically, without real
     * wall-clock timing sensitivity, by {@link #sustainedThrottle_pausesStealing_thenRecovers()}
     * above (see the timing note below for why this method does not also assert it).
     *
     * <p>Adversarial: an engine that turned the throttle into a fatal {@code ListingException}
     * fails the producer (Awaitility surfaces it); one that only fed the gauge from
     * successful-page {@code httpStatus} leaves {@code T == workers} (never collapsed) and fails
     * the floor assertion; a floor underflow to 0 strands every worker on {@code acquireSlot}
     * and the run never completes; dropped/duplicated keys fail the exactly-once check.
     *
     * <p><b>Real wall-clock timing.</b> This test uses REAL wall-clock
     * backoff sleeps (no injected sleeper), so the 7 floor-no-op throttles that follow the 3 real
     * ×0.7 steps (6→4→2→1) can, with real retry/backoff timing, take long enough that the 10 s
     * {@code CLEAN_WINDOW} armed by the LAST REAL decrease elapses before the storm's thrown
     * throttles are exhausted. A floor no-op does not re-arm the cool-down, so
     * once the real-decrease cool-down clears, a subsequent clean success can take one paced +1
     * recovery step before the run finishes — an intended effect of the
     * design (§4.2/§6.2: {@code growth_blocked_cooldown} observed lower), not a wiring regression.
     * {@code onSuccess} unconditionally sets {@code stealingAllowed = true} once past the cool-down
     * ({@code ConcurrencyGauge#onSuccess}), so that
     * same post-storm recovery step also re-enables stealing; the final-state {@code
     * isStealingAllowed() == false} assertion does not hold at the very end of a
     * long run, so it is not asserted here — the pause itself is still fully guarded by the sibling deterministic test above.
     * The {@code effectiveT} assertion below is a bounded range, not an exact floor pin,
     * that still catches the original wiring failure modes (never collapsed — {@code T == workers};
     * underflow — {@code T == 0}, impossible per the RD-C floor invariant) while tolerating the
     * legitimate post-storm recovery.
     */
    @Tag("deep")   // latency-injecting: drives the real GaugedFetcher backoff on sustained thrown 503s
    @Test
    @Timeout(90)
    void fullEngine_realThrownThrottle_reducesT_pausesSteals_completes(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < 1500; i++) {
            keyspace.add(b(String.format("data/%05d", i)));
        }
        int workers = 6;
        int maxKeys = 24;   // small pages ⇒ many pages, workers idle (steals paused under throttle)

        // The first `throttleCalls` fetches THROW a throttle (10 ≫ the 3 ×0.7 steps that floor T from 6),
        // then the store serves cleanly. T must collapse to the floor and stealing must pause;
        // the single live worker then drains the seed to the very end.
        int throttleCalls = 10;
        RunResult throttled = runEngine(keyspace, workers, maxKeys, dir.resolve("throttle.sqlite"),
                new ThrottleThrowingInterceptor(throttleCalls));

        // Headline: terminated within the bound, every key exactly once, no dropped keys.
        EngineHarness.assertExactlyOnce(throttled.emitted(), keyspace);

        // GaugedFetcher seam: T collapsed toward the floor via the REAL gauge-aware fetcher path,
        // driven by the THROWN throttle — not by any test helper. A wiring that ignored the thrown
        // throttle (fatal abort, or success-only reporting) would leave T == workers (6) here; a
        // floor underflow (impossible per the RD-C invariant) would be T == 0. The bound is a
        // range, not an exact floor pin (see the method javadoc timing note): after the
        // post-storm cool-down clears against REAL wall-clock, each additional 1s GROWTH_PACE step
        // before the run drains grants another legitimate +1 — timing-dependent on a loaded 2-core
        // CI runner (the recurring flake class), so the bound pins only the CONTRACT: collapsed
        // well below workers (never 6 = "storm ignored") and never underflowed (never 0).
        assertThat(throttled.gauge().effectiveT())
                .as("T collapsed via the real gauge-aware fetcher under thrown 503s (post-storm paced "
                        + "recovery steps are wall-clock-dependent; the contract is collapsed-not-ignored)")
                .isGreaterThanOrEqualTo(1)
                .isLessThan(6);

        // Structure math (flake-free, NOT timing): the T bound above would pass even if only ONE of
        // the injected throttles reached AIMD (a 6->4 alone lands in [1,6)). Pin that EVERY thrown throttle
        // fed the gauge as a genuine AIMD down-vote: swath.aimd.votes is incremented exactly once per
        // THROTTLE-kind multiplicativeDecrease (ConcurrencyGauge's multiplicativeDecrease calls
        // metrics.recordAimdVote), so the honest
        // real-503 denominator must equal the throttle count the interceptor injects — independent of how
        // many were REAL ×0.7 steps vs floor no-ops, and independent of wall-clock timing.
        assertThat(throttled.metrics().registry().get("swath.aimd.votes").counter().count())
                .as("every one of the %d injected thrown 503s reached AIMD as an honest down-vote "
                        + "(votes == throttles), not just the first that collapsed T", throttleCalls)
                .isEqualTo((double) throttleCalls);
    }

    // ---- helpers -------------------------------------------------------------------

    private record RunResult(List<byte[]> emitted, ConcurrencyGauge gauge, RunMetrics metrics) {
    }

    /**
     * Build and run one full {@link WorkStealingScan} over {@code keyspace} on a separate thread,
     * awaited with a BOUNDED Awaitility timeout so a deadlock fails the test cleanly at the bound
     * instead of hanging the suite. Returns the emitted keys AND the engine's
     * {@link ConcurrencyGauge} (via {@link WorkStealingScan#gauge()}) so callers can assert the
     * REAL gauge-aware fetcher path reacted to the thrown throttle.
     */
    private static RunResult runEngine(List<byte[]> keyspace, int workers, int maxKeys, Path ckpt,
                                       MockPageFetcher.PageInterceptor interceptor)
            throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).interceptor(interceptor).build();
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(ckpt)) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics),
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
                // BOUNDED liveness wait: a deadlocked engine never finishes ⇒ fails here, fast.
                await().atMost(Duration.ofSeconds(60)).until(f::isDone);
            } finally {
                exec.shutdownNow();
            }
            if (err.get() != null) {
                fail("engine run failed under sustained throttle", err.get());
            }
            return new RunResult(emitted, engine.gauge(), metrics);
        }
    }

    /**
     * Injects throttling in the REAL production shape: THROWS a {@link ThrottleException} (a 503
     * {@code SlowDown} surfaced after the SDK's own retries) for the first {@code throttleCalls}
     * fetches, then passes pages through cleanly. The thrown throttle delivers NO keys — exactly
     * how a real 503 behaves — so progress depends on the engine retrying the fetch after AIMD
     * backoff, not on the throttle smuggling entries through.
     */
    private record ThrottleThrowingInterceptor(int throttleCalls) implements MockPageFetcher.PageInterceptor {
        @Override
        public ListPage intercept(PageRequest req, int callIndex, ListPage computed) throws ThrottleException {
            if (callIndex < throttleCalls) {
                throw ThrottleException.slowDown("injected SlowDown (call " + callIndex + ")");
            }
            return computed;
        }
    }
}
