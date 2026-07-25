/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.model.ListEntry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.StopReason;
import io.varve.swath.runtime.CancelSource;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.runtime.LivenessWatchdog;
import io.varve.swath.store.ListPage;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that a tail attempt-timeout burst is RIDDEN OUT under {@link RetryPolicy#RIDE_OUT}
 * (generalizing {@code SeedRideOutHealsContractTest} / {@code SeedRideOutWatchdogStopContractTest}
 * to the composition harness, driven at the engine level).
 *
 * <p><b>Invariant this guards.</b> A tail attempt-timeout burst must be RIDDEN OUT under
 * {@link RetryPolicy#RIDE_OUT}: if it heals, the run completes byte-exact; if it never heals, death is
 * owned SOLELY by the watchdog ({@code stop_source = liveness_watchdog}, resumable STUCK) — and it is
 * NEVER a zero-output cancel (the head ranges already emitted). The per-fetch retry cap must not own
 * the burst's death: it fires long before the {@link LivenessWatchdog} can act, so a cap-owned cancel
 * misattributes the failure and kills a burst that would otherwise have healed just past the cap.
 *
 * <p><b>Ablation proof (always-on, in the spirit of {@code ShapeRegressionCorpusGuardProofTest}).</b>
 * {@link #boundedCapAblationFailsRideOut} runs the SAME healing timeline under the
 * {@link RetryPolicy#BOUNDED} ablation and proves it FAILS the ride-out: it dies via the retry cap
 * ({@code stop_source = transient_retry_cap}) instead of completing — demonstrating the RIDE_OUT
 * composition is load-bearing and this file's assertions are non-vacuous. {@code BOUNDED} is exactly
 * the cap-owns-death disposition described above, still selectable, so the ablation exercises it
 * directly.
 *
 * <p>Deterministic (a fixed {@value #STORM_THROWS}-throw tail burst, far past the
 * {@code MAX_TRANSIENT_RETRIES = 8} cap; no-op backoff sleeper; production gauge clock — no shed is
 * involved, so no compression needed).
 *
 * <p><b>Why the heal threshold ({@value #STORM_THROWS}) sits so far above the cap.</b>
 * {@code transientRetries} is LOCAL to one logical {@code fetchPage} call, and the tail range is
 * <em>not</em> in fact pinned to a single logical fetch: a thief's {@code maxKeys=1} floor probe reads the
 * tail keys UNFAULTED (the interceptor only faults bulk {@code maxKeys>1} reads), so the thief can
 * still split/steal the un-drained tail mid-burst and hand it to a fresh worker fetch whose local retry
 * counter starts back at 0 — CORRECT engine behavior, but it spreads the burst's throws across a bounded
 * handful of logical fetches. Measured, that steal/handoff spread is bounded (~9-18 throws before some ONE
 * fetch finally accumulates 9 consecutive throws and trips the BOUNDED cap) and does NOT scale with the
 * heal threshold. {@value #STORM_THROWS} (&gt;2x that measured spread) makes the cap win decisively and
 * well below the heal point on every schedule, without weakening the proof: it still shows the BOUNDED
 * ablation dies via the retry cap strictly before the burst would have healed.
 */
@Tag("deep")
final class TailTimeoutRideOutTest {

    private static final int HEAD_PREFIXES = 8;
    private static final int PER_PREFIX = 600;      // ~4800 head keys, all emit fast (instant, fault-free)
    private static final int TAIL_KEYS = 5;         // the "zzzz/" sentinel tail the storm concentrates on
    private static final int WORKERS = 16;
    private static final int MAX_KEYS = 1000;
    /**
     * Consecutive tail-read timeouts before the burst heals: {@value} &gt;&gt; the
     * {@code MAX_TRANSIENT_RETRIES = 8} cap. Deliberately &gt;2x the measured bounded steal/handoff spread
     * (~9-18 throws-to-cap) so the BOUNDED ablation's per-fetch cap wins decisively and well below the heal
     * point on every schedule — see the class javadoc's "Why the heal threshold sits so far above the cap"
     * note. The ride-out ({@link #healingTailStorm_ridesOutPastTheCap_completesByteExact})
     * heals at exactly this many throws.
     */
    private static final int STORM_THROWS = 40;
    /** Any bulk read whose page reaches into the {@code zzzz/} tail is the burst target. */
    private static final byte[] TAIL_THRESHOLD = "zzzz".getBytes(StandardCharsets.UTF_8);

    private static final RetryConfig RIDE_OUT = new RetryConfig(RetryPolicy.RIDE_OUT, ms -> { });
    private static final RetryConfig BOUNDED = new RetryConfig(RetryPolicy.BOUNDED, ms -> { });

    /**
     * Explicit two-range seed: a HEAD range {@code (⊥, "z"]} (all {@code data/} keys, drains instantly)
     * and an isolated TAIL range {@code ("z", ⊤]} (only the {@code zzzz/} keys). Isolating the burst to its
     * own seed range keeps the head keyspace emitting fault-free (the "never a zero-output cancel"
     * property) and keeps the burst on the {@code zzzz/} tail.
     *
     * <p>NB: this does NOT pin the burst to a single logical fetch. The tail worker's first BULK read
     * faults, but a thief's {@code maxKeys=1} floor probe reads the tail keys UNFAULTED and can still
     * split/steal the un-drained tail, handing it to a fresh worker fetch (whose local {@code
     * transientRetries} restarts at 0) — so the burst's throws spread across a bounded handful of logical
     * fetches. The proof is robust to that spread by margin, not by concentration: see {@link #STORM_THROWS}
     * and the class javadoc.
     */
    private static final byte[] SPLIT = "z".getBytes(StandardCharsets.UTF_8);
    private static final List<AdaptiveControlHarness.RangeBound> TWO_RANGE_SEED = List.of(
            new AdaptiveControlHarness.RangeBound(null, SPLIT), new AdaptiveControlHarness.RangeBound(SPLIT, null));

    // ---- ride-out: a HEALING tail timeout burst completes byte-exact -----------------------------------

    @Test
    @Timeout(60)
    void healingTailStorm_ridesOutPastTheCap_completesByteExact(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = keyspace();
        AtomicInteger thrown = new AtomicInteger();
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace)
                .interceptor(tailStorm(thrown, /* healing */ true))
                .build();

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        AdaptiveControlHarness.Result r = AdaptiveControlHarness.runWithSeeds(
                "r195-heal", keyspace, fetcher, EngineToggles.DEFAULT, WORKERS, MAX_KEYS,
                TWO_RANGE_SEED, /* production gauge */ null, RIDE_OUT, new CancellationToken(), metrics, dir);

        assertThat(r.completed()).as("a storm that HEALS is ridden out to a normal completion").isTrue();
        AdaptiveControlHarness.assertExactlyOnce(r.emitted(), keyspace);
        assertThat(thrown.get())
                .as("the ride-out endured %d consecutive tail timeouts (past the cap of 8)", STORM_THROWS)
                .isEqualTo(STORM_THROWS);
        assertThat(r.steal("TRANSIENT.storm_ride_out_worker"))
                .as("the worker path genuinely entered storm ride-out").isGreaterThan(0L);
    }

    // ---- watchdog: a NEVER-HEALING tail timeout burst dies STUCK via the watchdog, never zero-output ---

    @Test
    @Timeout(60)
    void neverHealingTailStorm_diesStuckViaWatchdog_neverZeroOutput(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = keyspace();
        AtomicInteger thrown = new AtomicInteger();
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace)
                .interceptor(tailStorm(thrown, /* healing */ false))
                .build();

        CancellationToken token = new CancellationToken();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // Arm ONLY the zero-real-progress backstop (stall window off), exactly the rung pin of
        // SeedRideOutWatchdogStopContractTest: the head commits pages then the tail timeout burst freezes
        // real progress, so this rung — not the total-freeze rung — owns the STUCK disposition.
        AdaptiveControlHarness.Result r;
        try (LivenessWatchdog watchdog =
                     LivenessWatchdog.arm(token, metrics, Duration.ZERO, Duration.ofMillis(400), 75)) {
            r = AdaptiveControlHarness.runWithSeeds(
                    "r195-never", keyspace, fetcher, EngineToggles.DEFAULT, WORKERS, MAX_KEYS,
                    TWO_RANGE_SEED, /* production gauge */ null, RIDE_OUT, token, metrics, dir);
        }

        assertThat(r.completed()).as("a never-healing storm does not complete").isFalse();
        assertThat(token.stopReason())
                .as("the run is disposed STUCK (resumable), never a fatal crash").isEqualTo(StopReason.STUCK);
        assertThat(token.source())
                .as("storm death is owned by the WATCHDOG, not the retry cap")
                .isEqualTo(CancelSource.LIVENESS_WATCHDOG);
        assertThat(r.emitted())
                .as("NEVER a zero-output cancel — the head ranges emitted before the tail storm froze progress")
                .isNotEmpty();
        assertThat(thrown.get())
                .as("the storm rode out FAR past the legacy cap of 9 before the watchdog ended it")
                .isGreaterThan(15);
    }

    // ---- ablation proof: the BOUNDED-cap ablation FAILS the ride-out -----------------------------

    @Test
    @Timeout(60)
    void boundedCapAblationFailsRideOut(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = keyspace();
        AtomicInteger thrown = new AtomicInteger();
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace)
                .interceptor(tailStorm(thrown, /* healing */ true))   // SAME healing timeline as the ride-out
                .build();

        CancellationToken token = new CancellationToken();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        AdaptiveControlHarness.Result r = AdaptiveControlHarness.runWithSeeds(
                "r195-dod", keyspace, fetcher, EngineToggles.DEFAULT, WORKERS, MAX_KEYS,
                TWO_RANGE_SEED, /* production gauge */ null, BOUNDED, token, metrics, dir);

        // The BOUNDED disposition: the per-fetch cap kills a burst that WOULD have healed, and
        // misattributes death to the cap (not the watchdog) — i.e. it FAILS every invariant this file guards.
        assertThat(r.completed())
                .as("the BOUNDED (prior) ablation FAILS to ride out a healing storm — it is cancelled")
                .isFalse();
        assertThat(token.stopReason()).isEqualTo(StopReason.STUCK);
        assertThat(token.source())
                .as("the prior ablation dies via the RETRY CAP, proving RIDE_OUT is load-bearing")
                .isEqualTo(CancelSource.TRANSIENT_RETRY_CAP);
        assertThat(thrown.get())
                .as("the cap tripped strictly before the storm's %dth throw would have healed", STORM_THROWS)
                .isLessThan(STORM_THROWS);
    }

    // ---- fixtures ------------------------------------------------------------------------------------

    /**
     * The tail-timeout-burst interceptor: any bulk read ({@code maxKeys > 1}) whose page reaches into the
     * {@code zzzz/} tail throws {@code ATTEMPT_TIMEOUT}. When {@code healing}, it throws for the first
     * {@link #STORM_THROWS} tail attempts then serves; when not, forever. Head reads (below the tail
     * threshold) are never faulted, so the head keyspace always emits.
     */
    private static MockPageFetcher.PageInterceptor tailStorm(AtomicInteger thrown, boolean healing) {
        return (req, callIndex, page) -> {
            if (req.maxKeys() > 1 && reachesTail(page)) {
                if (!healing || thrown.get() < STORM_THROWS) {
                    thrown.incrementAndGet();
                    throw ThrottleException.attemptTimeout("tail attempt-timeout storm");
                }
            }
            return page;
        };
    }

    private static boolean reachesTail(ListPage page) {
        if (page.entries().isEmpty()) {
            return false;
        }
        List<ListEntry> entries = page.entries();
        byte[] last = entries.getLast().key().raw();
        return Arrays.compareUnsigned(last, TAIL_THRESHOLD) >= 0;
    }

    private static List<byte[]> keyspace() {
        List<byte[]> keys = new ArrayList<>(HEAD_PREFIXES * PER_PREFIX + TAIL_KEYS);
        for (int p = 0; p < HEAD_PREFIXES; p++) {
            for (int o = 0; o < PER_PREFIX; o++) {
                keys.add(("data/p%d/obj-%05d".formatted(p, o)).getBytes(StandardCharsets.UTF_8));
            }
        }
        for (int t = 0; t < TAIL_KEYS; t++) {
            keys.add(("zzzz/tail-%03d".formatted(t)).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }
}
