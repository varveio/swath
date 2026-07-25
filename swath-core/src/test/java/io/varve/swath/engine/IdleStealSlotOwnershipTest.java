/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * CONC adversarial guard for {@link IdleStealBackoff}'s fleet-wide one-attempt bound: the in-flight
 * slot is <b>owned</b> by its acquirer and released only by {@link IdleStealBackoff#releaseSlot()}.
 *
 * <p>The bound used to leak. {@link IdleStealBackoff#reset()} cleared {@code attemptInFlight}, and
 * it is called by <b>unrelated</b> workers on every ordinary claim ({@code WorkStealingScan}
 * § nextClaim) and every non-empty page commit (§ signalStealableProgress) — so under load the
 * effective bound was {@code 1 + (reset rate x attempt duration)}, tens in flight against
 * multi-second probes. {@link #resetFromUnrelatedWorkersNeverHandsAwayAnInFlightAttemptSlot()} is
 * written to FAIL against that behaviour: it is exactly the interleaving the leak needs, driven
 * deterministically rather than hoped for.
 *
 * <p>The counterpart at engine scale — max <i>concurrent probe fetches</i> is exactly one across a
 * live scan — is
 * {@code IdleStealProbeConcurrencyTest#stealProbeFetchesNeverOverlapUnderCommitDrivenResets}. This
 * class is the fast, deterministic per-commit backstop for that {@code deep} test, so the contract
 * line is never guarded only off the per-commit gate (TESTING.md § tag convention).
 */
final class IdleStealSlotOwnershipTest {

    private static final int UNRELATED_WORKERS = 4;
    private static final int RESETS_PER_WORKER = 2_000;
    private static final int WAITERS = 8;
    /** Production's own pacing shape (WorkStealingScan's PARK_NANOS / cap / in-flight backstop). */
    private static final long PACING_BASE_NANOS = TimeUnit.MILLISECONDS.toNanos(5);
    private static final long PACING_CAP_NANOS = TimeUnit.MILLISECONDS.toNanos(50);
    private static final long ATTEMPT_PARK_NANOS = TimeUnit.SECONDS.toNanos(1);

    private static IdleStealBackoff backoff() {
        return backoff(new SimpleMeterRegistry());
    }

    private static IdleStealBackoff backoff(MeterRegistry registry) {
        return backoff(registry, PACING_BASE_NANOS, PACING_CAP_NANOS);
    }

    private static IdleStealBackoff backoff(MeterRegistry registry, long baseNanos, long capNanos) {
        return new IdleStealBackoff(baseNanos, capNanos, ATTEMPT_PARK_NANOS, new RunMetrics(registry));
    }

    private static double denials(MeterRegistry registry, String reason) {
        return registry.get("swath.steal_reason").tag("outcome", "IDLE_SLOT").tag("reason", reason)
                .counter().count();
    }

    /**
     * A held slot survives the two high-frequency reset paths, and is handed to exactly one waiter
     * when — and only when — its owner releases it.
     */
    @Test
    @Timeout(30)
    void resetFromUnrelatedWorkersNeverHandsAwayAnInFlightAttemptSlot() throws Exception {
        IdleStealBackoff backoff = backoff();
        assertThat(backoff.tryAcquireAttemptSlot()).as("owner acquires the sole slot").isTrue();

        AtomicInteger stolenWhileHeld = new AtomicInteger();
        CountDownLatch resetsDone = new CountDownLatch(UNRELATED_WORKERS);
        for (int i = 0; i < UNRELATED_WORKERS; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    for (int n = 0; n < RESETS_PER_WORKER; n++) {
                        // Exactly what a claim / non-empty page commit does, from a worker that
                        // does NOT own the attempt slot.
                        backoff.reset();
                        if (backoff.tryAcquireAttemptSlot()) {
                            stolenWhileHeld.incrementAndGet();
                            backoff.releaseSlot();   // don't strand it; the assertion already failed
                        }
                    }
                } finally {
                    resetsDone.countDown();
                }
            });
        }
        assertThat(resetsDone.await(20, TimeUnit.SECONDS)).isTrue();

        assertThat(stolenWhileHeld)
                .as("reset() clears pacing state only — it must never release another worker's slot")
                .hasValue(0);

        // Release, and exactly one of a racing fleet takes the slot — the bound is one, not zero.
        AtomicInteger acquired = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch waitersDone = new CountDownLatch(WAITERS);
        for (int i = 0; i < WAITERS; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    start.await();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    // Every waiter attempts at least once; a loser stops as soon as it can see the
                    // winner, so no thread burns a carrier spinning out the deadline. The deadline
                    // is the fixture's own backstop, never the expected exit.
                    do {
                        if (backoff.tryAcquireAttemptSlot()) {
                            acquired.incrementAndGet();
                            return;   // hold it, as a real attempt would
                        }
                        Thread.sleep(1);   // sleep, not spin: unmounts the virtual thread
                    } while (acquired.get() == 0 && System.nanoTime() < deadline);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    waitersDone.countDown();
                }
            });
        }
        backoff.releaseSlot();
        start.countDown();
        assertThat(waitersDone.await(20, TimeUnit.SECONDS)).isTrue();
        assertThat(acquired).as("the released slot admits exactly one successor").hasValue(1);
    }

    /**
     * A non-productive outcome paces the fleet but does not release ownership either — the owner's
     * {@code finally} does. Pins that {@code recordNonProductive()} no longer doubles as a release,
     * which is what made the leak reachable from the ordinary (non-throwing) path.
     */
    @Test
    @Timeout(10)
    void recordNonProductivePacesTheFleetWithoutReleasingTheSlot() {
        // Minute-scale window for the same reason as the attribution test: a 5 ms window could
        // expire between arming it and reading it back, and the assertion would turn on timing.
        long window = TimeUnit.MINUTES.toNanos(1);
        IdleStealBackoff backoff = backoff(new SimpleMeterRegistry(), window, window);
        assertThat(backoff.tryAcquireAttemptSlot()).isTrue();

        backoff.recordNonProductive();

        assertThat(backoff.tryAcquireAttemptSlot()).as("still owned after a non-productive outcome").isFalse();
        backoff.releaseSlot();
        assertThat(backoff.parkNanos())
                .as("released, but the backoff window it armed still paces the next attempt")
                .isGreaterThan(TimeUnit.SECONDS.toNanos(30));
    }

    /**
     * A worker denied the slot parks on the seconds-scale in-flight backstop, not the ~5 ms pacing
     * base: the release signals the ledger, so re-polling at the base interval is pure denial churn
     * (~11k denials/sec measured against a multi-second probe).
     */
    @Test
    @Timeout(10)
    void aDeniedWorkerParksOnTheInFlightBackstopNotThePacingBase() {
        IdleStealBackoff backoff = backoff();

        assertThat(backoff.parkNanos()).as("idle fleet, no attempt in flight").isEqualTo(PACING_BASE_NANOS);

        assertThat(backoff.tryAcquireAttemptSlot()).isTrue();
        assertThat(backoff.parkNanos())
                .as("an attempt is in flight — wait for its release signal, don't poll")
                .isEqualTo(ATTEMPT_PARK_NANOS);

        backoff.releaseSlot();
        assertThat(backoff.parkNanos()).as("released: back to the pacing base").isEqualTo(PACING_BASE_NANOS);
    }

    /**
     * The two denial regimes are separable post-hoc (§5). {@code slot_denied} alone cannot tell
     * "the bound is holding, workers are waiting on a release" from "the fleet is in exponential
     * backoff with the slot free" — opposite situations that call for opposite responses.
     */
    @Test
    @Timeout(10)
    void denialsAreAttributedToTheRegimeThatRefused() {
        MeterRegistry registry = new SimpleMeterRegistry();
        // A minute-scale pacing window, not production's 5 ms: the `paced` half asserts a refusal
        // that only holds while the armed window is unexpired, and at 5 ms a GC or scheduler pause
        // between the two lines would let the acquire through and fail the test on timing alone.
        IdleStealBackoff backoff = backoff(registry, TimeUnit.MINUTES.toNanos(1), TimeUnit.MINUTES.toNanos(1));

        assertThat(backoff.tryAcquireAttemptSlot()).isTrue();
        assertThat(backoff.tryAcquireAttemptSlot()).as("refused: another worker owns the slot").isFalse();
        assertThat(denials(registry, "in_flight")).isEqualTo(1.0);

        backoff.recordNonProductive();   // arms the pacing window
        backoff.releaseSlot();           // slot free, but the fleet is now paced
        assertThat(backoff.tryAcquireAttemptSlot()).as("refused: exponential backoff, not ownership").isFalse();
        assertThat(denials(registry, "paced")).isEqualTo(1.0);

        assertThat(registry.get("swath.idle_backoff.slot_denied").counter().count())
                .as("the aggregate still counts both")
                .isEqualTo(2.0);
    }
}
