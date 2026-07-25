/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.CancelledException;
import io.varve.swath.observability.StopReason;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * {@link CancellationToken} stop-reason attribution is <b>first-writer-wins and
 * deterministic</b>. The FIRST attributed cancel to fire sticks; no later cancel —
 * attributed or the no-arg engine-teardown {@link CancellationToken#cancel()} — ever
 * overwrites it. This is what keeps the terminal summary's {@code stop_reason} and the
 * process exit code in agreement: a {@code --max-duration} deadline that
 * fired first stays {@code max_duration} (exit 0) even if a SIGTERM lands a moment later,
 * and a signal that fired first stays {@code signal} (exit 130) even if the deadline then
 * elapses — so a signalled process never exits 0 (nor a timebox stop exit 130).
 */
final class CancellationTokenTest {

    @Test
    void deadlineThenSignal_keepsMaxDuration() {
        CancellationToken token = new CancellationToken();
        token.cancel(StopReason.MAX_DURATION);
        token.cancel(StopReason.SIGNAL);   // a SIGTERM racing in just after the deadline fired
        assertThat(token.isCancelled()).isTrue();
        assertThat(token.stopReason()).isEqualTo(StopReason.MAX_DURATION);
    }

    @Test
    void signalThenDeadline_keepsSignal() {
        CancellationToken token = new CancellationToken();
        token.cancel(StopReason.SIGNAL);
        token.cancel(StopReason.MAX_DURATION);   // the deadline elapsing just after the signal
        assertThat(token.isCancelled()).isTrue();
        assertThat(token.stopReason()).isEqualTo(StopReason.SIGNAL);
    }

    @Test
    void laterNoArgCancel_neverOverwritesAnAttributedReason() {
        CancellationToken token = new CancellationToken();
        token.cancel(StopReason.SIGNAL);
        token.cancel();   // engine-internal completion / receiver-gone teardown
        assertThat(token.stopReason()).isEqualTo(StopReason.SIGNAL);
    }

    @Test
    void firstAttributedReasonAfterANoArgCancel_stillWins() {
        // The no-arg cancel carries NO reason, so the first ATTRIBUTED cancel is the first reason
        // and sticks — even though the flag was already set by the no-arg cancel.
        CancellationToken token = new CancellationToken();
        token.cancel();
        token.cancel(StopReason.MAX_DURATION);
        token.cancel(StopReason.SIGNAL);
        assertThat(token.stopReason()).isEqualTo(StopReason.MAX_DURATION);
    }

    @Test
    void repeatedLaterCancels_neverFlipTheReason() {
        CancellationToken token = new CancellationToken();
        token.cancel(StopReason.MAX_DURATION);
        for (int i = 0; i < 1000; i++) {
            token.cancel(StopReason.SIGNAL);
            token.cancel();
        }
        assertThat(token.stopReason()).isEqualTo(StopReason.MAX_DURATION);
    }

    @Test
    void throwIfCancelled_surfacesCancelledOnlyAfterCancel() throws CancelledException {
        CancellationToken token = new CancellationToken();
        // Not cancelled: no throw.
        token.throwIfCancelled();
        token.cancel(StopReason.SIGNAL);
        assertThatThrownBy(token::throwIfCancelled).isInstanceOf(CancelledException.class);
    }

    /**
     * {@code cancel(StopReason, CancelSource)} returns whether THIS
     * call won the first-writer-wins attribution — the structural seam
     * {@code RunMetrics#recordTransientRetryCapExhaustion} callers gate their own local-state recording
     * on, so a losing cap-trip's fault history can never overwrite the winner's.
     */
    @Test
    void cancelWithSource_returnsWhetherThisCallWonTheAttribution() {
        CancellationToken token = new CancellationToken();
        assertThat(token.cancel(StopReason.STUCK, CancelSource.TRANSIENT_RETRY_CAP))
                .as("the first attributed cancel wins")
                .isTrue();
        assertThat(token.cancel(StopReason.STUCK, CancelSource.LIVENESS_WATCHDOG))
                .as("a later cancel loses even though isCancelled() is already true")
                .isFalse();
        assertThat(token.source())
                .as("the WINNER's source sticks, never the loser's")
                .isEqualTo(CancelSource.TRANSIENT_RETRY_CAP);
    }

    /**
     * Same invariant as {@link #cancelWithSource_returnsWhetherThisCallWonTheAttribution()} under a
     * genuine two-thread race: repeated to exercise many interleavings; EXACTLY one of the two racing
     * {@code cancel(StopReason, CancelSource)} calls must return {@code true} on every run, regardless
     * of which one wins.
     */
    @RepeatedTest(200)
    void concurrentCancelWithSource_exactlyOneCallerWins() throws Exception {
        CancellationToken token = new CancellationToken();
        CyclicBarrier gate = new CyclicBarrier(2);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> wonA = new AtomicReference<>();
        AtomicReference<Boolean> wonB = new AtomicReference<>();

        Runnable rangeA = () -> {
            try {
                gate.await();
                wonA.set(token.cancel(StopReason.STUCK, CancelSource.TRANSIENT_RETRY_CAP));
            } catch (Exception e) {
                failure.set(e);
            }
        };
        Runnable rangeB = () -> {
            try {
                gate.await();
                wonB.set(token.cancel(StopReason.STUCK, CancelSource.TRANSIENT_RETRY_CAP));
            } catch (Exception e) {
                failure.set(e);
            }
        };

        Thread a = new Thread(rangeA, "range-a");
        Thread b = new Thread(rangeB, "range-b");
        a.start();
        b.start();
        a.join();
        b.join();

        assertThat(failure.get()).isNull();
        assertThat(wonA.get() ^ wonB.get())
                .as("exactly one of the two racing cancels won the attribution")
                .isTrue();
    }

    /**
     * The {@code (reason, source)} pair must be published ATOMICALLY — a mixed-SOURCE race must
     * never pair one caller's reason with another caller's source. Two threads race with DISTINCT,
     * coherent pairs — {@code (MAX_DURATION, TIMEBOX)} vs {@code (STUCK, LIVENESS_WATCHDOG)} — so a
     * cross-pairing (e.g. {@code MAX_DURATION} + {@code LIVENESS_WATCHDOG}) would be detectable. On EVERY
     * interleaving exactly one caller wins, and the surviving pair is wholly that winner's: reason and
     * source are inseparable. Publishing the pair via two independent CAS operations could land an
     * incoherent pair (A wins reason, B wins source) while A still returns {@code true}.
     */
    @RepeatedTest(500)
    void concurrentCancelWithDifferentSources_publishesOneCoherentPair() throws Exception {
        CancellationToken token = new CancellationToken();
        CyclicBarrier gate = new CyclicBarrier(2);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> wonA = new AtomicReference<>();
        AtomicReference<Boolean> wonB = new AtomicReference<>();

        Runnable timebox = () -> {
            try {
                gate.await();
                wonA.set(token.cancel(StopReason.MAX_DURATION, CancelSource.TIMEBOX));
            } catch (Exception e) {
                failure.set(e);
            }
        };
        Runnable watchdog = () -> {
            try {
                gate.await();
                wonB.set(token.cancel(StopReason.STUCK, CancelSource.LIVENESS_WATCHDOG));
            } catch (Exception e) {
                failure.set(e);
            }
        };

        Thread a = new Thread(timebox, "timebox");
        Thread b = new Thread(watchdog, "watchdog");
        a.start();
        b.start();
        a.join();
        b.join();

        assertThat(failure.get()).isNull();
        assertThat(wonA.get() ^ wonB.get())
                .as("exactly one of the two racing cancels won the attribution")
                .isTrue();
        StopReason reason = token.stopReason();
        CancelSource source = token.source();
        if (Boolean.TRUE.equals(wonA.get())) {
            assertThat(reason).as("the timebox caller won — its WHOLE pair is published").isEqualTo(StopReason.MAX_DURATION);
            assertThat(source).as("...never crossed with the watchdog's source").isEqualTo(CancelSource.TIMEBOX);
        } else {
            assertThat(reason).as("the watchdog caller won — its WHOLE pair is published").isEqualTo(StopReason.STUCK);
            assertThat(source).as("...never crossed with the timebox source").isEqualTo(CancelSource.LIVENESS_WATCHDOG);
        }
    }

    /**
     * Under a genuine two-thread race the winner is whichever CAS lands first, but the reason must
     * be published EXACTLY ONCE and then be immutable — a last-writer-wins field could momentarily
     * (or finally) show the loser. Repeated to exercise many interleavings; each run asserts the
     * observed reason is a legitimate one and never mutates after both cancels complete.
     */
    @RepeatedTest(200)
    void concurrentAttributedCancels_publishOneReasonImmutably() throws Exception {
        CancellationToken token = new CancellationToken();
        CyclicBarrier gate = new CyclicBarrier(2);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Runnable deadline = () -> {
            try {
                gate.await();
                token.cancel(StopReason.MAX_DURATION);
            } catch (Exception e) {
                failure.set(e);
            }
        };
        Runnable signal = () -> {
            try {
                gate.await();
                token.cancel(StopReason.SIGNAL);
            } catch (Exception e) {
                failure.set(e);
            }
        };

        Thread a = new Thread(deadline, "deadline");
        Thread b = new Thread(signal, "signal");
        a.start();
        b.start();
        a.join();
        b.join();

        assertThat(failure.get()).isNull();
        assertThat(token.isCancelled()).isTrue();
        StopReason winner = token.stopReason();
        assertThat(winner).isIn(StopReason.MAX_DURATION, StopReason.SIGNAL);
        // Immutable once both cancels have completed: no further read flips it.
        for (int i = 0; i < 100; i++) {
            assertThat(token.stopReason()).isEqualTo(winner);
        }
    }
}
