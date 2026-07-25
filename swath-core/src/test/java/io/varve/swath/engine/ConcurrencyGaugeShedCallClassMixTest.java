/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * FAST/deterministic unit tests for the {@code SHED.timeout_storm_worker_fed}/{@code
 * _probe_fed} call-class decomposition on {@link ConcurrencyGauge}'s sustained-timeout shed —
 * same injected-clock/fixed-shed-window seam as {@link ConcurrencyGaugeShedTest} (no real sleep,
 * per-commit tier).
 *
 * <p><b>Probe timeouts LEAVE the shed decision.</b> {@code
 * onTransientTimeout(boolean)} feeds ONLY worker timeouts into {@code shedWindowTimeouts}
 * and {@code maybeShed()}. A pure PROBE-timeout storm cannot trip the sustained-timeout shed
 * (collapsing {@code T}, pausing stealing), because a slow structure/pivot probe carries no
 * S3-backpressure signal — feeding it into the gate would be a client-vs-server false positive. Probe
 * timeouts keep incrementing the VISIBILITY split ({@code shedWindowProbeTimeouts}, published as {@code
 * timeout_storm_probe_fed} at any shed a WORKER storm fires) but never feed the shed gate;
 * only WORKER timeouts trip the shed. These tests pin that: a worker storm still
 * sheds identically; a probe-only storm never sheds; and in a mixed window it is the worker
 * count that gates while the probe count is visibility-only.
 */
final class ConcurrencyGaugeShedCallClassMixTest {

    private static final long WINDOW = ConcurrencyGauge.SHED_WINDOW_BASE_NANOS;

    private final AtomicLong now = new AtomicLong(1_000_000_000_000L);

    private ConcurrencyGauge newGauge(int tMax, RunMetrics metrics) {
        return new ConcurrencyGauge(tMax, metrics, now::get, () -> WINDOW, tMax);
    }

    private static double stealReason(RunMetrics metrics, String outcome, String reason) {
        var c = metrics.registry().find("swath.steal_reason").tags("outcome", outcome, "reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }

    @Test
    void legacyZeroArgOverloadAttributesToWorkerAndStillShedsExactlyAsBefore() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, metrics);

        // The shed gate at T=256 is max(SHED_K, ceil(SHED_ALPHA*256)) = 77, so the shed fires ON the
        // 77th call within this same (unrolled) window -- the remaining 23 calls in this loop still
        // feed the window's raw counters but publish nothing further (shedFiredWindowNs already
        // latched this window; see ConcurrencyGauge#maybeShed).
        for (int i = 0; i < 100; i++) {
            gauge.onTransientTimeout();   // the legacy 0-arg entry point, defaulting every timeout to WORKER
        }
        now.addAndGet(WINDOW + 1);
        // One more timeout after the window elapsed rolls the window (resets the raw counters) and
        // re-checks the gate at the NEW T=128 (needs 39) -- this lone call cannot re-trip it.
        gauge.onTransientTimeout();

        assertThat(gauge.effectiveT()).as("shed fired exactly like the prior zero-arg path").isEqualTo(128);
        assertThat(stealReason(metrics, "SHED", "timeout_storm_worker_fed"))
                .as("the legacy 0-arg overload defaults every timeout to WORKER attribution -- 77 fed "
                        + "the window AT THE INSTANT the gate tripped, not the full 100+1 fed overall")
                .isEqualTo(77.0);
        assertThat(stealReason(metrics, "SHED", "timeout_storm_probe_fed")).isZero();
    }

    @Test
    void mixedStorm_onlyWorkerTimeoutsGateTheShed_probesAreVisibilityOnly() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, metrics);

        // 40 probe timeouts arrive FIRST -- they increment the visibility split only and do
        // NOT feed the shed gate, so they cannot advance T toward a shed on their own.
        for (int i = 0; i < 40; i++) {
            gauge.onTransientTimeout(false);   // probe (slotGated=false) -- visibility only, never gates
        }
        // Then a worker storm: the T=256 gate is ceil(0.3*256)=77, and ONLY worker timeouts count
        // toward it, so it trips on the 77th WORKER timeout (not the 37th, as it would if the 40 probes
        // still fed the total). At that instant the visibility split is 77 worker / 40 probe.
        for (int i = 0; i < 100; i++) {
            gauge.onTransientTimeout(true);    // worker (slotGated=true) -- gates the shed
        }
        now.addAndGet(WINDOW + 1);
        gauge.onTransientTimeout(true);   // rolls the window; the new T=128 gate (39) is not re-tripped

        assertThat(gauge.effectiveT()).as("worker storm sheds 256 -> 128").isEqualTo(128);
        assertThat(stealReason(metrics, "SHED", "timeout_storm_worker_fed"))
                .as("it took 77 WORKER timeouts to trip the gate -- the 40 earlier probes did NOT "
                        + "count toward it (before probe timeouts were excluded from the shed gate, it "
                        + "would have tripped on the 37th worker)")
                .isEqualTo(77.0);
        assertThat(stealReason(metrics, "SHED", "timeout_storm_probe_fed"))
                .as("the 40 probe timeouts are still recorded for VISIBILITY at shed-fire time, they "
                        + "just no longer gate the shed")
                .isEqualTo(40.0);
    }

    @Test
    void pureProbeStorm_doesNotShed_probeTimeoutsLeaveTheShedDecision() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, metrics);

        // A pure probe-timeout storm -- far past any conceivable gate -- across TWO windows. Probe
        // timeouts never feed the shed gate, so no shed ever fires: T stays at Tmax and stealing
        // is never paused.
        for (int i = 0; i < 200; i++) {
            gauge.onTransientTimeout(false);
        }
        now.addAndGet(WINDOW + 1);
        for (int i = 0; i < 200; i++) {
            gauge.onTransientTimeout(false);
        }

        assertThat(gauge.effectiveT())
                .as("a probe-only storm does NOT shed -- T stays at Tmax").isEqualTo(256);
        assertThat(gauge.isStealingAllowed())
                .as("no shed fired, so stealing was never paused").isTrue();
        assertThat(stealReason(metrics, "SHED", "timeout_storm"))
                .as("no sustained-timeout shed fired from probe pressure").isZero();
        assertThat(stealReason(metrics, "SHED", "timeout_storm_worker_fed")).isZero();
        assertThat(stealReason(metrics, "SHED", "timeout_storm_probe_fed"))
                .as("no shed fired, so the fire-time mix is never published").isZero();
    }

    @Test
    void probeOnlyWindowAfterAWorkerShed_doesNotShedAgain() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, metrics);

        // Window 1: a worker-only storm sheds 256 -> 128 (gate 77, tripped on the 77th worker call).
        for (int i = 0; i < 100; i++) {
            gauge.onTransientTimeout(true);
        }
        now.addAndGet(WINDOW + 1);
        // Rolls window 1 -> window 2 (elapsed clock); becomes window 2's own first WORKER-counted call.
        gauge.onTransientTimeout(true);
        assertThat(gauge.effectiveT()).isEqualTo(128);
        assertThat(stealReason(metrics, "SHED", "timeout_storm")).as("window 1 shed once").isEqualTo(1.0);
        assertThat(stealReason(metrics, "SHED", "timeout_storm_worker_fed")).isEqualTo(77.0);

        // Window 2: a pure probe-timeout storm. Probe timeouts do NOT feed the gate, so the
        // window's worker count stays at 1 (the roll call above) -- far below the T=128 gate of 39 --
        // and NO second shed fires. T stays at 128.
        for (int i = 0; i < 100; i++) {
            gauge.onTransientTimeout(false);
        }
        now.addAndGet(WINDOW + 1);
        gauge.onTransientTimeout(false);
        assertThat(gauge.effectiveT())
                .as("a probe-only window does NOT shed further").isEqualTo(128);
        assertThat(stealReason(metrics, "SHED", "timeout_storm"))
                .as("still exactly ONE shed (window 1's worker storm) -- the probe window added none")
                .isEqualTo(1.0);
        assertThat(stealReason(metrics, "SHED", "timeout_storm_worker_fed"))
                .as("unchanged from window 1's worker-fed shed").isEqualTo(77.0);
        assertThat(stealReason(metrics, "SHED", "timeout_storm_probe_fed"))
                .as("no shed fired in the probe window, so no probe mix was published").isZero();
    }
}
