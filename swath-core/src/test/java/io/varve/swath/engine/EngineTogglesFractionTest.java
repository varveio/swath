/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.testkit.WorkerStates;
import org.junit.jupiter.api.Test;

/**
 * A direct unit test on {@link EngineToggles#farAheadFraction(WorkerState)}
 * covering all four {@code far_ahead}/{@code density_ewma} combinations. {@link
 * EngineTogglesParseTest} covers parsing; this covers the precedence/substitution behavior the
 * parsed record drives at the two {@code farAheadFraction} call sites ({@link Thief} and {@link
 * WorkStealingScan}'s owner-split site) — a regression that ignored {@code far_ahead} or dropped
 * its precedence over {@code density_ewma} would pass parsing but silently mis-pivot here.
 */
final class EngineTogglesFractionTest {

    /**
     * Builds a victim whose {@link WorkerState#densityFraction()} is a real EWMA-derived value
     * strictly between the two off-path constants ({@code 0.5} plain midpoint, {@code 0.75}
     * {@code density_ewma=off}'s ceiling) — {@code 0.625} — so a mutation that hardcodes either
     * off-path constant instead of delegating is caught, not accidentally matched.
     *
     * <p>{@code lo=[0], hi=[200]} single-byte, no common prefix, so {@code StealMath} fractions are
     * exact byte/256 arithmetic: consumed = spanIn(lo,cursor=50) = 50/256, emitted=1000 ⇒
     * overall density = 5120; the recorded page [30,50]/count=200 ⇒ local density = 2560 ⇒
     * ratio = 2560/5120 = 0.5 ⇒ f = 0.5 + 0.25*0.5 = 0.625 exactly (dyadic fractions, no rounding).
     */
    private static WorkerState victimWithMidBandDensityFraction() {
        byte[] lo = {(byte) 0};
        byte[] hi = {(byte) 200};
        WorkerState w = WorkerStates.of(1, lo, null, hi);
        w.lock().lock();
        try {
            w.setCursor(new byte[] {50});
            w.addKeysEmitted(1000);
        } finally {
            w.lock().unlock();
        }
        w.recordPage(new byte[] {30}, new byte[] {50}, 200);
        return w;
    }

    @Test
    void bothOnDelegatesToVictimDensityFraction() {
        WorkerState victim = victimWithMidBandDensityFraction();
        assertThat(victim.densityFraction()).isEqualTo(0.625);

        EngineToggles toggles = EngineToggles.DEFAULT;

        assertThat(toggles.farAheadFraction(victim)).isEqualTo(victim.densityFraction());
        assertThat(toggles.farAheadFraction(victim)).isEqualTo(0.625);
    }

    @Test
    void densityEwmaOffWithFarAheadOnReturnsMaxFarFractionConstant() {
        WorkerState victim = victimWithMidBandDensityFraction();

        EngineToggles toggles = EngineToggles.DEFAULT.withDensityEwma(false);

        // density_ewma=off substitutes the constant ceiling (0.75) instead of the real 0.625 signal.
        assertThat(toggles.farAheadFraction(victim)).isEqualTo(0.75);
        assertThat(toggles.farAheadFraction(victim)).isNotEqualTo(victim.densityFraction());
    }

    @Test
    void farAheadOffWithDensityEwmaOnReturnsPlainMidpoint() {
        WorkerState victim = victimWithMidBandDensityFraction();

        EngineToggles toggles = EngineToggles.DEFAULT.withFarAhead(false);

        assertThat(toggles.farAheadFraction(victim)).isEqualTo(0.5);
        assertThat(toggles.farAheadFraction(victim)).isNotEqualTo(victim.densityFraction());
    }

    @Test
    void farAheadOffWinsOverDensityEwmaOffPrecedence() {
        WorkerState victim = victimWithMidBandDensityFraction();

        EngineToggles toggles = EngineToggles.DEFAULT.withDensityEwma(false).withFarAhead(false);

        // Both off: far_ahead=off is checked first (EngineToggles javadoc), so the plain midpoint
        // (0.5) wins, NOT density_ewma=off's 0.75 ceiling.
        assertThat(toggles.farAheadFraction(victim)).isEqualTo(0.5);
    }
}
