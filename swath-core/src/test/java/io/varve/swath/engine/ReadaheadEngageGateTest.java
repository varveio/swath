/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Engage/disengage gate-mechanics units (deterministic, no wall-clock): the {@link AdoptionWindow}
 * tumbling-window verdict and the {@link ReadaheadConfig} defaults. The engine-level wiring of these
 * (engage deferred by the longer streak; disengage + re-engage over a real drain) lives in
 * {@link RangeScannerReadaheadTest}; the adversarial PROP/RES/CONC suite is owned separately.
 */
final class ReadaheadEngageGateTest {

    // ---- AdoptionWindow tumbling-window verdict -------------------------------------------------

    @Test
    void noVerdictBeforeTheFirstFullWindow() {
        AdoptionWindow w = new AdoptionWindow(4, 0.40);
        // Three low pages: window (4) not yet complete -> no verdict latched yet.
        w.record(false);
        w.record(false);
        w.record(false);
        assertThat(w.shouldDisengage()).isFalse();
    }

    @Test
    void disengagesWhenAdoptedFractionAtOrBelowFloorOverAFullWindow() {
        AdoptionWindow w = new AdoptionWindow(10, 0.40);
        // 4 adopted of 10 == 0.40 == floor -> at-or-below -> disengage (a transient-stretch rate, ~25%,
        // is well under this).
        for (int i = 0; i < 4; i++) {
            w.record(true);
        }
        for (int i = 0; i < 6; i++) {
            w.record(false);
        }
        assertThat(w.shouldDisengage()).isTrue();
    }

    @Test
    void staysEngagedWhenAdoptedFractionIsAboveFloor() {
        AdoptionWindow w = new AdoptionWindow(10, 0.40);
        // 5 adopted of 10 == 0.50 > 0.40 -> stay engaged (a healthy sustained drain).
        for (int i = 0; i < 5; i++) {
            w.record(true);
        }
        for (int i = 0; i < 5; i++) {
            w.record(false);
        }
        assertThat(w.shouldDisengage()).isFalse();
    }

    @Test
    void verdictIsTumbling_aRecoveringWindowClearsAPriorLowVerdict() {
        AdoptionWindow w = new AdoptionWindow(4, 0.40);
        // Window 1: all misses -> low verdict latched.
        for (int i = 0; i < 4; i++) {
            w.record(false);
        }
        assertThat(w.shouldDisengage()).isTrue();
        // Window 2: all adopted -> the fresh window re-evaluates and clears the verdict (no stale carry).
        for (int i = 0; i < 4; i++) {
            w.record(true);
        }
        assertThat(w.shouldDisengage()).isFalse();
    }

    @Test
    void windowOfZeroDisablesTheGateEntirely() {
        AdoptionWindow w = new AdoptionWindow(0, 0.40);
        for (int i = 0; i < 100; i++) {
            w.record(false);   // never adopted, but the gate is disabled
        }
        assertThat(w.shouldDisengage()).isFalse();
    }

    // ---- ReadaheadConfig defaults ----------------------------------------------------------

    @Test
    void defaultsCarryTheTightenedEngageGate() {
        // Streak kept at 6 (raising it further to reduce engagement count collapses
        // engagement for all buckets under work-stealing — see ReadaheadConfig#DEFAULT_ENGAGE_AFTER_FULL_PAGES);
        // disengage window shrunk 32→16 (the verdict renders within a typical engagement); the
        // est-remaining floor is DISABLED (0) because a non-zero floor collapsed a sustained real-world drain.
        assertThat(ReadaheadConfig.DEFAULT_ENGAGE_AFTER_FULL_PAGES).isEqualTo(6);
        assertThat(ReadaheadConfig.DEFAULT_DISENGAGE_WINDOW).isEqualTo(16);
        assertThat(ReadaheadConfig.DEFAULT_DISENGAGE_MIN_ADOPTION).isEqualTo(0.40);
        assertThat(ReadaheadConfig.DEFAULT_MIN_ENGAGE_REMAINING_PAGES).isEqualTo(0.0);

        for (ReadaheadConfig cfg : new ReadaheadConfig[]{
                ReadaheadConfig.off(), ReadaheadConfig.on(),
                ReadaheadConfig.onWithK(4), ReadaheadConfig.onWithKAndBudget(4, 1000L)}) {
            assertThat(cfg.engageAfterFullPages()).isEqualTo(6);
            assertThat(cfg.disengageWindow()).isEqualTo(16);
            assertThat(cfg.disengageMinAdoption()).isEqualTo(0.40);
            assertThat(cfg.minEngageRemainingPages()).isEqualTo(0.0);
        }
    }
}
