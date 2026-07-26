/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Pins {@link ThiefPolicy}'s tuned, load-bearing constants to their LITERAL values. {@code
 * ThiefPolicyCascadeTest}/{@code ThiefPolicySelectionTest}/{@code
 * ThiefPolicyAdaptiveStructureCappedTest} and {@code DecisionTraceGoldenTest} all reference several
 * of these constants only symbolically (or spell the literal out in a comment, never an assertion),
 * so a change to any one of them is invisible to those suites — they would stay green testing the
 * NEW value against itself. Each constant here is a deliberately tuned probe budget/threshold/
 * fallback whose accidental change should fail a build, not silently retune the engine; {@code
 * BOTTOM} (the {@code ⊥} sentinel byte array) is deliberately NOT pinned here — it is a structural
 * value (the empty range start), not a tuning knob.
 */
class ThiefPolicyConstantsTest {

    @Test
    void tunedConstantsArePinnedToTheirLiteralValues() {
        assertThat(ThiefPolicy.STRUCTURE_PROBE_MAX_KEYS).isEqualTo(32);
        assertThat(ThiefPolicy.MAX_STRUCTURE_BACKOUT_LEVELS).isEqualTo(3);
        assertThat(ThiefPolicy.EMPTY_UPPER_BISECTION_MARGIN).isEqualTo(6);
        assertThat(ThiefPolicy.BAND_WIDTH_SUFFIX_BYTES).isEqualTo(2);
        assertThat(ThiefPolicy.OPEN_FRONTIER_BAND_WIDTH_FALLBACK).isEqualTo(40L);
        assertThat(ThiefPolicy.STRUCTURE_ZERO_FANOUT_SUPPRESS_THRESHOLD).isEqualTo(8);
        assertThat(ThiefPolicy.STRUCTURE_TIMEOUT_SUPPRESS_THRESHOLD).isEqualTo(2);
        assertThat(ThiefPolicy.STRUCTURE_SUPPRESS_RETRY_DIVISOR).isEqualTo(64);
    }
}
