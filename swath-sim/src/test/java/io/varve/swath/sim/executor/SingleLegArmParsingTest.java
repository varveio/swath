/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Locale;
import org.junit.jupiter.api.Test;

/**
 * {@link SingleLegRunTest#parseArm} in isolation — the perf harness itself is {@code @Tag("perf")}
 * and skips without its {@code -D} properties, so its argument handling would otherwise never be
 * exercised by a normal build.
 *
 * <p>The behaviour under test is a diagnostic-ergonomics fix: a mistyped arm used to surface as the
 * bare {@code IllegalArgumentException} from {@link Enum#valueOf}, whose message names the bad input
 * but none of the valid alternatives.
 */
class SingleLegArmParsingTest {

    @Test
    void aMistypedArmNamesEveryValidAlternative() {
        assertThatThrownBy(() -> SingleLegRunTest.parseArm("RATE_ANCHORED_FLOOR_QUARTERR"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(SingleLegRunTest.ARM_PROPERTY)
                .hasMessageContaining("RATE_ANCHORED_FLOOR_QUARTERR")
                // the point of the fix: every alternative is offered, not just the rejection
                .satisfies(e -> {
                    for (SensingVariant v : SensingVariant.values()) {
                        assertThat(e).hasMessageContaining(v.name());
                    }
                })
                // the original cause stays reachable rather than being swallowed
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aValidArmParsesAfterTrimmingAndCaseFolding() {
        SensingVariant expected = SensingVariant.values()[0];
        assertThat(SingleLegRunTest.parseArm("  " + expected.name().toLowerCase(Locale.ROOT) + "  "))
                .isEqualTo(expected);
    }
}
