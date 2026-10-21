/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ReplayServerAppDurationTest {

    @Test
    void serveTimeoutAcceptsIsoSecondsBeforeShortSuffixes() {
        assertThat(ReplayServerApp.parseDuration(" PT10S ")).isEqualTo(Duration.ofSeconds(10));
        assertThat(ReplayServerApp.parseDuration("PT0.5S")).isEqualTo(Duration.ofMillis(500));
        assertThat(ReplayServerApp.parseDuration("PT1M")).isEqualTo(Duration.ofMinutes(1));
        assertThat(ReplayServerApp.parseDuration("10s")).isEqualTo(Duration.ofSeconds(10));
        assertThat(ReplayServerApp.parseDuration("250ms")).isEqualTo(Duration.ofMillis(250));
    }

    @Test
    void malformedServeTimeoutStillFailsClearly() {
        assertThatThrownBy(() -> ReplayServerApp.parseDuration("PTbadS"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid serve timeout");
    }
}
