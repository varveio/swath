/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ThrottleException}'s {@link ThrottleType} carrier: the SLOWDOWN default
 * and the explicitly-typed constructor.
 */
final class ThrottleExceptionTest {

    @Test
    void messageOnlyConstructorDefaultsToSlowdown() {
        ThrottleException e = ThrottleException.slowDown("boom");
        assertThat(e.type()).isEqualTo(ThrottleType.SLOWDOWN);
        assertThat(e.getMessage()).isEqualTo("boom");
        assertThat(e.getCause()).isNull();
    }

    @Test
    void messageAndCauseConstructorDefaultsToSlowdown() {
        Throwable cause = new RuntimeException("cause");
        ThrottleException e = ThrottleException.slowDown("boom", cause);
        assertThat(e.type()).isEqualTo(ThrottleType.SLOWDOWN);
        assertThat(e.getCause()).isSameAs(cause);
    }

    @Test
    void threeArgConstructorCarriesTheGivenType() {
        Throwable cause = new RuntimeException("cause");
        for (ThrottleType type : ThrottleType.values()) {
            ThrottleException e = ThrottleException.classifiedTransient("boom", cause, type);
            assertThat(e.type()).isEqualTo(type);
            assertThat(e.getCause()).isSameAs(cause);
        }
    }

    @Test
    void isAListingExceptionWithExitCode1() {
        ThrottleException e = ThrottleException.slowDown("boom");
        assertThat(e).isInstanceOf(ListingException.class);
        assertThat(e.exitCode()).isEqualTo(1);
    }
}
