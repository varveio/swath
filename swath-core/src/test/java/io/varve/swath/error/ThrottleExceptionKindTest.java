/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The AIMD-voting split encoded on {@link ThrottleException.Kind}: only genuine store backpressure
 * (503 SlowDown / 5xx) votes the concurrency target down; a client-side attempt-timeout or
 * exhausted network fault is retryable-transient but must NOT vote. The retry wrapper reads exactly
 * this flag, so it is the single source of truth for that decision.
 */
class ThrottleExceptionKindTest {

    @Test
    void onlyRealBackpressureKindsVoteAimdDown() {
        assertThat(ThrottleException.Kind.SLOWDOWN.votesAimdDown()).isTrue();
        assertThat(ThrottleException.Kind.SERVER_5XX.votesAimdDown()).isTrue();
        assertThat(ThrottleException.Kind.ATTEMPT_TIMEOUT.votesAimdDown()).isFalse();
        assertThat(ThrottleException.Kind.NETWORK.votesAimdDown()).isFalse();
    }

    @Test
    void convenienceConstructorsDefaultToSlowdown() {
        assertThat(ThrottleException.slowDown("x").kind()).isEqualTo(ThrottleException.Kind.SLOWDOWN);
        assertThat(ThrottleException.slowDown("x", new RuntimeException()).kind())
                .isEqualTo(ThrottleException.Kind.SLOWDOWN);
    }

    @Test
    void explicitKindIsPreservedAndStillFatalWhenEscaping() {
        ThrottleException te = ThrottleException.attemptTimeout("x");
        assertThat(te.kind()).isEqualTo(ThrottleException.Kind.ATTEMPT_TIMEOUT);
        // Still a ListingException (exit 1) if it ever escapes the bounded retry wrapper.
        assertThat(te).isInstanceOf(ListingException.class);
    }
}
