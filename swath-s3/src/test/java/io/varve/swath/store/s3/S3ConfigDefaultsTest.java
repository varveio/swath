/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The shipped {@link S3Config} defaults: a single logical LIST call must be bounded
 * (10 s per attempt, 60 s overall), and swath — not the SDK — owns retry ({@code maxAttempts=1}).
 */
class S3ConfigDefaultsTest {

    @Test
    void attemptTimeoutDefaultIsTenSeconds() {
        assertThat(S3Config.DEFAULT_ATTEMPT_TIMEOUT).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void overallCallTimeoutDefaultIsSixtySeconds() {
        assertThat(S3Config.DEFAULT_API_CALL_TIMEOUT).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    void maxAttemptsDefaultIsOne_swathIsSoleRetrier() {
        assertThat(S3Config.DEFAULT_MAX_ATTEMPTS).isEqualTo(1);
    }

    @Test
    void recordCarriesBothAttemptAndOverallTimeouts() {
        S3Config config = new S3Config(
                null, null, false,
                S3Config.DEFAULT_MAX_PARALLEL,
                S3Config.DEFAULT_MAX_ATTEMPTS,
                S3Config.DEFAULT_ATTEMPT_TIMEOUT,
                S3Config.DEFAULT_API_CALL_TIMEOUT,
                null,
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);
        assertThat(config.apiCallAttemptTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(config.apiCallTimeout()).isEqualTo(Duration.ofSeconds(60));
    }

    /**
     * The 9-arg constructor is the compatibility overload every pre-existing call site (tests,
     * {@code LocalStackSupport}, the replay-server ITs) still uses unmodified — it must default
     * {@code bearerTokenSupplier} to {@code null} (normal SigV4/{@code credentials} signing), not
     * silently require every caller to be updated for an unrelated GCS-only feature.
     */
    @Test
    void nineArgConstructorDefaultsBearerTokenSupplierToNull() {
        S3Config config = new S3Config(
                null, null, false,
                S3Config.DEFAULT_MAX_PARALLEL,
                S3Config.DEFAULT_MAX_ATTEMPTS,
                S3Config.DEFAULT_ATTEMPT_TIMEOUT,
                S3Config.DEFAULT_API_CALL_TIMEOUT,
                null,
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);
        assertThat(config.bearerTokenSupplier()).isNull();
    }

    @Test
    void tenArgConstructorCarriesTheBearerTokenSupplier() {
        BearerTokenSupplier supplier = () -> "tok";
        S3Config config = new S3Config(
                null, null, false,
                S3Config.DEFAULT_MAX_PARALLEL,
                S3Config.DEFAULT_MAX_ATTEMPTS,
                S3Config.DEFAULT_ATTEMPT_TIMEOUT,
                S3Config.DEFAULT_API_CALL_TIMEOUT,
                null,
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT,
                supplier);
        assertThat(config.bearerTokenSupplier()).isSameAs(supplier);
    }
}
