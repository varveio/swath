/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ListingMode;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * {@link PageRequest} grew a 10th canonical constructor arg
 * ({@code apiCallAttemptTimeoutOverride}). A Java record's canonical constructor arity is part
 * of its public API — NOT binary-compatible across an arity change — so the 9-arg convenience
 * constructor must keep compiling and behave exactly like the canonical constructor called with
 * a {@code null} override.
 */
final class PageRequestTest {

    @Test
    void nineArgConstructorDefaultsTheOverrideToNull() {
        byte[] prefix = {1, 2};
        byte[] delimiter = {3};
        byte[] startAfter = {4, 5};
        byte[] endBefore = {6};
        byte[] keyMarker = {7};

        PageRequest nineArg = new PageRequest(ListingMode.OBJECTS, 1000, prefix, delimiter,
                startAfter, endBefore, "token", keyMarker, "v1");
        PageRequest tenArg = new PageRequest(ListingMode.OBJECTS, 1000, prefix, delimiter,
                startAfter, endBefore, "token", keyMarker, "v1", null);

        assertThat(nineArg).isEqualTo(tenArg);
        assertThat(nineArg.apiCallAttemptTimeoutOverride()).isNull();
    }

    @Test
    void nineArgConstructorResultCanStillBeEscalatedViaTheWither() {
        PageRequest req = new PageRequest(ListingMode.OBJECTS, 1000, null, null, null, null, null, null, null);

        PageRequest escalated = req.withApiCallAttemptTimeoutOverride(Duration.ofSeconds(20));

        assertThat(req.apiCallAttemptTimeoutOverride()).isNull();
        assertThat(escalated.apiCallAttemptTimeoutOverride()).isEqualTo(Duration.ofSeconds(20));
        assertThat(escalated.mode()).isEqualTo(ListingMode.OBJECTS);
        assertThat(escalated.maxKeys()).isEqualTo(1000);
    }
}
