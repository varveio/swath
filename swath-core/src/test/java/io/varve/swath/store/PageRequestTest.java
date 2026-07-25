/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ListingMode;
import org.junit.jupiter.api.Test;

/**
 * {@link PageRequest} grew a 10th canonical constructor arg
 * ({@code attemptTimeoutEscalationLevel}). A Java record's canonical constructor arity is part
 * of its public API — NOT binary-compatible across an arity change — so the 9-arg convenience
 * constructor must keep compiling and behave exactly like the canonical constructor called with
 * level {@code 0} (the store's base budget).
 */
final class PageRequestTest {

    @Test
    void nineArgConstructorDefaultsTheEscalationLevelToZero() {
        byte[] prefix = {1, 2};
        byte[] delimiter = {3};
        byte[] startAfter = {4, 5};
        byte[] endBefore = {6};
        byte[] keyMarker = {7};

        PageRequest nineArg = new PageRequest(ListingMode.OBJECTS, 1000, prefix, delimiter,
                startAfter, endBefore, "token", keyMarker, "v1");
        PageRequest tenArg = new PageRequest(ListingMode.OBJECTS, 1000, prefix, delimiter,
                startAfter, endBefore, "token", keyMarker, "v1", 0);

        assertThat(nineArg).isEqualTo(tenArg);
        assertThat(nineArg.attemptTimeoutEscalationLevel()).isZero();
    }

    @Test
    void nineArgConstructorResultCanStillBeEscalatedViaTheWither() {
        PageRequest req = new PageRequest(ListingMode.OBJECTS, 1000, null, null, null, null, null, null, null);

        PageRequest escalated = req.withAttemptTimeoutEscalationLevel(1);

        assertThat(req.attemptTimeoutEscalationLevel()).as("the wither does not mutate").isZero();
        assertThat(escalated.attemptTimeoutEscalationLevel()).isEqualTo(1);
        assertThat(escalated.mode()).isEqualTo(ListingMode.OBJECTS);
        assertThat(escalated.maxKeys()).isEqualTo(1000);
    }
}
