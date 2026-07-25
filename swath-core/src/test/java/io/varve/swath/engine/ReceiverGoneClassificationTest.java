/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.error.CancelledException;
import org.junit.jupiter.api.Test;

final class ReceiverGoneClassificationTest {

    @Test
    void cancellationIsCleanOnlyWhenReceiverWasDropped() {
        CancelledException cancelled = new CancelledException("listing cancelled");

        assertThat(WorkStealingScan.isReceiverGoneTeardown(cancelled, true)).isTrue();
        assertThat(WorkStealingScan.isReceiverGoneTeardown(cancelled, false)).isFalse();
    }

    @Test
    void interruptionIsCleanOnlyWhenReceiverWasDropped() {
        InterruptedException interrupted = new InterruptedException("sleep interrupted");

        assertThat(WorkStealingScan.isReceiverGoneTeardown(interrupted, true)).isTrue();
        assertThat(WorkStealingScan.isReceiverGoneTeardown(interrupted, false)).isFalse();
    }
}
