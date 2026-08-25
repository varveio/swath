/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class PeriodicDataSyncTest {

    @Test
    void thresholdAndResidualAccountingAreFormatNeutral() throws Exception {
        PeriodicDataSync sync = new PeriodicDataSync(PeriodicDataSync.MIN_INTERVAL_BYTES);
        AtomicInteger forces = new AtomicInteger();

        assertThat(sync.maybeSync(
                PeriodicDataSync.MIN_INTERVAL_BYTES - 1, forces::incrementAndGet)).isZero();
        assertThat(sync.maybeSync(
                PeriodicDataSync.MIN_INTERVAL_BYTES, forces::incrementAndGet))
                .isEqualTo(PeriodicDataSync.MIN_INTERVAL_BYTES);
        assertThat(sync.maybeSync(
                PeriodicDataSync.MIN_INTERVAL_BYTES * 2 + 17, forces::incrementAndGet))
                .isEqualTo(PeriodicDataSync.MIN_INTERVAL_BYTES + 17);
        assertThat(forces).hasValue(2);
        assertThat(sync.residualBytes(PeriodicDataSync.MIN_INTERVAL_BYTES * 2 + 99))
                .isEqualTo(82);
    }

    @Test
    void firstForceFailurePermanentlyPoisonsPublication() {
        PeriodicDataSync sync = new PeriodicDataSync(PeriodicDataSync.MIN_INTERVAL_BYTES);

        assertThatThrownBy(() -> sync.maybeSync(
                PeriodicDataSync.MIN_INTERVAL_BYTES,
                () -> { throw new IOException("disk rejected writeback"); }))
                .isInstanceOf(IOException.class)
                .hasMessage("disk rejected writeback");
        assertThatThrownBy(sync::requirePublishable)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("refusing to publish")
                .hasRootCauseMessage("disk rejected writeback");
        assertThatThrownBy(() -> sync.maybeSync(
                PeriodicDataSync.MIN_INTERVAL_BYTES * 2, () -> { }))
                .isInstanceOf(IOException.class)
                .hasRootCauseMessage("disk rejected writeback");
    }

    @Test
    void intervalHasOneSharedFloor() {
        assertThat(new PeriodicDataSync(0).enabled()).isFalse();
        assertThatThrownBy(() -> new PeriodicDataSync(PeriodicDataSync.MIN_INTERVAL_BYTES - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(String.valueOf(PeriodicDataSync.MIN_INTERVAL_BYTES));
    }
}
