/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.varve.swath.observability.StopReason;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The {@code --max-duration} deadline seam ({@link DeadlineCanceller}). */
final class DeadlineCancellerTest {

    @Test
    void firesAndAttributesTheCancelAsMaxDuration() {
        CancellationToken token = new CancellationToken();
        try (DeadlineCanceller ignored = DeadlineCanceller.arm(token, Duration.ofMillis(20))) {
            await().atMost(5, TimeUnit.SECONDS).until(token::isCancelled);
            assertThat(token.stopReason()).isEqualTo(StopReason.MAX_DURATION);
        }
    }

    @Test
    void nullDurationDisarmsAndNeverCancels() throws Exception {
        CancellationToken token = new CancellationToken();
        try (DeadlineCanceller ignored = DeadlineCanceller.arm(token, null)) {
            Thread.sleep(60);
            assertThat(token.isCancelled()).isFalse();
            assertThat(token.stopReason()).isNull();
        }
    }

    @Test
    void closeBeforeTheDeadlineCancelsThePendingTask() throws Exception {
        CancellationToken token = new CancellationToken();
        DeadlineCanceller canceller = DeadlineCanceller.arm(token, Duration.ofSeconds(30));
        canceller.close();   // normal completion tears the deadline down before it can fire
        Thread.sleep(60);
        assertThat(token.isCancelled()).isFalse();
    }
}
