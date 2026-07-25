/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidConfigException;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Pins the {@code --concurrency} bound: without a lower-bound check,
 * {@code T < 1} (e.g. {@code 0} or a negative value from a programmatic sweep script) would size
 * the worker pool / concurrency semaphore to nothing/negative and misbehave instead of failing
 * cleanly at parse time (exit 2).
 */
class MaxParallelListingsValidationTest {

    @Test
    void acceptsOrdinaryValuesIncludingTheDefault() throws Exception {
        assertThat(ConnectionOptions.resolveMaxParallelListings(1)).isEqualTo(1);
        assertThat(ConnectionOptions.resolveMaxParallelListings(64)).isEqualTo(64);   // default
        assertThat(ConnectionOptions.resolveMaxParallelListings(ConnectionOptions.MAX_MAX_PARALLEL_LISTINGS))
                .isEqualTo(ConnectionOptions.MAX_MAX_PARALLEL_LISTINGS);
    }

    @Test
    void rejectsZeroAndNegativeValues() {
        for (int bad : new int[]{0, -1, -64, Integer.MIN_VALUE}) {
            assertThatThrownBy(() -> ConnectionOptions.resolveMaxParallelListings(bad))
                    .as("--concurrency=%d must be rejected", bad)
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining("--concurrency");
        }
    }

    @Test
    void rejectsAboveTheSanityCeiling() {
        for (int bad : new int[]{ConnectionOptions.MAX_MAX_PARALLEL_LISTINGS + 1, Integer.MAX_VALUE}) {
            assertThatThrownBy(() -> ConnectionOptions.resolveMaxParallelListings(bad))
                    .as("--concurrency=%d must be rejected", bad)
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining("--concurrency");
        }
    }

    @Test
    void callRejectsAZeroMaxParallelListingsBeforeRunning() {
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--checkpoint", "none",
                "--concurrency", "0");

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("--concurrency");
    }
}
