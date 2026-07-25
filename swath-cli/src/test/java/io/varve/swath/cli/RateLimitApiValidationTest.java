/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.RateLimitedPageFetcher;
import io.varve.swath.store.StoreCapabilities;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * {@code --request-rate} resolution and the {@code maybeRateLimited} wiring — unset/{@code
 * 0} both mean "no cap" (mirrors {@code --part-rotation-interval}'s "0/none disables" shape), a
 * negative value is rejected at exit 2, and the wrapper is a true no-op (not merely inert) when
 * unset: it returns the exact same {@link PageFetcher} instance, so there is zero added
 * indirection/overhead on the default hot path.
 */
class RateLimitApiValidationTest {

    @Test
    void unsetAndExplicitZeroBothResolveToNoCap() throws Exception {
        assertThat(ConnectionOptions.resolveRateLimitApi(null)).isNull();
        assertThat(ConnectionOptions.resolveRateLimitApi(0.0)).isNull();
    }

    @Test
    void positiveValuesAreAccepted() throws Exception {
        assertThat(ConnectionOptions.resolveRateLimitApi(5.0)).isEqualTo(5.0);
        assertThat(ConnectionOptions.resolveRateLimitApi(0.5)).isEqualTo(0.5);
    }

    @Test
    void rejectsNegativeValues() {
        for (double bad : new double[]{-1.0, -0.001, -1000.0}) {
            assertThatThrownBy(() -> ConnectionOptions.resolveRateLimitApi(bad))
                    .as("--request-rate=%s must be rejected", bad)
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining("--request-rate");
        }
    }

    @Test
    void callRejectsANegativeRateLimitApiBeforeRunning() {
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--checkpoint", "none",
                "--request-rate", "-3");

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("--request-rate");
    }

    /**
     * {@code NaN} must be rejected explicitly — a bare {@code < 0} check alone is not enough,
     * because any comparison with {@code NaN} is {@code false}, and it would slip through to
     * {@code ApiRateLimiter}'s constructor, which throws an unchecked
     * {@code IllegalArgumentException}: an undocumented exit 1 instead of the contractual exit 2,
     * and on the checkpointed path that would fire AFTER {@code openRun} had already inserted the
     * run's {@code RUNNING} row.
     */
    @Test
    void rejectsNonFiniteValues() {
        for (double bad : new double[]{Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            assertThatThrownBy(() -> ConnectionOptions.resolveRateLimitApi(bad))
                    .as("--request-rate=%s must be rejected", bad)
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining("--request-rate");
        }
    }

    @Test
    void callRejectsNaNAndInfiniteRateLimitApiBeforeAnyCheckpointMutation(@TempDir Path dir) throws Exception {
        Path dbPath = dir.resolve("run.sqlite");
        for (String bad : new String[]{"NaN", "Infinity", "-Infinity"}) {
            ListCommand cmd = new ListCommand();
            new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--checkpoint", dbPath.toString(),
                    "--request-rate", bad);

            assertThatThrownBy(cmd::call)
                    .as("--request-rate=%s must be rejected", bad)
                    .isInstanceOf(InvalidConfigException.class)
                    .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(2))
                    .hasMessageContaining("--request-rate");
            assertThat(Files.exists(dbPath))
                    .as("--request-rate=%s must be validated before any checkpoint is opened/written", bad)
                    .isFalse();
        }
    }

    @Test
    void unsetRateLimitApiReturnsTheSameFetcherInstanceUnwrapped() {
        ListCommand cmd = new ListCommand();
        PageFetcher fetcher = stubFetcher();

        PageFetcher wrapped = cmd.connection.maybeRateLimited(fetcher, new RunMetrics(new SimpleMeterRegistry()));

        assertThat(wrapped).isSameAs(fetcher);
    }

    @Test
    void configuredRateLimitApiWrapsTheFetcher() {
        ListCommand cmd = new ListCommand();
        cmd.connection.rateLimitApi = 10.0;
        PageFetcher fetcher = stubFetcher();

        PageFetcher wrapped = cmd.connection.maybeRateLimited(fetcher, new RunMetrics(new SimpleMeterRegistry()));

        assertThat(wrapped).isInstanceOf(RateLimitedPageFetcher.class);
        assertThat(wrapped).isNotSameAs(fetcher);
    }

    private static PageFetcher stubFetcher() {
        return new PageFetcher() {
            @Override
            public ListPage fetchPage(PageRequest req) {
                throw new UnsupportedOperationException("not exercised in this test");
            }

            @Override
            public StoreCapabilities capabilities() {
                return StoreCapabilities.s3();
            }
        };
    }
}
