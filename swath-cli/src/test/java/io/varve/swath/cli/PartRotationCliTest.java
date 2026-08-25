/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.store.s3.S3Config;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * CLI-level coverage for the {@code --part-rotation-interval} /
 * {@code --part-rotation-max-rows} flags — the production entry point that
 * {@code ParquetRotationCadenceTest} (mechanism) and {@code ResumeParquetCadenceTest}
 * (resume-RPO guard) exercise below the CLI. Follows the same
 * parse-then-assert pattern as {@link ParquetWritersValidationTest}.
 */
class PartRotationCliTest {

    /** {@code parquetSpec} only reads {@code region()} off this (for the JSON-summary run-config), unused here. */
    private static final S3Config TEST_CONFIG = new S3Config(
            null, null, false, 64, S3Config.DEFAULT_MAX_ATTEMPTS, S3Config.DEFAULT_ATTEMPT_TIMEOUT,
            S3Config.DEFAULT_API_CALL_TIMEOUT, null, S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);

    private static ListCommand parse(String... args) {
        ListCommand list = new ListCommand();
        String[] full = new String[args.length + 3];
        full[0] = "s3://bucket/prefix";
        System.arraycopy(args, 0, full, 1, args.length);
        full[full.length - 2] = "--checkpoint";
        full[full.length - 1] = "none";
        new CommandLine(list).parseArgs(full);
        return list;
    }

    @Test
    void defaultsAreThirtySecondsAndTwoMillionRows() throws Exception {
        ListCommand list = parse();

        assertThat(list.output.resolvePartRotationInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(list.output.resolvePartRotationInterval().toNanos()).isEqualTo(30_000_000_000L);
        assertThat(list.output.resolvePartRotationMaxRows()).isEqualTo(2_000_000L);
    }

    @Test
    void zeroNoneOrOffDisableTheIntervalFlag() throws Exception {
        for (String disable : new String[] {"0", "none", "off", "NONE", "OFF"}) {
            ListCommand list = parse("--part-rotation-interval", disable);
            assertThat(list.output.resolvePartRotationInterval())
                    .as("--part-rotation-interval=%s must disable the trigger", disable)
                    .isEqualTo(Duration.ZERO);
        }
    }

    @Test
    void zeroDisablesTheMaxRowsFlag() throws Exception {
        ListCommand list = parse("--part-rotation-max-rows", "0");
        assertThat(list.output.resolvePartRotationMaxRows()).isZero();
    }

    @Test
    void parsesSuffixedAndIsoDurations() throws Exception {
        assertThat(parse("--part-rotation-interval", "10s").output.resolvePartRotationInterval())
                .isEqualTo(Duration.ofSeconds(10));
        assertThat(parse("--part-rotation-interval", "500ms").output.resolvePartRotationInterval())
                .isEqualTo(Duration.ofMillis(500));
        assertThat(parse("--part-rotation-interval", "PT1M").output.resolvePartRotationInterval())
                .isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    void malformedIntervalIsRejected() {
        for (String bad : new String[] {"foo", "", "10x"}) {
            assertThatThrownBy(() -> OutputOptions.parsePartRotationInterval(bad))
                    .as("reject --part-rotation-interval=%s", bad)
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining("--part-rotation-interval");
        }
    }

    @Test
    void negativeIntervalIsRejected() {
        // "-5s" (suffix form) fails ISO parsing before the sign is even inspected, landing in the
        // generic "invalid" branch — same as --progress-interval's suffix grammar. An ISO-8601
        // negative duration reaches the dedicated negative check.
        assertThatThrownBy(() -> OutputOptions.parsePartRotationInterval("-PT5S"))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("--part-rotation-interval")
                .hasMessageContaining("negative");
    }

    /**
     * A positive-but-below-{@code MIN_PART_ROTATION_INTERVAL}
     * value must be rejected — it would otherwise feed a near-zero {@code poll()} timeout
     * into the writer pool's lane loop and spin the CPU (see {@code ParquetWriterPool}'s
     * {@code ROTATION_POLL_FLOOR_NANOS} for the pool-side defense-in-depth).
     */
    @Test
    void positiveIntervalBelowTheMinimumIsRejected() {
        for (String tooSmall : new String[] {"1ms", "50ms", "99ms", "PT0.000000001S", "PT0.05S"}) {
            assertThatThrownBy(() -> OutputOptions.parsePartRotationInterval(tooSmall))
                    .as("reject --part-rotation-interval=%s (below the 100ms minimum)", tooSmall)
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining("--part-rotation-interval")
                    .hasMessageContaining(OutputOptions.MIN_PART_ROTATION_INTERVAL.toString());
        }
    }

    /** Zero/none/off must still disable the trigger even though they are technically "below the minimum". */
    @Test
    void zeroStillDisablesDespiteBeingBelowTheMinimum() throws Exception {
        assertThat(OutputOptions.parsePartRotationInterval("0")).isEqualTo(Duration.ZERO);
        assertThat(OutputOptions.parsePartRotationInterval("none")).isEqualTo(Duration.ZERO);
        assertThat(OutputOptions.parsePartRotationInterval("off")).isEqualTo(Duration.ZERO);
    }

    /** A value at exactly the minimum, and comfortably above it, must be accepted. */
    @Test
    void intervalAtOrAboveTheMinimumIsAccepted() throws Exception {
        assertThat(OutputOptions.parsePartRotationInterval("100ms"))
                .isEqualTo(OutputOptions.MIN_PART_ROTATION_INTERVAL);
        assertThat(OutputOptions.parsePartRotationInterval("500ms")).isEqualTo(Duration.ofMillis(500));
        assertThat(OutputOptions.parsePartRotationInterval("30s")).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void negativeMaxRowsIsRejected() throws Exception {
        ListCommand list = parse("--part-rotation-max-rows", "-1");
        assertThatThrownBy(list.output::resolvePartRotationMaxRows)
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("--part-rotation-max-rows");
    }

    /**
     * A single-file destination (a {@code .parquet} extension on {@code -o},
     * replacing the old {@code --single-file} flag) forces both cadence triggers off in {@link
     * ListCommand#parquetSpec}, even when the user passes non-zero {@code --part-rotation-interval}/
     * {@code --part-rotation-max-rows} explicitly — a single-file run must stay exactly one part.
     */
    @Test
    void singleFileDestinationOverridesNonZeroRotationFlagsToDisabled() throws Exception {
        ListCommand list = parse("--part-rotation-interval", "10s", "--part-rotation-max-rows", "500",
                "--writeback-size", "4mb");
        list.output.resolvedKind = OutputOptions.DestinationKind.FILE;   // as resolveOutput() would set it for -o out.parquet

        S3Uri s3uri = S3Uri.parse("s3://bucket/prefix");
        ListRunner.ParquetSpec spec = list.parquetSpec(s3uri, 1000, 1000, FilterChain.EMPTY, "hash", TEST_CONFIG);

        assertThat(spec.rotationIntervalNanos()).isZero();
        assertThat(spec.rotationMaxRows()).isZero();
        assertThat(spec.writebackBytes()).isZero();
    }

    /** With a directory-dataset destination (the default), the same non-zero flags flow straight through. */
    @Test
    void withoutSingleFileTheRotationFlagsFlowThroughUnchanged() throws Exception {
        ListCommand list = parse("--part-rotation-interval", "10s", "--part-rotation-max-rows", "500",
                "--writeback-size", "4mb");

        S3Uri s3uri = S3Uri.parse("s3://bucket/prefix");
        ListRunner.ParquetSpec spec = list.parquetSpec(s3uri, 1000, 1000, FilterChain.EMPTY, "hash", TEST_CONFIG);

        assertThat(spec.rotationIntervalNanos()).isEqualTo(Duration.ofSeconds(10).toNanos());
        assertThat(spec.rotationMaxRows()).isEqualTo(500L);
        assertThat(spec.writebackBytes()).isEqualTo(4L * 1024 * 1024);
    }
}
