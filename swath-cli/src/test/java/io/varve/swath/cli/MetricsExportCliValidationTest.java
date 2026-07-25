/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.observability.MeterRegistries;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/** CLI validation for the consolidated OTLP endpoint plus the explicit egress kill-switch. */
class MetricsExportCliValidationTest {

    @Test
    void endpointPresenceEnablesOtlp() {
        ListCommand cmd = parsed("--metrics-endpoint", "http://localhost:4318/v1/metrics");
        assertThat(cmd.otlp.metricsEndpoint).isEqualTo("http://localhost:4318/v1/metrics");
        assertThat(cmd.otlp.resolvedExportMode()).isEqualTo("otlp");
    }

    @Test
    void noMetricsWinsOverAnEndpoint() throws Exception {
        ListCommand cmd = parsed("--metrics-endpoint", "http://localhost:4318/v1/metrics",
                "--no-metrics");
        assertThat(cmd.otlp.resolvedExportMode()).isEqualTo("none");
        assertThat(MeterRegistries.fromCliConfig(
                        cmd.otlp.resolvedExportMode(), cmd.otlp.metricsEndpoint,
                        cmd.otlp.resolveMetricsInterval(), Map.of()))
                .isInstanceOf(SimpleMeterRegistry.class);
    }

    @Test
    void malformedEndpointFailsBeforeCheckpointMutation(@TempDir Path dir) throws Exception {
        Path dbPath = dir.resolve("run.sqlite");
        // A directory-dataset destination with an explicit checkpoint path is a real durable
        // vehicle: the stdout/single-file checkpoint guard doesn't apply to a DIRECTORY kind, so
        // this still proves the metrics-endpoint validation fires before SqliteCheckpointStore ever
        // opens dbPath (a stdout-destined command would now be refused earlier for the unrelated
        // structural reason that stdout can't carry a checkpoint at all).
        Path outputDir = dir.resolve("dataset");
        ListCommand cmd = parsed("-o", outputDir.toString(), "--format", "parquet",
                "--checkpoint", dbPath.toString(), "--metrics-endpoint", "http://[bad");

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(2))
                .hasMessageContaining("--metrics-endpoint");
        assertThat(Files.exists(dbPath)).isFalse();
    }

    @Test
    void removedMetricsModeAndCadenceFlagsAreRejected() {
        for (String removed : new String[]{"--metrics-export", "--metrics-interval"}) {
            ListCommand cmd = new ListCommand();
            assertThatThrownBy(() -> new CommandLine(cmd).parseArgs(
                    "s3://bucket/prefix", removed, "value"))
                    .isInstanceOf(CommandLine.UnmatchedArgumentException.class);
        }
    }

    private static ListCommand parsed(String... options) {
        ListCommand cmd = new ListCommand();
        String[] args = new String[options.length + 1];
        args[0] = "s3://bucket/prefix";
        System.arraycopy(options, 0, args, 1, options.length);
        new CommandLine(cmd).parseArgs(args);
        cmd.syncArgGroups();
        return cmd;
    }
}
