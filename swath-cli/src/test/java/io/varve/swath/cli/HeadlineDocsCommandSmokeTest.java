/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** Parses the historical Java guides with the Java CLI. */
class HeadlineDocsCommandSmokeTest {

    private static final String SMALL_PUBLIC_PREFIX =
            "s3://noaa-gestofs-pds/stofs_2d_glo.20230113/";
    private static final String FULL_PUBLIC_BUCKET = "s3://noaa-gestofs-pds/";

    @Test
    void javaGettingStartedCommandsParse() throws Exception {
        String gettingStarted =
                Files.readString(Path.of("..", "docs", "getting-started.md"));

        assertThat(gettingStarted)
                .contains("list " + SMALL_PUBLIC_PREFIX)
                .contains("--no-sign-request --region us-east-1")
                .contains("--format tsv")
                .contains("--format parquet -o /out/stofs-20230113")
                .contains("resume /out/stofs-20230113");

        parse(
                "list",
                SMALL_PUBLIC_PREFIX,
                "--no-sign-request",
                "--region",
                "us-east-1",
                "--format",
                "tsv");

        parse(
                "list",
                SMALL_PUBLIC_PREFIX,
                "--no-sign-request",
                "--region",
                "us-east-1",
                "--format",
                "parquet",
                "-o",
                "/out/stofs-20230113");

        parse("resume", "/out/stofs-20230113");
    }

    @Test
    void fullScaleDemoKeepsTheRecordedCommandOutOfTheQuickstart() throws Exception {
        String demo = Files.readString(Path.of("..", "docs", "full-scale-demo.md"));

        assertThat(demo)
                .contains("swath v0.2.1")
                .contains("list " + FULL_PUBLIC_BUCKET)
                .contains("--concurrency 128")
                .contains("--format parquet -o /out/noaa-gestofs-pds")
                .contains("resume /out/noaa-gestofs-pds");

        parse(
                "list",
                FULL_PUBLIC_BUCKET,
                "--no-sign-request",
                "--region",
                "us-east-1",
                "--concurrency",
                "128",
                "--format",
                "parquet",
                "-o",
                "/out/noaa-gestofs-pds");

        parse("resume", "/out/noaa-gestofs-pds");
    }

    private static void parse(String... args) {
        assertThat(App.commandLine().parseArgs(args).commandSpec().name()).isNotBlank();
    }
}
