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

/** Parses the launch commands readers meet first; execution would require a real S3 endpoint. */
class HeadlineDocsCommandSmokeTest {

    private static final String PUBLIC_DEMO_BUCKET = "s3://noaa-gestofs-pds/";
    private static final String PUBLIC_DEMO_SLICE =
            PUBLIC_DEMO_BUCKET + "stofs_2d_glo.20260803/00/";

    @Test
    void readmeQuickstartUsesAnExplicitResumableFormatAndConsistentLauncher() throws Exception {
        String readme = Files.readString(Path.of("..", "README.md"));
        assertThat(readme).contains("export PATH=\"$PWD/swath-cli/build/install/swath/bin:$PATH\"")
                .contains("swath list " + PUBLIC_DEMO_SLICE)
                .contains("--no-sign-request --region us-east-1")
                .contains("--format parquet -o out/noaa-gestofs-sample")
                .contains("swath resume out/noaa-gestofs-sample");

        parse("list", PUBLIC_DEMO_SLICE, "--no-sign-request", "--region", "us-east-1",
                "--format", "parquet", "-o", "out/noaa-gestofs-sample");
        parse("resume", "out/noaa-gestofs-sample");
    }

    @Test
    void gettingStartedCommandsParse() throws Exception {
        String gettingStarted = Files.readString(Path.of("..", "docs", "getting-started.md"));
        assertThat(gettingStarted).contains(PUBLIC_DEMO_SLICE)
                .contains("--no-sign-request --region us-east-1")
                .contains("--format parquet -o /out/noaa-gestofs-sample")
                .contains("resume /out/noaa-gestofs-sample")
                .contains("--format parquet -o /out/noaa-gestofs-pds");

        parse("list", PUBLIC_DEMO_SLICE,
                "--no-sign-request", "--region", "us-east-1");
        parse("list", PUBLIC_DEMO_SLICE, "--no-sign-request", "--region", "us-east-1",
                "--format", "parquet", "-o", "/out/noaa-gestofs-sample");
        parse("resume", "/out/noaa-gestofs-sample");
        parse("list", PUBLIC_DEMO_BUCKET, "--no-sign-request", "--region", "us-east-1",
                "--concurrency", "128", "--format", "parquet", "-o", "/out/noaa-gestofs-pds");
    }

    private static void parse(String... args) {
        assertThat(App.commandLine().parseArgs(args).commandSpec().name()).isNotBlank();
    }
}
