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

    @Test
    void readmeQuickstartUsesAnExplicitResumableFormatAndConsistentLauncher() throws Exception {
        String readme = Files.readString(Path.of("..", "README.md"));
        assertThat(readme).contains("export PATH=\"$PWD/swath-cli/build/install/swath/bin:$PATH\"")
                .contains("swath list s3://my-bucket/prefix/ "
                        + "--no-sign-request --format parquet -o out/")
                .contains("swath resume out/");

        parse("list", "s3://my-bucket/prefix/", "--no-sign-request", "--format", "parquet", "-o", "out/");
        parse("resume", "out/");
    }

    @Test
    void installQuickstartCommandsParse() throws Exception {
        String install = Files.readString(Path.of("..", "docs", "install.md"));
        assertThat(install).contains("swath list s3://my-bucket/prefix/ --no-sign-request")
                .contains("swath list s3://my-bucket/prefix/ --no-sign-request -o out/ --format parquet")
                .contains("swath resume out/");

        parse("list", "s3://my-bucket/prefix/", "--no-sign-request");
        parse("list", "s3://my-bucket/prefix/", "--no-sign-request", "-o", "out/", "--format", "parquet");
        parse("resume", "out/");
    }

    private static void parse(String... args) {
        assertThat(App.commandLine().parseArgs(args).commandSpec().name()).isNotBlank();
    }
}
