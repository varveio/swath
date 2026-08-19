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
                .contains("swath list s3://cmas-smoke-testcase/smoke_example_case/"
                        + "2018gg_18j/inputs/htap/")
                .contains("--region us-east-1 --no-sign-request --format parquet -o out/")
                .contains("swath resume out/");

        parse("list", "s3://cmas-smoke-testcase/smoke_example_case/2018gg_18j/inputs/htap/",
                "--region", "us-east-1", "--no-sign-request",
                "--format", "parquet", "-o", "out/");
        parse("resume", "out/");
    }

    @Test
    void gettingStartedCommandsParse() throws Exception {
        String gettingStarted = Files.readString(Path.of("..", "docs", "getting-started.md"));
        String publicSmokeTarget =
                "s3://cmas-smoke-testcase/smoke_example_case/2018gg_18j/inputs/htap/";
        assertThat(gettingStarted).contains(publicSmokeTarget)
                .contains("--region us-east-1 --no-sign-request")
                .contains("--format parquet -o /out")
                .contains("resume /out");

        parse("list", publicSmokeTarget, "--region", "us-east-1", "--no-sign-request");
        parse("list", publicSmokeTarget, "--region", "us-east-1", "--no-sign-request",
                "--format", "parquet", "-o", "/out");
        parse("resume", "/out");
    }

    private static void parse(String... args) {
        assertThat(App.commandLine().parseArgs(args).commandSpec().name()).isNotBlank();
    }
}
