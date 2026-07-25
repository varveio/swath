/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.store.s3.LocalStackSupport;
import io.varve.swath.testkit.Keyspaces;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * INT-12 as the contract actually states it: an <b>actual
 * {@code swath … | head} process</b> exits <b>0</b> with no stack trace when the
 * downstream reader closes the pipe early. The in-JVM {@code ListRunnerTest} only injects a
 * fake {@code Writer} exception and never exercises a real broken pipe / process exit code. Here
 * we run the built {@code App} in a child JVM, pipe it through {@code head -n 5} under {@code
 * pipefail}, and assert the pipeline (hence swath) exits 0 cleanly.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class Int12BrokenPipeProcessIT {

    @Container
    static final LocalStackContainer LOCALSTACK = LocalStackSupport.s3Container();

    private static S3Client s3;

    @BeforeAll
    static void setUp() {
        s3 = LocalStackSupport.client(LOCALSTACK);
    }

    @AfterAll
    static void tearDown() {
        if (s3 != null) {
            s3.close();
        }
    }

    @Test
    @Timeout(120)
    void swathPipedToHeadExitsZeroWithNoStackTrace(@TempDir Path tmp) throws Exception {
        String bucket = "int12-pipe";
        LocalStackSupport.createBucket(s3, bucket);
        // Many keys so swath keeps writing well after head has taken its 5 lines and
        // closed the pipe — that's what triggers the broken-pipe path on swath's stdout.
        LocalStackSupport.putObjects(s3, bucket, Keyspaces.exactly(8000), 96);

        String java = System.getProperty("java.home") + "/bin/java";
        String classpath = System.getProperty("java.class.path");
        Path errFile = tmp.resolve("swath.err");

        // pipefail ⇒ the pipeline's exit status is swath's (head always exits 0), so a
        // swath crash would surface as a non-zero pipeline exit.
        String swath = quote(java) + " -cp " + quote(classpath) + " io.varve.swath.cli.App list "
                + "s3://" + bucket + " --format jsonl"
                + " --endpoint-url " + quote(LOCALSTACK.getEndpoint().toString())
                + " --force-path-style --region " + LOCALSTACK.getRegion();
        String script = "set -o pipefail; " + swath + " 2>" + quote(errFile.toString()) + " | head -n 5";

        ProcessBuilder pb = new ProcessBuilder("bash", "-c", script);
        pb.environment().put("AWS_ACCESS_KEY_ID", LOCALSTACK.getAccessKey());
        pb.environment().put("AWS_SECRET_ACCESS_KEY", LOCALSTACK.getSecretKey());
        pb.environment().put("AWS_REGION", LOCALSTACK.getRegion());
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);

        Process p = pb.start();
        if (!p.waitFor(120, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new AssertionError("swath | head timed out");
        }

        String stderr = Files.exists(errFile)
                ? Files.readString(errFile, StandardCharsets.UTF_8) : "";

        assertThat(p.exitValue())
                .as("swath | head must exit 0 (clean broken-pipe), stderr=<%s>", stderr)
                .isZero();
        assertThat(stderr)
                .as("no stack trace / unexpected-error on a broken pipe")
                .doesNotContain("\tat ")
                .doesNotContain("Exception")
                .doesNotContain("unexpected error")
                // The end-to-end pin on wasBrokenPipe() -> completionStatus -> the auto summary
                // being suppressed ENTIRELY, not merely reworded: `swath list | head` is the most
                // ordinary interactive workflow there is and must never be dressed up as an
                // incident, so neither the INCOMPLETE marker, nor the neutral broken-pipe
                // disposition, nor the statistics block itself may reach a default-flags stderr.
                .doesNotContain("INCOMPLETE")
                .doesNotContain("downstream closed")
                .doesNotContain("objects in")
                .doesNotContain("API calls");
    }

    private static String quote(String s) {
        return "'" + s.replace("'", "'\\''") + "'";
    }
}
