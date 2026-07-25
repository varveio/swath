/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.store.s3.LocalStackSupport;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.ParquetReads;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * <b>True SIGKILL of a real {@code swath} subprocess → {@code swath resume} exactly-once.</b>
 *
 * <p>Unlike the deterministic in-JVM stand-in
 * ({@code io.varve.swath.engine.HardCrashResumeExactlyOnceTest}), this launches the built {@code App} as a child
 * process listing a LocalStack bucket to a Parquet directory (its checkpoint auto-colocates there), lets it emit and
 * finalize some parts, {@code kill -9}s it mid-run via {@link Process#destroyForcibly()} (no clean flush —
 * the open part is left footerless on disk), then relaunches with {@code swath resume} to completion. It asserts
 * the union of every Parquet part equals the full bucket <b>exactly once</b>:
 * {@code count(*) == count(DISTINCT key) == bucket}, no dropped key, no duplicate.
 *
 * <h2>Why this is opt-in ({@code -Dswath.it.sigkill=true})</h2>
 * This class stays env-gated (rather than folded into the default suite) because it spawns real
 * subprocesses and does a real {@code kill -9} — heavier and slightly riskier under CI resource
 * contention than the rest of the fast/PR gate; the deterministic stand-in ({@code
 * HardCrashResumeExactlyOnceTest}) still provides the CI-running exactly-once guarantee across
 * arbitrary crash offsets on every commit.
 *
 * <p><b>Wired into CI.</b> {@code ci.yml}'s {@code deep-tests} job (every main-merge/
 * dispatch) and {@code nightly.yml} both run this exact class with
 * {@code -Dswath.it.sigkill=true} via a dedicated, untagged {@code --tests} invocation (a plain
 * {@code -Pdeep}/{@code -PonlyIntegration} run would tag-filter it out, since this class is
 * {@code @Tag("integration")}, not {@code "deep"}). The default/fast/PR gate and the bulk
 * {@code integration-tests} job never set that system property, so it stays skipped there.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
@EnabledIfSystemProperty(named = "swath.it.sigkill", matches = "true",
        disabledReason = "opt-in: -Dswath.it.sigkill=true (blocked in this environment; see class javadoc)")
class HardCrashSigkillResumeProcessIT {

    private static final int OBJECTS = 20_000;

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
    @Timeout(300)
    void sigkillMidRunThenResumeIsExactlyOnce(@TempDir Path tmp) throws Exception {
        String bucket = "sigkill-resume";
        LocalStackSupport.createBucket(s3, bucket);
        List<byte[]> keyspace = Keyspaces.exactly(OBJECTS);
        LocalStackSupport.putObjects(s3, bucket, keyspace, 96);

        Path outDir = tmp.resolve("out");
        Files.createDirectories(outDir);

        // First run: crash it mid-flight. Small row-count rotation so parts finalize early — the pre-crash
        // durable prefix the resume must carry.
        Process first = launch(bucket, outDir, false, tmp.resolve("first.err"));
        try {
            boolean sawFinalizedPart = awaitFinalizedPart(outDir, first, 120_000);
            assertThat(sawFinalizedPart)
                    .as("swath finalized at least one Parquet part before the crash (durable prefix exists)")
                    .isTrue();
            assertThat(first.isAlive()).as("still mid-run when we SIGKILL it").isTrue();
        } finally {
            first.destroyForcibly();   // SIGKILL on Unix — no clean shutdown / flush
        }
        assertThat(first.waitFor(60, TimeUnit.SECONDS)).as("the killed process exited").isTrue();
        assertThat(first.exitValue()).as("a SIGKILLed process never exits 0").isNotEqualTo(0);

        // The finalized prefix survived the hard crash (footerless open parts are unreadable and ignored).
        long durableRows = readableKeys(outDir).size();
        assertThat(durableRows).as("finalized rows survived the SIGKILL").isGreaterThan(0);
        assertThat(durableRows).as("but the run did not finish (mid-crash)").isLessThan(OBJECTS);

        // Resume the same args to completion.
        Process second = launch(bucket, outDir, true, tmp.resolve("second.err"));
        assertThat(second.waitFor(240, TimeUnit.SECONDS)).as("the resumed run finished").isTrue();
        assertThat(second.exitValue())
                .as("resume exits 0, stderr=<%s>", readErr(tmp.resolve("second.err")))
                .isZero();

        // Exactly-once: the whole bucket, no dropped key, no duplicate.
        List<String> union = readableKeys(outDir);
        TreeSet<String> distinct = new TreeSet<>(union);
        assertThat(union.size()).as("no duplicate rows (count == count DISTINCT)").isEqualTo(distinct.size());
        assertThat(distinct).as("union == full bucket keyspace exactly once")
                .containsExactlyInAnyOrderElementsOf(expectedKeys(keyspace));
    }

    private Process launch(String bucket, Path outDir, boolean resume, Path errFile) throws Exception {
        String java = System.getProperty("java.home") + "/bin/java";
        String classpath = System.getProperty("java.class.path");

        List<String> cmd = new ArrayList<>(List.of(java, "-cp", classpath,
                "io.varve.swath.cli.App"));
        if (resume) {
            // The output directory is the whole run handle: resume opens its co-located
            // <dir>/.swath/checkpoint.sqlite (the same DB the create run auto-colocated).
            cmd.addAll(List.of("resume", outDir.toString()));
        } else {
            cmd.addAll(List.of("list", "s3://" + bucket,
                    "--format", "parquet", "-o", outDir.toString(),
                    "--part-rotation-max-rows", "500",
                    "--endpoint-url", LOCALSTACK.getEndpoint().toString(),
                    "--force-path-style", "--region", LOCALSTACK.getRegion()));
        }

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.environment().put("AWS_ACCESS_KEY_ID", LOCALSTACK.getAccessKey());
        pb.environment().put("AWS_SECRET_ACCESS_KEY", LOCALSTACK.getSecretKey());
        pb.environment().put("AWS_REGION", LOCALSTACK.getRegion());
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.to(errFile.toFile()));
        return pb.start();
    }

    /** Poll until at least one Parquet part is finalized (readable with a footer) while the process runs. */
    private static boolean awaitFinalizedPart(Path dir, Process p, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (!readableKeys(dir).isEmpty()) {
                return true;
            }
            if (!p.isAlive()) {
                return false;   // completed or died before finalizing a part
            }
            Thread.sleep(20);
        }
        return false;
    }

    /** Keys from every part file whose footer is present; footerless (mid-write) parts are skipped. */
    private static List<String> readableKeys(Path dir) {
        List<String> keys = new ArrayList<>();
        try {
            for (Path part : DatasetLayout.of(dir).dataParts()) {
                try {
                    keys.addAll(ParquetReads.keys(part));
                } catch (Exception ignored) {
                    // footerless / mid-write part — not durable, skip it
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("failed listing parts in " + dir, e);
        }
        return keys;
    }

    private static List<String> expectedKeys(List<byte[]> keyspace) {
        return keyspace.stream().map(k -> new String(k, StandardCharsets.UTF_8)).toList();
    }

    private static String readErr(Path errFile) {
        try {
            return Files.exists(errFile) ? Files.readString(errFile, StandardCharsets.UTF_8) : "";
        } catch (Exception e) {
            return "<unreadable: " + e + ">";
        }
    }
}
