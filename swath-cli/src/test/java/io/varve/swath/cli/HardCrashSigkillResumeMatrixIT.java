/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.store.s3.LocalStackSupport;
import io.varve.swath.testkit.CrashResumeOracle;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.ParquetReads;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * <b>Parametric external-kill crash-safety matrix.</b> A real {@code kill -9} of a
 * built {@code swath} subprocess at MANY lifecycle points, in BOTH {@code --no-sort} and
 * {@code --sort} modes, resumed (possibly repeatedly) to completion, asserting the final output is
 * <b>identical to a clean un-killed baseline</b> via the {@link CrashResumeOracle}.
 *
 * <p>This generalizes {@link HardCrashSigkillResumeProcessIT} (which kills at a single point, no-sort
 * only, and asserts exactly-once inline) into a JUnit {@link ParameterizedTest} over an enum of
 * {@link KillPoint}s. Each kill point pairs a <b>mode</b> ({@code sorted}) with a
 * <b>filesystem-observed</b> kill predicate — because the subprocess DISCARDs stdout there is no
 * waitable log marker. Most kills poll the output/staging tree for the artifact that marks that phase
 * (extending {@code awaitFinalizedPart}). The second no-sort kill instead polls the checkpoint's
 * monotonic {@code listing_node.pages_emitted}: bare {@code swath resume} deliberately does not replay
 * the first invocation's output-rotation fixture flags, so another finalized part is not an
 * emission-equivalent mid-listing marker. Both predicates are strictly increasing progress signals.
 *
 * <h2>Honesty about whether a kill actually landed</h2>
 * The window between "the observed artifact appeared" and "the subprocess would otherwise have
 * finished on its own" can be arbitrarily small — a run can legitimately race to completion before
 * {@code destroyForcibly()} lands. Silently falling back to a clean-run oracle check in that case would
 * make a kill point <b>perpetually vacuous</b> with nothing surfaced. This class is honest about it:
 * <ul>
 *   <li>{@link KillPoint#required} points ({@code NOSORT_MID_LISTING},
 *       {@code NOSORT_MID_LISTING_REPEATED}, {@code SORT_MID_LISTING}) are <b>reliably observable</b>
 *       reliably observable — a degrade there FAILS the test outright, since it
 *       is a regression signal, not benign flakiness.</li>
 *   <li>Genuinely-transient points ({@code SORT_MID_MERGE}, {@code SORT_POST_MERGE_PRE_SUCCESS},
 *       {@code SORT_POST_SUCCESS}) get a bounded retry — up to {@link #MAX_TRANSIENT_ATTEMPTS} full
 *       relaunch attempts to catch the window — before accepting a degrade. The deterministic in-JVM
 *       tests ({@code SortMergeReentryContractTest}, {@code SortResumeCascadeScaleTest},
 *       {@code SortResumeRelocatedWorkdirTest}) remain the load-bearing guard for those points; this
 *       matrix is the end-to-end confirmation when it manages to catch the window.</li>
 *   <li>Every case prints a one-line, greppable outcome to stdout —
 *       {@code kill_point=<NAME> outcome=KILLED} or {@code outcome=DEGRADED_TO_CLEAN} — so a run's
 *       actual kill/degrade tally is visible in the test log, never silent.</li>
 *   <li>The SIGKILL-vs-natural-exit race itself (destroy the process, then discover it had already
 *       exited 0 on its own) is treated as a degrade, not a spurious "SIGKILLed process exited 0"
 *       assertion failure.</li>
 * </ul>
 *
 * <h2>The kill-point matrix</h2>
 * <ul>
 *   <li><b>NOSORT_MID_LISTING</b> (required) — first finalized {@code data/part-*.parquet}
 *       (footer-readable).</li>
 *   <li><b>NOSORT_MID_LISTING_REPEATED</b> (required) — kill at a finalized part, resume, then kill
 *       again once MORE parts have finalized, then resume to completion.</li>
 *   <li><b>SORT_MID_LISTING</b> (required) — a staging segment ({@code seg-*.pageseg}, tolerating
 *       the legacy {@code seg-*.parquet} suffix) appears.</li>
 *   <li><b>SORT_MID_MERGE</b> (transient, retried) — a merge intermediate ({@code merge-*.pageseg} /
 *       {@code *.tmp}) appears in staging (see the mid-merge reconciliation note below).</li>
 *   <li><b>SORT_POST_MERGE_PRE_SUCCESS</b> (transient, retried) — a final {@code data/*.parquet}
 *       exists but no manifest.</li>
 *   <li><b>SORT_POST_SUCCESS</b> (transient, retried) — {@code _SUCCESS} present. Whether or not the
 *       kill itself lands (a near-terminal window), this point <b>always</b> additionally drives an
 *       explicit {@code swath resume} against the now-complete dataset and asserts it is a no-op (exit 0,
 *       oracle-identical) — the deterministic way to exercise post-complete idempotency without
 *       depending on catching a microsecond kill window.</li>
 * </ul>
 *
 * <p>The staging/artifact predicates glob defensively for both the current
 * {@code .swath-sort-segments} staging directory and a possible future {@code _staging} rename, and
 * for both the {@code sorted-} and {@code part-} final-file prefixes, so the matrix survives that
 * rename exactly as the oracle does.
 *
 * <p><b>The mid-merge kill point does not force a full re-list.</b> A crash during the sort merge
 * might appear to discard the entire listing, since the checkpoint is listing-only — but the resume
 * after a mid-merge kill re-uses the durable staging segments and re-runs only the merge — ZERO new
 * LIST fetches — which the deterministic in-JVM guards ({@code SortMergeReentryContractTest}, {@code
 * SortResumeCascadeScaleTest}, {@code SortResumeRelocatedWorkdirTest}) prove structurally ({@code
 * runSortMergeOnly} takes no fetcher) and via {@code SORT.merge_redone == 1}. This subprocess case is
 * the end-to-end confirmation when it lands a real kill.
 *
 * <p>Opt-in ({@code -Dswath.it.sigkill=true}), {@code @Tag("integration")}, Docker-gated — same gate
 * as {@link HardCrashSigkillResumeProcessIT}. The full matrix runs nightly/deep; a fast local run
 * proves at least one no-sort and one --sort kill point end-to-end.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
@EnabledIfSystemProperty(named = "swath.it.sigkill", matches = "true",
        disabledReason = "opt-in: -Dswath.it.sigkill=true (real kill -9 of a swath subprocess; see class javadoc)")
class HardCrashSigkillResumeMatrixIT {

    private static final int OBJECTS = 20_000;
    private static final String BUCKET = "sigkill-matrix";
    // Force a genuine multi-pass merge cascade in --sort mode so mid-merge intermediates are observable:
    // small segment-entries ⇒ ~100 sealed segments; small fan-in ⇒ multiple merge passes.
    private static final int SORT_SEGMENT_ENTRIES = 200;
    private static final int SORT_FAN_IN = 8;
    /** Bounded retry for genuinely-transient kill points: relaunch up to this many
     * times trying to catch a microsecond window before accepting (and honestly reporting) a degrade. */
    private static final int MAX_TRANSIENT_ATTEMPTS = 3;

    @Container
    static final LocalStackContainer LOCALSTACK = LocalStackSupport.s3Container();

    private static S3Client s3;
    private static List<byte[]> keyspace;
    private static Path baselineRoot;
    /** Clean un-killed baseline dataset roots, keyed by sorted-mode, built once and reused. */
    private static final Map<Boolean, Path> BASELINES = new HashMap<>();

    @BeforeAll
    static void setUp() throws Exception {
        s3 = LocalStackSupport.client(LOCALSTACK);
        LocalStackSupport.createBucket(s3, BUCKET);
        keyspace = Keyspaces.exactly(OBJECTS);
        LocalStackSupport.putObjects(s3, BUCKET, keyspace, 96);
        baselineRoot = Files.createTempDirectory("swath-crash-matrix-baseline");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (s3 != null) {
            s3.close();
        }
        if (baselineRoot != null) {
            deleteRecursively(baselineRoot);
        }
    }

    /** Whether a kill-round sequence actually landed a real SIGKILL, or the run degraded to a clean
     * completion (either the process finished before the window opened, or won the destroy-vs-exit
     * race) — reported honestly rather than silently treated as equivalent to a real kill. */
    private enum Outcome { KILLED, DEGRADED_TO_CLEAN }

    /**
     * A crash-safety matrix entry: SIGKILL a real {@code swath} subprocess when a filesystem-observed
     * progress signal first exceeds the last-observed value, resume (possibly repeatedly) to
     * completion, and assert the output is identical-to-clean via {@link CrashResumeOracle}.
     * {@link KillPoint#required} points must land a real kill (a degrade FAILS); transient points get
     * a bounded retry and, on exhaustion, honestly report a degrade rather than passing silently.
     */
    @ParameterizedTest(name = "{0}")
    @EnumSource(KillPoint.class)
    @Timeout(1800)
    void killThenResumeMatchesBaseline(KillPoint kp, @TempDir Path tmp) throws Exception {
        Path baseline = baselineFor(kp.sorted);

        Path outDir;
        Path ckpt;
        Outcome outcome;

        if (kp.required) {
            outDir = Files.createDirectories(tmp.resolve("out"));
            ckpt = CheckpointOptions.CheckpointMode.colocatedCheckpoint(outDir);
            outcome = runKillRounds(kp, outDir, ckpt, tmp);
            logOutcome(kp, outcome);
            assertThat(outcome)
                    .as("kill point %s is reliably-observable in this harness (confirmed KILLED by an "
                            + "repeated observation) — a degrade here is a REGRESSION, not benign flakiness", kp)
                    .isEqualTo(Outcome.KILLED);
        } else {
            Path attemptOut = null;
            Path attemptCkpt = null;
            Outcome last = null;
            for (int attempt = 0; attempt < MAX_TRANSIENT_ATTEMPTS; attempt++) {
                Path attemptDir = tmp.resolve("attempt-" + attempt);
                attemptOut = Files.createDirectories(attemptDir.resolve("out"));
                attemptCkpt = CheckpointOptions.CheckpointMode.colocatedCheckpoint(attemptOut);
                last = runKillRounds(kp, attemptOut, attemptCkpt, attemptDir);
                if (last == Outcome.KILLED) {
                    break;
                }
            }
            outDir = attemptOut;
            ckpt = attemptCkpt;
            outcome = last;
            // Transient points are allowed to exhaust their retries without failing — but the outcome
            // is always printed, so a perpetually-degraded point is visible in the test log, not silent.
            logOutcome(kp, outcome);
        }

        if (kp == KillPoint.SORT_POST_SUCCESS) {
            // Deterministic idempotency check, independent of whether the kill itself landed —
            // drive an EXPLICIT resume against the now-complete dataset and assert it's a no-op.
            Path explicitErr = tmp.resolve("post-success-explicit-resume.err");
            Process explicit = launch(BUCKET, outDir, kp.sorted, true, explicitErr);
            assertThat(explicit.waitFor(120, TimeUnit.SECONDS))
                    .as("explicit swath resume of a completed --sort dataset finished").isTrue();
            assertThat(explicit.exitValue())
                    .as("explicit swath resume of a completed --sort dataset is a no-op, exits 0, stderr=<%s>",
                            readErr(explicitErr))
                    .isZero();
        }

        // Identical-to-clean: same object set + exactly-once (+ global order for --sort).
        CrashResumeOracle.assertResumeMatchesBaseline(baseline, outDir, kp.sorted);
    }

    /**
     * Run {@code kp.rounds} kill attempts against {@code outDir}/{@code ckpt}, each launching (or
     * resuming) the subprocess and waiting for its progress signal to strictly exceed the
     * last-observed value before {@code destroyForcibly()}. The repeated no-sort resume watches the
     * checkpoint page counter rather than output parts: unlike the old {@code list --resume} form,
     * {@code swath resume <dir>} does not inherit this fixture's {@code --part-rotation-max-rows 500}, so its
     * next part may finalize only while the process is closing. Returns {@link Outcome#DEGRADED_TO_CLEAN}
     * the moment any round fails to land a real kill (process finished on its own, or won the
     * destroy-vs-exit race); otherwise, after every round lands, issues one final
     * explicit {@code swath resume} to completion and returns {@link Outcome#KILLED}.
     */
    private static Outcome runKillRounds(KillPoint kp, Path outDir, Path ckpt, Path errDir) throws Exception {
        long threshold = 0;
        for (int round = 0; round < kp.rounds; round++) {
            boolean resume = round > 0;
            Path errFile = errDir.resolve("k" + round + ".err");
            long checkpointPageThreshold = resume && kp == KillPoint.NOSORT_MID_LISTING_REPEATED
                    ? SigkillResumeHarnessSupport.listingPagesEmitted(ckpt)
                    : 0;
            Process p = launch(BUCKET, outDir, kp.sorted, resume, errFile);
            long observed = resume && kp == KillPoint.NOSORT_MID_LISTING_REPEATED
                    ? awaitCheckpointPagesAbove(ckpt, p, checkpointPageThreshold, 300_000)
                    : awaitProgressAbove(outDir, p, kp.progress, threshold, 300_000);
            if (!p.isAlive()) {
                // Ran to completion before the kill window opened — a benign degrade to a clean run.
                assertThat(p.waitFor(60, TimeUnit.SECONDS)).isTrue();
                assertThat(p.exitValue())
                        .as("subprocess completed before the kill window, stderr=<%s>", readErr(errFile))
                        .isZero();
                return Outcome.DEGRADED_TO_CLEAN;
            }
            long observedThreshold = resume && kp == KillPoint.NOSORT_MID_LISTING_REPEATED
                    ? checkpointPageThreshold
                    : threshold;
            assertThat(observed).as("kill point %s reached (progress %d > %d)", kp, observed, observedThreshold)
                    .isGreaterThan(observedThreshold);
            p.destroyForcibly();   // SIGKILL on Unix — no clean flush
            assertThat(p.waitFor(60, TimeUnit.SECONDS)).as("the killed process exited").isTrue();
            if (p.exitValue() == 0) {
                // Destroy-vs-exit race — the process had already finished cleanly on its own before
                // the SIGKILL took effect. Not a real kill; treat exactly like the !isAlive() branch.
                return Outcome.DEGRADED_TO_CLEAN;
            }
            threshold = observed;
        }
        // Every round landed a real kill: finish with one explicit resume to completion.
        Path resumeErr = errDir.resolve("resume-final.err");
        Process last = launch(BUCKET, outDir, kp.sorted, true, resumeErr);
        assertThat(last.waitFor(300, TimeUnit.SECONDS)).as("the resumed run finished").isTrue();
        assertThat(last.exitValue()).as("resume exits 0, stderr=<%s>", readErr(resumeErr)).isZero();
        return Outcome.KILLED;
    }

    private static void logOutcome(KillPoint kp, Outcome outcome) {
        System.out.printf("kill_point=%s outcome=%s%n", kp, outcome);
    }

    /** Build (once, lazily) a clean un-killed baseline dataset for the given mode and cache its root. */
    private static synchronized Path baselineFor(boolean sorted) throws Exception {
        Path cached = BASELINES.get(sorted);
        if (cached != null) {
            return cached;
        }
        String tag = sorted ? "sort" : "nosort";
        Path out = baselineRoot.resolve(tag);
        Files.createDirectories(out);
        Process p = launch(BUCKET, out, sorted, false, baselineRoot.resolve(tag + ".err"));
        assertThat(p.waitFor(300, TimeUnit.SECONDS)).as("clean %s baseline finished", tag).isTrue();
        assertThat(p.exitValue())
                .as("clean %s baseline exits 0, stderr=<%s>", tag, readErr(baselineRoot.resolve(tag + ".err")))
                .isZero();
        BASELINES.put(sorted, out);
        return out;
    }

    // ------------------------------------------------------------------------
    // The kill-point matrix.
    // ------------------------------------------------------------------------

    @FunctionalInterface
    interface FsProgress {
        long of(Path root) throws IOException;
    }

    enum KillPoint {
        /** no-sort, mid-listing: first finalized {@code data/part-*.parquet} (durable prefix survives).
         * Required — reliably observable. */
        NOSORT_MID_LISTING(false, 1, true, HardCrashSigkillResumeMatrixIT::footerReadableParts),
        /** no-sort: kill, resume, kill again after MORE parts finalize, resume to completion.
         * Required — reliably observable. */
        NOSORT_MID_LISTING_REPEATED(false, 2, true, HardCrashSigkillResumeMatrixIT::footerReadableParts),
        /** --sort, mid-listing: a staging segment appears. Required — reliably observable. */
        SORT_MID_LISTING(true, 1, true, HardCrashSigkillResumeMatrixIT::stagingSegmentCount),
        /** --sort, mid-merge: a merge intermediate / {@code *.tmp} appears in staging (see the
         * mid-merge reconciliation note in the class javadoc). Transient — genuinely narrow window,
         * retried. */
        SORT_MID_MERGE(true, 1, false, HardCrashSigkillResumeMatrixIT::mergeIntermediateCount),
        /** --sort, post-merge pre-{@code _SUCCESS}: a final part exists but no manifest yet.
         * Transient — retried. */
        SORT_POST_MERGE_PRE_SUCCESS(true, 1, false, HardCrashSigkillResumeMatrixIT::finalPresentWithoutManifest),
        /** --sort, just after {@code _SUCCESS}. Transient — retried; ALSO always driven through an
         * explicit post-complete {@code swath resume} idempotency check regardless of outcome (see class
         * javadoc). */
        SORT_POST_SUCCESS(true, 1, false, HardCrashSigkillResumeMatrixIT::successMarkerPresent);

        final boolean sorted;
        final int rounds;
        /** Reliably-observable in this harness: a degrade here FAILS the test (regression signal), no
         * retry. False ⇒ genuinely-transient: bounded retry, honest degrade on exhaustion. */
        final boolean required;
        final FsProgress progress;

        KillPoint(boolean sorted, int rounds, boolean required, FsProgress progress) {
            this.sorted = sorted;
            this.rounds = rounds;
            this.required = required;
            this.progress = progress;
        }
    }

    // ------------------------------------------------------------------------
    // Filesystem-observed progress signals (each strictly increases through its phase).
    // ------------------------------------------------------------------------

    /** Count of {@code data/} parts whose Parquet footer is present (an in-flight open part is skipped). */
    private static long footerReadableParts(Path root) throws IOException {
        long n = 0;
        for (Path part : DatasetLayout.of(root).dataParts()) {
            try {
                ParquetReads.schema(part);   // footer-only open; no full row scan
                n++;
            } catch (Exception footerlessOpenPart) {
                // mid-write / footerless — not durable, does not count
            }
        }
        return n;
    }

    /** Count of sealed staging segments under either staging-dir spelling: the current page-run
     * {@code seg-*.pageseg} suffix and the older columnar {@code seg-*.parquet} suffix, so the kill
     * point fires on the current format and stays tolerant of the legacy one. */
    private static long stagingSegmentCount(Path root) throws IOException {
        Path dir = stagingDir(root);
        return countMatching(dir, "seg-", ".pageseg") + countMatching(dir, "seg-", ".parquet");
    }

    /** Count of merge intermediates / in-flight final tmp files under staging — the mid-merge signal.
     * Counts both the current page-run {@code merge-*.pageseg} intermediates and the older columnar
     * {@code merge-*.parquet} ones, plus the final part's {@code .tmp}. */
    private static long mergeIntermediateCount(Path root) throws IOException {
        Path dir = stagingDir(root);
        long merges = countMatching(dir, "merge-", ".pageseg") + countMatching(dir, "merge-", ".parquet");
        long tmps = countSuffix(dir, ".tmp");
        return merges + tmps;
    }

    /** 1 once a final {@code data/*.parquet} exists but the manifest has not yet been written. */
    private static long finalPresentWithoutManifest(Path root) throws IOException {
        DatasetLayout layout = DatasetLayout.of(root);
        boolean anyFinal = !layout.dataParts().isEmpty();
        boolean published = Files.exists(layout.manifest());
        return (anyFinal && !published) ? 1 : 0;
    }

    /** 1 once the {@code _SUCCESS} marker is present (the fully-published signal). */
    private static long successMarkerPresent(Path root) {
        return Files.exists(DatasetLayout.of(root).success()) ? 1 : 0;
    }

    /** The sort staging directory: tolerates both the current name and a possible future {@code
     * _staging} rename. */
    private static Path stagingDir(Path root) {
        Path current = root.resolve(ListCommand.SORT_STAGING_DIR);
        if (Files.isDirectory(current)) {
            return current;
        }
        Path renamed = root.resolve("_staging");
        return Files.isDirectory(renamed) ? renamed : current;
    }

    private static long countMatching(Path dir, String prefix, String suffix) throws IOException {
        if (!Files.isDirectory(dir)) {
            return 0;
        }
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.map(p -> p.getFileName().toString())
                    .filter(n -> n.startsWith(prefix) && n.endsWith(suffix)).count();
        }
    }

    private static long countSuffix(Path dir, String suffix) throws IOException {
        if (!Files.isDirectory(dir)) {
            return 0;
        }
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(suffix)).count();
        }
    }

    // ------------------------------------------------------------------------
    // Subprocess machinery (extends HardCrashSigkillResumeProcessIT's launch/await pattern).
    // ------------------------------------------------------------------------

    /**
     * Poll the output tree until {@code progress(root) > threshold} while {@code p} is alive; returns
     * the observed value, or the last-seen value if the process exits first (caller re-checks
     * {@code p.isAlive()}).
     */
    private static long awaitProgressAbove(Path root, Process p, FsProgress progress, long threshold,
            long timeoutMs) throws Exception {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        long last = threshold;
        while (System.nanoTime() < deadline) {
            long current = progress.of(root);
            last = current;
            if (current > threshold) {
                return current;
            }
            if (!p.isAlive()) {
                return last;   // completed / died before the window opened
            }
            Thread.sleep(15);
        }
        return last;
    }

    /**
     * Wait for the resumed engine to commit another LIST page. This marker is invocation-equivalent:
     * both {@code list --resume} and {@code swath resume} advance {@code pages_emitted} in the same
     * checkpoint transaction, before that page can be emitted (I1). It therefore opens the second
     * kill window during listing even when bare resume uses the default part-rotation cadence.
     */
    private static long awaitCheckpointPagesAbove(Path ckpt, Process p, long threshold,
            long timeoutMs) throws Exception {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        long last = threshold;
        while (System.nanoTime() < deadline) {
            long current = SigkillResumeHarnessSupport.listingPagesEmitted(ckpt);
            last = current;
            if (current > threshold) {
                return current;
            }
            if (!p.isAlive()) {
                return last;
            }
            Thread.sleep(15);
        }
        return last;
    }

    private static Process launch(String bucket, Path outDir, boolean sorted, boolean resume,
            Path errFile) throws Exception {
        String java = System.getProperty("java.home") + "/bin/java";
        String classpath = System.getProperty("java.class.path");

        List<String> cmd = new ArrayList<>(List.of(java, "-cp", classpath));
        if (sorted) {
            // Force a genuine multi-pass merge cascade so the mid-merge window is observable.
            cmd.add("-Dswath.sort.segment-entries=" + SORT_SEGMENT_ENTRIES);
            cmd.add("-Dswath.sort.fan-in=" + SORT_FAN_IN);
        }
        cmd.add("io.varve.swath.cli.App");
        if (resume) {
            // The output directory is the whole run handle: resume opens its co-located
            // <dir>/.swath/checkpoint.sqlite (the same DB the create run auto-colocated).
            cmd.addAll(List.of("resume", outDir.toString()));
        } else {
            cmd.addAll(List.of("list", "s3://" + bucket,
                    "--format", "parquet", "-o", outDir.toString(),
                    "--endpoint-url", LOCALSTACK.getEndpoint().toString(),
                    "--force-path-style", "--region", LOCALSTACK.getRegion()));
            if (sorted) {
                cmd.add("--sort");
                cmd.add("--tune");
                cmd.add("sort.ignore-disk-check=on");
            } else {
                // Small row-count rotation so parts finalize early — the pre-crash durable prefix.
                cmd.add("--part-rotation-max-rows");
                cmd.add("500");
            }
        }

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.environment().put("AWS_ACCESS_KEY_ID", LOCALSTACK.getAccessKey());
        pb.environment().put("AWS_SECRET_ACCESS_KEY", LOCALSTACK.getSecretKey());
        pb.environment().put("AWS_REGION", LOCALSTACK.getRegion());
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.to(errFile.toFile()));
        return pb.start();
    }

    private static String readErr(Path errFile) {
        try {
            return Files.exists(errFile) ? Files.readString(errFile, StandardCharsets.UTF_8) : "";
        } catch (Exception e) {
            return "<unreadable: " + e + ">";
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            });
        }
    }
}
