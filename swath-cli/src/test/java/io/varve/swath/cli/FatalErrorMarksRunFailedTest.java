/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.error.CancelledException;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.ProtocolViolationException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.JsonRunSummaryWriter;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.runtime.ArgsHashFields;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.ForwardingCheckpointStore;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * A fatal, non-cancellation error escaping the CLI's engine dispatch (a real {@code
 * ListingException}/{@code OutputException}/{@code CheckpointException}/{@code
 * RegionRedirectException}, exhausted retries, etc.) must (a) mark {@code run_meta.status
 * =FAILED} — distinct from the {@code RUNNING} a SIGKILL/interrupt or a graceful {@code
 * --max-duration}/signal cancel leaves behind — and (b) still land a terminal {@code
 * summary.json} carrying a failure {@code stop_reason} so the bucket yields an analyzable
 * failed metrics row. A subsequent {@code swath resume} against that FAILED run must refuse
 * rather than silently re-attempting a deterministic re-failure.
 *
 * <p>{@link ListCommand#runEngineGuarded} is the CLI-level seam ({@code runWithCheckpoint}'s
 * three engine dispatch call sites) — deliberately NOT inside {@link ListRunner} itself, since
 * several engine-level crash-resume tests (e.g. {@code HardCrashResumeExactlyOnceTest}) inject
 * an in-process {@code ListingException} as a stand-in for a real {@code kill -9} and call
 * {@link ListRunner} directly, then resume exactly like a real SIGKILL would (which never runs
 * any of this code) leaves the run resumable as {@code RUNNING}; this guard only fires for a
 * genuine CLI invocation.
 *
 * <p>{@code status==FAILED} alone does NOT distinguish a deterministic fatal error
 * from a broken-pipe truncation ({@link ListRunner} marks FAILED directly on broken pipe too,
 * precisely so INT-12 stays resumable) — the nullable {@code run_meta.fatal_error} column
 * (surfaced as {@link RunMeta#fatalError()}) is the actual distinguisher, set ONLY by the two
 * fatal marks ({@link io.varve.swath.checkpoint.CheckpointStore#markRunFatalUnlessFinished} for a
 * generic unrecoverable failure — this guard / the seed-probe catch — and {@link
 * io.varve.swath.checkpoint.CheckpointStore#markRunUnresumable} for a protocol violation), never by
 * broken pipe's plain {@code markRunFinished}.
 */
final class FatalErrorMarksRunFailedTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String PREFIX = "data/";
    private static final String NO_FILTER_SPEC =
            FilterSpecCodec.encode(null, null, null, null, null, null, null);

    private static RunKey textKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "fatal-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static JsonRunSummaryWriter.Config summaryConfig(Path path) {
        JsonRunSummaryWriter.RunConfig runConfig = new JsonRunSummaryWriter.RunConfig(
                "s3://bucket", "us-east-1", "jsonl", 4, false, null, 30_000L, List.of(),
                EngineToggles.DEFAULT, null, false, null, false);
        return new JsonRunSummaryWriter.Config(path, Duration.ofMinutes(10), "fatal-hash", runConfig,
                List.of("list", "s3://bucket"));
    }

    /**
     * The exact seam {@code runWithCheckpoint} wires: a real {@link ListRunner} engine call that
     * throws, wrapped in {@link ListCommand#runEngineGuarded}. Proves both halves together —
     * the run_meta row flips to FAILED AND the sidecar still lands with a failure
     * {@code stop_reason} — using the real {@link SqliteCheckpointStore} (not a test double).
     */
    @Test
    void fatalListingExceptionMarksRunFailedAndStillWritesASummary(@TempDir Path dir) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(500))
                .interceptor((req, idx, page) -> {
                    // Inject on the FIRST fetch (idx==0), NOT a mid-stream index. Deterministic:
                    // at least one fetchPage always happens, so the fatal always fires exactly once.
                    // A mid-stream idx (e.g. idx==2) only fires if enough work-stealing seed-probe
                    // pre-fetches occur to reach that global count — a scheduling-dependent count that
                    // is 0 here (500 keys < maxKeys 1000 fit in ONE page), so idx==2 flaked in CI.
                    // The initial seed listing is always the first fetch (a thief can only probe a
                    // victim that has already committed a page — WorkStealingScan progress-gated
                    // stealing), so idx==0 has no sibling-cancellation race over which exception the
                    // scope surfaces: exactly one ListingException is thrown and it propagates cleanly.
                    // Do NOT change this back to a mid-stream index.
                    if (idx == 0) {
                        throw new ListingException("injected fatal listing error", null);
                    }
                    return page;
                })
                .build();
        Path sidecar = dir.resolve("summary.json");
        Path db = dir.resolve("c.sqlite");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(textKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);
            ListRunner.Spec spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8000, 1000,
                    FilterChain.EMPTY, null, summaryConfig(sidecar));
            RunContext ctx = RunContext.create();

            assertThatThrownBy(() -> ListCommand.runEngineGuarded(store, run.id(), () -> {
                new ListRunner().runWorkStealing(
                        ctx, fetcher, new StringWriter(), spec, store, run.id(), 4, seeds);
                return null;
            })).isInstanceOf(ListingException.class);

            assertThat(CheckpointDbProbe.runStatusEnum(db, run.id()))
                    .as("a fatal in-process error marks the run FAILED, not left RUNNING")
                    .isEqualTo(RunStatus.FAILED);
        }

        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("completed").asBoolean()).isFalse();
        assertThat(root.get("stop_reason").asText()).isEqualTo("crash");
    }

    /** CancelledException (a graceful --max-duration/signal stop) must NOT mark the run FAILED. */
    @Test
    void cancelledExceptionDoesNotMarkTheRunFailed() throws Exception {
        AtomicBoolean markedFailed = new AtomicBoolean();
        CheckpointStore store = failingMarkTracker(markedFailed);

        assertThatThrownBy(() -> ListCommand.runEngineGuarded(store, 1L, () -> {
            throw new CancelledException("timebox");
        })).isInstanceOf(CancelledException.class);

        assertThat(markedFailed).as("a cancellation must leave the run RUNNING, not FAILED").isFalse();
    }

    /** A plain InterruptedException (SIGKILL/interrupt analog) must NOT mark the run FAILED either. */
    @Test
    void interruptedExceptionDoesNotMarkTheRunFailed() throws Exception {
        AtomicBoolean markedFailed = new AtomicBoolean();
        CheckpointStore store = failingMarkTracker(markedFailed);

        assertThatThrownBy(() -> ListCommand.runEngineGuarded(store, 1L, () -> {
            throw new InterruptedException("interrupted");
        })).isInstanceOf(InterruptedException.class);

        assertThat(markedFailed).as("an interrupt must leave the run RUNNING, not FAILED").isFalse();
    }

    /**
     * A protocol violation discovered while the scan is already unwinding is attached to whatever
     * ended the scan as a SUPPRESSED exception — the failure that ended it stays the primary story.
     * When that primary is a cancellation, every classification keyed off it says "resumable
     * partial", so the run would stay resumable and the next {@code swath resume} would walk
     * straight back into the endpoint that violated the protocol. The run must end fatal-FAILED, and
     * the marker must name the violation's {@code error_class} so the refusal is diagnosable.
     */
    @Test
    void aProtocolViolationSuppressedUnderACancelMarksTheRunFatal(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        CancelledException cancelled = new CancelledException("signal");
        cancelled.addSuppressed(ProtocolViolationException.oversizedPage(BUCKET, 1000, 1400, 0));

        Logger logger =
                (Logger) LoggerFactory.getLogger(ListCommand.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level previous = logger.getLevel();
        logger.setLevel(Level.ERROR);
        logger.addAppender(appender);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(textKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));

            assertThatThrownBy(() -> ListCommand.runEngineGuarded(store, run.id(), () -> {
                throw cancelled;
            })).isSameAs(cancelled);

            RunMeta reopened = store.openRun(textKey(), true, false);
            assertThat(reopened.status())
                    .as("a run that hit a protocol violation must not be left resumable")
                    .isEqualTo(RunStatus.FAILED);
            assertThat(reopened.fatalError())
                    .as("only the fatal flag makes `swath resume` refuse the run")
                    .isTrue();
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }

        String marker = appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("list_protocol_violation_fatal"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no list_protocol_violation_fatal marker emitted"));
        assertThat(marker).contains("error_class=oversized_page");
        assertThat(ExitCodes.forThrowable(cancelled))
                .as("the exit code must not advertise a resumable cancel either")
                .isEqualTo(1);
    }

    /**
     * The violation can just as well land on a cause of the terminal exception rather than on the
     * exception itself, so the classification reads the whole chain — cause links and the suppressed
     * exceptions hanging off each of them.
     */
    @Test
    void aProtocolViolationSuppressedOnACauseMarksTheRunFatal() throws Exception {
        AtomicBoolean markedFailed = new AtomicBoolean();
        CheckpointStore store = failingMarkTracker(markedFailed);

        ListingException scanFailure = new ListingException("connection reset", null);
        scanFailure.addSuppressed(ProtocolViolationException.oversizedPage(BUCKET, 1000, 1400, 0));
        InvalidConfigException terminal =
                new InvalidConfigException("bad config", scanFailure);

        assertThatThrownBy(() -> ListCommand.runEngineGuarded(store, 1L, () -> {
            throw terminal;
        })).isSameAs(terminal);

        assertThat(markedFailed)
                .as("a violation nested under the terminal exception is still a violation")
                .isTrue();
    }

    /**
     * A fatal seed-probe failure inserts ZERO nodes; without marking the run FAILED, a later
     * bare {@code swath resume} would find {@code nodes.isEmpty()} and report a false "nothing to
     * resume, must be done" completed no-op (exit 0) — silently losing a bucket that never
     * listed anything. {@code seedFreshRun}'s catch marks the run fatal-FAILED (exactly what
     * this seeds), so the resume-refusal check (which runs BEFORE {@code loadResumable}/the
     * empty-nodes check) catches it first.
     */
    @Test
    void seedProbeFailureLeavesAFatalFailedRunNotAFalseCompletedNoOp(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path sidecar = dir.resolve("summary.json");
        RunKey key = checkpointKey(OutputFormat.JSONL);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            // ZERO nodes inserted — exactly what a seed-probe failure before insertNodes leaves.
            store.markRunFatalUnlessFinished(run.id());   // the seed-probe catch's mark call
        }

        ListCommand cmd = resumeCommand(db, sidecar);
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(2));

        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("stop_reason").asText())
                .as("must be refused, not a false completed no-op for a run that never listed anything")
                .isEqualTo("resume_refused");
    }

    /**
     * A bare `swath resume` against a run the fatal-error guard marked FAILED (fatal_error=1)
     * refuses instead of silently re-attempting a deterministic re-failure.
     */
    @Test
    void resumeAgainstAFatalFailedRunRefuses(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path sidecar = dir.resolve("summary.json");
        seedFatalFailedRun(db);

        ListCommand cmd = resumeCommand(db, sidecar);

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("FAILED")
                .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(2));

        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("completed").asBoolean()).isFalse();
        assertThat(root.get("stop_reason").asText()).isEqualTo("resume_refused");
    }

    /**
     * A broken-pipe FAILED run (status=FAILED, {@code fatal_error} left unset — exactly how
     * {@link ListRunner}'s broken-pipe path marks it, INT-12) must NOT be refused; {@code swath
     * resume} continuing a truncated-stdout listing is the whole point of that path. Seeded with
     * its one node already COMPLETED so the resumed call takes the network-free "nothing left to
     * resume" completed no-op, proving it got PAST the FAILED check rather than being refused.
     */
    @Test
    void resumeAgainstABrokenPipeFailedRunIsNotRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path sidecar = dir.resolve("summary.json");
        seedBrokenPipeFailedRun(db);

        ListCommand cmd = resumeCommand(db, sidecar);
        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);

        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("stop_reason").asText())
                .as("a broken-pipe FAILED run must resume normally, never hit resume_refused")
                .isNotEqualTo("resume_refused");
    }

    /** A regression guard: the existing SIGKILL-left-RUNNING resume path is unaffected. */
    @Test
    void resumeAgainstARunningRunStillWorks(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        RunKey key = checkpointKey(OutputFormat.JSONL);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            // left RUNNING — no markRunFinished call, exactly like a SIGKILL mid-run.

            RunMeta resumed = store.openRun(key, true, false);
            assertThat(resumed.resumed()).isTrue();
            assertThat(resumed.status()).isEqualTo(RunStatus.RUNNING);
            assertThat(resumed.fatalError()).isFalse();
        }
    }

    /**
     * An exit-2 user-correctable config/args error (bad flag, missing {@code -o}, …) escaping
     * the guarded body must NOT mark the run fatal-FAILED — it is not the unrecoverable
     * in-process listing/output/checkpoint failure this guard exists to flag, and marking it
     * would wrongly refuse a LATER, corrected invocation's resume.
     */
    @Test
    void invalidConfigExceptionDoesNotMarkTheRunFailed() throws Exception {
        AtomicBoolean markedFailed = new AtomicBoolean();
        CheckpointStore store = failingMarkTracker(markedFailed);

        assertThatThrownBy(() -> ListCommand.runEngineGuarded(store, 1L, () -> {
            throw new InvalidConfigException("Parquet output requires -o <dir>");
        })).isInstanceOf(InvalidConfigException.class);

        assertThat(markedFailed).as("a user-correctable config error must not mark the run fatal").isFalse();
    }

    /**
     * {@code --format parquet} with no {@code -o} throws deep inside the guarded {@code
     * runEngineParquet} body ({@code openParquetDir()}). Seeded as a resumed run (skips the
     * network seed probe) with one still-pending node so dispatch reaches {@code
     * runEngineGuarded}; the checkpoint never had an output path stored either, so {@code
     * openParquetDir()} still fails post-restore exactly like a fresh {@code --format parquet}
     * invocation with no {@code -o} would. Asserts the run is left exactly as it was (RUNNING,
     * not fatal-FAILED) — a later corrected invocation (with {@code -o} supplied) would resume
     * normally, not be refused.
     */
    @Test
    void parquetMissingOutputDirDoesNotMarkTheRunFatal(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        RunKey key = checkpointKey(OutputFormat.PARQUET);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));   // left PENDING — a non-empty resume

            ListCommand cmd = new ListCommand();
            cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
            cmd.connection.endpointUrl = ENDPOINT;
            cmd.checkpoint.location = db.toString();
            cmd.checkpoint.resume = true;
            // cmd.output.format / cmd.output.destination deliberately left unset: format restores PARQUET from the
            // checkpoint, but no output path was ever stored (never had -o) — openParquetDir()
            // must still refuse with InvalidConfigException, not silently default anything.

            assertThatThrownBy(cmd::call)
                    .isInstanceOf(InvalidConfigException.class)
                    .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(2));

            RunMeta reopened = store.openRun(key, true, false);
            assertThat(reopened.status())
                    .as("a fixable -o typo must not be treated as an unrecoverable in-process failure")
                    .isEqualTo(RunStatus.RUNNING);
            assertThat(reopened.fatalError()).isFalse();
        }
    }

    /**
     * {@code runEngineGuarded} must NEVER downgrade an already-COMPLETED row — a failure in a
     * post-completion step (already past the body's own {@code markRunFinished(COMPLETED)}, e.g.
     * building the final summary/statistics) must leave the durable COMPLETED status untouched,
     * not flip it to FAILED.
     */
    @Test
    void guardDoesNotDowngradeAnAlreadyCompletedRun(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(textKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));

            assertThatThrownBy(() -> ListCommand.runEngineGuarded(store, run.id(), () -> {
                // Simulates the real body: markRunFinished(COMPLETED) already committed, THEN a
                // later post-completion step (e.g. logSummary/finish/statistics) throws.
                store.markRunFinished(run.id(), RunStatus.COMPLETED);
                throw new ListingException("injected post-completion failure", null);
            })).isInstanceOf(ListingException.class);

            assertThat(CheckpointDbProbe.runStatusEnum(db, run.id()))
                    .as("a post-completion failure must never downgrade a durable COMPLETED row")
                    .isEqualTo(RunStatus.COMPLETED);
        }
    }

    /**
     * A failed output publish is the other FAILED the engine records ON PURPOSE with the flag left
     * unset ({@link ListRunner}'s publish catch marks it, then rethrows the {@code IOException}
     * straight into this guard). The cause is external and usually transient — a full disk, a
     * stalled mount — so the run must stay resumable: upgrading it to fatal would cost the operator
     * every completed node of the campaign and force a whole-bucket {@code --restart} for a problem
     * that freeing disk space fixes. Seeded with its one node COMPLETED so the later resume is
     * network-free.
     */
    @Test
    void aPublishFailureLeavesTheRunResumable(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path sidecar = dir.resolve("summary.json");
        RunKey key = checkpointKey(OutputFormat.JSONL);
        IOException publishFailure = new IOException("No space left on device");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));

            assertThatThrownBy(() -> ListCommand.runEngineGuarded(store, run.id(), () -> {
                // Exactly ListRunner's publish catch: FAILED, fatal flag deliberately untouched.
                store.markRunFinished(run.id(), RunStatus.FAILED);
                throw publishFailure;
            })).isSameAs(publishFailure);

            RunMeta reopened = store.openRun(key, true, false);
            assertThat(reopened.status()).isEqualTo(RunStatus.FAILED);
            assertThat(reopened.fatalError())
                    .as("the guard must not upgrade a deliberately resumable FAILED to fatal")
                    .isFalse();
        }

        ListCommand cmd = resumeCommand(db, sidecar);
        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);

        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("stop_reason").asText())
                .as("the operator frees space and resumes; a publish failure must never poison that")
                .isNotEqualTo("resume_refused");
    }

    /**
     * A protocol violation on a run RESUMED from a flag-unset FAILED row must still be marked
     * fatal. {@code openRun(..., resume=true)} admits such a row (that is how a broken-pipe
     * truncation stays resumable, INT-12) without transitioning it back to RUNNING, so the row is
     * FAILED for the whole second attempt: a marker that only covered RUNNING would no-op here,
     * leave the flag unset, and let the very next {@code swath resume} walk straight back into the
     * endpoint that violated the protocol.
     */
    @Test
    void aProtocolViolationOnARunResumedFromFailedIsMarkedFatal(@TempDir Path dir) throws Exception {
        assertViolationOnResumedFailedRunIsFatal(dir,
                ProtocolViolationException.oversizedPage(BUCKET, 1000, 1400, 0));
    }

    /** The same, with the violation suppressed under the cancellation that ended the scan. */
    @Test
    void aSuppressedProtocolViolationOnARunResumedFromFailedIsMarkedFatal(@TempDir Path dir)
            throws Exception {
        CancelledException cancelled = new CancelledException("signal");
        cancelled.addSuppressed(ProtocolViolationException.oversizedPage(BUCKET, 1000, 1400, 0));
        assertViolationOnResumedFailedRunIsFatal(dir, cancelled);
    }

    /**
     * Overriding a resumable FAILED must not cost the COMPLETED protection on the violation path
     * either: a violation surfacing after the body already committed {@code
     * markRunFinished(COMPLETED)} leaves the durable COMPLETED row exactly as it is.
     */
    @Test
    void aProtocolViolationDoesNotDowngradeAnAlreadyCompletedRun(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        CancelledException cancelled = new CancelledException("signal");
        cancelled.addSuppressed(ProtocolViolationException.oversizedPage(BUCKET, 1000, 1400, 0));

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(textKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));

            assertThatThrownBy(() -> ListCommand.runEngineGuarded(store, run.id(), () -> {
                store.markRunFinished(run.id(), RunStatus.COMPLETED);
                throw cancelled;
            })).isSameAs(cancelled);

            assertThat(CheckpointDbProbe.runStatusEnum(db, run.id()))
                    .as("a durable COMPLETED row must survive the violation marker")
                    .isEqualTo(RunStatus.COMPLETED);
            assertThat(store.openRun(textKey(), true, false).fatalError()).isFalse();
        }
    }

    /**
     * Seeds a run with unfinished work marked FAILED WITHOUT the fatal flag (the broken-pipe
     * shape), resumes it, drives {@code terminal} through the guard, and asserts the run ends
     * fatal-flagged and the next {@code swath resume} is refused.
     */
    private static void assertViolationOnResumedFailedRunIsFatal(Path dir, Exception terminal)
            throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path sidecar = dir.resolve("summary.json");
        RunKey key = checkpointKey(OutputFormat.JSONL);

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));   // left PENDING — work still unfinished
            store.markRunFinished(run.id(), RunStatus.FAILED);

            RunMeta resumed = store.openRun(key, true, false);
            assertThat(resumed.status())
                    .as("resume admits the FAILED row as-is; it is never moved back to RUNNING")
                    .isEqualTo(RunStatus.FAILED);

            assertThatThrownBy(() -> ListCommand.runEngineGuarded(store, resumed.id(), () -> {
                throw terminal;
            })).isSameAs(terminal);

            assertThat(store.openRun(key, true, false).fatalError())
                    .as("the mark must not no-op just because the resumed row was already FAILED")
                    .isTrue();
        }

        ListCommand cmd = resumeCommand(db, sidecar);
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("FAILED")
                .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(2));
    }

    /** The checkpoint key a bare {@code swath resume} through {@link #resumeCommand} reopens. */
    private static RunKey checkpointKey(OutputFormat format) {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        return new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, format.name());
    }

    /** A fatal in-process failure: the CLI's own guard marked it (fatal_error=1). */
    private static void seedFatalFailedRun(Path db) throws Exception {
        RunKey key = checkpointKey(OutputFormat.JSONL);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            store.markRunFatalUnlessFinished(run.id());
        }
    }

    /**
     * A broken-pipe truncation: {@link ListRunner} marks this directly via plain {@code
     * markRunFinished(FAILED)} — WITHOUT the fatal flag — so it stays normally resumable
     * (INT-12). The one node is committed COMPLETED so the resumed call is network-free.
     */
    private static void seedBrokenPipeFailedRun(Path db) throws Exception {
        RunKey key = checkpointKey(OutputFormat.JSONL);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            store.markRunFinished(run.id(), RunStatus.FAILED);
        }
    }

    private static ListCommand resumeCommand(Path db, Path sidecar) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.output.format = OutputFormat.JSONL;
        cmd.checkpoint.location = db.toString();
        cmd.checkpoint.resume = true;
        cmd.output.summaryJson = sidecar.toString();
        return cmd;
    }

    /** A minimal {@link CheckpointStore} double that only tracks whether either fatal mark fired. */
    private static CheckpointStore failingMarkTracker(AtomicBoolean markedFailed) {
        return new ForwardingCheckpointStore(null) {
            @Override
            public void markRunFatalUnlessFinished(long runId) {
                markedFailed.set(true);
            }

            @Override
            public void markRunUnresumable(long runId) {
                markedFailed.set(true);
            }

            @Override
            public void close() {
            }
        };
    }
}
