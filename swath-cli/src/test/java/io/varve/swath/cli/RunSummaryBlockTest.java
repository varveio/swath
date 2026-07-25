/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.runtime.ArgsHashFields;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * The end-of-run block as an operator actually meets it — end-to-end through {@link
 * ListCommand#call()} with a mock fetcher, reading the real stderr the process would write.
 *
 * <p>The load-bearing case is a <b>redirected</b> stderr: {@code isatty} decides whether swath may
 * colorize, never whether the operator is told what happened, so a captured log gets the same
 * content a terminal does. That is the case a naive terminal gate silently breaks, and for an
 * overnight or fleet run it is the only artifact there is.
 */
final class RunSummaryBlockTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String BUCKET = "bucket";
    private static final String PREFIX = "data/";

    private final ch.qos.logback.classic.Logger swathLogger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("io.varve.swath");
    private Level originalLevel;

    /**
     * A directly-constructed {@link ListCommand} never runs picocli's execution strategy, so pin
     * the default verbosity the real CLI installs there — these tests are about what the default
     * tier writes to stderr.
     */
    @BeforeEach
    void defaultVerbosity() {
        originalLevel = swathLogger.getLevel();
        CliLogging.configure(0, 0);
    }

    @AfterEach
    void restoreVerbosity() {
        swathLogger.setLevel(originalLevel);
    }

    /** Long enough to clear {@link SummaryRenderer#AUTO_MIN_ELAPSED} without a flag forcing it. */
    private static final long OVER_THRESHOLD_MS = 1_800L;

    @Test
    void everyFdCombinationGetsIdenticalContent(@TempDir Path dir) throws Exception {
        List<String> rendered = new ArrayList<>();
        for (boolean stdoutTty : List.of(true, false)) {
            for (boolean stderrTty : List.of(true, false)) {
                ListCommand cmd = listCommand(dir.resolve("out-" + stdoutTty + stderrTty + ".jsonl"));
                cmd.output.stats = true;
                cmd.terminalOverride = new TerminalCapabilities(fd ->
                        fd == TerminalCapabilities.STDOUT_FD ? stdoutTty : stderrTty);
                // Figures differ run to run (rate, RSS, and whether a rate crosses into
                // thousands); the CONTENT — which fields, in which shape — must not.
                rendered.add(block(runCapturingStderr(cmd)).replaceAll("[0-9][0-9,.]*", "#"));
            }
        }

        assertThat(rendered).as("the fd probe selects form, never whether: all four cells "
                        + "carry the same summary content")
                .containsOnly(rendered.getFirst());
        assertThat(rendered.getFirst()).contains("objects in").contains("API calls");
        assertThat(rendered.getFirst()).doesNotContain("\u001B[");
    }

    @Test
    void aRunLongEnoughToWaitOnWritesTheBlockToARedirectedStderrAsPlainText(@TempDir Path dir)
            throws Exception {
        ListCommand cmd = listCommand(dir.resolve("out.jsonl"), OVER_THRESHOLD_MS);
        cmd.terminalOverride = new TerminalCapabilities(fd -> false);   // stderr is a file

        String stderr = runCapturingStderr(cmd);

        assertThat(block(stderr))
                .as("no flag was passed: the run earned its summary by taking long enough to wait on")
                .contains("objects in").contains("API calls");
        assertThat(stderr).as("plain text, no ANSI, into a captured stderr")
                .doesNotContain("\u001B[");
    }

    @Test
    void theBlockAgreesWithTheReportJsonFieldForField(@TempDir Path dir) throws Exception {
        Path report = dir.resolve("report.json");
        ListCommand cmd = listCommand(dir.resolve("out.jsonl"));
        cmd.output.stats = true;
        cmd.output.summaryJson = report.toString();

        ListAppender<ILoggingEvent> logged = new ListAppender<>();
        logged.start();
        swathLogger.setLevel(Level.INFO);   // the -v tier, where list_run_summary lives
        swathLogger.addAppender(logged);
        String stderr;
        try {
            stderr = runCapturingStderr(cmd);
        } finally {
            swathLogger.detachAppender(logged);
        }
        String logLine = logged.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(line -> line.startsWith("list_run_summary "))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no list_run_summary line was logged"));

        JsonNode root = MAPPER.readTree(report.toFile());
        long objects = root.get("objects").asLong();
        long apiCalls = root.get("cost").get("api_calls").asLong();
        long peakInFlight = root.get("engine").get("peak_in_flight").asLong();
        assertThat(block(stderr))
                .as("the block and the report render one source, so their figures cannot disagree")
                .contains(objects + " objects in")
                .contains(apiCalls + " API calls")
                .contains("peak " + peakInFlight);
        assertThat(logLine)
                .as("the -v log line is the THIRD rendering of that one source, and scrapers read "
                        + "it: it must agree with the other two field for field")
                .contains("objects=" + objects)
                .contains("api_calls=" + apiCalls)
                .contains("peak_in_flight=" + peakInFlight);
        assertThat(root.get("cost").get("basis").get("rate_per_1k_usd").asDouble())
                .isEqualTo(RunMetrics.LIST_COST_PER_1K_USD);
        assertThat(root.get("cost").get("basis").get("source").asText())
                .isEqualTo(RunMetrics.LIST_COST_SOURCE);
        assertThat(root.get("engine").has("time_to_first_steal_ms"))
                .as("the ramp-up timings are report fields too, not only -v log fields")
                .isTrue();
        assertThat(root.get("engine").has("time_to_peak_in_flight_ms")).isTrue();
    }

    /**
     * A {@code swath resume <dir>} learns its destination only when the checkpoint's run context is
     * restored, long after the summary sink is installed — so a resume that finishes in well under
     * the auto threshold still earns its block, because the durable dataset it produced is the
     * reason to report. Read the preferences at install time instead and every resume renders as a
     * short stdout run and says nothing at all.
     */
    @Test
    void aShortResumeStillEarnsTheBlockForTheDatasetItProduced(@TempDir Path dir) throws Exception {
        Path outputDir = seedResumableDataset(dir);

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;
        cmd.fetcherOverride = mockObjects(20, 0L);

        String stderr = runCapturingStderr(cmd::call, ExitCodes.SUCCESS);

        assertThat(block(stderr)).contains("objects in").contains("API calls");
    }

    /**
     * And the resume that was interrupted — the run most likely to be interrupted again — offers
     * the resume that will work. {@code swath resume} hands the checkpoint over as an explicit
     * location, so the hint cannot be derived from the checkpoint mode: it is the run handle. The
     * fields set here are exactly what {@link ResumeCommand} wires onto its delegate, plus the
     * timebox it has no flag for.
     */
    @Test
    void anInterruptedResumeOffersTheRunHandleItWasInvokedOn(@TempDir Path dir) throws Exception {
        Path outputDir = seedResumableDataset(dir);

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        cmd.resumeCommandCheckpoint = colocatedCheckpoint(outputDir);
        cmd.resumeCommandRunHandle = outputDir;
        cmd.checkpoint.resume = true;
        cmd.checkpoint.location = colocatedCheckpoint(outputDir).toString();
        cmd.liveness.maxDuration = "250ms";
        cmd.fetcherOverride = mockObjects(400, 400L);

        String stderr = runCapturingStderr(cmd::call, ExitCodes.TIMEBOX);

        assertThat(block(stderr).lines().findFirst().orElseThrow().strip())
                .startsWith("INCOMPLETE (")
                .endsWith("— resume: swath resume " + outputDir);
    }

    /**
     * The W1 silence invariant, as amended by the summary landing: a healthy run's stderr carries
     * the destination echo and the block and <b>nothing log-shaped</b> — no timestamped level
     * lines, no warnings. {@code --no-stats} restores literal silence for scripts that want it.
     */
    @Test
    void healthyRunCarriesNoLogShapedLinesAndNoStatsRestoresSilence(@TempDir Path dir) throws Exception {
        Path out = dir.resolve("out.jsonl");
        String withSummary = runCapturingStderr(listCommand(out));

        assertThat(withSummary).startsWith("→ writing jsonl to " + out);
        assertThat(withSummary.lines()).as("a healthy run logs nothing: no WARN, no timestamped line")
                .noneMatch(line -> line.matches("\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d .*"));
        assertThat(block(withSummary)).contains("objects in");

        ListCommand silent = listCommand(dir.resolve("silent.jsonl"));
        silent.output.stats = false;

        assertThat(runCapturingStderr(silent).lines())
                .as("--no-stats leaves exactly the destination echo")
                .containsExactly("→ writing jsonl to " + dir.resolve("silent.jsonl"));
    }

    @Test
    void unknownProviderWithholdsTheDollarButStillTagsTheReportBasis(@TempDir Path dir) throws Exception {
        Path report = dir.resolve("report.json");
        ListCommand cmd = listCommand(dir.resolve("out.jsonl"));
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.output.stats = true;
        cmd.output.summaryJson = report.toString();

        String stderr = runCapturingStderr(cmd);

        assertThat(block(stderr)).as("--endpoint-url: unknown provider, so no dollar figure at all")
                .doesNotContain("$");
        assertThat(MAPPER.readTree(report.toFile()).get("cost").get("basis").get("source").asText())
                .as("the report still names the rate it derived cost_usd from")
                .isEqualTo(RunMetrics.LIST_COST_SOURCE);
    }

    /** Everything the summary block writes, i.e. stderr minus the destination echo. */
    private static String block(String stderr) {
        return stderr.lines().filter(line -> !line.startsWith("→ writing"))
                .reduce("", (a, b) -> a + b + "\n");
    }

    private static String runCapturingStderr(ListCommand cmd) throws Exception {
        return runCapturingStderr(cmd::call, ExitCodes.SUCCESS);
    }

    private static String runCapturingStderr(Callable<Integer> run, int expectedExit) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.err;
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            assertThat(run.call()).isEqualTo(expectedExit);
        } finally {
            System.setErr(original);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }

    /**
     * A directory dataset with listing work still outstanding and its checkpoint co-located under
     * the run handle: the state {@code swath resume <dir>} exists to pick up.
     */
    private static Path seedResumableDataset(Path parent) throws Exception {
        Path outputDir = parent.resolve("dataset");
        Path db = colocatedCheckpoint(outputDir);
        Files.createDirectories(db.getParent());
        RunKey key = new RunKey("s3", null, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                ArgsHashFields.forListing("s3", "", BUCKET, PREFIX).hash(),
                "auto", ListingMode.OBJECTS, new ListCommand().filters.spec(),
                OutputFormat.PARQUET.name(),
                new SoftRestoreContext(true, null, "us-east-1", false, false,
                        outputDir.toString(), false, null, null),
                false);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k1".getBytes(StandardCharsets.UTF_8), false));
        }
        return outputDir;
    }

    private static Path colocatedCheckpoint(Path outputDir) {
        return CheckpointOptions.CheckpointMode.colocatedCheckpoint(outputDir);
    }

    /** A mock keyspace under the seeded prefix, optionally slowed to outlast a timebox. */
    private static MockPageFetcher mockObjects(int count, long perPageDelayMs) {
        List<byte[]> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(String.format(PREFIX + "key-%05d", i).getBytes(StandardCharsets.UTF_8));
        }
        MockPageFetcher.Builder fetcher = MockPageFetcher.builder().keys(keys).maxKeysCap(20);
        if (perPageDelayMs > 0) {
            fetcher.interceptor((req, idx, page) -> {
                if (idx >= 1) {
                    TimeUnit.MILLISECONDS.sleep(perPageDelayMs);
                }
                return page;
            });
        }
        return fetcher.build();
    }

    private static ListCommand listCommand(Path out) {
        return listCommand(out, 0L);
    }

    /** A checkpoint-free run over a mock keyspace, optionally slowed to earn an auto summary. */
    private static ListCommand listCommand(Path out, long firstPageDelayMs) {
        MockPageFetcher.Builder fetcher = MockPageFetcher.builder()
                .keys(List.of("data/a".getBytes(StandardCharsets.UTF_8),
                        "data/b".getBytes(StandardCharsets.UTF_8)));
        if (firstPageDelayMs > 0) {
            fetcher.interceptor((req, idx, page) -> {
                TimeUnit.MILLISECONDS.sleep(firstPageDelayMs);
                return page;
            });
        }
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://bucket/data/";
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        cmd.checkpoint.location = "none";
        cmd.output.format = OutputFormat.JSONL;
        cmd.output.destination = out.toString();
        cmd.fetcherOverride = fetcher.build();
        return cmd;
    }
}
