/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PartFinalize;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.observability.JsonRunSummaryWriter;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.RunContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@code error_class} of a classified sort abort must reach {@code summary.json}, not just
 * stderr: {@link SegmentCorruptionException} carries the class, {@code ListRunner} records it on
 * {@code RunMetrics#recordFatalErrorClass} as the merge failure unwinds, and the CRASH terminal
 * status ({@code ListRunner#terminalStatus()}, via {@code JsonRunSummaryWriter#close}) reads it
 * back — a staged page-run segment whose page {@code minKey}s regress must not be indistinguishable,
 * in the one machine-readable artifact a fleet supervisor reads, from any other crash.
 *
 * <p><b>Level.</b> This drives the REAL {@link ListRunner#runSortMergeOnly} merge-reentry entry point over
 * a REAL checkpoint store and REAL staged {@code .pageseg} bytes, and reads the REAL sidecar
 * {@link JsonRunSummaryWriter} left on disk — so {@code terminalStatus()} + the JSON writer are genuinely
 * exercised end to end. Nothing here asserts on a hand-built {@code TerminalStatus}.
 */
final class SortMinRegressionErrorClassSummaryTest {

    private static final String ARGS_HASH = "sort-274-error-class";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ListEntryComparator cmp = new ListEntryComparator();
    private int seq;

    @Test
    @Timeout(60)
    void aMinRegressedStagingSegmentAbortsTheRunWithAClassifiedErrorClassInTheSummary(@TempDir Path tmp)
            throws Exception {
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        Path db = tmp.resolve("c.sqlite");
        Path sidecar = tmp.resolve("summary.json");

        RunContext ctx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, ctx.metrics())) {
            RunMeta run = store.openRun(sortKey(), false, false);
            long nodeId = store.insertNode(NodeSpec.rootRange(run.id()));

            // Durable staging exactly as a completed listing leaves it — .pageseg segments tracked in the
            // checkpoint — except that one segment's pages are physically stored with REGRESSING minKeys
            // ([m..n] before [a..b]). Hand-framed (PageRunRawFixtures): byte-perfect header/CRC32C/trailer,
            // illegal only in page order, so the LOGICAL guard is what fires — not a physical-corruption
            // check. This is what a writer regression or a corrupt staged file looks like on disk.
            Path corrupt = stagingDir.resolve("seg-0.pageseg");
            PageRunRawFixtures.writeRawPageRun(corrupt,
                    List.of(List.of(obj("m"), obj("n")), List.of(obj("a"), obj("b"))), cmp);
            Path healthy = stagingDir.resolve("seg-1.pageseg");
            PageRunRawFixtures.writeRawPageRun(healthy, List.of(List.of(obj("c"), obj("d"))), cmp);
            recordSegment(store, run.id(), nodeId, corrupt, 4);
            recordSegment(store, run.id(), nodeId, healthy, 2);

            Throwable thrown = catchThrowable(() -> new ListRunner().runSortMergeOnly(ctx, outputDir,
                    stagingDir, store, run.id(), singlePass(), SortMode.OBJECTS, spec(sidecar)));

            assertThat(thrown).as("a corrupt staged segment must abort the run, never publish").isNotNull();
            assertThat(Files.exists(DatasetLayout.of(outputDir).manifest()))
                    .as("nothing published from a corrupt merge").isFalse();

            // The artifact under test: the terminal sidecar the aborting run leaves behind.
            assertThat(sidecar).exists();
            JsonNode summary = MAPPER.readTree(sidecar.toFile());

            // Soft: report EVERY terminal fact in one run — which of them the plumbing gets right is
            // exactly the question, so a first failure must not mask the rest.
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(summary.get("completed").asBoolean())
                        .as("an aborted merge never completes").isFalse();
                softly.assertThat(summary.get("stop_reason").asText())
                        .as("the unwind is a crash terminal (ListRunner#terminalStatus's CRASH branch)")
                        .isEqualTo("crash");
                // The classified cause IS available to any catch that looks: it sits in the cause chain of
                // whatever unwound the run. (The merge read failure is wrapped by the merger, so the outer
                // throwable is an UncheckedIOException — which is why ListRunner#sortMergeAndPublish's
                // `catch (IOException e)` never runs its recordFatalErrorClass call. See below.)
                softly.assertThat(hasCorruptionCause(thrown))
                        .as("the unwinding throwable carries the classified SegmentCorruptionException in "
                                + "its cause chain (observed outer type: %s)", thrown.getClass().getName())
                        .isTrue();
                // The engagement counter is bumped BEFORE the throw, so it must survive the abort into the
                // serialized registry (the post-hoc evidence that this path fired at all).
                softly.assertThat(meterValue(summary, "swath.steal_reason", "SORT", "page_run_min_regression"))
                        .as("SORT.page_run_min_regression must reach summary.json's meters[] despite the abort")
                        .isEqualTo(1.0);
                // THE assertion: the crash is CLASSIFIED in the machine-readable artifact.
                softly.assertThat(summary.get("error_class").asText(null))
                        .as("a page-min regression must be greppable from summary.json alone — an operator "
                                + "must not have to grep stderr to tell this apart from a generic crash "
                                + "(the run unwound as %s)", thrown.getClass().getName())
                        .isEqualTo("page_run_min_regression");
                softly.assertThat(summary.get("exit_code").asInt(-1))
                        .as("a fatal classified abort reports exit 1 in the sidecar (observed: %s)",
                                summary.get("exit_code"))
                        .isEqualTo(1);
            });
        }
    }

    /** True iff {@code t}'s cause chain holds a {@link SegmentCorruptionException} — the shape
     *  {@code ListRunner#segmentErrorClass}'s walk depends on. */
    private static boolean hasCorruptionCause(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof SegmentCorruptionException) {
                return true;
            }
        }
        return false;
    }

    // -------------------------------------------------------------------------------------- helpers

    /** The counter {@code value} of {@code name{outcome,reason}} in the sidecar's {@code meters[]}. */
    private static double meterValue(JsonNode summary, String name, String outcome, String reason) {
        for (JsonNode meter : summary.get("meters")) {
            JsonNode tags = meter.get("tags");
            if (name.equals(meter.get("name").asText())
                    && tags != null
                    && outcome.equals(tags.path("outcome").asText(null))
                    && reason.equals(tags.path("reason").asText(null))) {
                return meter.get("value").asDouble();
            }
        }
        return 0.0;
    }

    private static void recordSegment(SqliteCheckpointStore store, long runId, long nodeId, Path segment,
                                      long rows) throws Exception {
        store.partFinalized(new PartFinalize(runId, 0, segment.getFileName().toString(),
                new PageRunFormat(PageRunSegmentWriter.FORMAT_VERSION,
                        PageRunFormat.ABSENT_EXTENSION),
                rows, Files.size(segment),
                List.of(new PartFinalize.DurableAdvance(nodeId, KeyBytes.ofUtf8("z").raw()))));
    }

    private static RunKey sortKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet",
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
    }

    /** A sidecar-wired spec (sortEnabled=true), mirroring {@code SortMergeReentryContractTest#spec(Path)}. */
    private static ListRunner.ParquetSpec spec(Path sidecar) {
        JsonRunSummaryWriter.RunConfig rc = new JsonRunSummaryWriter.RunConfig(
                "s3://bucket", "us-east-1", "parquet", 2, false, null, 30_000L, List.of(),
                EngineToggles.DEFAULT, null, false, null, false)
                        .withSortEnabled(true);
        JsonRunSummaryWriter.Config summary =
                new JsonRunSummaryWriter.Config(sidecar, Duration.ofMinutes(10), ARGS_HASH, rc,
                        List.of("list", "s3://bucket", "--sort"));
        return new ListRunner.ParquetSpec(new byte[0], 256, 32, FilterChain.EMPTY, 2, 1024, 16, ARGS_HASH,
                null, null, 0L, 0L, "")
                        .withJsonSummary(summary);
    }

    /** fan-in 512 ⇒ the single-pass merge (the §0.1 design point). */
    private static SortConfig singlePass() {
        return SortConfigs.base()
                .withSegmentEntries(32);
    }

    private ListEntry obj(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), seq++, 0L, null, null, "v1",
                false, null, null, null, null);
    }
}
