/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.PartFinalize;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.error.MergePendingException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.observability.JsonRunSummaryWriter;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.sorted.SortedParquetStamp;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortArm;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortLaneMeters;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.finalize.CascadeCapacityExhaustedException;
import io.varve.swath.sort.stage.SpillLane;
import io.varve.swath.sort.stage.StagedRunCommitter;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.ParquetReads;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A cascade that cannot open its two minimum input streams is a budget refusal, not a broken run:
 * every staged segment is still durable and nothing was published, so the merge must be deferred and
 * the checkpoint left eligible for the zero-LIST merge-only retry once the budget is raised.
 *
 * <p><b>Level.</b> This drives the real {@link ListRunner#runSortMergeOnly} merge-reentry entry
 * point over a real {@link SqliteCheckpointStore} and real staged {@code .pageseg} bytes, twice: once
 * under a merge budget too small to price two streams, then again under a workable one. The refusal
 * is forced through configuration alone — a per-stream price above the whole merge budget — so it
 * reproduces regardless of the host's open-file limit.
 */
final class SortCascadeCapacityRefusalTest {

    private static final String ARGS_HASH = "sort-cascade-capacity-refusal";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ListEntryComparator cmp = new ListEntryComparator();

    @Test
    @Timeout(120)
    void aCascadeCapacityRefusalDefersTheMergeAndTheNextInvocationRepublishes(@TempDir Path tmp)
            throws Exception {
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        Path db = tmp.resolve("c.sqlite");
        Path refusedSidecar = tmp.resolve("refused.json");
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(200);

        RunContext refused = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, refused.metrics())) {
            RunMeta run = store.openRun(sortKey(), false, false);
            long nodeId = store.insertNode(NodeSpec.rootRange(run.id()));

            // Durable staging exactly as a completed listing leaves it: segments tracked through
            // partFinalized, the root node committed COMPLETED, durable_cursor latched — the
            // merge-pending state ListCommand's sorted dispatch reads before calling merge-only.
            StagedRunCommitter sink = result -> store.partFinalized(new PartFinalize(run.id(), 0,
                    result.path().getFileName().toString(), result.pageRunFormat(),
                    result.rows(), result.bytes(), result.perNodeMaxKeys().entrySet().stream()
                    .map(e -> new PartFinalize.DurableAdvance(e.getKey(), e.getValue())).toList()));
            SpillLane lane = new SpillLane(healthy(), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                    SortLaneMeters.NO_OP, stagingDir, "seg-" + run.id() + "-x", sink);
            for (List<ListEntry> page : pages(keyspace, 32)) {
                lane.admit(nodeId, page);
            }
            lane.close();
            store.commitPage(new PageCommit(nodeId, null, true));
            store.markOutputComplete(run.id());

            List<PartRef> segRows = store.finalizedParts(run.id());
            assertThat(segRows).as("two or more sources, so a cascade width is genuinely required")
                    .hasSizeGreaterThan(1);

            Throwable thrown = catchThrowable(() -> new ListRunner().runSortMergeOnly(refused,
                    outputDir, stagingDir, store, run.id(), starvedCascade(), SortMode.OBJECTS,
                    spec(refusedSidecar)));

            assertThat(thrown)
                    .as("a capacity refusal defers the merge rather than failing the run")
                    .isInstanceOf(MergePendingException.class)
                    .hasRootCauseInstanceOf(CascadeCapacityExhaustedException.class);
            assertThat(refused.metrics().fatalErrorClass())
                    .isEqualTo(CascadeCapacityExhaustedException.ERROR_CLASS);
            assertThat(counter(refused, "swath.steal_reason", "merge_fanin_floor_exhausted"))
                    .as("the refusal is greppable as an engaged path, not just an exception")
                    .isEqualTo(1.0);
            assertThat(Files.exists(DatasetLayout.of(outputDir).manifest()))
                    .as("a deferred merge publishes nothing").isFalse();

            JsonNode summary = MAPPER.readTree(refusedSidecar.toFile());
            assertThat(summary.get("error_class").asText(null))
                    .as("the refusal is classified in the one machine-readable artifact")
                    .isEqualTo(CascadeCapacityExhaustedException.ERROR_CLASS);

            RunMeta retryable = store.openRun(sortKey(), true, false);
            assertThat(retryable.status())
                    .as("nothing marks the deferred run FAILED").isEqualTo(RunStatus.RUNNING);
            assertThat(retryable.fatalError())
                    .as("only the fatal flag makes a later resume refuse the run").isFalse();
            assertThat(store.loadResumable(run.id(), true))
                    .as("every listing node stays complete, so retry can take merge-only")
                    .isEmpty();
            assertThat(store.finalizedParts(run.id()))
                    .as("a refusal never touches checkpoint staging identity")
                    .containsExactlyElementsOf(segRows);

            // The retry an operator makes after raising the budget: the same durable staging, a
            // workable merge budget, zero new LIST fetches.
            RunContext republished = RunContext.create();
            new ListRunner().runSortMergeOnly(republished, outputDir, stagingDir, store, run.id(),
                    healthy(), SortMode.OBJECTS, spec(tmp.resolve("published.json")));

            assertThat(counter(republished, "swath.steal_reason", "merge_redone"))
                    .as("the retry re-entered merge-only, listing nothing").isEqualTo(1.0);
            Path finalFile = DatasetLayout.of(outputDir).dataFile("part-00000.parquet");
            assertThat(ParquetReads.keys(finalFile))
                    .containsExactlyElementsOf(sortedStrings(keyspace));
            assertThat(SortedParquetStamp.read(finalFile)).isPresent();
            assertThat(store.sortPhase(run.id())).isEqualTo(SortPhase.PUBLISHED);
            assertThat(Files.exists(DatasetLayout.of(outputDir).manifest())).isTrue();
        }
    }

    /** A per-stream price above the whole merge budget: no cascade width at all is affordable. */
    private static SortConfig starvedCascade() {
        return healthy().withMergeBudgetBytes(1_024L).withMergePerStreamBytes(4_096L);
    }

    private static SortConfig healthy() {
        return SortConfigs.base().withSegmentEntries(32);
    }

    private static RunKey sortKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet",
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
    }

    private static ListRunner.ParquetSpec spec(Path sidecar) {
        JsonRunSummaryWriter.RunConfig rc = new JsonRunSummaryWriter.RunConfig(
                "s3://bucket", "us-east-1", "parquet", 2, false, null, 30_000L, List.of(),
                EngineToggles.DEFAULT, null, false, null, SortArm.NONE, false)
                        .withSortEnabled(true);
        JsonRunSummaryWriter.Config summary =
                new JsonRunSummaryWriter.Config(sidecar, Duration.ofMinutes(10), ARGS_HASH, rc,
                        List.of("list", "s3://bucket", "--sort"));
        return new ListRunner.ParquetSpec(new byte[0], 256, 32, FilterChain.EMPTY, 2, 1024, 16,
                ARGS_HASH, null, null, 0L, 0L, "")
                        .withJsonSummary(summary);
    }

    private static List<List<ListEntry>> pages(List<byte[]> keyspace, int pageSize) {
        List<byte[]> sorted = new ArrayList<>(keyspace);
        sorted.sort(Arrays::compareUnsigned);
        List<List<ListEntry>> pages = new ArrayList<>();
        for (int i = 0; i < sorted.size(); i += pageSize) {
            List<ListEntry> page = new ArrayList<>();
            for (int j = i; j < Math.min(i + pageSize, sorted.size()); j++) {
                page.add(new ObjectEntry(KeyBytes.of(sorted.get(j)), 1L, 0L, null, null, null,
                        false, null, null, null, null));
            }
            pages.add(page);
        }
        return pages;
    }

    private static List<String> sortedStrings(List<byte[]> keyspace) {
        return keyspace.stream().map(k -> new String(k, StandardCharsets.UTF_8)).sorted().toList();
    }

    private static double counter(RunContext ctx, String name, String reason) {
        Counter c = ctx.metrics().registry().find(name)
                .tag("outcome", "SORT").tag("reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }
}
