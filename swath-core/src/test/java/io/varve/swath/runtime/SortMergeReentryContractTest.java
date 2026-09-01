/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PartFinalize;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.observability.JsonRunSummaryWriter;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.output.parquet.sorted.SortedParquetStamp;
import io.varve.swath.output.parquet.sorted.SortedParquetWriter;
import io.varve.swath.output.sorted.SortedDatasetCoordinator;
import io.varve.swath.output.sorted.SortedDatasetResult;
import io.varve.swath.output.sorted.StaleFinalSweep;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.EqualKeyPolicy;
import io.varve.swath.sort.FinalPassListener;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SegmentResult;
import io.varve.swath.sort.SegmentSink;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortLane;
import io.varve.swath.sort.SortLaneMeters;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortedFileWriter;
import io.varve.swath.sort.SortedFileWriterFactory;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.ParquetReads;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * SORT-RESUME-2 — a crash mid-merge (and mid-publish) re-enters the merge and produces output
 * identical to the ground truth with ZERO new LIST fetches. Scale is synthesized; no store is ever
 * consulted for LIST work — the merge takes no fetcher, so "zero LIST fetches" holds by construction.
 *
 * <ul>
 *   <li><b>mid-merge crash (single-pass, the design point):</b> a merge that fails while writing the
 *       final file leaves stale {@code *.tmp}; re-running {@link SortedDatasetCoordinator#transform} over the
 *       still-durable staging segments is idempotent — it cleans the stale tmp and republishes the
 *       exact sorted whole.</li>
 *   <li><b>crash after the final rename but BEFORE the manifest:</b> the state machine treats a
 *       missing {@code manifest.json} as merge-pending; {@link ListRunner#runSortMergeOnly} re-runs
 *       the merge from the checkpoint-tracked segments (zero LIST fetches, {@code SORT.merge_redone}),
 *       overwriting the orphaned final file + stale intermediates and writing the manifest last.</li>
 * </ul>
 *
 * <p>The cascade (multi-pass) mid-merge crash is intentionally probed separately in
 * {@code cascadeMidMergeCrash_*} — see its comment for the crash-recoverability fix it verifies.
 */
final class SortMergeReentryContractTest {

    private static final String ARGS_HASH = "sort-resume2-hash";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final ListEntryComparator cmp = new ListEntryComparator();

    private static RunKey sortKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet",
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
    }

    private static ListRunner.ParquetSpec spec() {
        return new ListRunner.ParquetSpec(new byte[0], 256, 32, FilterChain.EMPTY,
                2, 1024, 16, ARGS_HASH, null, null, 0L, 0L, "");
    }

    /** As {@link #spec()}, but with a JSON summary sidecar wired (sortEnabled=true so the
     * writer emits the {@code sort} block, needed to read back {@code objects}/{@code
     * sort.segments}/{@code sort.merge_only_resume}). */
    private static ListRunner.ParquetSpec spec(Path sidecar) {
        JsonRunSummaryWriter.RunConfig rc = new JsonRunSummaryWriter.RunConfig(
                "s3://bucket", "us-east-1", "parquet", 2, false, null, 30_000L, List.of(),
                EngineToggles.DEFAULT, null, false, null, io.varve.swath.sort.SortArm.NONE, false)
                        .withSortEnabled(true);
        JsonRunSummaryWriter.Config summary =
                new JsonRunSummaryWriter.Config(sidecar, Duration.ofMinutes(10), ARGS_HASH, rc,
                        List.of("list", "s3://bucket", "--sort"));
        return new ListRunner.ParquetSpec(new byte[0], 256, 32, FilterChain.EMPTY, 2, 1024, 16, ARGS_HASH,
                null, null, 0L, 0L, "")
                        .withJsonSummary(summary);
    }

    /** fan-in 512 ⇒ a single-pass merge (the §0.1 design point): the merge never deletes an original. */
    private static SortConfig singlePass() {
        return SortConfigs.base()
                .withSegmentEntries(32);
    }

    // ------------------------------------------------------------------
    // (a) mid-merge crash — single-pass — idempotent re-run.
    // ------------------------------------------------------------------

    @Test
    @Timeout(60)
    void midMergeCrashThenReRun_isIdempotentAndProducesTheGroundTruth(@TempDir Path tmp) throws Exception {
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(240);

        List<Path> segments = buildStagingSegments(keyspace, stagingDir);
        assertThat(segments).as("multiple durable staging segments").hasSizeGreaterThan(1);

        SortedDatasetCoordinator transform = new SortedDatasetCoordinator(new SortRun(singlePass(), cmp,
                DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW, SortMetrics.NO_OP,
                new SortedParquetWriterFactoryLocal(singlePass(), SortMode.OBJECTS),
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));

        // First attempt crashes partway through writing the final file (a real mid-merge kill).
        AtomicBoolean crashArmed = new AtomicBoolean(true);
        SortedDatasetCoordinator crashingTransform = new SortedDatasetCoordinator(new SortRun(singlePass(), cmp,
                DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW, SortMetrics.NO_OP,
                new CrashingFactory(singlePass(), SortMode.OBJECTS, crashArmed, 100),
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));
        assertThatThrownBy(() -> crashingTransform.transform(segments, outputDir, stagingDir,
                (files, rows) -> { }, units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(IOException.class);
        assertThat(Files.exists(DatasetLayout.of(outputDir).manifest())).as("nothing published on crash").isFalse();

        // Re-run over the SAME still-durable segments: idempotent, cleans stale tmp, republishes.
        SortedDatasetResult result = transform.transform(segments, outputDir, stagingDir,
                (files, rows) -> writeManifest(outputDir,
                        files.stream().map(io.varve.swath.sort.FinalPart::path).toList()),
                units -> { }, FinalPassListener.NO_OP);

        Path finalFile = outputDir.resolve("part-00000.parquet");
        assertThat(ParquetReads.keys(finalFile)).containsExactlyElementsOf(sortedStrings(keyspace));
        assertThat(SortedParquetStamp.read(finalFile)).isPresent();
        assertThat(result.totalRows()).isEqualTo(keyspace.size());
        try (var tmpFiles = Files.newDirectoryStream(outputDir, "*.tmp")) {
            assertThat(tmpFiles.iterator().hasNext()).as("no stale *.tmp survives the idempotent re-run").isFalse();
        }
    }

    // ------------------------------------------------------------------
    // (b) crash after final rename, before manifest — runSortMergeOnly redo, zero LIST fetches.
    // ------------------------------------------------------------------

    @Test
    @Timeout(60)
    void finalFileRenamedButManifestMissing_reEntryReRunsMergeWithZeroListFetches(@TempDir Path tmp)
            throws Exception {
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        Path db = tmp.resolve("c.sqlite");
        Path sidecar = tmp.resolve("summary.json");
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(200);

        RunContext ctx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, ctx.metrics())) {
            RunMeta run = store.openRun(sortKey(), false, false);
            long nodeId = store.insertNode(NodeSpec.rootRange(run.id()));

            // Durable staging segments tracked in the checkpoint (a completed listing).
            SegmentSink sink = result -> store.partFinalized(new PartFinalize(run.id(), 0,
                    result.path().getFileName().toString(), result.pageRunFormat(),
                    result.rows(), result.bytes(), result.perNodeMaxKeys().entrySet().stream()
                    .map(e -> new PartFinalize.DurableAdvance(e.getKey(), e.getValue())).toList()));
            SortLane lane = new SortLane(singlePass(), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                    SortLaneMeters.NO_OP, stagingDir, "seg-" + run.id() + "-x", sink);
            for (List<ListEntry> page : pages(keyspace, 32)) {
                lane.admit(nodeId, page);
            }
            lane.close();
            List<PartRef> segRows = store.finalizedParts(run.id());
            assertThat(segRows).allMatch(p -> ListRunner.SORT_SEGMENT_FORMAT.equals(p.format()));

            // A crash strictly between the final rename and the manifest write: the final file is on
            // disk (renamed) but no manifest, plus leftover crash debris — a stale *.tmp and a stale
            // cascade intermediate the re-run must clean. Finals live under <root>/data/, so the
            // crashed prior attempt's orphans sit there too.
            Path dataDir = Files.createDirectories(DatasetLayout.of(outputDir).dataDir());
            Files.writeString(dataDir.resolve("part-00000.parquet"), "orphaned pre-manifest content");
            Files.writeString(dataDir.resolve("part-00000.parquet.tmp"), "stale tmp");
            Files.writeString(stagingDir.resolve("merge-0.parquet"), "stale cascade intermediate");
            assertThat(Files.exists(DatasetLayout.of(outputDir).manifest())).isFalse();

            new ListRunner().runSortMergeOnly(ctx, outputDir, stagingDir, store, run.id(),
                    singlePass(), SortMode.OBJECTS, spec(sidecar));

            Path finalFile = dataDir.resolve("part-00000.parquet");
            assertThat(ParquetReads.keys(finalFile))
                    .as("orphaned final file overwritten with the correct merge output")
                    .containsExactlyElementsOf(sortedStrings(keyspace));
            assertThat(SortedParquetStamp.read(finalFile)).isPresent();
            DatasetLayout layout = DatasetLayout.of(outputDir);
            JsonNode manifest = MAPPER.readTree(layout.manifest().toFile());
            assertThat(manifest.path("files")).hasSize(1);
            assertThat(manifest.path("files").get(0).path("key").asText())
                    .isEqualTo("data/part-00000.parquet");
            assertThat(manifest.path("files").get(0).path("rowCount").asLong())
                    .isEqualTo(keyspace.size());
            assertThat(manifest.path("files").get(0).path("size").asLong())
                    .isEqualTo(Files.size(finalFile));
            assertThat(manifest.path("files").get(0).path("MD5checksum").asText()).isNotBlank();
            assertThat(layout.success()).as("runtime listener writes the authoritative marker last").exists();
            assertThat(Files.readString(layout.symlink())).isEqualTo("data/part-00000.parquet\n");
            assertThat(Manifest.readIdentity(outputDir)).contains(
                    new Manifest.Identity(ARGS_HASH, run.id()));
            assertThat(store.sortPhase(run.id())).isEqualTo(SortPhase.PUBLISHED);
            assertThat(store.finalizedParts(run.id()))
                    .as("merge-only publication never rewrites checkpoint staging identity")
                    .containsExactlyElementsOf(segRows);
            assertThat(Files.exists(stagingDir)).as("staging removed after republish").isFalse();
            try (var files = Files.walk(outputDir)) {
                assertThat(files.filter(Files::isRegularFile)
                        .map(path -> path.getFileName().toString())
                        .filter(name -> name.endsWith(".tmp")
                                || name.startsWith("merge-")
                                || name.startsWith("prange-")))
                        .as("no final tmp, cascade, range, or proof debris survives re-entry")
                        .isEmpty();
            }
            assertThat(counter(ctx, "swath.steal_reason", "merge_redone"))
                    .as("SORT.merge_redone fired (zero LIST fetches)").isEqualTo(1.0);

            // A merge-only resume must NOT under-report objects/segments as 0 in the JSON summary —
            // the listing/staging phase never ran in THIS process, but the merge published the
            // FULL, correct output (all 200 keys, all of segRows' segments). The reporting carries
            // an explicit sort.merge_only_resume marker so a reader knows the counts are
            // recovered-from-checkpoint, not freshly listed.
            JsonNode summaryJson = MAPPER.readTree(sidecar.toFile());
            assertThat(summaryJson.get("completed").asBoolean()).isTrue();
            assertThat(summaryJson.get("objects").asLong())
                    .as("merge-only resume summary.objects reflects the true published row count, not 0")
                    .isEqualTo(keyspace.size());
            assertThat(summaryJson.get("sort").get("segments").asLong())
                    .as("merge-only resume summary.sort.segments reflects the true recovered segment count, not 0")
                    .isEqualTo(segRows.size())
                    .isGreaterThan(0L);
            assertThat(summaryJson.get("sort").get("merge_only_resume").asBoolean())
                    .as("summary.sort.merge_only_resume marks these as recovered-from-checkpoint counts")
                    .isTrue();
            assertThat(summaryJson.get("sort").get("arm").asText())
                    .as("summary.sort.arm is selected by merge-only entry path, not a counter")
                    .isEqualTo("MERGE_ONLY_PAGE_RUN");
            // ...and every one of those rows is attributed as RECOVERED, because an earlier process
            // listed them. This one issued zero LIST calls, so the figures measured against ITS work
            // must read zero rather than credit a whole bucket to the merge's wall clock.
            assertThat(summaryJson.get("recovered_objects").asLong())
                    .as("a merge-only resume listed none of the rows it republished")
                    .isEqualTo(keyspace.size());
            assertThat(summaryJson.get("efficiency").get("keys_per_sec").asDouble())
                    .as("objects - recovered_objects is 0, so this process's listing rate is 0 — "
                            + "not a bucket's worth of keys divided by the merge's seconds")
                    .isZero();
            assertThat(summaryJson.get("efficiency").get("api_calls_per_1k_objects").asDouble())
                    .as("no LIST call and no self-listed object: the per-object call figure is 0, "
                            + "never a division against rows this process never fetched")
                    .isZero();
        }
    }

    // ------------------------------------------------------------------
    // (a') mid-merge crash — CASCADE (multi-pass) — the crash-recovery GAP, now FIXED.
    // ------------------------------------------------------------------

    /**
     * A crash during a <b>cascaded</b> (multi-pass) merge is recoverable by the production redo path.
     * Do not let {@link io.varve.swath.sort.KWayMerge#merge} delete original input segments on any
     * pass: the checkpoint's {@code finalizedParts} names the {@code seg-*.parquet} files, and once a
     * redo can no longer find them it throws {@link java.io.FileNotFoundException} forever.
     * {@code KWayMerge} deletes <b>only its own intermediates from an earlier pass of the same
     * merge</b> — the caller's original input segments are never deleted by the merge itself, at any
     * pass; {@link io.varve.swath.output.sorted.SortedDatasetCoordinator} reclaims originals (and any surviving cascade
     * intermediate) together, only after a successful publish. See {@code KWayMerge}'s class javadoc
     * for the full deletion-policy rationale, including the accepted transient ~2× staging disk cost
     * during an in-flight cascade.
     *
     * <p>This test asserts the contract directly — a crash mid-sort redoes only the sort, idempotently
     * — via a deterministic multi-pass cascade crashed with a real {@code kill -9}-style throw. The
     * single-pass design point is unaffected (covered green above).
     */
    @Test
    @Timeout(60)
    void cascadeMidMergeCrashThenReRun_shouldBeIdempotent(@TempDir Path tmp) throws Exception {
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(240);
        SortConfig cascade = SortConfigs.base()
                .withSegmentEntries(24)
                .withFanIn(2);

        List<Path> segments = new ArrayList<>();
        SortLane lane = new SortLane(cascade, cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                SortLaneMeters.NO_OP, stagingDir, "seg-c", (SegmentResult r) -> segments.add(r.path()));
        for (List<ListEntry> page : pages(keyspace, 24)) {
            lane.admit(1L, page);
        }
        lane.close();
        assertThat(segments).as("more than fan-in segments ⇒ a multi-pass cascade").hasSizeGreaterThan(2);

        AtomicBoolean armed = new AtomicBoolean(true);
        SortedDatasetCoordinator crashing = new SortedDatasetCoordinator(new SortRun(cascade, cmp,
                DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW, SortMetrics.NO_OP,
                new CrashingFactory(cascade, SortMode.OBJECTS, armed, 100),
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));
        assertThatThrownBy(() -> crashing.transform(segments, outputDir, stagingDir,
                (f, r) -> { }, units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(IOException.class);

        // Contract: re-running the merge from the checkpoint's (original) segment list must republish
        // the exact sorted whole — the originals are still on disk (KWayMerge's deletion-policy fix).
        SortedDatasetCoordinator redo = new SortedDatasetCoordinator(new SortRun(cascade, cmp,
                DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW, SortMetrics.NO_OP,
                new SortedParquetWriterFactoryLocal(cascade, SortMode.OBJECTS),
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));
        SortedDatasetResult result = redo.transform(segments, outputDir, stagingDir,
                (files, rows) -> writeManifest(outputDir,
                        files.stream().map(io.varve.swath.sort.FinalPart::path).toList()),
                units -> { }, FinalPassListener.NO_OP);
        assertThat(ParquetReads.keys(outputDir.resolve("part-00000.parquet")))
                .containsExactlyElementsOf(sortedStrings(keyspace));
        assertThat(result.totalRows()).isEqualTo(keyspace.size());
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private List<Path> buildStagingSegments(List<byte[]> keyspace, Path stagingDir) throws Exception {
        List<Path> paths = new ArrayList<>();
        SegmentSink collect = (SegmentResult r) -> paths.add(r.path());
        SortLane lane = new SortLane(SortConfigs.base()
                .withSegmentEntries(32),
                cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, SortLaneMeters.NO_OP, stagingDir, "seg-a", collect);
        for (List<ListEntry> page : pages(keyspace, 32)) {
            lane.admit(1L, page);
        }
        lane.close();
        return paths;
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

    private static void writeManifest(Path outputDir, List<Path> finalFiles) {
        try {
            StringBuilder sb = new StringBuilder("{\"files\":[");
            for (int i = 0; i < finalFiles.size(); i++) {
                sb.append(i == 0 ? "" : ",").append('"').append(finalFiles.get(i).getFileName()).append('"');
            }
            sb.append("]}");
            Files.writeString(DatasetLayout.of(outputDir).manifest(), sb.toString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static double counter(RunContext ctx, String name, String reason) {
        Counter c = ctx.metrics().registry().find(name).tag("outcome", "SORT").tag("reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }

    /** A plain stamped-writer factory (mirrors production's {@code SortedParquetWriterFactory}). */
    private record SortedParquetWriterFactoryLocal(SortConfig config, SortMode mode)
            implements SortedFileWriterFactory {
        @Override
        public SortedFileWriter create(Path path, int fileIndex) throws IOException {
            return new SortedParquetWriter(path, config, mode, fileIndex);
        }
    }

    /** Wraps the stamped writer and throws once, mid-write, to model a mid-merge process kill. */
    private record CrashingFactory(SortConfig config, SortMode mode, AtomicBoolean armed, long crashAfterRows)
            implements SortedFileWriterFactory {
        @Override
        public SortedFileWriter create(Path path, int fileIndex) throws IOException {
            SortedFileWriter delegate = new SortedParquetWriter(path, config, mode, fileIndex);
            return new CrashingWriter(delegate, armed, crashAfterRows);
        }
    }

    private static final class CrashingWriter implements SortedFileWriter {
        private final SortedFileWriter delegate;
        private final AtomicBoolean armed;
        private final long crashAfterRows;
        private long written;

        CrashingWriter(SortedFileWriter delegate, AtomicBoolean armed, long crashAfterRows) {
            this.delegate = delegate;
            this.armed = armed;
            this.crashAfterRows = crashAfterRows;
        }

        @Override
        public void write(ListEntry e) throws IOException {
            if (armed.get() && written >= crashAfterRows && armed.compareAndSet(true, false)) {
                throw new IOException("injected SORT-RESUME-2 mid-merge crash after " + written + " rows");
            }
            delegate.write(e);
            written++;
        }

        @Override
        public long rows() {
            return delegate.rows();
        }

        @Override
        public long dataSize() {
            return delegate.dataSize();
        }

        @Override
        public void markFinal() {
            delegate.markFinal();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
