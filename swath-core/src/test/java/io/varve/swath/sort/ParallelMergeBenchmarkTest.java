/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.checkpoint.PartFinalize;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.runtime.ListRunner;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;

class ParallelMergeBenchmarkTest {

    @Test
    void benchmarkArmIsExplicitlyDistinctFromManagedMergeReentry() {
        assertThat(ParallelMergeBenchmark.ARM).isEqualTo("MERGE_BENCH_PAGE_RUN");
    }

    @Test
    @ResourceLock("SYSTEM_PROPERTIES")
    void externalPropertyBuildsOneCheckpointCatalogSnapshot(@TempDir Path staging) throws Exception {
        Path segment = writeSegment(staging, "seg-1.pageseg", "a");
        String previous = System.getProperty("swath.bench.staging-dir");
        try {
            System.setProperty("swath.bench.staging-dir", staging.toString());
            ParallelMergeBenchmark.CorpusCatalog catalog = ParallelMergeBenchmark.externalStaging(
                    ignored -> List.of(segment));

            assertThat(catalog.source()).isEqualTo("checkpoint");
            assertThat(catalog.paths()).containsExactly(segment);
            assertThat(catalog.identity()).hasSize(64);
        } finally {
            restoreProperty("swath.bench.staging-dir", previous);
        }
    }

    @Test
    @ResourceLock("SYSTEM_PROPERTIES")
    void externalPropertyReadsTheRealCheckpointCatalog(@TempDir Path root) throws Exception {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        Path segment = writeSegment(staging, "seg-1.pageseg", "a");
        createCheckpointCatalog(output, segment);
        String previous = System.getProperty("swath.bench.staging-dir");
        try {
            System.setProperty("swath.bench.staging-dir", staging.toString());
            assertThat(ParallelMergeBenchmark.externalStaging().paths()).containsExactly(segment);
        } finally {
            restoreProperty("swath.bench.staging-dir", previous);
        }
    }

    @Test
    @ResourceLock("SYSTEM_PROPERTIES")
    void externalPropertyRejectsAMissingDirectory(@TempDir Path temp) {
        String previous = System.getProperty("swath.bench.staging-dir");
        try {
            System.setProperty("swath.bench.staging-dir", temp.resolve("missing").toString());
            assertThatIllegalArgumentException().isThrownBy(() ->
                    ParallelMergeBenchmark.externalStaging(ignored -> List.of()))
                    .withMessageContaining("must name a directory");
        } finally {
            restoreProperty("swath.bench.staging-dir", previous);
        }
    }

    @Test
    void emptyCheckpointCatalogAndGeneratedZeroCorpusFailBeforeAnyArm(@TempDir Path staging) {
        assertThatIllegalArgumentException().isThrownBy(() ->
                ParallelMergeBenchmark.snapshotCatalog("checkpoint", staging, List.of()))
                .withMessageContaining("checkpoint corpus contains no page-run inputs");
        assertThatIllegalArgumentException().isThrownBy(() ->
                ParallelMergeBenchmark.snapshotCatalog("generated", staging, List.of()))
                .withMessageContaining("generated corpus contains no page-run inputs");
    }

    @Test
    void catalogRejectsBogusAndStalePageRunDebris(@TempDir Path staging) throws Exception {
        Path valid = writeSegment(staging, "seg-1.pageseg", "a");
        Path bogus = Files.createFile(staging.resolve("seg-2.pageseg"));
        assertThatThrownBy(() -> ParallelMergeBenchmark.snapshotCatalog("checkpoint", staging,
                List.of(valid, bogus))).isInstanceOf(Exception.class);
        Files.delete(bogus);

        Files.createFile(staging.resolve("merge-1.pageseg"));
        assertThatIllegalArgumentException().isThrownBy(() ->
                ParallelMergeBenchmark.snapshotCatalog("checkpoint", staging, List.of(valid)))
                .withMessageContaining("untracked page-run segment")
                .withMessageContaining("stale merge or fixture debris");
    }

    @Test
    void hardLinkedArmPreservesTheValidatedSource(@TempDir Path root) throws Exception {
        Path staging = Files.createDirectories(root.resolve("staging"));
        Path source = writeSegment(staging, "seg-1.pageseg", "a");
        ParallelMergeBenchmark.CorpusCatalog catalog =
                ParallelMergeBenchmark.snapshotCatalog("checkpoint", staging, List.of(source));
        byte[] before = Files.readAllBytes(source);
        Path arm = Files.createDirectory(root.resolve("arm"));

        List<Path> materialized = catalog.materialize(arm);

        assertThat(materialized).hasSize(1);
        assertThat(Files.isSameFile(source, materialized.getFirst())).isTrue();
        assertThat(Files.readAllBytes(source)).isEqualTo(before);
    }

    @Test
    void everyBenchLineCarriesArmSourceCorpusGitAndFingerprint(@TempDir Path staging) throws Exception {
        Path segment = writeSegment(staging, "seg-1.pageseg", "a");
        ParallelMergeBenchmark.CorpusCatalog catalog =
                ParallelMergeBenchmark.snapshotCatalog("checkpoint", staging, List.of(segment));
        String line = ParallelMergeBenchmark.benchLine(
                new ParallelMergeBenchmark.BenchContext(catalog, "deadbeef"),
                "BENCH_ROW", "0123", "rows=1");

        assertThat(line).startsWith("BENCH_ROW ")
                .contains("arm=MERGE_BENCH_PAGE_RUN", "source=checkpoint",
                        "corpus_id=" + catalog.identity(), "git_sha=deadbeef",
                        "logical_output_fingerprint=0123");
    }

    private static Path writeSegment(Path staging, String name, String key) throws Exception {
        return SortTestSupport.writePageRun(staging.resolve(name),
                List.of(SortTestSupport.object(key)), new ListEntryComparator());
    }

    private static void createCheckpointCatalog(Path output, Path segment) throws Exception {
        Path checkpoint = Files.createDirectories(output.resolve(".swath")).resolve("checkpoint.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(checkpoint)) {
            var run = store.openRun(new RunKey("s3", null, "bucket", new byte[0], "hash",
                    "WORK_STEALING", ListingMode.OBJECTS, "", OutputFormat.PARQUET.name(),
                    SoftRestoreContext.NONE, true), false, false);
            store.partFinalized(new PartFinalize(run.id(), 0, segment.getFileName().toString(),
                    ListRunner.SORT_SEGMENT_FORMAT, 1, Files.size(segment), List.of()));
            Manifest.writeState(output, "hash", run.id());
        }
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}
