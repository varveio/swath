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
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.runtime.ListRunner;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;

class ParallelMergeBenchmarkTest {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    @Test
    void benchmarkArmIsExplicitlyDistinctFromManagedMergeReentry() {
        assertThat(ParallelMergeBenchmark.ARM).isEqualTo("MERGE_BENCH_PAGE_RUN");
    }

    @Test
    @ResourceLock("SYSTEM_PROPERTIES")
    void externalPreparationIsCheckpointAuthoritativeImmutableAndCreatesNoSqliteCompanions(
            @TempDir Path root) throws Exception {
        Retained retained = completedRetained(root, "hash", "seg");
        BenchmarkMasterSnapshot before = BenchmarkMasterSnapshot.capture(retained.output());

        ParallelMergeBenchmark.CorpusCatalog catalog = external(retained.staging());

        assertThat(catalog.source()).isEqualTo("checkpoint");
        assertThat(catalog.paths()).containsExactly(retained.segment());
        assertThat(catalog.runId()).isEqualTo(retained.runId());
        assertThat(catalog.argsHash()).isEqualTo("hash");
        assertThat(catalog.oracle().rows()).isEqualTo(2);
        before.verifyUnchanged();
        assertNoSqliteCompanions(retained.checkpoint());
    }

    @Test
    @ResourceLock("SYSTEM_PROPERTIES")
    void incompleteRunAndMismatchedIdentityAreRejectedWithoutMutation(@TempDir Path root)
            throws Exception {
        Retained incomplete = retained(root.resolve("incomplete"), "hash", "seg", false, true);
        BenchmarkMasterSnapshot incompleteBefore = BenchmarkMasterSnapshot.capture(incomplete.output());
        assertThatIllegalArgumentException().isThrownBy(() -> external(incomplete.staging()))
                .withMessageContaining("not completed sorted PUBLISHED");
        incompleteBefore.verifyUnchanged();

        Retained mismatch = completedRetained(root.resolve("mismatch"), "checkpoint-hash", "seg");
        Manifest.writeState(mismatch.output(), "wrong-hash", mismatch.runId());
        BenchmarkMasterSnapshot mismatchBefore = BenchmarkMasterSnapshot.capture(mismatch.output());
        assertThatIllegalArgumentException().isThrownBy(() -> external(mismatch.staging()))
                .withMessageContaining("args_hash does not match");
        mismatchBefore.verifyUnchanged();
    }

    @Test
    @ResourceLock("SYSTEM_PROPERTIES")
    void missingSuccessAndTrackedMergeNamingAreRejected(@TempDir Path root) throws Exception {
        Retained noSuccess = retained(root.resolve("no-success"), "hash", "seg", true, false);
        assertThatIllegalArgumentException().isThrownBy(() -> external(noSuccess.staging()))
                .withMessageContaining("completed _SUCCESS");

        Retained merge = completedRetained(root.resolve("merge"), "hash", "merge");
        assertThatIllegalArgumentException().isThrownBy(() -> external(merge.staging()))
                .withMessageContaining("non-original sort segment row");
    }

    @Test
    @ResourceLock("SYSTEM_PROPERTIES")
    void crcCorruptTrackedBodyAndUntrackedStaleDebrisFailBeforeAnyArm(@TempDir Path root)
            throws Exception {
        Retained corrupt = completedRetained(root.resolve("corrupt"), "hash", "seg");
        byte[] bytes = Files.readAllBytes(corrupt.segment());
        bytes[PageRunSegmentWriter.HEADER_BYTES + 8] ^= 0x7F;
        Files.write(corrupt.segment(), bytes);
        assertThatThrownBy(() -> external(corrupt.staging()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC32C mismatch");

        Retained stale = completedRetained(root.resolve("stale"), "hash", "seg");
        Files.createFile(stale.staging().resolve("merge-99.pageseg"));
        assertThatIllegalArgumentException().isThrownBy(() -> external(stale.staging()))
                .withMessageContaining("untracked page-run segment");
    }

    @Test
    @ResourceLock("SYSTEM_PROPERTIES")
    void successfulAndFailingActualArmsLeaveExternalMasterByteIdentical(@TempDir Path root)
            throws Exception {
        Retained retained = completedRetained(root.resolve("master"), "hash", "seg");
        ParallelMergeBenchmark.CorpusCatalog catalog = external(retained.staging());
        BenchmarkMasterSnapshot before = BenchmarkMasterSnapshot.capture(retained.output());
        Path scratch = Files.createDirectory(root.resolve("scratch"));
        try {
            ParallelMergeBenchmark.ArmResult result = ParallelMergeBenchmark.runArm(
                    scratch, catalog, 1, "success",
                    config -> new SortedParquetWriterFactory(config, SortMode.OBJECTS));
            assertThat(result.totalRows).isEqualTo(catalog.oracle().rows());
            before.verifyUnchanged();

            assertThatThrownBy(() -> ParallelMergeBenchmark.runArm(
                    scratch, catalog, 1, "failure", config -> (path, index) -> {
                        throw new IOException("injected benchmark writer failure");
                    })).isInstanceOf(IOException.class)
                    .hasMessageContaining("injected benchmark writer failure");
            before.verifyUnchanged();
        } finally {
            SortBenchCorpus.deleteTree(scratch);
        }
        before.verifyUnchanged();
    }

    @Test
    void rowOracleRejectsOrderAndMultiplicityAndKeepsStableOrderedFingerprint() throws Exception {
        ListEntry first = object("a", 1);
        ListEntry second = object("a", 2);
        ListEntry third = object("b", 3);
        List<ListEntry> input = List.of(first, second, third);
        BenchmarkRowOracle.InputOracle oracle = BenchmarkRowOracle.inputForTesting(input);

        BenchmarkRowOracle.OutputValidation original =
                BenchmarkRowOracle.validateEntriesForTesting(input, oracle, CMP);
        BenchmarkRowOracle.OutputValidation comparatorEqualSwap =
                BenchmarkRowOracle.validateEntriesForTesting(List.of(second, first, third), oracle, CMP);
        assertThat(comparatorEqualSwap.multisetDigest()).isEqualTo(original.multisetDigest());
        assertThat(comparatorEqualSwap.orderedFingerprint()).isNotEqualTo(original.orderedFingerprint());

        assertThatThrownBy(() -> BenchmarkRowOracle.validateEntriesForTesting(
                List.of(third, first, second), oracle, CMP)).hasMessageContaining("physically sorted");
        assertThatThrownBy(() -> BenchmarkRowOracle.validateEntriesForTesting(
                List.of(first, second), oracle, CMP)).hasMessageContaining("input oracle");
        assertThatThrownBy(() -> BenchmarkRowOracle.validateEntriesForTesting(
                List.of(first, second, third, third), oracle, CMP)).hasMessageContaining("input oracle");
    }

    @Test
    void generatedEmptyAndBodyCorruptPreparationFailFast(@TempDir Path staging) throws Exception {
        assertThatIllegalArgumentException().isThrownBy(() ->
                ParallelMergeBenchmark.snapshotCatalog("generated", staging, List.of()))
                .withMessageContaining("generated corpus contains no page-run inputs");
        Path segment = writeSegment(staging, "seg-generated-0.pageseg");
        byte[] bytes = Files.readAllBytes(segment);
        bytes[PageRunSegmentWriter.HEADER_BYTES + 8] ^= 0x7F;
        Files.write(segment, bytes);
        assertThatThrownBy(() -> ParallelMergeBenchmark.snapshotCatalog(
                "generated", staging, List.of(segment)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC32C mismatch");
    }

    @Test
    void realGeneratedPreparationCleansFailureAndProducesValidatedOracle(@TempDir Path parent)
            throws Exception {
        assertThatIllegalArgumentException().isThrownBy(() ->
                ParallelMergeBenchmark.prepareGenerated(parent, 2, 0, 10, 5))
                .withMessageContaining("generated corpus contains no page-run inputs");
        try (var entries = Files.list(parent)) {
            assertThat(entries).isEmpty();
        }

        ParallelMergeBenchmark.PreparedGenerated prepared =
                ParallelMergeBenchmark.prepareGenerated(parent, 2, 40, 10, 5);
        try {
            assertThat(prepared.catalog().oracle().rows()).isEqualTo(40);
            assertThat(prepared.catalog().oracle().trailerEntries()).isEqualTo(40);
        } finally {
            SortBenchCorpus.deleteTree(prepared.root());
        }
    }

    @Test
    void everyBenchLineCarriesCompleteContext(@TempDir Path staging) throws Exception {
        Path segment = writeSegment(staging, "seg-generated-0.pageseg");
        ParallelMergeBenchmark.CorpusCatalog catalog =
                ParallelMergeBenchmark.snapshotCatalog("generated", staging, List.of(segment));
        String line = ParallelMergeBenchmark.benchLine(
                new ParallelMergeBenchmark.BenchContext(catalog, "deadbeef"),
                "BENCH_ROW", "0123", "rows=2");

        assertThat(line).startsWith("BENCH_ROW ")
                .contains("arm=MERGE_BENCH_PAGE_RUN", "source=generated",
                        "corpus_id=" + catalog.identity(), "git_sha=deadbeef",
                        "run_id=-1", "args_hash=not_applicable", "cache_state=warm_primed",
                        "logical_output_fingerprint=0123");
    }

    @Test
    @ResourceLock("SYSTEM_PROPERTIES")
    void externalPropertyRejectsAMissingDirectory(@TempDir Path temp) {
        assertThatIllegalArgumentException().isThrownBy(() -> external(temp.resolve("missing")))
                .withMessageContaining("must name a directory");
    }

    private static ParallelMergeBenchmark.CorpusCatalog external(Path staging) throws Exception {
        String previous = System.getProperty("swath.bench.staging-dir");
        try {
            System.setProperty("swath.bench.staging-dir", staging.toString());
            return ParallelMergeBenchmark.externalStaging();
        } finally {
            restoreProperty("swath.bench.staging-dir", previous);
        }
    }

    private static Retained completedRetained(Path root, String argsHash, String kind) throws Exception {
        return retained(root, argsHash, kind, true, true);
    }

    private static Retained retained(Path root, String argsHash, String kind,
                                     boolean completeRun, boolean success) throws Exception {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        Path checkpoint = Files.createDirectories(output.resolve(".swath")).resolve("checkpoint.sqlite");
        long runId;
        Path segment;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(checkpoint)) {
            var run = store.openRun(new RunKey("s3", null, "bucket", new byte[0], argsHash,
                    "WORK_STEALING", ListingMode.OBJECTS, "", OutputFormat.PARQUET.name(),
                    SoftRestoreContext.NONE, true), false, false);
            runId = run.id();
            String name = kind.equals("seg")
                    ? "seg-" + runId + "-test-0.pageseg"
                    : "merge-1.pageseg";
            segment = writeSegment(staging, name);
            store.partFinalized(new PartFinalize(runId, 0, name,
                    ListRunner.SORT_SEGMENT_FORMAT, 2, Files.size(segment), List.of()));
            if (completeRun) {
                store.setSortPhase(runId, SortPhase.PUBLISHED);
                store.markRunFinished(runId, RunStatus.COMPLETED);
            }
        }
        Manifest.writeState(output, argsHash, runId);
        if (success) {
            Manifest.writeSuccess(output);
        }
        assertNoSqliteCompanions(checkpoint);
        return new Retained(output, staging, checkpoint, segment, runId);
    }

    private static Path writeSegment(Path staging, String name) throws Exception {
        return SortTestSupport.writePageRun(staging.resolve(name),
                List.of(object("a", 1), object("b", 2)), CMP);
    }

    private static ObjectEntry object(String key, long size) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), size, 0L, null, null,
                null, false, null, null, null, null);
    }

    private static void assertNoSqliteCompanions(Path checkpoint) {
        assertThat(Files.exists(checkpoint.resolveSibling(checkpoint.getFileName() + "-wal"))).isFalse();
        assertThat(Files.exists(checkpoint.resolveSibling(checkpoint.getFileName() + "-shm"))).isFalse();
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    private record Retained(Path output, Path staging, Path checkpoint, Path segment, long runId) {
    }
}
