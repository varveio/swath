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
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.output.parquet.fixture.ParquetEntryReader;
import io.varve.swath.output.parquet.sorted.SortedParquetWriter;
import io.varve.swath.output.parquet.sorted.SortedParquetWriterFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;

class ParallelMergeBenchmarkTest {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    @Test
    void benchmarkArmIsExplicitlyDistinctFromManagedMergeReentry() {
        assertThat(ParallelMergeBenchmark.ARM).isEqualTo(SortArm.MERGE_BENCH_PAGE_RUN.name());
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
    void incompleteIdentityIsRefusedAsAnArgumentError(@TempDir Path root) throws Exception {
        Retained retained = completedRetained(root, "hash", "seg");

        assertThatIllegalArgumentException().isThrownBy(() -> BenchmarkCheckpointCatalog.read(
                        retained.output(), retained.staging(), new Manifest.Identity(null, retained.runId())))
                .withMessageContaining("complete checkpoint-backed run identity");
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
    void foreignSchemaVersionsModesAndHotJournalsAreRejected(@TempDir Path root) throws Exception {
        Retained schema = completedRetained(root.resolve("schema"), "hash", "seg");
        executeSql(schema.checkpoint(), "PRAGMA user_version=999");
        assertThatIllegalArgumentException().isThrownBy(() -> external(schema.staging()))
                .withMessageContaining("schema version");

        Retained mode = completedRetained(root.resolve("mode"), "hash", "seg");
        executeSql(mode.checkpoint(), "UPDATE run_meta SET mode='VERSIONS' WHERE id=" + mode.runId());
        assertThatIllegalArgumentException().isThrownBy(() -> external(mode.staging()))
                .withMessageContaining("mode=VERSIONS");

        for (String suffix : List.of("-journal", "-wal", "-shm")) {
            Retained journal = completedRetained(root.resolve("journal" + suffix), "hash", "seg");
            Files.createFile(journal.checkpoint().resolveSibling(
                    journal.checkpoint().getFileName() + suffix));
            assertThatIllegalArgumentException().isThrownBy(() -> external(journal.staging()))
                    .withMessageContaining("live SQLite companion");
        }
    }

    @Test
    @ResourceLock("SYSTEM_PROPERTIES")
    void symlinkedAuthorityDirectoriesAreRejected(@TempDir Path root) throws Exception {
        Retained stagingLink = completedRetained(root.resolve("staging-link"), "hash", "seg");
        Path externalStaging = root.resolve("external-staging");
        Files.move(stagingLink.staging(), externalStaging);
        Files.createSymbolicLink(stagingLink.staging(), externalStaging);
        assertThatIllegalArgumentException().isThrownBy(() -> external(stagingLink.staging()))
                .withMessageContaining("non-symlink directory");

        Retained checkpointLink = completedRetained(root.resolve("checkpoint-link"), "hash", "seg");
        Path checkpointDir = checkpointLink.output().resolve(".swath");
        Path externalCheckpointDir = root.resolve("external-checkpoint");
        Files.move(checkpointDir, externalCheckpointDir);
        Files.createSymbolicLink(checkpointDir, externalCheckpointDir);
        assertThatIllegalArgumentException().isThrownBy(() -> external(checkpointLink.staging()))
                .withMessageContaining("checkpoint directory")
                .withMessageContaining("non-symlink");
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
    @ResourceLock("SYSTEM_PROPERTIES")
    void retainedType2VersionlessLatestTrueCanonicalizesToParquetFalse(@TempDir Path root)
            throws Exception {
        ObjectEntry liveObject = new ObjectEntry(
                KeyBytes.ofUtf8("a"),
                42L,
                "2026-08-28T12:34:56.123456789Z",
                "0123456789abcdef0123456789abcdef",
                "STANDARD",
                null,
                true,
                "owner-id",
                "owner-display",
                "SHA256",
                "FULL_OBJECT");
        Retained retained = completedRetained(
                root.resolve("canonical-row"), "hash", "seg", List.of(liveObject));
        ParallelMergeBenchmark.CorpusCatalog catalog = external(retained.staging());
        Path scratch = Files.createDirectory(root.resolve("scratch-canonical-row"));
        try {
            ParallelMergeBenchmark.ArmResult result = ParallelMergeBenchmark.runArm(
                    scratch, catalog, 1, "canonical-row",
                    config -> new SortedParquetWriterFactory(config, SortMode.OBJECTS));

            assertThat(result.totalRows).isEqualTo(1);
            assertThat(result.multisetDigest).isEqualTo(catalog.oracle().multisetDigest());
            try (ParquetEntryReader reader = new ParquetEntryReader(result.finalFiles.getFirst())) {
                assertThat(reader.next()).isEqualTo(new ObjectEntry(
                        liveObject.key(), liveObject.size(), liveObject.lastModifiedEpochMicros(),
                        liveObject.etag(), liveObject.storageClass(), null, false,
                        liveObject.ownerId(), liveObject.ownerDisplayName(),
                        liveObject.checksumAlgorithm(), liveObject.checksumType()));
                assertThat(reader.hasNext()).isFalse();
            }
        } finally {
            SortBenchCorpus.deleteTree(scratch);
        }
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
    void rowOracleNormalizesOnlyTheCanonicalParquetRepresentation(@TempDir Path root)
            throws Exception {
        ObjectEntry source = new ObjectEntry(
                KeyBytes.ofUtf8("a"), 42L, "2026-08-28T14:34:56.123456+02:00",
                "etag", "STANDARD", null, true, "owner", "display", "SHA256", "FULL_OBJECT");
        ObjectEntry canonical = new ObjectEntry(
                source.key(), source.size(), source.lastModifiedEpochMicros(),
                source.etag(), source.storageClass(), null, false, source.ownerId(),
                source.ownerDisplayName(), source.checksumAlgorithm(), source.checksumType());
        BenchmarkRowOracle.InputOracle oracle = BenchmarkRowOracle.inputForTesting(List.of(source));

        Path canonicalFile = writeParquet(root.resolve("canonical.parquet"), List.of(source));
        assertThat(BenchmarkRowOracle.validateOutput(List.of(canonicalFile), oracle, CMP).rows())
                .isEqualTo(1);
        try (ParquetEntryReader reader = new ParquetEntryReader(canonicalFile)) {
            assertThat(reader.next()).isEqualTo(canonical);
            assertThat(reader.hasNext()).isFalse();
        }

        ObjectEntry changedSize = new ObjectEntry(
                canonical.key(), canonical.size() + 1, canonical.lastModifiedEpochMicros(),
                canonical.etag(), canonical.storageClass(), null, false, canonical.ownerId(),
                canonical.ownerDisplayName(), canonical.checksumAlgorithm(), canonical.checksumType());
        Path changedFile = writeParquet(root.resolve("changed-size.parquet"), List.of(changedSize));
        assertThatThrownBy(() -> BenchmarkRowOracle.validateOutput(List.of(changedFile), oracle, CMP))
                .hasMessageContaining("input oracle");
    }

    @Test
    void versionedAndDeleteMarkerLatestBitsRemainPartOfTheFullRowOracle(@TempDir Path root)
            throws Exception {
        ObjectEntry object = new ObjectEntry(
                KeyBytes.ofUtf8("a"), 42L, 1_777_000_000_123_456L,
                "etag", "STANDARD", "object-version", true,
                "owner", "display", "SHA256", "FULL_OBJECT");
        DeleteMarkerEntry marker = new DeleteMarkerEntry(
                KeyBytes.ofUtf8("b"), "delete-version", false,
                1_777_000_000_654_321L, "owner");
        List<ListEntry> source = List.of(object, marker);
        BenchmarkRowOracle.InputOracle oracle = BenchmarkRowOracle.inputForTesting(source);

        Path faithfulFile = writeParquet(root.resolve("faithful.parquet"), source);
        assertThat(BenchmarkRowOracle.validateOutput(List.of(faithfulFile), oracle, CMP).rows())
                .isEqualTo(2);
        try (ParquetEntryReader reader = new ParquetEntryReader(faithfulFile)) {
            assertThat(reader.next()).isEqualTo(object);
            assertThat(reader.next()).isEqualTo(marker);
            assertThat(reader.hasNext()).isFalse();
        }

        ObjectEntry flippedObject = new ObjectEntry(
                object.key(), object.size(), object.lastModifiedEpochMicros(),
                object.etag(), object.storageClass(), object.versionId(), false,
                object.ownerId(), object.ownerDisplayName(),
                object.checksumAlgorithm(), object.checksumType());
        Path flippedObjectFile = writeParquet(
                root.resolve("flipped-object.parquet"), List.of(flippedObject, marker));
        assertThatThrownBy(() -> BenchmarkRowOracle.validateOutput(
                List.of(flippedObjectFile), oracle, CMP))
                .hasMessageContaining("rows=2 expected_rows=2");

        DeleteMarkerEntry flippedMarker = new DeleteMarkerEntry(
                marker.key(), marker.versionId(), true,
                marker.lastModifiedEpochMicros(), marker.ownerId());
        Path flippedMarkerFile = writeParquet(
                root.resolve("flipped-marker.parquet"), List.of(object, flippedMarker));
        assertThatThrownBy(() -> BenchmarkRowOracle.validateOutput(
                List.of(flippedMarkerFile), oracle, CMP))
                .hasMessageContaining("rows=2 expected_rows=2");
    }

    @Test
    void masterSnapshotHashesEveryRegularFileEvenWhenMetadataIsRestored(@TempDir Path output)
            throws Exception {
        Path finalPart = Files.writeString(output.resolve("part.parquet"), "first");
        var modified = Files.getLastModifiedTime(finalPart);
        BenchmarkMasterSnapshot snapshot = BenchmarkMasterSnapshot.capture(output);

        Files.writeString(finalPart, "other");
        Files.setLastModifiedTime(finalPart, modified);

        assertThatThrownBy(snapshot::verifyUnchanged)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("modified retained master tree");
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
    void currentFormatCorpusRecordPinsCanonicalGeneratedCorpus(@TempDir Path parent)
            throws Exception {
        ParallelMergeBenchmark.PreparedGenerated prepared =
                ParallelMergeBenchmark.prepareGenerated(parent, 2, 40, 10, 5);
        try {
            Path record = parent.resolve("CORPUS.varve");
            Files.writeString(record, corpusRecord(prepared.catalog()));

            BenchmarkCorpusRecord.verify(record.toString(), prepared.catalog());

            Files.writeString(record, corpusRecord(prepared.catalog())
                    .replace(BenchmarkCorpusRecord.FORMAT, "swath-page-run-corpus-v3"));
            assertThatThrownBy(() -> BenchmarkCorpusRecord.verify(
                    record.toString(), prepared.catalog()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("format")
                    .hasMessageContaining(BenchmarkCorpusRecord.FORMAT);

            Files.writeString(record, corpusRecord(prepared.catalog())
                    + "legacy_page_run_format=v3\n");
            assertThatThrownBy(() -> BenchmarkCorpusRecord.verify(
                    record.toString(), prepared.catalog()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("fields disagree");
        } finally {
            SortBenchCorpus.deleteTree(prepared.root());
        }
    }

    @Test
    void corpusRecordRejectsGeometryAndIdentityDrift(@TempDir Path staging) throws Exception {
        Path segment = writeSegment(staging, "seg-generated-0.pageseg");
        ParallelMergeBenchmark.CorpusCatalog catalog =
                ParallelMergeBenchmark.snapshotCatalog("generated", staging, List.of(segment));
        Path record = staging.resolveSibling("CORPUS.varve");
        Files.writeString(record, corpusRecord(catalog).replace(
                "corpus_id=" + catalog.identity(), "corpus_id=" + "0".repeat(64)));

        assertThatThrownBy(() -> BenchmarkCorpusRecord.verify(record.toString(), catalog))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("corpus_id");

        Files.writeString(record, corpusRecord(catalog).replace(
                "bytes=" + Files.size(segment), "bytes=" + (Files.size(segment) + 1)));
        assertThatThrownBy(() -> BenchmarkCorpusRecord.verify(record.toString(), catalog))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("bytes");
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
                .contains("arm=" + SortArm.MERGE_BENCH_PAGE_RUN.name(), "source=generated",
                        "corpus_id=" + catalog.identity(), "git_sha=deadbeef",
                        "run_id=-1", "args_hash=not_applicable", "cache_state=warm_primed",
                        "logical_output_fingerprint=0123");
    }

    @Test
    void benchmarkParallelismLabelsDescribePipelineEncoders() {
        ParallelMergeBenchmark.ArmResult pipeline = new ParallelMergeBenchmark.ArmResult();
        pipeline.requestedEncoders = 4;
        pipeline.actualEncoders = 2;
        pipeline.finalizationParallelism = 2;

        assertThat(pipeline.parallelismFields())
                .isEqualTo("requested_encoders=4 actual_encoders=2 finalization_parallelism=2");
    }

    @Test
    void bracketStatisticsUseOverflowSafeMediansAndRejectHighVariance() {
        ParallelMergeBenchmark.SampleStats stable = ParallelMergeBenchmark.sampleStats(
                List.of(sample(100), sample(105), sample(110)));
        assertThat(stable.minNanos()).isEqualTo(100);
        assertThat(stable.medianNanos()).isEqualTo(105);
        assertThat(stable.maxNanos()).isEqualTo(110);
        assertThat(stable.spreadPct()).isCloseTo(1000.0 / 105.0,
                org.assertj.core.data.Offset.offset(0.0001));
        assertThat(stable.stable(15.0)).isTrue();

        ParallelMergeBenchmark.SampleStats unstable = ParallelMergeBenchmark.sampleStats(
                List.of(sample(100), sample(200)));
        assertThat(unstable.medianNanos()).isEqualTo(150);
        assertThat(unstable.stable(15.0)).isFalse();

        ParallelMergeBenchmark.SampleStats nearOverflow = ParallelMergeBenchmark.sampleStats(
                List.of(sample(Long.MAX_VALUE - 10), sample(Long.MAX_VALUE - 2)));
        assertThat(nearOverflow.medianNanos()).isEqualTo(Long.MAX_VALUE - 6);
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

    private static Retained completedRetained(
            Path root, String argsHash, String kind, List<ListEntry> entries) throws Exception {
        return retained(root, argsHash, kind, true, true, entries);
    }

    private static Retained retained(Path root, String argsHash, String kind,
                                     boolean completeRun, boolean success) throws Exception {
        return retained(root, argsHash, kind, completeRun, success,
                List.of(object("a", 1), object("b", 2)));
    }

    private static Retained retained(Path root, String argsHash, String kind,
                                     boolean completeRun, boolean success,
                                     List<ListEntry> entries) throws Exception {
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
            segment = writeSegment(staging, name, entries);
            store.partFinalized(new PartFinalize(runId, 0, name,
                    new PageRunFormat(PageRunFormat.CURRENT_FORMAT_VERSION,
                            PageRunFormat.ABSENT_EXTENSION),
                    entries.size(), Files.size(segment), List.of()));
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
        return writeSegment(staging, name, List.of(object("a", 1), object("b", 2)));
    }

    private static Path writeSegment(Path staging, String name, List<ListEntry> entries) throws Exception {
        return SortTestSupport.writePageRun(staging.resolve(name), entries, CMP);
    }

    private static Path writeParquet(Path path, List<ListEntry> entries) throws IOException {
        try (SortedFileWriter writer = new SortedParquetWriter(
                path, SortConfig.fromProperties(ignored -> null), SortMode.VERSIONS, 1)) {
            for (ListEntry entry : entries) {
                writer.write(entry);
            }
        }
        return path;
    }

    private static ObjectEntry object(String key, long size) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), size, 0L, null, null,
                null, false, null, null, null, null);
    }

    private static void assertNoSqliteCompanions(Path checkpoint) {
        assertThat(Files.exists(checkpoint.resolveSibling(checkpoint.getFileName() + "-wal"))).isFalse();
        assertThat(Files.exists(checkpoint.resolveSibling(checkpoint.getFileName() + "-shm"))).isFalse();
    }

    private static void executeSql(Path checkpoint, String sql) throws Exception {
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + checkpoint.toAbsolutePath());
             var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    private static String corpusRecord(ParallelMergeBenchmark.CorpusCatalog catalog) {
        long bytes = catalog.inputs().stream()
                .mapToLong(ParallelMergeBenchmark.CorpusInput::size)
                .sum();
        return String.join("\n",
                "format=" + BenchmarkCorpusRecord.FORMAT,
                "corpus=" + catalog.stagingDir().toAbsolutePath().normalize(),
                "rows=" + catalog.oracle().rows(),
                "segments=" + catalog.inputs().size(),
                "bytes=" + bytes,
                "corpus_id=" + catalog.identity(),
                "multiset=" + catalog.oracle().multisetDigest(),
                "created_by_head=" + "a".repeat(40)) + "\n";
    }

    private static ParallelMergeBenchmark.ArmResult sample(long elapsedNanos) {
        ParallelMergeBenchmark.ArmResult result = new ParallelMergeBenchmark.ArmResult();
        result.elapsedNanos = elapsedNanos;
        return result;
    }

    private record Retained(Path output, Path staging, Path checkpoint, Path segment, long runId) {
    }
}
