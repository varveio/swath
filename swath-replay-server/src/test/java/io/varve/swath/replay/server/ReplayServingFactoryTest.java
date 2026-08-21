/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.replay.fixture.SortedFixtures;
import io.varve.swath.replay.protocol.ByteKeys;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.store.DuckDbListingStore;
import io.varve.swath.replay.store.SortedParquetStore;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortTransformResult;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code --serving-mode} resolution: {@code sorted} serves a stamped fixture and <b>hard-fails</b>,
 * naming the reason, on every shape of ineligible one; {@code duckdb} always forces role-1 and still
 * serves those shapes correctly. Fixtures are built through the production paths ({@link
 * CaptureSorter} for stamped, {@code PartWriter} for unstamped).
 *
 * <p>These used to assert that {@code auto} <em>fell back</em> to DuckDB on each of those shapes.
 * The mode is gone — a measured run whose serving path can change without saying so is not a
 * measurement — so each case now pins the pair that replaced it: the named hard failure, and role-1
 * still serving the same fixture. The eligibility reasons themselves are covered exactly as before.
 */
class ReplayServingFactoryTest {

    @Test
    void sortedModeServesAStampedObjectsFixture(@TempDir Path dir) throws IOException {
        Path sorted = sortedFixture(dir, "b", "a", "c");

        ReplayServingFactory.Result result = ReplayServingFactory.open(sorted, ServingMode.SORTED, 1);
        try {
            assertThat(result.resolvedMode()).isEqualTo(ServingMode.SORTED);
            assertThat(result.parquetConnections()).isEqualTo(1);
            assertThat(result.requestAdmissionLimit())
                    .as("prefetch lookup and hits must run before backing-read admission")
                    .isZero();
            result.fixture().list(new S3ListRequest("bucket", null, null, null, null, 1000, true, false));
            assertThat(result.metrics().registry().find("swath.replay.serving.path")
                    .tag("mode", "sorted").counter().count()).isEqualTo(1.0);
            // The index-derive meters land on the same shared registry as the request meters.
            assertThat(result.metrics().registry().find("swath.replay.index.load.latency")
                    .tag("source", "derived").timer().count()).isEqualTo(1L);
            assertThat(result.metrics().registry().find("swath.replay.index.entries").summary()).isNotNull();
        } finally {
            result.fixture().close();
        }
    }

    @Test
    void duckDbStillServesAnUnstampedPartThatSortedModeRefuses(@TempDir Path dir) throws IOException {
        Path plain = dir.resolve("plain.parquet");
        writeUnsortedPart(plain, "a", "b", "c");

        ReplayServingFactory.Result result = ReplayServingFactory.open(plain, ServingMode.DUCKDB, 1);
        try {
            assertThat(result.resolvedMode()).isEqualTo(ServingMode.DUCKDB);
            assertThat(result.requestAdmissionLimit()).isEqualTo(1);
            assertThat(keys(result.fixture().list(
                    new S3ListRequest("bucket", null, null, null, null, 1000, true, false))))
                    .containsExactly("a", "b", "c");
        } finally {
            result.fixture().close();
        }
    }

    @Test
    void sortedModeHardFailsWithSanityFailedOnMisorderedStampedFiles(@TempDir Path dir) throws IOException {
        // A genuine 2-file rolled publish (file 1 honestly stamped file_index=1, file 2 file_index=2
        // + file_final=true) so the completeness check passes — but the two files' NAMES are
        // then swapped, so resolveFiles' lexical order disagrees with each file's own key content
        // (the file named "00001" actually holds the higher keys) — only the ascending-first-key
        // sanity check catches this, and it must still fire even though completeness is satisfied.
        Path fixtureDir = Files.createDirectories(dir.resolve("fixture"));
        rollToNFiles(dir, fixtureDir, List.of("a", "d"));   // part-00001=a (idx 1), part-00002=d (idx 2, final)
        Path first = fixtureDir.resolve("part-00001.parquet");
        Path second = fixtureDir.resolve("part-00002.parquet");
        Path tmp = fixtureDir.resolve("swap.tmp");
        Files.move(first, tmp);
        Files.move(second, first);
        Files.move(tmp, second);
        // Now "part-00001.parquet" holds "d" (file_index=2, final) and "part-00002.parquet" holds
        // "a" (file_index=1) — completeness still holds (indices {1,2} present, final on max), but the
        // flattened first-key sequence in lexical order is d, a — not ascending.

        assertThatThrownBy(() -> ReplayServingFactory.open(fixtureDir, ServingMode.SORTED, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sanity_failed");
    }

    @Test
    void sortedModeHardFailsOnAnUnstampedFixture(@TempDir Path dir) throws IOException {
        Path plain = dir.resolve("plain.parquet");
        writeUnsortedPart(plain, "a", "b", "c");

        assertThatThrownBy(() -> ReplayServingFactory.open(plain, ServingMode.SORTED, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no_stamp");
    }

    /**
     * (a): a stamped {@code mode=objects} fixture whose rows are actually a mix of {@code OBJECT} and
     * {@code COMMON_PREFIX} (the exact shape {@code sort-fixture} produces from
     * a legacy delimiter'd capture, since it never inspects {@code row_type}) must NOT be served
     * sorted, and role-1's own {@code row_type='OBJECT'} materialisation filter still serves it correctly:
     * the returned object keys are byte-identical to role-1 serving the equivalent objects-only
     * fixture.
     */
    @Test
    void duckDbServesAMixedRowTypeFixtureCorrectly(@TempDir Path dir) throws IOException {
        Path mixed = mixedRowTypeSortedFixture(dir);
        Path objectsOnly = dir.resolve("objects-only.parquet");
        writeUnsortedPart(objectsOnly, "a1", "a3", "a5", "a7");

        ReplayServingFactory.Result mixedResult = ReplayServingFactory.open(mixed, ServingMode.DUCKDB, 1);
        try {
            assertThat(mixedResult.resolvedMode()).isEqualTo(ServingMode.DUCKDB);

            List<String> mixedKeys = keys(mixedResult.fixture().list(
                    new S3ListRequest("bucket", null, null, null, null, 1000, true, false)));

            ReplayServingFactory.Result objectsOnlyResult =
                    ReplayServingFactory.open(objectsOnly, ServingMode.DUCKDB, 1);
            try {
                List<String> objectsOnlyKeys = keys(objectsOnlyResult.fixture().list(
                        new S3ListRequest("bucket", null, null, null, null, 1000, true, false)));
                assertThat(mixedKeys).containsExactly("a1", "a3", "a5", "a7").isEqualTo(objectsOnlyKeys);
            } finally {
                objectsOnlyResult.fixture().close();
            }
        } finally {
            mixedResult.fixture().close();
        }
    }

    /** (b): {@code --serving-mode sorted} hard-fails on the same mixed-row-type fixture. */
    @Test
    void sortedModeHardFailsOnMixedRowTypes(@TempDir Path dir) throws IOException {
        Path mixed = mixedRowTypeSortedFixture(dir);

        assertThatThrownBy(() -> ReplayServingFactory.open(mixed, ServingMode.SORTED, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mixed_row_types");
    }

    /**
     * A multi-file directory where the FIRST file is a good stamped-objects fixture but a LATER file
     * is unstamped (e.g. a stray legacy part accidentally left in the fixture directory) must not
     * serve sorted. Every resolved file's stamp must be checked.
     */
    @Test
    void sortedModeHardFailsWhenALaterFileInAMultiFileDirIsUnstamped(@TempDir Path dir) throws IOException {
        Path fixtureDir = Files.createDirectories(dir.resolve("fixture"));
        sortToNamed(dir, fixtureDir.resolve("part-00001.parquet"), "a", "b");
        // A stray unstamped part, lexically AFTER the good stamped file, sharing the directory.
        writeUnsortedPart(fixtureDir.resolve("part-00002.parquet"), "c", "d");

        assertThatThrownBy(() -> ReplayServingFactory.open(fixtureDir, ServingMode.SORTED, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no_stamp");

        ReplayServingFactory.Result result = ReplayServingFactory.open(fixtureDir, ServingMode.DUCKDB, 1);
        try {
            // DuckDB role-1 still serves every key from every file in the directory, correctly.
            List<String> served = keys(result.fixture().list(
                    new S3ListRequest("bucket", null, null, null, null, 1000, true, false)));
            assertThat(served).containsExactly("a", "b", "c", "d");
        } finally {
            result.fixture().close();
        }
    }

    /**
     * Multi-file publish isn't atomic across files — a crash after renaming a prefix of the roll
     * sequence leaves a stamped PREFIX of the true output on disk. Simulated here by deleting the
     * true last file of a rolled fixture after the fact: {@code sorted} must refuse it by name (never
     * silently serve a truncated listing sorted), DuckDB role-1 must still serve the remaining files
     * correctly, and — critically — the UNTOUCHED complete set must still serve sorted.
     */
    @Test
    void sortedModeRefusesATruncatedMultiFilePublishButServesTheCompleteSet(@TempDir Path dir)
            throws IOException {
        Path completeDir = Files.createDirectories(dir.resolve("complete"));
        Path truncatedDir = Files.createDirectories(dir.resolve("truncated"));
        List<String> keys = List.of("a", "b", "c", "d");
        rollToNFiles(dir, completeDir, keys);
        rollToNFiles(dir, truncatedDir, keys);

        List<Path> completeFiles = new ArrayList<>(SortedFixtures.resolveFiles(completeDir));
        List<Path> truncatedFiles = new ArrayList<>(SortedFixtures.resolveFiles(truncatedDir));
        assertThat(completeFiles).hasSizeGreaterThan(1);
        assertThat(truncatedFiles).hasSize(completeFiles.size());
        // Simulate the crash: delete the TRUE last file (highest file_index) as if its rename never
        // happened.
        Files.delete(truncatedFiles.get(truncatedFiles.size() - 1));

        assertThatThrownBy(() -> ReplayServingFactory.open(truncatedDir, ServingMode.SORTED, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incomplete_multifile");

        ReplayServingFactory.Result truncatedResult = ReplayServingFactory.open(truncatedDir, ServingMode.DUCKDB, 1);
        try {
            assertThat(truncatedResult.resolvedMode()).isEqualTo(ServingMode.DUCKDB);
            List<String> served = keys(truncatedResult.fixture().list(
                    new S3ListRequest("bucket", null, null, null, null, 1000, true, false)));
            // DuckDB still serves whatever remains, correctly (fewer keys — the deleted file's keys
            // are genuinely gone from disk, same as any other missing-parquet-file scenario).
            assertThat(served).hasSizeLessThan(keys.size());
            assertThat(keys).containsAll(served);
        } finally {
            truncatedResult.fixture().close();
        }

        ReplayServingFactory.Result completeResult = ReplayServingFactory.open(completeDir, ServingMode.SORTED, 1);
        try {
            assertThat(completeResult.resolvedMode()).isEqualTo(ServingMode.SORTED);
            assertThat(keys(completeResult.fixture().list(
                    new S3ListRequest("bucket", null, null, null, null, 1000, true, false))))
                    .isEqualTo(keys);
        } finally {
            completeResult.fixture().close();
        }
    }

    @Test
    void duckDbModeForcesRoleOneEvenOnAStampedFixture(@TempDir Path dir) throws IOException {
        Path sorted = sortedFixture(dir, "a", "b", "c");

        ReplayServingFactory.Result result = ReplayServingFactory.open(sorted, ServingMode.DUCKDB, 1);
        try {
            assertThat(result.resolvedMode()).isEqualTo(ServingMode.DUCKDB);
            // Still serves (materialised role-1 over the same file).
            var page = result.fixture().list(
                    new S3ListRequest("bucket", null, null, null, null, 1000, true, false));
            assertThat(page.entries()).hasSize(3);
        } finally {
            result.fixture().close();
        }
    }

    // --- helpers ---

    private static Path sortedFixture(Path dir, String... keys) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("cap-" + System.nanoTime()));
        writeUnsortedPart(capture.resolve("part-0.parquet"), keys);
        Path out = Files.createDirectories(dir.resolve("out-" + System.nanoTime()));
        SortTransformResult result = new CaptureSorter(SortConfig.fromSystemProperties()).sort(capture, out);
        return result.finalFiles().getFirst();
    }

    private static void sortToNamed(Path dir, Path target, String... keys) throws IOException {
        Path fixture = sortedFixture(dir, keys);
        Files.move(fixture, target, StandardCopyOption.REPLACE_EXISTING);
    }

    /** Rolls {@code keys} into one file per key under {@code outputDir} (tiny {@code final-file-bytes}). */
    private static void rollToNFiles(Path dir, Path outputDir, List<String> keys) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("cap-" + System.nanoTime()));
        writeUnsortedPart(capture.resolve("part-0.parquet"), keys.toArray(String[]::new));
        SortConfig rolling = SortConfigs.rolledPerEntry();
        SortTransformResult result = new CaptureSorter(rolling).sort(capture, outputDir);
        assertThat(result.finalFiles()).hasSize(keys.size());
    }

    private static void writeUnsortedPart(Path path, String... keys) throws IOException {
        try (var writer = ParquetFixtures.open(path)) {
            for (String k : keys) {
                writer.write(ObjectEntries.bare(k));
            }
        }
    }

    /**
     * Builds a stamped {@code mode=objects} sorted file whose rows are actually a mix of
     * {@code OBJECT} and {@code COMMON_PREFIX} — the real-world shape {@code sort-fixture} produces
     * from a legacy delimiter'd capture (it never inspects {@code row_type}, only {@code version_id}
     * and adjacent-equal-key).
     */
    private static Path mixedRowTypeSortedFixture(Path dir) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("mixed-capture-" + System.nanoTime()));
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            writer.write(objectEntry("a1"));
            writer.write(new CommonPrefixEntry(KeyBytes.ofUtf8("a2/")));
            writer.write(objectEntry("a3"));
            writer.write(new CommonPrefixEntry(KeyBytes.ofUtf8("a4/")));
            writer.write(objectEntry("a5"));
            writer.write(new CommonPrefixEntry(KeyBytes.ofUtf8("a6/")));
            writer.write(objectEntry("a7"));
        }
        Path out = Files.createDirectories(dir.resolve("mixed-out-" + System.nanoTime()));
        SortTransformResult result = new CaptureSorter(SortConfig.fromSystemProperties()).sort(capture, out);
        return result.finalFiles().getFirst();
    }

    private static ObjectEntry objectEntry(String key) {
        return ObjectEntries.bare(key);
    }

    private static List<String> keys(S3ListResult result) {
        return result.entries().stream()
                .filter(e -> e instanceof S3ResultEntry.ObjectResult)
                .map(e -> ByteKeys.utf8(e.key()))
                .toList();
    }

    /**
     * The default belongs to the mode, and the mode is resolved here rather than by the caller.
     *
     * <p>Regression: the {@code serve} and {@code bench} commands used to resolve the default
     * themselves, both via {@link DuckDbListingStore#defaultConnectionCount()}. Because that result
     * is positive, the mode-aware branch below became unreachable from the CLI and
     * {@code --serving-mode sorted} with no flag opened four readers instead of the eight-to-
     * thirty-two the sorted store asks for. The factory was already right; nothing asserted that its
     * callers let it be right, so the two tests below go through {@link ReplayServer} -- the seam the
     * commands actually use -- and not through {@code open} alone.
     */
    @Test
    void sortedModeWithNoRequestedCountTakesTheSortedStoreDefault(@TempDir Path dir) throws Exception {
        Path sorted = sortedFixture(dir, "b", "a", "c");

        try (ReplayServer server = new ReplayServer("127.0.0.1", 0, "bucket", sorted, 0, ServingMode.SORTED)) {
            assertThat(server.resolvedServingMode()).isEqualTo(ServingMode.SORTED);
            assertThat(server.resolvedParquetConnections())
                    .isEqualTo(SortedParquetStore.defaultConnectionCount())
                    .isNotEqualTo(DuckDbListingStore.defaultConnectionCount());
        }
    }

    @Test
    void duckDbModeWithNoRequestedCountTakesTheDuckDbDefault(@TempDir Path dir) throws Exception {
        Path plain = dir.resolve("plain.parquet");
        writeUnsortedPart(plain, "a", "b", "c");

        try (ReplayServer server = new ReplayServer("127.0.0.1", 0, "bucket", plain, 0, ServingMode.DUCKDB)) {
            assertThat(server.resolvedServingMode()).isEqualTo(ServingMode.DUCKDB);
            assertThat(server.resolvedParquetConnections())
                    .isEqualTo(DuckDbListingStore.defaultConnectionCount());
        }
    }

    @Test
    void anExplicitCountIsHonouredOverTheModeDefault(@TempDir Path dir) throws Exception {
        Path sorted = sortedFixture(dir, "b", "a", "c");

        try (ReplayServer server = new ReplayServer("127.0.0.1", 0, "bucket", sorted, 3, ServingMode.SORTED)) {
            assertThat(server.resolvedParquetConnections()).isEqualTo(3);
        }
    }
}
