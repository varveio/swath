/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.RowGroupOrderException;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortedFileIndex;
import io.varve.swath.sort.SortedFileWriter;
import io.varve.swath.sort.SortedParquetWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * <b>A fixture that is not in ascending key order must stop the run that loads it</b> — the guard is
 * inline in the loops each tier already runs, so this suite is what proves each tier's claim, tier by
 * tier, against a fixture built to be out of order on purpose.
 *
 * <p>The fixtures here are written straight through {@link SortedParquetWriter}, deliberately
 * bypassing {@code CaptureSorter}: the writer stamps whatever it is handed, so what comes out is
 * stamped, {@code mode=objects}, pure-{@code OBJECT}, complete — i.e. sorted-<em>eligible</em> by every
 * check a reader can make from the footer — and internally disordered. That is not a hypothetical
 * shape: a listing published by an older or foreign producer can carry exactly it, and the whole point
 * of an inline check is that such a fixture fails where it is read rather than being simulated as if
 * it were a real bucket's key order.
 *
 * <p>Which tier catches what — and which one cannot — is documented on {@link SimStoreFactory}; this
 * suite pins both halves, because a masked tier that is only described in prose is a claim, not a
 * behaviour.
 */
class UnsortedFixtureGuardTest {

    @TempDir
    private Path dir;

    /**
     * The tier a corpus-scale fixture actually lands on. Its row groups are decoded as cursors reach
     * them, and the first row that is not strictly above its predecessor fails that decode — naming
     * the file, the row group and the row, because the caller is a sweep over many fixtures and
     * "something was unsorted" is not an actionable report.
     *
     * <p>That report has to be machine-readable too: the sweep sees a run that ended in an exception,
     * and must classify the exclusion from {@link RowGroupOrderException#reason()} and the counter the
     * decode bumps <b>before</b> it rethrows — never by matching the message.
     */
    @Test
    void theStreamingTierFailsOnTheFirstUnsortedRowNamingFileRowGroupAndRow() throws IOException {
        Path fixture = stampedFixture("out", List.of("a/1", "a/3", "a/2", "a/4"));
        Path file = soleFile(fixture);

        SimStoreFactory.Result result = SimStoreFactory.open(fixture, SimStoreBackend.STREAMING);
        try (ListingStore store = result.store()) {
            assertThatThrownBy(() -> store.rows(null, true, null, 10, Projection.KEYS_ONLY))
                    .isInstanceOfSatisfying(RowGroupOrderException.class, e -> {
                        assertThat(e.reason()).isEqualTo(RowGroupOrderException.ROW_GROUP_DISORDER);
                        assertThat(e.file()).isEqualTo(file);
                        assertThat(e.rowGroup()).isZero();
                        assertThat(e.row()).isEqualTo(2);
                    })
                    .hasMessageContaining("row group 0 of " + file)
                    .hasMessageContaining("strictly ascending")
                    .hasMessageContaining("key 2");
        }
        assertThat(result.metrics().registry().find(SimStoreMetrics.SEGMENT_REFUSED_METRIC)
                .tag("reason", RowGroupOrderException.ROW_GROUP_DISORDER).counter().count()).isEqualTo(1);
    }

    /**
     * {@code AUTO} over a fixture too large for the arena resolves to the streaming tier, which is how
     * every real sweep fixture is served — so the guard has to survive the resolution, not just a
     * forced backend. The arena is declined here by budget (one byte), the same way a 10-million-key
     * fixture declines it by size.
     */
    @Test
    void autoResolvedToStreamingCarriesTheSameFailure() throws IOException {
        Path fixture = stampedFixture("out", List.of("a/1", "a/3", "a/2", "a/4"));
        SimStoreConfig arenaCannotFit = new SimStoreConfig(1L, SimStoreConfig.DEFAULT_STREAMING_MAX_RESIDENT_BYTES);

        SimStoreFactory.Result result = SimStoreFactory.open(fixture, SimStoreBackend.AUTO, arenaCannotFit);
        assertThat(result.resolvedBackend()).isEqualTo(SimStoreBackend.STREAMING);
        try (ListingStore store = result.store()) {
            assertThatThrownBy(() -> store.rows(null, true, null, 10, Projection.KEYS_ONLY))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("row group 0 of " + soleFile(fixture))
                    .hasMessageContaining("strictly ascending");
        }
    }

    /**
     * The arena is loaded through the Parquet store, whose reads are {@code ORDER BY key}, so a
     * duplicate is the one violation that survives to reach its check — and the failure must name the
     * <b>fixture</b>, which the arena itself cannot know (it is built over a store handle, not a path).
     */
    @Test
    void theArenaTierNamesTheFixtureWhenItsKeysAreNotUnique() throws IOException {
        Path fixture = Files.createDirectory(dir.resolve("cap"));
        try (var writer = ParquetFixtures.open(fixture.resolve("part-0.parquet"))) {
            writer.write(ObjectEntries.bare("a/1"));
            writer.write(ObjectEntries.bare("a/1"));
        }

        assertThatThrownBy(() -> SimStoreFactory.open(fixture, SimStoreBackend.ARENA))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fixture " + fixture)
                .hasMessageContaining("duplicate");
    }

    /**
     * The documented masking, pinned rather than described: the arena tier reads through a store that
     * sorts, so a disordered fixture loads without complaint and is served in key order. Nothing is
     * lost and nothing is out of order — but the disorder is <b>not reported</b>, which is exactly why
     * the tiers that read a fixture in its physical order carry the guard and this one does not
     * pretend to.
     */
    @Test
    void theArenaTierNormalisesDisorderInsteadOfReportingIt() throws IOException {
        Path fixture = Files.createDirectory(dir.resolve("cap"));
        try (var writer = ParquetFixtures.open(fixture.resolve("part-0.parquet"))) {
            for (String key : List.of("a/1", "a/3", "a/2", "a/4")) {
                writer.write(ObjectEntries.bare(key));
            }
        }

        SimStoreFactory.Result result = SimStoreFactory.open(fixture, SimStoreBackend.ARENA);
        assertThat(result.resolvedBackend()).isEqualTo(SimStoreBackend.ARENA);
        try (ListingStore store = result.store()) {
            List<ListedObject> rows = store.rows(null, true, null, 10, Projection.KEYS_ONLY);
            assertThat(rows.stream().map(row -> new String(row.key(), StandardCharsets.UTF_8)).toList())
                    .containsExactly("a/1", "a/2", "a/3", "a/4");
        }
    }

    /**
     * Disorder that spans row groups is caught a level earlier, at index derive, and so is not a
     * decode failure but an eligibility one: the tier refuses the fixture outright rather than serving
     * routing built on a broken order. Kept here alongside the intra-group case so the two halves of
     * "sorted" — between groups and within one — are visibly both covered.
     */
    @Test
    void disorderAcrossRowGroupsIsRefusedAtEligibilityRatherThanServed() throws IOException {
        // 1 KiB row groups over ~200-byte keys: enough rows to make several groups, written wholly
        // descending, so the disorder is visible between group first keys and not only within a group.
        Path fixture = Files.createDirectory(dir.resolve("out"));
        int rows = 600;
        try (SortedFileWriter writer = new SortedParquetWriter(fixture.resolve("part-00001.parquet"),
                SortConfigs.manySmallRowGroups(), SortMode.OBJECTS, 1)) {
            writer.markFinal();
            for (int i = 0; i < rows; i++) {
                writer.write(ObjectEntries.bare(String.format("%08d", rows - i) + "x".repeat(190)));
            }
        }
        assertThat(SortedFileIndex.rowGroupSpans(soleFile(fixture))).hasSizeGreaterThan(1);

        assertThatThrownBy(() -> SimStoreFactory.open(fixture, SimStoreBackend.STREAMING))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sorted-eligible");
    }

    /** A stamped, complete, single-row-group fixture holding {@code keys} in exactly the order given. */
    private Path stampedFixture(String name, List<String> keys) throws IOException {
        Path out = Files.createDirectory(dir.resolve(name));
        try (SortedFileWriter writer = new SortedParquetWriter(out.resolve("part-00001.parquet"),
                SortConfigs.base(), SortMode.OBJECTS, 1)) {
            writer.markFinal();
            for (String key : keys) {
                writer.write(ObjectEntries.bare(key));
            }
        }
        return out;
    }

    private static Path soleFile(Path fixture) throws IOException {
        try (var files = Files.list(fixture)) {
            return files.toList().getFirst();
        }
    }
}
