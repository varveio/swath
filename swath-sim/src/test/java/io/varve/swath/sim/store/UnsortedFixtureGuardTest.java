/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.output.parquet.sorted.RowGroupOrderException;
import io.varve.swath.output.parquet.sorted.SortedParquetIndex;
import io.varve.swath.output.parquet.sorted.SortedParquetWriter;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListObjectsV2Pager;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.server.ReplayMetrics;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortedFileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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

    /** A finite upper bound past every plain-ASCII key here — the skip-scan declines an open one. */
    private static final ByteKey PAST_EVERY_KEY = ByteKey.copyOf(new byte[] {(byte) 0xFF});

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
        assertThat(SortedParquetIndex.rowGroupSpans(soleFile(fixture))).hasSizeGreaterThan(1);

        assertThatThrownBy(() -> SimStoreFactory.open(fixture, SimStoreBackend.STREAMING))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sorted-eligible");
    }

    /**
     * The windowed tier's <b>guarded</b> half. Its {@code delimiter=/} skip-scan reads the fixture in
     * its physical order through {@code SortedParquetRowGroupReader.KeyCursor}, so it sees the disorder and
     * refuses — with the same typed reason the streaming tier raises, counted on the replay module's
     * side of the seam because that is the store doing the reading. The simulator reaches this path on
     * every structure probe a split issues, so it is not an exotic corner of the tier.
     */
    @Test
    void theWindowedTiersDelimiterPathRefusesADisorderedRowGroup() throws IOException {
        Path fixture = disorderedTwoFileFixture("windowed-rollup");

        SimStoreFactory.Result result = SimStoreFactory.open(fixture, SimStoreBackend.WINDOWED);
        try (ListingStore store = result.store()) {
            assertThatThrownBy(() -> store.delimitedRollup(null, true, PAST_EVERY_KEY, new byte[0],
                    new byte[] {'/'}, 1000, Projection.KEYS_ONLY))
                    .isInstanceOfSatisfying(RowGroupOrderException.class,
                            e -> assertThat(e.reason()).isEqualTo(RowGroupOrderException.ROW_GROUP_DISORDER))
                    .hasMessageContaining("strictly ascending");
        }
        assertThat(result.metrics().registry().find("swath.replay.serving.refused")
                .tag("reason", RowGroupOrderException.ROW_GROUP_DISORDER).counter().count()).isEqualTo(1);
    }

    /**
     * The windowed tier's <b>masked</b> half, pinned rather than described — and it is worse than the
     * "short page" it is easy to assume. Its plain range reads trust the derived index, which
     * describes an order the file does not have: rows come back in the order they physically sit in,
     * and a key that sorts below the cursor but sits above it on disk is never returned at all.
     * Nothing is short and nothing is refused; the client walks to a clean, un-truncated end of
     * listing and is simply missing a key.
     *
     * <p>The fixture's first file holds {@code a/1, aa/9, a/2, z/9} in that physical order. A whole
     * listing walk returns {@code a/1, aa/9, z/9} from it — {@code a/2} is passed over, because by the
     * time the pager asks for "the next key after {@code aa/9}" the reader has already stepped past
     * where {@code a/2} lies — and then the second file's keys, so {@code z/9} is emitted before
     * {@code b/1} it should sort after.
     *
     * <p><b>This assertion changed when the store stopped answering range reads with a SQL query.</b>
     * That query carried an {@code ORDER BY key}, which re-sorted each page and hid the disorder
     * behind it: the walk came back fully sorted and complete, and the loss only showed up on a
     * resumed cursor. The page-index reader has no sort to hide behind, so the same fixture now loses
     * a key on the very first page. Neither answer is right — a fixture that lies about its order gets
     * a wrong listing either way — but the new one is wrong <em>visibly</em>, which is the better
     * failure of the two.
     *
     * <p>This test asserts the wrong answer on purpose. It is the reason the sim's own tier
     * ({@code STREAMING}) reads in physical order and refuses, and part of the reason the windowed
     * tier is forced-only: if this behaviour ever changes, the change is a fix, and this is where it
     * is noticed.
     */
    @Test
    void theWindowedTiersRangeReadsSilentlyLoseKeysRatherThanShorteningAPage() throws IOException {
        Path fixture = disorderedTwoFileFixture("windowed-range");

        SimStoreFactory.Result whole = SimStoreFactory.open(fixture, SimStoreBackend.WINDOWED);
        try (ListingStore store = whole.store()) {
            assertThat(walk(store, null))
                    .as("a/2 sits physically after aa/9, so a cursor past aa/9 never comes back for it")
                    .containsExactly("a/1", "aa/9", "z/9", "b/1", "b/2");
        }

        // The same fixture, read by a cursor that starts where a steal or a split would put it.
        SimStoreFactory.Result resumed = SimStoreFactory.open(fixture, SimStoreBackend.WINDOWED);
        try (ListingStore store = resumed.store()) {
            assertThat(walk(store, "b/1".getBytes(StandardCharsets.UTF_8)))
                    .as("z/9 is above b/1 and in the fixture, and this walk never sees it")
                    .containsExactly("b/2");
        }
        assertThat(resumed.metrics().registry().find("swath.replay.serving.refused").counter()).isNull();
    }

    /**
     * Every key a paginating client sees walking from {@code startAfter} to the end of the listing —
     * driven through {@link ListObjectsV2Pager}, the same pager the simulator's own view drives, so the
     * truncation the client would act on is the one being observed. Ends only on an un-truncated page.
     */
    private static List<String> walk(ListingStore store, byte[] startAfter) {
        ListObjectsV2Pager pager = new ListObjectsV2Pager(store, new ReplayMetrics());
        List<String> walked = new ArrayList<>();
        byte[] cursor = startAfter;
        boolean truncated;
        do {
            S3ListResult page = pager.list(new S3ListRequest("sim", new byte[0], null, cursor,
                    null, 2, false, false));
            for (S3ResultEntry entry : page.entries()) {
                walked.add(new String(entry.key(), StandardCharsets.UTF_8));
                cursor = entry.key();
            }
            truncated = page.truncated();
        } while (truncated);
        return walked;
    }

    /**
     * A stamped, complete, <b>two-file</b> fixture that passes sorted eligibility and is disordered
     * inside the first file's only row group. Eligibility reads each row group's first key and proves
     * <em>those</em> ascend ({@code a/1} then {@code b/1}); it never reads the rest, so neither the
     * inversion at {@code a/2} nor {@code z/9} — physically the last row of the first file, above
     * every key of the second — is visible to it. Written straight through
     * {@link SortedParquetWriter} because the sorter cannot produce this; what it stands in for is a
     * listing published by some other producer and stamped sorted.
     */
    private Path disorderedTwoFileFixture(String name) throws IOException {
        Path out = Files.createDirectory(dir.resolve(name));
        writeStamped(out.resolve("part-00001.parquet"), 1, false, List.of("a/1", "aa/9", "a/2", "z/9"));
        writeStamped(out.resolve("part-00002.parquet"), 2, true, List.of("b/1", "b/2"));
        return out;
    }

    private static void writeStamped(Path file, int fileIndex, boolean last, List<String> keys)
            throws IOException {
        try (SortedFileWriter writer = new SortedParquetWriter(file, SortConfigs.base(),
                SortMode.OBJECTS, fileIndex)) {
            if (last) {
                writer.markFinal();
            }
            for (String key : keys) {
                writer.write(ObjectEntries.bare(key));
            }
        }
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
