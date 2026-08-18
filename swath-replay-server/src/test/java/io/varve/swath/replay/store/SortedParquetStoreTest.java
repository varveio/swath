/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.replay.fixture.FixtureMetrics;
import io.varve.swath.replay.fixture.SortedFixtures;
import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.fixture.SortedFixtures.IndexLoadResult;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ByteKeys;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.UpperBound;
import io.varve.swath.replay.server.ReplayMetrics;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.RowGroupOrderException;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortedFileWriter;
import io.varve.swath.sort.SortedParquetWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link SortedParquetStore} over real, multi-row-group stamped sorted files (built through {@link
 * CaptureSorter}, the production path). Correctness is checked against a brute-force expectation: a
 * range read must return the first {@code limit} object keys in {@code [from, toExclusive)} — so if
 * the upper-bound invariant ever under-provisioned across a row-group boundary the store would
 * short-return and the assertion would catch it.
 */
class SortedParquetStoreTest {

    /** Tiny final row groups force several groups from a few hundred rows so the multi-group path is real. */
    private static SortConfig manySmallGroups() {
        return SortConfigs.manySmallRowGroups();
    }

    /** Tiny final-file-bytes rolls the output into several range-disjoint files. */
    private static SortConfig rolledSmallFiles() {
        return SortConfigs.base().withFinalFileBytes(4096L).withFinalRowGroupBytes(1024L).withMergeBudgetBytes(64L << 20);
    }

    @Test
    void returnsLimitRowsAcrossManyRowGroups(@TempDir Path dir) throws IOException {
        List<String> keys = keys(600);
        Fixture fixture = writeSorted(dir, manySmallGroups(), keys);
        assertThat(fixture.index).hasSizeGreaterThan(1);   // the multi-group path is actually exercised

        try (SortedParquetStore store = store(fixture)) {
            // A window straddling row-group boundaries at exactly the limit: from the middle, 50 rows.
            String from = keys.get(275);
            List<ListedObject> rows = store.rows(key(from), false, null, 50, Projection.KEYS_ONLY);
            assertThat(keyStrings(rows)).isEqualTo(expected(keys, from, false, null, 50));

            // Inclusive lower bound returns `from` itself.
            List<ListedObject> inclusive = store.rows(key(from), true, null, 10, Projection.KEYS_ONLY);
            assertThat(keyStrings(inclusive)).isEqualTo(expected(keys, from, true, null, 10));

            // Bounded upper.
            String to = keys.get(400);
            List<ListedObject> bounded = store.rows(key(from), false, key(to), 1000, Projection.KEYS_ONLY);
            assertThat(keyStrings(bounded)).isEqualTo(expected(keys, from, false, to, 1000));
        }
    }

    @Test
    void emptyFixtureReturnsNoRows(@TempDir Path dir) throws IOException {
        Fixture fixture = writeSorted(dir, manySmallGroups());   // no keys
        assertThat(fixture.index).isEmpty();

        try (SortedParquetStore store = store(fixture)) {
            assertThat(store.rows(null, true, null, 1000, Projection.WITH_OWNER)).isEmpty();
            assertThat(store.rows(key("anything"), false, null, 1000, Projection.KEYS_ONLY)).isEmpty();
        }
    }

    @Test
    void rangeStartBeforeTheFirstKeyReturnsFromTheStart(@TempDir Path dir) throws IOException {
        List<String> keys = keys(300);
        Fixture fixture = writeSorted(dir, manySmallGroups(), keys);

        try (SortedParquetStore store = store(fixture)) {
            List<ListedObject> rows = store.rows(key("!before"), false, null, 5, Projection.KEYS_ONLY);
            assertThat(keyStrings(rows)).isEqualTo(keys.subList(0, 5));
        }
    }

    @Test
    void rangePastTheLastRowGroupReturnsNothing(@TempDir Path dir) throws IOException {
        List<String> keys = keys(300);
        Fixture fixture = writeSorted(dir, manySmallGroups(), keys);

        try (SortedParquetStore store = store(fixture)) {
            List<ListedObject> rows = store.rows(key("zzzzzz"), false, null, 5, Projection.KEYS_ONLY);
            assertThat(rows).isEmpty();
        }
    }

    @Test
    void spansMultipleRolledFiles(@TempDir Path dir) throws IOException {
        List<String> keys = keys(400);
        Fixture fixture = writeSorted(dir, rolledSmallFiles(), keys);
        assertThat(fixture.files).hasSizeGreaterThan(1);   // actually rolled into several files

        try (SortedParquetStore store = store(fixture)) {
            List<ListedObject> all = store.rows(null, true, null, 10_000, Projection.KEYS_ONLY);
            assertThat(keyStrings(all)).isEqualTo(keys);   // whole listing, in order, across files
        }
    }

    /**
     * The skip-scan ({@link SortedParquetStore#delimitedRollup}) against an independent brute-force
     * rollup over the same sorted key list, across a hierarchy deep and wide enough to force many
     * hops through many row groups: nested directories, several bare objects directly under the scan
     * prefix, and a resume ({@code from}) landing both between directories and inside one.
     */
    @Test
    void delimitedRollupMatchesBruteForceAcrossManyRowGroupsAndDirectories(@TempDir Path dir) throws IOException {
        List<String> keys = hierarchy();
        Fixture fixture = writeSorted(dir, manySmallGroups(), keys);
        assertThat(fixture.index).hasSizeGreaterThan(1);

        try (SortedParquetStore store = store(fixture)) {
            assertRollupMatches(store, keys, "a/", null, true, 1000);
            assertRollupMatches(store, keys, "a/", null, true, 3);   // truncates mid-hierarchy
            assertRollupMatches(store, keys, "a/", key("a/dir1/"), false, 1000);   // resume between dirs
            assertRollupMatches(store, keys, "a/", key("a/dir1/mid"), false, 1000);   // resume inside a dir
            assertRollupMatches(store, keys, "", null, true, 1000);   // root-level rollup, own directories
            assertRollupMatches(store, keys, "", null, true, 5);   // root-level, truncated
        }
    }

    /**
     * A resume boundary that lands strictly inside a directory must not re-emit that directory's
     * common prefix (S3 never re-lists a {@code CommonPrefix} the client has already seen past) — the
     * boundary guard co-located with the skip-scan hop loop, not a separate pass.
     */
    @Test
    void delimitedRollupSkipsACommonPrefixAtOrBelowFrom(@TempDir Path dir) throws IOException {
        Fixture fixture = writeSorted(dir, manySmallGroups(),
                "a/1.txt", "a/2.txt", "b/mid.txt", "b/zzz.txt", "c/1.txt");

        try (SortedParquetStore store = store(fixture)) {
            List<ListingStore.DelimitedEntry> rollup = store.delimitedRollup(
                    key("b/mid.txt"), false, upperOf(""), new byte[0], slash(), 1000, Projection.KEYS_ONLY);
            assertThat(entryStrings(rollup)).containsExactly("CP:c/");
        }
    }

    /**
     * {@code successor(P)} is not always a fresh key — S3 explicitly allows a real object to sit
     * exactly at a rolled-up prefix's successor (e.g. {@code successor("a/") == "a0"}). The skip-scan
     * must resume there inclusively and emit it as the very next entry.
     */
    @Test
    void delimitedRollupHandlesSuccessorLandingOnARealKey(@TempDir Path dir) throws IOException {
        Fixture fixture = writeSorted(dir, manySmallGroups(), "a-1.txt", "a/1.txt", "a0", "a1.txt");

        try (SortedParquetStore store = store(fixture)) {
            List<ListingStore.DelimitedEntry> rollup = store.delimitedRollup(
                    null, true, upperOf(""), new byte[0], slash(), 1000, Projection.KEYS_ONLY);
            assertThat(entryStrings(rollup)).containsExactly("OBJ:a-1.txt", "CP:a/", "OBJ:a0", "OBJ:a1.txt");
        }
    }

    /** An object key exactly equal to a common prefix's boundary rolls into that same prefix, once. */
    @Test
    void delimitedRollupAbsorbsAnObjectKeyEqualToTheCommonPrefixItself(@TempDir Path dir) throws IOException {
        Fixture fixture = writeSorted(dir, manySmallGroups(), "a/", "a/1.txt", "a/2.txt", "b.txt");

        try (SortedParquetStore store = store(fixture)) {
            List<ListingStore.DelimitedEntry> rollup = store.delimitedRollup(
                    null, true, upperOf(""), new byte[0], slash(), 1000, Projection.KEYS_ONLY);
            assertThat(entryStrings(rollup)).containsExactly("CP:a/", "OBJ:b.txt");
        }
    }

    @Test
    void delimitedRollupSpansMultipleRolledFiles(@TempDir Path dir) throws IOException {
        List<String> keys = hierarchy();
        Fixture fixture = writeSorted(dir, rolledSmallFiles(), keys);
        assertThat(fixture.files).hasSizeGreaterThan(1);

        try (SortedParquetStore store = store(fixture)) {
            assertRollupMatches(store, keys, "a/", null, true, 1000);
        }
    }

    @Test
    void delimitedRollupProjectsOwnerColumnsOnlyWhenAsked(@TempDir Path dir) throws IOException {
        Fixture fixture = writeSorted(dir, manySmallGroups(), "top.txt");

        try (SortedParquetStore store = store(fixture)) {
            ByteKey upper = upperOf("");
            ListedObject withOwner = store.delimitedRollup(null, true, upper, new byte[0], slash(), 1000,
                    Projection.WITH_OWNER).getFirst().object();
            assertThat(withOwner.ownerId()).isEqualTo("owner-id");
            assertThat(withOwner.ownerDisplayName()).isEqualTo("owner-display");

            ListedObject keysOnly = store.delimitedRollup(null, true, upper, new byte[0], slash(), 1000,
                    Projection.KEYS_ONLY).getFirst().object();
            assertThat(keysOnly.ownerId()).isNull();
            assertThat(keysOnly.ownerDisplayName()).isNull();
            assertThat(keysOnly.storageClass()).isEqualTo("STANDARD");
        }
    }

    /**
     * The no-prefix root rollup's real shape — {@code toExclusive == null} and {@code prefix == null}
     * (a genuinely open upper bound, {@link UpperBound.Open}) — must now be answered natively rather
     * than declined (public issue #77: an open upper bound used to fall through to the pager's
     * O(directories) range walk even on the sorted store). The only shape this store still declines is
     * a delimiter it does not know how to skip-scan.
     */
    @Test
    void delimitedRollupAnswersAnOpenUpperBoundAndDeclinesOnlyNonSlashDelimiters(@TempDir Path dir)
            throws IOException {
        List<String> keys = List.of("a/1.txt", "b/1.txt");
        Fixture fixture = writeSorted(dir, manySmallGroups(), "a/1.txt", "b/1.txt");

        try (SortedParquetStore store = store(fixture)) {
            List<ListingStore.DelimitedEntry> rootRollup =
                    store.delimitedRollup(null, true, null, null, slash(), 1000, Projection.KEYS_ONLY);
            assertThat(rootRollup).isNotNull();
            assertThat(entryStrings(rootRollup)).isEqualTo(expectedRollup(keys, "", null, true, 1000));

            ByteKey upper = upperOf("");
            assertThat(store.delimitedRollup(null, true, upper, new byte[0], new byte[] {'/', '/'}, 1000,
                    Projection.KEYS_ONLY)).isNull();   // multi-byte delimiter still declines
        }
    }

    /**
     * The regression this whole fast path exists to fix (public issue #77): a root {@code delimiter=/}
     * rollup over a wide-flat keyspace must cost O(prefixes emitted), never O(keys under them). A
     * dense fixture of 50 prefixes x 500 keys forces many row groups per prefix (tiny {@code
     * manySmallGroups} row groups), so a naive per-row scan would open on the order of the fixture's
     * row-group count once per key; the skip-scan's zero-I/O whole-group shortcut instead resolves most
     * of each prefix's span without opening a single row group. No wall-clock assertion: the row-group-open
     * counter pins the cost directly.
     */
    @Test
    void delimitedRollupCostIsBoundedByPrefixesNotKeys(@TempDir Path dir) throws IOException {
        int prefixCount = 50;
        int keysPerPrefix = 500;
        List<String> keys = new ArrayList<>(prefixCount * keysPerPrefix);
        for (int p = 0; p < prefixCount; p++) {
            for (int i = 0; i < keysPerPrefix; i++) {
                keys.add(String.format("prefix-%03d/obj-%04d", p, i));
            }
        }
        Fixture fixture = writeSorted(dir, manySmallGroups(), keys);
        assertThat(fixture.index).hasSizeGreaterThan(prefixCount);   // many more row groups than prefixes

        ReplayMetrics metrics = new ReplayMetrics();
        try (SortedParquetStore store = new SortedParquetStore(fixture.files, fixture.index, metrics, 2)) {
            List<ListingStore.DelimitedEntry> rollup =
                    store.delimitedRollup(null, true, null, null, slash(), prefixCount + 10, Projection.KEYS_ONLY);
            assertThat(rollup).hasSize(prefixCount);   // every prefix rolls up to exactly one CommonPrefix

            double rowGroupOpens = metrics.registry()
                    .find("swath.replay.delimiter.skipscan.row_group_opens").counter().count();
            assertThat(rowGroupOpens)
                    .as("row-group opens must scale with prefixes (%d), not with keys (%d)",
                            prefixCount, keys.size())
                    .isLessThan(prefixCount * 4.0)
                    .isLessThan(keys.size() / 10.0);
        }
    }

    /**
     * {@code delimitedRollup} honors only the range bounds it is given — like {@link
     * ListingStore#rows}, it never applies prefix semantics itself (see the {@code
     * ListingStore#delimitedRollup} contract). The pager always resolves a null/no-boundary {@code
     * from} to {@code prefix} itself, inclusive ({@code ListObjectsV2Pager#lowerBound}); mirror that
     * here so a {@code null} passed to this helper means "a fresh, non-continuation request", exactly
     * as it does in production, rather than "no floor at all".
     */
    private static void assertRollupMatches(SortedParquetStore store, List<String> keys, String prefix,
                                            ByteKey from, boolean fromInclusive, int limit) {
        ByteKey effectiveFrom = from != null ? from : (prefix.isEmpty() ? null : key(prefix));
        boolean effectiveInclusive = from != null ? fromInclusive : true;
        List<ListingStore.DelimitedEntry> rollup = store.delimitedRollup(effectiveFrom, effectiveInclusive,
                upperOf(prefix), prefix.getBytes(StandardCharsets.UTF_8), slash(), limit, Projection.KEYS_ONLY);
        assertThat(entryStrings(rollup)).isEqualTo(expectedRollup(keys, prefix,
                effectiveFrom == null ? null : ByteKeys.utf8(effectiveFrom.toByteArray()), effectiveInclusive, limit));
    }

    /**
     * The exclusive upper bound for a scan prefix, for tests to pass directly to {@link
     * SortedParquetStore#delimitedRollup} (which — unlike the pager — never derives it itself). A
     * non-empty prefix uses the real {@link ByteKeys#prefixUpper} carry; the empty (root) prefix has
     * no finite carry — the store now serves that open bound natively (see {@link
     * #delimitedRollupAnswersAnOpenUpperBoundAndDeclinesOnlyNonSlashDelimiters}), but tests that
     * want a BOUNDED root-level scan use a sentinel past every key this file's fixtures
     * ever write.
     */
    private static ByteKey upperOf(String prefix) {
        if (prefix.isEmpty()) {
            return ByteKey.copyOf(new byte[] {(byte) 0xFF});   // past every plain-ASCII test key
        }
        return switch (ByteKeys.prefixUpper(prefix.getBytes(StandardCharsets.UTF_8))) {
            case UpperBound.Bounded(ByteKey exclusiveUpper) -> exclusiveUpper;
            case UpperBound.Open ignored ->
                    throw new IllegalStateException("expected a finite upper bound for prefix " + prefix);
        };
    }

    /** Independent brute-force {@code delimiter=/} rollup over an in-memory sorted key list. */
    private static List<String> expectedRollup(List<String> keys, String prefix, String from,
                                                boolean fromInclusive, int limit) {
        List<String> out = new ArrayList<>();
        String openCommonPrefix = null;
        for (String k : keys) {
            if (!k.startsWith(prefix)) {
                continue;
            }
            if (from != null) {
                int cmp = k.compareTo(from);
                if (fromInclusive ? cmp < 0 : cmp <= 0) {
                    continue;
                }
            }
            String rest = k.substring(prefix.length());
            int slash = rest.indexOf('/');
            if (slash < 0) {
                out.add("OBJ:" + k);
                openCommonPrefix = null;
            } else {
                String cp = prefix + rest.substring(0, slash + 1);
                if (cp.equals(openCommonPrefix)) {
                    continue;   // already rolled up
                }
                openCommonPrefix = cp;
                if (from != null && cp.compareTo(from) <= 0) {
                    continue;   // boundary guard: a resume inside this directory does not re-emit it
                }
                out.add("CP:" + cp);
            }
            if (out.size() == limit + 1) {
                break;
            }
        }
        return out;
    }

    private static List<String> entryStrings(List<ListingStore.DelimitedEntry> rollup) {
        return rollup.stream()
                .map(e -> e.isCommonPrefix() ? "CP:" + ByteKeys.utf8(e.commonPrefix()) : "OBJ:" + ByteKeys.utf8(e.object().key()))
                .toList();
    }

    /** A moderately deep, wide hierarchy: several directories, nested subdirectories, and bare top-level objects. */
    private static List<String> hierarchy() {
        List<String> keys = new ArrayList<>();
        keys.add("a-before.txt");
        for (int d = 0; d < 6; d++) {
            for (int i = 0; i < 20; i++) {
                keys.add(String.format("a/dir%d/%03d.txt", d, i));
            }
            for (int i = 0; i < 5; i++) {
                keys.add(String.format("a/dir%d/deep/%03d.txt", d, i));
            }
        }
        for (int i = 0; i < 10; i++) {
            keys.add(String.format("a/bare-%03d.txt", i));
        }
        keys.add("z-after.txt");
        Collections.sort(keys);
        return keys;
    }

    private static byte[] slash() {
        return new byte[] {'/'};
    }

    @Test
    void projectsOwnerColumnsOnlyWhenAsked(@TempDir Path dir) throws IOException {
        Fixture fixture = writeSorted(dir, manySmallGroups(), keys(150));

        try (SortedParquetStore store = store(fixture)) {
            ListedObject withOwner = store.rows(null, true, null, 1, Projection.WITH_OWNER).getFirst();
            assertThat(withOwner.ownerId()).isEqualTo("owner-id");
            assertThat(withOwner.ownerDisplayName()).isEqualTo("owner-display");

            ListedObject keysOnly = store.rows(null, true, null, 1, Projection.KEYS_ONLY).getFirst();
            assertThat(keysOnly.ownerId()).isNull();
            assertThat(keysOnly.ownerDisplayName()).isNull();
            // Non-owner columns are still decoded regardless of the projection.
            assertThat(keysOnly.storageClass()).isEqualTo("STANDARD");
            assertThat(keysOnly.checksumAlgorithm()).isEqualTo("CRC32");
        }
    }

    @Test
    void recordsPageReadLatency(@TempDir Path dir) throws IOException {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ReplayMetrics metrics = new ReplayMetrics(registry);
        Fixture fixture = writeSorted(dir, manySmallGroups(), keys(120));

        try (SortedParquetStore store = new SortedParquetStore(fixture.files, fixture.index, metrics, 2)) {
            store.rows(null, true, null, 10, Projection.KEYS_ONLY);
        }
        assertThat(registry.find("swath.replay.page.read.latency").timer().count()).isEqualTo(1);
    }

    /**
     * <b>Red case.</b> Production code can no longer build an index like this one — {@code
     * SortedFixtures#loadIndex} now refuses any file with a row group that isn't provably pure {@code
     * row_type='OBJECT'} — but this test demonstrates directly, against {@link SortedParquetStore},
     * exactly the bug that eligibility gate exists to prevent: an index whose {@code rowCount}
     * (mis-)counts non-{@code OBJECT} rows toward the upper-bound invariant sets the bound too tight
     * and silently truncates a listing (fewer rows returned than actually exist past the bound, with
     * no truncation signal) — a legacy delimiter'd capture re-sorted by {@code sort-fixture} (which
     * stamps {@code mode=objects} unconditionally, never inspecting {@code row_type}) is exactly this
     * shape. The file here holds a0 (OBJECT), b0..b8 (nine COMMON_PREFIX rows), c0..c8 (nine OBJECT
     * rows) — 19 keys total, 10 of them OBJECT. A hand-built index reports {@code rowCount} the way
     * the buggy count would (every row, object or not) in three "row groups": {a0}=1, the b-run=9,
     * the c-run=9. Requesting {@code limit=9} (maxKeys=8) from the start: the invariant accumulates the
     * b-run's rowCount (9) and stops there, bounding the query at {@code c0} — but only ONE real
     * OBJECT row (a0) lies below that bound, so the DuckDB path returned just 1 row where a correct
     * (OBJECT-count-based) invariant would have returned 9 — silent truncation of the 9 real objects
     * (c0..c8) that exist just past the wrongly-tight bound.
     *
     * <p><b>The page-index range reader does not have this defect</b>, and the reason is structural
     * rather than lucky: the invariant bound exists to stop a SQL scan running to the end of the
     * file, so its correctness rests on the index's row counts being right. The reader has no such
     * bound — it reads forward from the file the index points at and stops when it holds {@code
     * limit} rows — so the index can miscount freely without truncating anything. This test now pins
     * that immunity. The truncation above remains real for {@code -Dswath.replay.sorted.range-reads=duckdb}.
     */
    @Test
    void redCase_mixedRowTypeIndexUndercountsRowsAndSilentlyTruncates(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("mixed.parquet");
        try (var writer = ParquetFixtures.open(file)) {
            writer.write(object("a0"));
            for (int i = 0; i < 9; i++) {
                writer.write(new CommonPrefixEntry(KeyBytes.ofUtf8(String.format("b%d", i))));
            }
            for (int i = 0; i < 9; i++) {
                writer.write(object(String.format("c%d", i)));
            }
        }
        // Fabricated to reproduce the counting bug directly: rowCount counts ALL rows in the
        // (logical) group, including the nine non-OBJECT rows — not a realistic physical row-group
        // layout (irrelevant here: read_parquet's WHERE/LIMIT operate on the file's logical rows
        // regardless of row-group boundaries; only the index drives bounds).
        List<IndexEntry> corruptedIndex = List.of(
                new IndexEntry(file, 0, key("a0"), 1),
                new IndexEntry(file, 1, key("b0"), 9),
                new IndexEntry(file, 2, key("c0"), 9));

        try (SortedParquetStore store = new SortedParquetStore(List.of(file), corruptedIndex, new ReplayMetrics(), 1)) {
            List<ListedObject> rows = store.rows(null, true, null, 9, Projection.KEYS_ONLY);   // maxKeys=8, limit=9
            // The page-index range reader is immune to the miscount, because it never asks the index
            // how many rows a bound must span -- it reads forward from the file the index points at
            // and stops once it holds `limit` OBJECT rows. The nine real objects come back, and the
            // nine COMMON_PREFIX rows are dropped on row_type rather than served as objects.
            assertThat(keyStrings(rows))
                    .containsExactly("a0", "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7");
        }
    }

    // --- helpers ---

    /**
     * A stamped fixture whose rows are not ascending <em>within</em> a row group must fail the
     * skip-scan that walks them, not answer hops from positions that mean nothing. Eligibility cannot
     * see this — it proves the ascent of row-group FIRST keys only, and a single-group file passes
     * that vacuously — so the check lives where the rows are actually stepped over
     * ({@code SortedRowGroupReader.KeyCursor}), and this is the caller's path arriving at it.
     *
     * <p>The fixture is written straight through {@link SortedParquetWriter},
     * bypassing {@link CaptureSorter}, because the sorter cannot produce the shape being guarded
     * against: what it stands in for is a listing published by some other producer and stamped
     * sorted while not being so.
     *
     * <p>The refusal is <b>counted before it is thrown</b>, and the reason is a typed constant, not a
     * phrase in the message: a corpus sweep excluding this capture has to be able to say <em>why</em>
     * from the metrics of a run that ended in an exception, which is the same discipline
     * {@code io.varve.swath.sort.PageRunSegmentIo}'s own pre-throw count keeps.
     */
    @Test
    void delimitedRollupRefusesARowGroupWhoseRowsAreNotAscending(@TempDir Path dir) throws IOException {
        Path out = Files.createDirectories(dir.resolve("unsorted"));
        Path file = out.resolve("part-00001.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(file, SortConfigs.base(), SortMode.OBJECTS, 1)) {
            writer.markFinal();
            for (String k : List.of("a/1", "c/1", "b/1", "d/1")) {
                writer.write(object(k));
            }
        }
        List<Path> files = SortedFixtures.resolveFiles(out);
        IndexLoadResult loaded = SortedFixtures.loadIndex(files, new FixtureMetrics());
        Fixture fixture = new Fixture(files, ((IndexLoadResult.Loaded) loaded).entries());
        ReplayMetrics metrics = new ReplayMetrics(new SimpleMeterRegistry(), ReplayMetrics.SERVING_MODE_SORTED);

        try (SortedParquetStore store = new SortedParquetStore(fixture.files, fixture.index, metrics, 2)) {
            assertThatThrownBy(() -> store.delimitedRollup(null, true, upperOf(""), new byte[0], slash(),
                    1000, Projection.KEYS_ONLY))
                    .isInstanceOfSatisfying(RowGroupOrderException.class, e -> {
                        assertThat(e.reason()).isEqualTo(RowGroupOrderException.ROW_GROUP_DISORDER);
                        assertThat(e.file()).isEqualTo(file);
                        assertThat(e.rowGroup()).isZero();
                    })
                    .hasMessageContaining("row group 0 of " + file)
                    .hasMessageContaining("strictly ascending");
        }
        assertThat(metrics.registry().find("swath.replay.serving.refused")
                .tag("reason", RowGroupOrderException.ROW_GROUP_DISORDER).counter().count()).isEqualTo(1);
    }

    private record Fixture(List<Path> files, List<IndexEntry> index) {
    }

    private static SortedParquetStore store(Fixture fixture) {
        return new SortedParquetStore(fixture.files, fixture.index, new ReplayMetrics(), 2);
    }

    private static Fixture writeSorted(Path dir, SortConfig config, List<String> keys) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("cap-" + Math.abs(keys.hashCode())));
        List<String> shuffled = new ArrayList<>(keys);
        Collections.shuffle(shuffled, new Random(42));
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            for (String k : shuffled) {
                writer.write(object(k));
            }
        }
        Path out = Files.createDirectories(dir.resolve("out-" + Math.abs(keys.hashCode())));
        new CaptureSorter(config).sort(capture, out);
        List<Path> files = SortedFixtures.resolveFiles(out);
        IndexLoadResult result = SortedFixtures.loadIndex(files, new FixtureMetrics());
        List<IndexEntry> index = ((IndexLoadResult.Loaded) result).entries();
        return new Fixture(files, index);
    }

    private static Fixture writeSorted(Path dir, SortConfig config, String... keys) throws IOException {
        return writeSorted(dir, config, List.of(keys));
    }

    private static List<String> keys(int count) {
        List<String> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(String.format("key-%05d", i));
        }
        return keys;
    }

    private static ObjectEntry object(String key) {
        return ObjectEntries.key(key).etag("etag").storageClass("STANDARD").isLatest(true)
                .owner("owner-id", "owner-display").checksum("CRC32", "FULL_OBJECT").build();
    }

    private static List<String> expected(List<String> sortedKeys, String from, boolean inclusive,
                                         String toExclusive, int limit) {
        List<String> out = new ArrayList<>();
        for (String k : sortedKeys) {
            int cmp = k.compareTo(from);
            if (inclusive ? cmp < 0 : cmp <= 0) {
                continue;
            }
            if (toExclusive != null && k.compareTo(toExclusive) >= 0) {
                break;
            }
            out.add(k);
            if (out.size() == limit) {
                break;
            }
        }
        return out;
    }

    private static ByteKey key(String value) {
        return ByteKey.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }

    private static List<String> keyStrings(List<ListedObject> rows) {
        return rows.stream().map(r -> ByteKeys.utf8(r.key())).toList();
    }
}
