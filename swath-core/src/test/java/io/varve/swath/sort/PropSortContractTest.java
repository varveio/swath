/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.ListEntryWriteSupport;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.sort.SortTestSupport.InMemorySegments;
import io.varve.swath.sort.SortedFileIndex.RowGroupKey;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

/**
 * PROP-SORT-1/2/3 — the adversarial property suite for {@code io.varve.swath.sort}, guarding the
 * contract, not the implementation:
 *
 * <ul>
 *   <li><b>PROP-SORT-1</b> — (i) {@link KWayMerge} over an arbitrary partition of an arbitrary
 *       sorted list reproduces the sorted whole (completeness + order); (ii) {@link
 *       ListEntryComparator} obeys the §0.3 total-preorder laws (reflexivity, sign-antisymmetry,
 *       transitivity, equivalence-transitivity, totality) across mixed subtypes incl. null
 *       {@code version_id} and equal-comparing cross-type entries; (iii) equal-comparing entries all
 *       survive the merge — none dropped — and keep stable input order within one segment.</li>
 *   <li><b>PROP-SORT-2</b> — canonical Parquet → {@link SegmentReader} round-trips every {@link
 *       ListEntry} subtype for arbitrary generated entries incl. NUL/U+10FFFF in keys,
 *       1&nbsp;KB keys, unicode, null optional fields, the empty segment, and a single
 *       record.</li>
 *   <li><b>PROP-SORT-3</b> — {@link SortedFileIndex#firstKeysPerRowGroup} equals each row group's
 *       true first row for adversarial key shapes, and provably NOT the (truncated) footer stats:
 *       at least one generated 1&nbsp;KB-key case where the truncated column min differs from the
 *       true first row, which derive must still return correctly.</li>
 * </ul>
 *
 * <p>In-package so the package-private merge/segment seams ({@link KWayMerge},
 * {@link SegmentReader}, {@link InMemorySegments}) are reachable. Try budgets are kept modest on the
 * Parquet-writing properties so the default suite stays fast.
 */
class PropSortContractTest {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    // ---------------------------------------------------------------------
    // PROP-SORT-1 (i): merge of an arbitrary partition == the sorted whole.
    // ---------------------------------------------------------------------

    @Property(tries = 150)
    void mergeOfArbitraryPartitionReproducesTheSortedWhole(
            @ForAll("collidingEntries") List<ListEntry> entries,
            @ForAll @IntRange(min = 1, max = 7) int segments,
            @ForAll @IntRange(min = 2, max = 6) int fanIn,
            @ForAll long partitionSeed) throws IOException {

        // The sorted whole under the §0.3 comparator (stable TimSort).
        List<ListEntry> whole = new ArrayList<>(entries);
        whole.sort(CMP);

        // Partition it into `segments` sub-lists, iterating in sorted order so every part is itself
        // sorted (a valid KWayMerge input) — an arbitrary assignment of each element to a segment.
        Random r = new Random(partitionSeed);
        List<List<ListEntry>> parts = new ArrayList<>();
        for (int i = 0; i < segments; i++) {
            parts.add(new ArrayList<>());
        }
        for (ListEntry e : whole) {
            parts.get(r.nextInt(segments)).add(e);
        }

        InMemorySegments io = new InMemorySegments();
        List<Integer> handles = new ArrayList<>();
        for (List<ListEntry> part : parts) {
            handles.add(io.add(part));
        }

        KWayMerge<Integer> merge = new KWayMerge<>(CMP, fanIn, io, DuplicateHook.NO_OP, SortMetrics.NO_OP);
        List<ListEntry> merged = drain(merge.merge(handles));

        assertThat(merged).as("completeness: every entry survives the merge").hasSameSizeAs(whole);
        assertThat(merged).as("global order under the §0.3 comparator").isSortedAccordingTo(CMP);
        assertThat(merged).as("no gap / no overlap — same multiset as the sorted whole")
                .containsExactlyInAnyOrderElementsOf(whole);
        // Cascade actually engaged when there were more segments than the fan-in.
        if (segments > fanIn) {
            assertThat(merge.cascadedPasses()).as("segments > fan-in must cascade").isGreaterThan(0);
        }
    }

    // ---------------------------------------------------------------------
    // PROP-SORT-1 (ii): the comparator is a deterministic total preorder.
    // ---------------------------------------------------------------------

    @Property(tries = 300)
    void comparatorObeysTotalPreorderLaws(@ForAll("entryList") List<ListEntry> es) {
        int n = es.size();
        for (int i = 0; i < n; i++) {
            ListEntry a = es.get(i);
            assertThat(CMP.compare(a, a)).as("reflexive: compare(a,a)==0").isZero();
            for (int j = 0; j < n; j++) {
                ListEntry b = es.get(j);
                int ab = CMP.compare(a, b);
                int ba = CMP.compare(b, a);
                assertThat(Integer.signum(ab))
                        .as("sign-antisymmetry / totality: sign(compare(a,b)) == -sign(compare(b,a))")
                        .isEqualTo(-Integer.signum(ba));
                for (int k = 0; k < n; k++) {
                    ListEntry c = es.get(k);
                    if (ab <= 0 && CMP.compare(b, c) <= 0) {
                        assertThat(CMP.compare(a, c) <= 0)
                                .as("transitivity: a<=b<=c ⇒ a<=c").isTrue();
                    }
                    if (ab == 0 && CMP.compare(b, c) == 0) {
                        assertThat(CMP.compare(a, c))
                                .as("equivalence transitive (antisymmetry up to equivalence): a==b==c ⇒ a==c")
                                .isZero();
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------------
    // PROP-SORT-1 (iii): equal-comparing entries all survive; stable within a segment.
    // ---------------------------------------------------------------------

    @Property(tries = 100)
    void equalComparingEntriesAllSurviveAndAreStableWithinOneSegment(
            @ForAll("collidingEntries") List<ListEntry> entries) throws IOException {
        // A single sorted segment: within one stream the merge is order-preserving, so equal-comparing
        // entries (same key+version+row_type, differing payload) must emerge in input order and none
        // may be dropped by the adjacent-duplicate hook (swath is a sorter, never a deduper — §0.5).
        List<ListEntry> sorted = new ArrayList<>(entries);
        sorted.sort(CMP);   // stable — the reference stable order for a single segment

        InMemorySegments io = new InMemorySegments();
        int handle = io.add(sorted);
        KWayMerge<Integer> merge = new KWayMerge<>(CMP, 4, io, DuplicateHook.NO_OP, SortMetrics.NO_OP);
        List<ListEntry> out = drain(merge.merge(List.of(handle)));

        assertThat(out).as("no user entry dropped, stable input order preserved within a segment")
                .containsExactlyElementsOf(sorted);
    }

    // ---------------------------------------------------------------------
    // PROP-SORT-2: segment write → read round-trips every subtype.
    // ---------------------------------------------------------------------

    @Property(tries = 40)
    void segmentRoundTripsEveryGeneratedSubtype(@ForAll("richEntryList") List<ListEntry> entries)
            throws IOException {
        assertRoundTrips(entries);
    }

    @Example
    void emptySegmentRoundTripsToEmpty() throws IOException {
        assertRoundTrips(List.of());
    }

    @Example
    void singleRecordRoundTrips() throws IOException {
        assertRoundTrips(List.of(new ObjectEntry(KeyBytes.of(kiloKey(1)), 42L, 1234L,
                "etag", "STANDARD", "v1", true, "owner", "Owner Name", "SHA256", "COMPOSITE")));
    }

    @Example
    void nulAndHighScalarAndKiloKeysRoundTrip() throws IOException {
        List<ListEntry> es = new ArrayList<>();
        es.add(new ObjectEntry(KeyBytes.of(new byte[]{0, 0, 0}), 1L, 0L, null, null, null, false,
                null, null, null, null));
        es.add(new ObjectEntry(KeyBytes.of("\uDBFF\uDFFF".getBytes(StandardCharsets.UTF_8)), 2L, 0L, null, null,
                null, false, null, null, null, null));
        es.add(new ObjectEntry(KeyBytes.of(kiloKey(7)), 3L, 0L, "日本語etag", "STANDARD", null, false,
                null, null, null, null));
        es.add(new CommonPrefixEntry(KeyBytes.of("café/😀/".getBytes(StandardCharsets.UTF_8))));
        es.add(new DeleteMarkerEntry(KeyBytes.of(new byte[]{0, (byte) 0xF4, (byte) 0x8F,
                (byte) 0xBF, (byte) 0xBF}),
                "vDel", true, 99L, "o"));
        assertRoundTrips(es);
    }

    // ---------------------------------------------------------------------
    // PROP-SORT-3: derive == true row-group first rows (never footer stats).
    // ---------------------------------------------------------------------

    @Property(tries = 25)
    void deriveReturnsTrueFirstRowsForAdversarialShapes(
            @ForAll KeyShape shape,
            @ForAll @IntRange(min = 1, max = 400) int n,
            @ForAll @LongRange(min = 2048, max = 65536) long rowGroupBytes) throws IOException {
        List<byte[]> keys = ascendingKeys(shape, n);
        Path file = Files.createTempFile("propsort3-", ".parquet");
        try {
            SortConfig cfg = configWithRowGroupBytes(rowGroupBytes);
            try (SortedFileWriter w = new SortedParquetWriter(file, cfg, SortMode.OBJECTS, 1)) {
                for (byte[] k : keys) {
                    w.write(object(k));
                }
            }

            List<RowGroupKey> derived = SortedFileIndex.firstKeysPerRowGroup(file);
            // Walk the row groups by their own reported row counts — locates each group's expected
            // true first key in the known ascending write order without predicting parquet-mr's flush
            // boundaries.
            long offset = 0;
            for (RowGroupKey rg : derived) {
                assertThat(rg.firstKey())
                        .as("row group first key is the TRUE first row, never (truncated) footer stats")
                        .isEqualTo(keys.get((int) offset));
                offset += rg.rowCount();
            }
            assertThat(offset).as("row-group row counts cover every written row exactly").isEqualTo(n);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Property(tries = 10)
    void derivedFirstKeyDivergesFromTruncatedFooterStats(@ForAll @IntRange(min = 2, max = 40) int n)
            throws IOException {
        // 1 KB keys + a forced 16-byte statistics-truncate length: the footer column min a naive
        // router might consult is a truncated prefix, provably NOT the true 1024-byte first row.
        List<byte[]> keys = ascendingKeys(KeyShape.KILO_ADVERSARIAL, n);
        Path file = Files.createTempFile("propsort3trunc-", ".parquet");
        try {
            writeWithTruncatedStatistics(file, keys, 4096, 16);

            List<RowGroupKey> derived = SortedFileIndex.firstKeysPerRowGroup(file);
            assertThat(derived).isNotEmpty();
            assertThat(derived.get(0).firstKey())
                    .as("derive returns the true full-length first row").isEqualTo(keys.get(0));

            byte[] truncatedMin = footerKeyColumnMinBytes(file);
            assertThat(truncatedMin).as("footer stats are truncated to <= 16 B").hasSizeLessThanOrEqualTo(16);
            assertThat(truncatedMin)
                    .as("the truncated stat is provably NOT the true first row — derive must not use it")
                    .isNotEqualTo(derived.get(0).firstKey());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    // ---------------------------------------------------------------------
    // Generators
    // ---------------------------------------------------------------------

    /** Small key/version pools so equal-comparing (colliding) entries actually occur — feeds the laws. */
    @Provide
    Arbitrary<List<ListEntry>> collidingEntries() {
        return collidingEntry().list().ofMinSize(1).ofMaxSize(40);
    }

    @Provide
    Arbitrary<List<ListEntry>> entryList() {
        return collidingEntry().list().ofMinSize(1).ofMaxSize(6);
    }

    private static final List<byte[]> KEY_POOL = List.of(
            "".getBytes(StandardCharsets.UTF_8),
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "m".getBytes(StandardCharsets.UTF_8),
            new byte[]{0},
            "\uDBFF\uDFFF".getBytes(StandardCharsets.UTF_8),
            "😀".getBytes(StandardCharsets.UTF_8));

    private Arbitrary<ListEntry> collidingEntry() {
        Arbitrary<Integer> kind = Arbitraries.integers().between(0, 2);
        Arbitrary<byte[]> key = Arbitraries.of(KEY_POOL);
        Arbitrary<String> version = Arbitraries.of("v1", "v2").injectNull(0.4);
        Arbitrary<Integer> payload = Arbitraries.integers().between(0, 4);   // payload variance ⇒ ties
        return Combinators.combine(kind, key, version, payload).as((k, ky, ver, pay) -> switch (k) {
            case 0 -> new ObjectEntry(KeyBytes.of(ky), pay, 0L, null, null, ver, ver != null && pay % 2 == 0,
                    null, null, null, null);
            case 1 -> new CommonPrefixEntry(KeyBytes.of(ky));
            default -> new DeleteMarkerEntry(KeyBytes.of(ky), ver, pay % 2 == 0, 0L, null);
        });
    }

    @Provide
    Arbitrary<List<ListEntry>> richEntryList() {
        return richEntry().list().ofMinSize(1).ofMaxSize(24);
    }

    private Arbitrary<ListEntry> richEntry() {
        Arbitrary<Integer> kind = Arbitraries.integers().between(0, 2);
        Arbitrary<byte[]> key = richKey();
        Arbitrary<String> optStr = Arbitraries.of("etag123", "W/\"weak\"", "日本語", "").injectNull(0.4);
        Arbitrary<String> version = Arbitraries.of("v-Ab1", "😀ver", "").injectNull(0.4);
        Arbitrary<Long> size = Arbitraries.longs().between(0, Long.MAX_VALUE);
        Arbitrary<Long> mtime = Arbitraries.longs().between(0, 1_900_000_000_000_000L);
        Arbitrary<Boolean> latest = Arbitraries.of(true, false);
        return Combinators.combine(kind, key, optStr, version, size, mtime, latest)
                .as((k, ky, s, ver, sz, mt, lat) -> switch (k) {
                    case 0 -> new ObjectEntry(KeyBytes.of(ky), sz, mt, s, s, ver, lat, s, s, s, s);
                    case 1 -> new CommonPrefixEntry(KeyBytes.of(ky));
                    default -> new DeleteMarkerEntry(KeyBytes.of(ky), ver, lat, mt, s);
                });
    }

    private Arbitrary<byte[]> richKey() {
        Arbitrary<byte[]> pool = Arbitraries.of(KEY_POOL);
        Arbitrary<byte[]> nulAndHighScalar = Arbitraries.of(
                new byte[]{0, 0, 0}, "\uDBFF\uDFFF".getBytes(StandardCharsets.UTF_8),
                new byte[]{0, (byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF,
                        0, (byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF});
        Arbitrary<byte[]> kilo = Arbitraries.integers().between(0, 1 << 20).map(PropSortContractTest::kiloKey);
        Arbitrary<byte[]> unicode = Arbitraries.of("café/1", "мир/2", "emoji/😀/x")
                .map(s -> s.getBytes(StandardCharsets.UTF_8));
        return Arbitraries.oneOf(pool, nulAndHighScalar, kilo, unicode);
    }

    enum KeyShape {
        /** 1 KB valid UTF-8 keys embedding NUL and U+10FFFF, ascending in a fixed tail. */
        KILO_ADVERSARIAL,
        /** Short ascending ASCII keys (small files → often a single row group). */
        ASCII_SHORT,
        /** ~200 B ascending keys (crosses tiny row-group thresholds → many groups). */
        ASCII_WIDE
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static void assertRoundTrips(List<ListEntry> entries) throws IOException {
        Path seg = Files.createTempFile("propsort2-", ".parquet");
        try {
            SortTestSupport.writeCanonicalParquet(seg, entries);
            List<ListEntry> back = new ArrayList<>();
            try (SegmentReader reader = new SegmentReader(seg)) {
                while (reader.hasNext()) {
                    back.add(reader.next());
                }
            }
            assertThat(back).hasSameSizeAs(entries);
            for (int i = 0; i < entries.size(); i++) {
                assertThat(back.get(i))
                        .as("row %d round-trips under the canonical codec", i)
                        .isEqualTo(canonicalize(entries.get(i)));
            }
        } finally {
            Files.deleteIfExists(seg);
        }
    }

    /**
     * The exact write→read contract of {@code ListEntryWriteSupport}/{@link SegmentReader}: an
     * {@code ObjectEntry}'s {@code is_latest} is only persisted when it carries a {@code version_id}
     * (the writer omits it otherwise), so a versionless object reads back {@code is_latest=false}.
     * Every other field round-trips verbatim ({@code last_modified==0} is the "absent" sentinel that
     * decodes to {@code 0}). Applied to the expected side so the property compares against the true
     * codec contract, not an over-strong identity.
     */
    private static ListEntry canonicalize(ListEntry e) {
        if (e instanceof ObjectEntry o && o.versionId() == null && o.isLatest()) {
            return new ObjectEntry(o.key(), o.size(), o.lastModifiedEpochMicros(), o.etag(),
                    o.storageClass(), o.versionId(), false, o.ownerId(), o.ownerDisplayName(),
                    o.checksumAlgorithm(), o.checksumType());
        }
        return e;
    }

    private static List<ListEntry> drain(SortedCursor cursor) {
        List<ListEntry> out = new ArrayList<>();
        try (cursor) {
            while (cursor.hasNext()) {
                out.add(cursor.next());
            }
        }
        return out;
    }

    private static List<byte[]> ascendingKeys(KeyShape shape, int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(switch (shape) {
                case KILO_ADVERSARIAL -> kiloKey(i);
                case ASCII_SHORT -> String.format("k%06d", i).getBytes(StandardCharsets.UTF_8);
                case ASCII_WIDE -> (String.format("%08d", i) + "x".repeat(190))
                        .getBytes(StandardCharsets.UTF_8);
            });
        }
        return keys;
    }

    /** {@code i}-th 1024-byte key: embeds NUL/U+10FFFF at the head, ascending in the tail. */
    private static byte[] kiloKey(int i) {
        byte[] key = new byte[1024];
        key[0] = 0x00;
        key[1] = (byte) 0xF4;
        key[2] = (byte) 0x8F;
        key[3] = (byte) 0xBF;
        key[4] = (byte) 0xBF;
        java.util.Arrays.fill(key, 5, key.length - 8, (byte) 'x');
        byte[] suffix = String.format("%08x", i).getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(suffix, 0, key, key.length - suffix.length, suffix.length);
        return key;
    }

    private static SortConfig configWithRowGroupBytes(long rowGroupBytes) {
        Map<String, String> overrides = Map.of("final-row-group-bytes", Long.toString(rowGroupBytes));
        return SortConfig.fromProperties(k -> overrides.get(k.substring("swath.sort.".length())));
    }

    private static ObjectEntry object(byte[] key) {
        return new ObjectEntry(KeyBytes.of(key), 1L, 0L, null, null, null, false, null, null, null, null);
    }

    private static byte[] footerKeyColumnMinBytes(Path path) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path))) {
            var columns = reader.getRowGroups().get(0).getColumns();
            for (var col : columns) {
                if (col.getPath().toDotString().equals("key")) {
                    return col.getStatistics().getMinBytes();
                }
            }
            throw new IllegalStateException("no key column in row group 0");
        }
    }

    /** A raw high-level writer with a forced small {@code statisticsTruncateLength} (test-only). */
    private static void writeWithTruncatedStatistics(Path path, List<byte[]> keys, long rowGroupBytes,
                                                     int truncateLength) throws IOException {
        Configuration conf = new Configuration(false);
        MessageType schema = ParquetSchema.canonical();
        try (ParquetWriter<ListEntry> writer = new TruncatingBuilder(new LocalOutputFile(path), schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withRowGroupSize(rowGroupBytes)
                .withPageSize(1024 * 1024)
                .withStatisticsTruncateLength(truncateLength)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            for (byte[] k : keys) {
                writer.write(object(k));
            }
        }
    }

    private static final class TruncatingBuilder extends ParquetWriter.Builder<ListEntry, TruncatingBuilder> {
        private final MessageType schema;

        TruncatingBuilder(OutputFile file, MessageType schema) {
            super(file);
            this.schema = schema;
        }

        @Override
        protected TruncatingBuilder self() {
            return this;
        }

        @Override
        @SuppressWarnings("deprecation")
        protected WriteSupport<ListEntry> getWriteSupport(Configuration conf) {
            return new ListEntryWriteSupport(schema);
        }
    }
}
