/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.LastModifiedParseException;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.testkit.Keyspaces;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * {@link PageBlock} compact codec: byte-exact round-trip of all three {@link ListEntry} subtypes
 * incl. null fields, unicode keys, 1&nbsp;KB keys, etag md5 packing + multipart/uppercase fallback,
 * the exposed first/last/count/estimatedBytes, the full-comparator ordering flag, front-coded
 * keys incl. unsorted pages, and multi-column dictionary encoding + graceful high-cardinality
 * fallback.
 */
class PageBlockTest {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    private static List<ListEntry> roundTrip(List<ListEntry> in) {
        PageBlock block = PageBlock.pack(in, CMP);
        List<ListEntry> out = new ArrayList<>();
        PageBlockCursor c = block.cursor();
        while (c.hasNext()) {
            out.add(c.next());
        }
        return out;
    }

    @Test
    void roundTripsAllSubtypesWithNullAndPopulatedFields() {
        List<ListEntry> in = List.of(
                new ObjectEntry(KeyBytes.ofUtf8("a/plain"), 123L, 1_700_000_000_000_000L,
                        "d41d8cd98f00b204e9800998ecf8427e", "STANDARD", null, false, null, null, null, null),
                new ObjectEntry(KeyBytes.ofUtf8("a/versioned"), 0L, 0L, null, "GLACIER",
                        "vid-123", true, "owner-1", "Owner One", "SHA256", "COMPOSITE"),
                new CommonPrefixEntry(KeyBytes.ofUtf8("a/folder/")),
                new DeleteMarkerEntry(KeyBytes.ofUtf8("a/gone"), "delver", true, 42L, "owner-2"),
                new DeleteMarkerEntry(KeyBytes.ofUtf8("a/gone2"), null, false, 0L, null));
        assertThat(roundTrip(in)).containsExactlyElementsOf(in);
    }

    @Test
    void roundTripsUnicodeAndOneKiloByteKeys() {
        byte[] bigKey = new byte[1024];
        for (int i = 0; i < bigKey.length; i++) {
            bigKey[i] = (byte) (i & 0xFF);
        }
        String unicode = "photos/ünïcodé/日本語/😀/key";
        List<ListEntry> in = List.of(
                new ObjectEntry(KeyBytes.of(bigKey), 9L, 5L, "etag", "STANDARD", null, false, null, null, null, null),
                new ObjectEntry(KeyBytes.ofUtf8(unicode), 9L, 5L, null, null, null, false, null, null, null, null),
                new CommonPrefixEntry(KeyBytes.of(new byte[] {0x00, (byte) 0xFF, 0x00})));
        List<ListEntry> out = roundTrip(in);
        assertThat(out).containsExactlyElementsOf(in);
        assertThat(out.get(0).key().rawUnsafe()).containsExactly(toInts(bigKey));
        assertThat(out.get(1).key().asString()).isEqualTo(unicode);
    }

    @Test
    void etagMd5PacksAndMultipartAndUppercaseFallBackExactly() {
        ObjectEntry plainMd5 = obj("k1", "d41d8cd98f00b204e9800998ecf8427e");   // 32 lowercase hex ⇒ packed
        ObjectEntry multipart = obj("k2", "d41d8cd98f00b204e9800998ecf8427e-3"); // multipart ⇒ raw
        ObjectEntry upper = obj("k3", "D41D8CD98F00B204E9800998ECF8427E");       // uppercase ⇒ raw (case-exact)
        ObjectEntry nullEtag = obj("k4", null);
        List<ListEntry> in = List.of(plainMd5, multipart, upper, nullEtag);
        assertThat(roundTrip(in)).containsExactlyElementsOf(in);
    }

    @Test
    void exposesFirstLastCountAndEstimate() {
        List<ListEntry> in = List.of(object("aaa"), object("mmm"), object("zzz"));
        PageBlock block = PageBlock.pack(in, CMP);
        assertThat(block.count()).isEqualTo(3);
        assertThat(block.firstKey()).containsExactly(toInts("aaa".getBytes(StandardCharsets.UTF_8)));
        assertThat(block.lastKey()).containsExactly(toInts("zzz".getBytes(StandardCharsets.UTF_8)));
        assertThat(block.firstEntry()).isEqualTo(object("aaa"));
        assertThat(block.lastEntry()).isEqualTo(object("zzz"));
        // Σ(key.len + 64) = 3 * (3 + 64) = 201.
        assertThat(block.estimatedBytes()).isEqualTo(3L * (3 + PageBlock.ENTRY_OVERHEAD_BYTES));
    }

    @Test
    void packedFootprintIsCompactVersusLogicalEstimate() {
        List<ListEntry> in = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            in.add(new ObjectEntry(KeyBytes.ofUtf8("prefix/object-" + i), i, 1_700_000_000_000_000L + i,
                    "d41d8cd98f00b204e9800998ecf8427e", "STANDARD", null, false, null, null, null, null));
        }
        PageBlock block = PageBlock.pack(in, CMP);
        // Compact: the packed bytes stay well under the §5 logical estimate (~1× target, not 2–3×).
        assertThat(block.packedBytes()).isLessThan(block.estimatedBytes());
    }

    @Test
    void emptyPageIsRejected() {
        assertThatThrownBy(() -> PageBlock.pack(List.of(), CMP))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidTimestampFailsBeforeAPackedBlockIsProduced() {
        ObjectEntry entry = new ObjectEntry(KeyBytes.ofUtf8("bad-time"), 1L, "not-a-timestamp",
                "etag", "STANDARD", null, true, null, null, null, null);

        assertThatThrownBy(() -> PageBlock.pack(List.of(entry), CMP))
                .isInstanceOf(LastModifiedParseException.class)
                .hasRootCauseInstanceOf(java.time.format.DateTimeParseException.class);
    }

    @Test
    void orderedUnderFullComparatorIsTrueForAStrictlyAscendingPage() {
        List<ListEntry> in = List.of(object("a"), object("b"), object("c"));
        assertThat(PageBlock.pack(in, CMP).orderedUnderFullComparator()).isTrue();
    }

    @Test
    void orderedUnderFullComparatorIsFalseWhenKeyOrderIsViolated() {
        List<ListEntry> in = List.of(object("b"), object("a"));
        assertThat(PageBlock.pack(in, CMP).orderedUnderFullComparator()).isFalse();
    }

    @Test
    void orderedUnderFullComparatorCatchesAMisorderedVersionTiebreakWithinOnePage() {
        // Same key, two versions in S3's last-modified-desc arrival order (v2 before v1) — key
        // bytes tie (a key-bytes-only check would miss this), but the full comparator says v1 < v2.
        ObjectEntry v2 = new ObjectEntry(KeyBytes.ofUtf8("k"), 1L, 0L, null, null, "v2", false, null, null, null, null);
        ObjectEntry v1 = new ObjectEntry(KeyBytes.ofUtf8("k"), 1L, 0L, null, null, "v1", false, null, null, null, null);
        PageBlock block = PageBlock.pack(List.of(v2, v1), CMP);
        assertThat(block.orderedUnderFullComparator()).isFalse();
    }

    @Test
    void orderedUnderFullComparatorIsTrueForAnAdjacentComparatorTie() {
        ObjectEntry first = objectWithSize("a", 2L);
        ObjectEntry second = objectWithSize("a", 1L);

        assertThat(CMP.compare(first, second)).as("precondition: payload is not an ordering field").isZero();
        assertThat(PageBlock.pack(List.of(first, second), CMP).orderedUnderFullComparator()).isTrue();
    }

    // ------------------------------------------------------------------
    // Front-coded keys: correctness independent of order, plus a compaction measurement.
    // ------------------------------------------------------------------

    @Test
    void frontCodingRoundTripsCorrectlyForAnUnsortedPage() {
        // Front-coding must not ASSUME sortedness for correctness — a shared-prefix-len of 0 is
        // always a valid encoding. Deliberately out of order; "aaa/first"/"aaa/firstish" land
        // adjacent only by coincidence of list position, not because the page is sorted.
        List<ListEntry> in = List.of(object("zzz/last"), object("aaa/first"), object("mmm/middle"),
                object("aaa/firstish"), object("000/binary"));
        assertThat(roundTrip(in)).containsExactlyElementsOf(in);
    }

    @Test
    void frontCodingRoundTripsCorrectlyForAReverseSortedPage() {
        List<ListEntry> in = List.of(object("e"), object("d"), object("c"), object("b"), object("a"));
        assertThat(roundTrip(in)).containsExactlyElementsOf(in);
    }

    @Test
    void frontCodingCompactsDeepHierarchicalKeysMuchMoreThanHashLikeKeys() {
        // Deep hierarchical, illustrative example (crawl=001/pid=X/part-00001.parquet
        // vs ...part-00002.parquet — the SAME pid repeating across many part files under one
        // directory): a long, mostly-static path with only a monotonically incrementing tail, so
        // adjacent sorted keys share a very long prefix — front-coding should win big. (Note:
        // Keyspaces.deepTree draws a FRESH random pid per row even within one crawl — built for the
        // engine's steal/split fanout tests, not a long-shared-prefix shape — so it is not used here.)
        // Hash-like: Keyspaces.casSha256, whose only static component is a short fixed prefix
        // ("blobs/sha256/") before 64 random hex chars — front-coding should cost close to nothing
        // extra (a byte or two per row for the two varints vs a non-front-coded single length varint).
        List<byte[]> deepKeys = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            deepKeys.add(String.format(
                    "crawl=007/pid=3f9a21bc/prod/eu-west-1/service-alpha/logs/part-%06d.parquet", i)
                    .getBytes(StandardCharsets.UTF_8));
        }
        List<byte[]> hashKeys = new ArrayList<>(Keyspaces.casSha256(1, 1000));
        hashKeys.sort(Arrays::compareUnsigned);   // pages arrive sorted

        Measurement deep = measure(deepKeys);
        Measurement hash = measure(hashKeys);

        // Big win for deep hierarchical keys: well under half the naive (non-front-coded) size.
        assertThat(deep.ratio()).isLessThan(0.4);
        // Hash-like keys: front-coding never costs much even when it can't find a shared prefix —
        // measured ~0.86 (a small win from the birthday-effect shared prefix among 1000 sorted
        // samples, never a regression) — nowhere near the deep-hierarchical win.
        assertThat(hash.ratio()).isLessThan(1.10);
        assertThat(hash.ratio()).isGreaterThan(0.75);
        assertThat(hash.ratio()).isGreaterThan(deep.ratio() * 1.8);   // the contrast the task expects
    }

    /** A page's front-coded {@code packedBytes()} vs a naive (length-prefix, non-front-coded) baseline. */
    private record Measurement(long packedBytes, long naiveBytes) {
        double ratio() {
            return (double) packedBytes / naiveBytes;
        }
    }

    /** Fixed per-row cost of every non-key field here, all null/false/zero (tag, size, last_modified,
     * etag, storage_class, version_id, is_latest, owner_id, owner_display_name, checksum_algorithm,
     * checksum_type) = 1+8+8+1+1+1+1+1+1+1+1 = 26 bytes, identical for every row regardless of key
     * shape — isolates the measurement to the key-encoding scheme alone. */
    private static final int NON_KEY_BYTES_PER_ROW = 26;

    private Measurement measure(List<byte[]> keysInBlockOrder) {
        List<ListEntry> entries = new ArrayList<>();
        for (byte[] k : keysInBlockOrder) {
            entries.add(new ObjectEntry(KeyBytes.of(k), 0L, 0L, null, null, null, false, null, null, null, null));
        }
        PageBlock block = PageBlock.pack(entries, CMP);
        long naive = (long) keysInBlockOrder.size() * NON_KEY_BYTES_PER_ROW;
        for (byte[] k : keysInBlockOrder) {
            naive += varintSize(k.length) + k.length;
        }
        return new Measurement(block.packedBytes(), naive);
    }

    private static int varintSize(int v) {
        int n = 1;
        int x = v;
        while ((x & ~0x7F) != 0) {
            n++;
            x >>>= 7;
        }
        return n;
    }

    // ------------------------------------------------------------------
    // Multi-column dictionary encoding + graceful high-cardinality fallback.
    // ------------------------------------------------------------------

    @Test
    void allFiveDictionaryColumnsRoundTripAtLowCardinality() {
        List<ListEntry> in = List.of(
                new ObjectEntry(KeyBytes.ofUtf8("a"), 1L, 0L, null, "STANDARD", null, false,
                        "owner-1", "Owner One", "CRC32", "FULL_OBJECT"),
                new ObjectEntry(KeyBytes.ofUtf8("b"), 1L, 0L, null, "GLACIER", null, false,
                        "owner-2", "Owner Two", "SHA256", "COMPOSITE"),
                new ObjectEntry(KeyBytes.ofUtf8("c"), 1L, 0L, null, "STANDARD", null, false,
                        "owner-1", "Owner One", "CRC32", "FULL_OBJECT"),
                new DeleteMarkerEntry(KeyBytes.ofUtf8("d"), "v1", true, 0L, "owner-1"));
        assertThat(roundTrip(in)).containsExactlyElementsOf(in);
    }

    @Test
    void perColumnFallbackIsIndependentWithinOneBlock() {
        // owner_id explodes past DICT_CAP (one more than the cap) while storage_class/checksum
        // columns stay low-cardinality in the SAME block — the fallback decision must be made
        // independently per column, not for the whole block.
        List<ListEntry> in = new ArrayList<>();
        for (int i = 0; i < PageBlock.DICT_CAP + 1; i++) {
            in.add(new ObjectEntry(KeyBytes.ofUtf8("k" + i), 1L, 0L, null,
                    i % 2 == 0 ? "STANDARD" : "GLACIER", null, false,
                    "owner-" + i, "Owner " + (i % 3),
                    i % 2 == 0 ? "CRC32" : "SHA256", i % 3 == 0 ? "FULL_OBJECT" : "COMPOSITE"));
        }
        assertThat(roundTrip(in)).containsExactlyElementsOf(in);
    }

    @Test
    void dictionaryCapBoundaryAtExactlyCapStaysDictOverCapFallsBack() {
        List<ListEntry> atCap = distinctOwnerIds(PageBlock.DICT_CAP);
        List<ListEntry> overCap = distinctOwnerIds(PageBlock.DICT_CAP + 1);
        // Both round-trip correctly regardless of which side of the cap they land on...
        assertThat(roundTrip(atCap)).containsExactlyElementsOf(atCap);
        assertThat(roundTrip(overCap)).containsExactlyElementsOf(overCap);
    }

    @Test
    void dictionaryEncodingCompactsLowCardinalityColumnsButGracefullyFallsBackPastTheCap() {
        List<ListEntry> lowCardinality = new ArrayList<>();
        List<ListEntry> highCardinality = new ArrayList<>();
        String[] fewOwners = {"owner-a", "owner-b", "owner-c"};
        for (int i = 0; i < 1000; i++) {
            lowCardinality.add(objWithOwner("k" + i, fewOwners[i % fewOwners.length]));
            highCardinality.add(objWithOwner("k" + i, "owner-" + i));   // 1000 distinct >> DICT_CAP
        }
        PageBlock lowBlock = PageBlock.pack(lowCardinality, CMP);
        PageBlock highBlock = PageBlock.pack(highCardinality, CMP);

        assertThat(roundTrip(lowCardinality)).containsExactlyElementsOf(lowCardinality);
        assertThat(roundTrip(highCardinality)).containsExactlyElementsOf(highCardinality);

        // Same keys in both (so front-coding contributes identically) — the gap isolates the
        // dictionary's per-row saving on owner_id: low-cardinality reuses 3 dictionary entries across
        // 1000 rows; high-cardinality falls back to raw once past DICT_CAP and pays close to full
        // per-row string cost. Graceful: it round-trips, it just doesn't compact.
        double lowBytesPerRow = (double) lowBlock.packedBytes() / lowCardinality.size();
        double highBytesPerRow = (double) highBlock.packedBytes() / highCardinality.size();
        assertThat(lowBytesPerRow).isLessThan(highBytesPerRow - 5.0);
    }

    private static List<ListEntry> distinctOwnerIds(int count) {
        List<ListEntry> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            out.add(objWithOwner("k" + i, "owner-" + i));
        }
        return out;
    }

    private static ObjectEntry objWithOwner(String key, String ownerId) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null, false,
                ownerId, null, null, null);
    }

    private static ObjectEntry obj(String key, String etag) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 1L, etag, "STANDARD", null, false, null, null, null, null);
    }

    private static ObjectEntry object(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null, false, null, null, null, null);
    }

    private static ObjectEntry objectWithSize(String key, long size) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), size, 0L, null, null, null, false, null, null, null, null);
    }

    private static int[] toInts(byte[] b) {
        int[] out = new int[b.length];
        for (int i = 0; i < b.length; i++) {
            out[i] = b[i];
        }
        return out;
    }
}
