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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link SegmentWriter#flush} → {@link SegmentReader} round-trip through a real Parquet segment,
 * plus per-node max-key accumulation and the flush engagement counters. Field fidelity follows the
 * canonical schema: {@code is_latest} is a versioned-only column, so it round-trips
 * only for objects carrying a {@code version_id}.
 */
class SegmentRoundTripTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    @Test
    void roundTripsAllSubtypesAndAccumulatesPerNodeMaxKeys(@TempDir Path dir) throws IOException {
        SortConfig config = SortConfig.fromSystemProperties();
        SortBuffer buffer = new SortBuffer(config, cmp);
        // Node 1 ascends: a, c, e ; node 2 ascends: b, d. Per-node max: node1=e, node2=d.
        buffer.admit(1L, List.of(objVersioned("a", "v1"), cp("c")));
        buffer.admit(1L, List.of(objPlain("e")));
        buffer.admit(2L, List.of(objVersioned("b", "v2"), dm("d")));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SegmentWriter writer = new SegmentWriter(cmp, DuplicateHook.NO_OP, metrics, 1L << 20);
        Path segment = dir.resolve("segment-1.parquet");

        SegmentResult result = writer.flush(buffer.seal(SealTrigger.DRAIN), segment);

        List<ListEntry> read = readAll(segment);
        assertThat(read).containsExactly(
                objVersioned("a", "v1"), objVersioned("b", "v2"), cp("c"), dm("d"), objPlain("e"));
        assertThat(result.rows()).isEqualTo(5);
        assertThat(result.bytes()).isGreaterThan(0);

        Map<Long, byte[]> maxKeys = result.perNodeMaxKeys();
        assertThat(maxKeys).containsOnlyKeys(1L, 2L);
        assertThat(maxKeys.get(1L)).containsExactly(ints("e"));
        assertThat(maxKeys.get(2L)).containsExactly(ints("d"));

        assertThat(metrics.count("SORT.segment_flushed")).isEqualTo(1);
        assertThat(metrics.count("SORT.buffer_byte_gated")).isZero();   // sealed on DRAIN, not the byte gate
    }

    @Test
    void byteGatedSealEmitsTheByteGateCounter(@TempDir Path dir) throws IOException {
        SortConfig config = SortConfig.fromSystemProperties();
        SortBuffer buffer = new SortBuffer(config, cmp);
        buffer.admit(7L, List.of(objPlain("k")));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SegmentWriter writer = new SegmentWriter(cmp, DuplicateHook.NO_OP, metrics, 1L << 20);
        writer.flush(buffer.seal(SealTrigger.BYTE_GATE), dir.resolve("s.parquet"));

        assertThat(metrics.count("SORT.segment_flushed")).isEqualTo(1);
        assertThat(metrics.count("SORT.buffer_byte_gated")).isEqualTo(1);
    }

    @Test
    void emptySegmentRoundTrips(@TempDir Path dir) throws IOException {
        SortConfig config = SortConfig.fromSystemProperties();
        SortBuffer buffer = new SortBuffer(config, cmp);
        SegmentWriter writer = new SegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, 1L << 20);
        Path segment = dir.resolve("empty.parquet");

        SegmentResult result = writer.flush(buffer.seal(SealTrigger.DRAIN), segment);

        assertThat(result.rows()).isZero();
        assertThat(readAll(segment)).isEmpty();
    }

    private List<ListEntry> readAll(Path segment) throws IOException {
        List<ListEntry> out = new ArrayList<>();
        try (SegmentReader reader = new SegmentReader(segment)) {
            while (reader.hasNext()) {
                out.add(reader.next());
            }
        }
        return out;
    }

    private static ObjectEntry objVersioned(String key, String versionId) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 10L, 1_700_000_000_000_000L,
                "d41d8cd98f00b204e9800998ecf8427e", "STANDARD", versionId, true, null, null, null, null);
    }

    private static ObjectEntry objPlain(String key) {
        // Non-versioned: is_latest must be false to round-trip (versioned-only column, §4).
        return new ObjectEntry(KeyBytes.ofUtf8(key), 20L, 1_700_000_000_000_001L,
                null, null, null, false, null, null, null, null);
    }

    private static CommonPrefixEntry cp(String key) {
        return new CommonPrefixEntry(KeyBytes.ofUtf8(key));
    }

    private static DeleteMarkerEntry dm(String key) {
        return new DeleteMarkerEntry(KeyBytes.ofUtf8(key), "dv", true, 5L, "own");
    }

    private static int[] ints(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        int[] out = new int[b.length];
        for (int i = 0; i < b.length; i++) {
            out[i] = b[i];
        }
        return out;
    }
}
