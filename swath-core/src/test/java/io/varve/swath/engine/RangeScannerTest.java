/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.ListingException;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.store.ListPage;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.RangeScanners;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * Sequential {@code runRange} (algorithms.md §2): correct count over a whole
 * range (INT-1 logic on the deterministic mock), the {@code (lo, hi]} boundary
 * stop-check, and the I9 stuck-token / truncation-without-keys defenses (INT-3
 * logic). The real LocalStack INT-1/INT-3 live in {@code S3PageFetcherIT}.
 */
class RangeScannerTest {

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private List<String> collect(MockPageFetcher f, byte[] hi) throws Exception {
        RangeScanner scanner = RangeScanners.of(f);
        List<String> keys = new ArrayList<>();
        scanner.runRange(new byte[0], ListingMode.OBJECTS, null, hi, null,
                batch -> batch.forEach(e -> keys.add(e.key().asString())));
        return keys;
    }

    @Test
    void emitsEveryObjectExactlyOnceAcrossPages() throws Exception {
        MockPageFetcher f = MockPageFetcher.builder().keys(Keyspaces.exactly(10_000)).build();
        List<String> keys = collect(f, null);
        assertThat(keys).hasSize(10_000);
        assertThat(keys).isSorted().doesNotHaveDuplicates();
        assertThat(f.apiCalls()).isEqualTo(10);   // ceil(10000/1000)
    }

    @Test
    void honoursInclusiveUpperBoundBoundaryBelongsLeft() throws Exception {
        MockPageFetcher f = MockPageFetcher.builder().keys(Keyspaces.exactly(10)).build();
        // hi = key/000000004 (inclusive) ⇒ emit keys 0..4 = 5 keys.
        List<String> keys = collect(f, utf8("key/000000004"));
        assertThat(keys).containsExactly(
                "key/000000000", "key/000000001", "key/000000002", "key/000000003", "key/000000004");
    }

    @Test
    void truncatedPageWithNoInRangeKeysIsAnError() {
        MockPageFetcher f = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(50))
                .interceptor((req, idx, computed) ->
                        new ListPage(List.of(), List.of(), true, null, null, null, 200, Duration.ZERO))
                .build();
        assertThatThrownBy(() -> collect(f, null))
                .isInstanceOf(ListingException.class)
                .hasMessageContaining("no keys");
    }

    @Test
    void stuckTokenNoForwardProgressIsAnError() {
        // The fetcher keeps returning the SAME truncated page → cursor never advances.
        ObjectEntry stuck = ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8("frozen/key"), 1L, 0L,
                "etag", "STANDARD", null, true, null, null);
        MockPageFetcher f = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(50))
                .interceptor((req, idx, computed) ->
                        new ListPage(List.<ListEntry>of(stuck), List.of(), true, null, null, null, 200, Duration.ZERO))
                .build();
        assertThatThrownBy(() -> collect(f, null))
                .isInstanceOf(ListingException.class)
                .hasMessageContaining("no forward progress");
    }

    @Test
    void terminalPageRepeatingStartAfterIsAnErrorBeforeEmit() {
        ObjectEntry stuck = ObjectEntry.withoutOwnerDisplayNameAndChecksumType(
                KeyBytes.ofUtf8("frozen/key"), 1L, 0L,
                "etag", "STANDARD", null, true, null, null);
        MockPageFetcher f = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(2))
                .interceptor((req, idx, computed) -> new ListPage(
                        List.<ListEntry>of(stuck), List.of(), idx == 0,
                        null, null, null, 200, Duration.ZERO))
                .build();
        List<String> emitted = new ArrayList<>();

        assertThatThrownBy(() -> RangeScanners.of(f).runRange(
                new byte[0], ListingMode.OBJECTS, null, null, null,
                batch -> batch.forEach(e -> emitted.add(e.key().asString()))))
                .isInstanceOf(ListingException.class)
                .hasMessageContaining("no forward progress");
        assertThat(emitted).containsExactly("frozen/key");
    }

    @Test
    void stuckPageIsNotEmittedBeforeTheErrorNoDuplicates() {
        // A fixed truncated page [k1,k2,k3] is returned every call (ignores start_after).
        ObjectEntry k1 = ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8("k1"), 1, 0, "e", "STANDARD", null, true, null, null);
        ObjectEntry k2 = ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8("k2"), 1, 0, "e", "STANDARD", null, true, null, null);
        ObjectEntry k3 = ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8("k3"), 1, 0, "e", "STANDARD", null, true, null, null);
        MockPageFetcher f = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(5))
                .interceptor((req, idx, computed) -> new ListPage(
                        List.<ListEntry>of(k1, k2, k3), List.of(), true, null, null, null, 200, Duration.ZERO))
                .build();

        List<String> emitted = new ArrayList<>();
        RangeScanner scanner = RangeScanners.of(f);
        assertThatThrownBy(() -> scanner.runRange(new byte[0], ListingMode.OBJECTS, null, null, null,
                batch -> batch.forEach(e -> emitted.add(e.key().asString()))))
                .isInstanceOf(ListingException.class)
                .hasMessageContaining("no forward progress");

        // The first page emitted once; the stuck second page was detected BEFORE emit → no duplicates.
        assertThat(emitted).containsExactly("k1", "k2", "k3");
    }

    @Test
    void multibyteAndSupplementaryKeysPaginateByteExactInUnsignedOrder() throws Exception {
        // Keys spanning ASCII, 2-byte (é), 3-byte private-use (U+E000), and 4-byte supplementary
        // (U+10000). maxKeys=1 round-trips EVERY key as a start_after boundary; the engine must
        // paginate byte-exactly in unsigned order (UTF-8 byte order, NOT UTF-16). The mock keyspace
        // is byte-faithful (unlike LocalStack), so this is the deterministic E2E proof.
        byte[] ascii = {'a'};
        byte[] z = {'z'};
        byte[] twoByte = {(byte) 0xC3, (byte) 0xA9};                       // é  U+00E9
        byte[] privateUse = {(byte) 0xEE, (byte) 0x80, (byte) 0x80};       // U+E000
        byte[] supplementary = {(byte) 0xF0, (byte) 0x90, (byte) 0x80, (byte) 0x80};  // U+10000
        List<byte[]> keys = new ArrayList<>(List.of(ascii, z, twoByte, privateUse, supplementary));

        MockPageFetcher f = MockPageFetcher.builder().keys(keys).build();
        List<byte[]> got = new ArrayList<>();
        RangeScanners.of(f, 1).runRange(new byte[0], ListingMode.OBJECTS, null, null, null,
                batch -> batch.forEach(e -> got.add(e.key().raw())));

        List<byte[]> expected = new ArrayList<>(keys);
        expected.sort(Arrays::compareUnsigned);
        assertThat(got).hasSize(5);
        for (int i = 0; i < got.size(); i++) {
            assertThat(got.get(i)).isEqualTo(expected.get(i));
        }
        // Unsigned byte order: ... z (7a) < é (C3) < U+E000 (EE) < U+10000 (F0).
        assertThat(got.getLast()).containsExactly(0xF0, 0x90, 0x80, 0x80);
        assertThat(got.get(3)).containsExactly(0xEE, 0x80, 0x80);
    }

    @Test
    void emptyBucketEmitsNothingAndStops() throws Exception {
        MockPageFetcher f = MockPageFetcher.builder().keys(Keyspaces.empty()).build();
        assertThat(collect(f, null)).isEmpty();
        assertThat(f.apiCalls()).isEqualTo(1);
    }

    // --- Per-key volatile hi re-read (in-flight page vs steal, edge-case 7) ---

    @Test
    void perKeyVolatileHiNarrowsMidPageStopsEmitting() throws Exception {
        // Single non-truncated page of 10 keys; the bound is unbounded for the first 5 keys
        // checked then narrows to key/000000004 — modelling a thief lowering hi mid-page. The
        // re-read must observe the narrowing and stop, so only keys 0..4 are emitted.
        MockPageFetcher f = MockPageFetcher.builder().keys(Keyspaces.exactly(10)).build();
        AtomicInteger checks = new AtomicInteger();
        Supplier<byte[]> hi = () -> checks.getAndIncrement() < 5 ? null : utf8("key/000000004");

        List<String> keys = new ArrayList<>();
        RangeScanners.of(f).runRange(new byte[0], ListingMode.OBJECTS, null, hi, null,
                (batch, lastKey, completed) -> batch.forEach(e -> keys.add(e.key().asString())));

        assertThat(keys).containsExactly(
                "key/000000000", "key/000000001", "key/000000002", "key/000000003", "key/000000004");
    }

    @Test
    void perKeyBoundIsStrictBoundaryKeyStaysLeft() throws Exception {
        // A constant narrowed bound m = key/000000004: STRICT k > hi means k == m is still
        // emitted (boundary-belongs-LEFT, I3) and the first key > m stops the scan.
        MockPageFetcher f = MockPageFetcher.builder().keys(Keyspaces.exactly(10)).build();
        Supplier<byte[]> hi = () -> utf8("key/000000004");

        List<String> keys = new ArrayList<>();
        RangeScanners.of(f).runRange(new byte[0], ListingMode.OBJECTS, null, hi, null,
                (batch, lastKey, completed) -> batch.forEach(e -> keys.add(e.key().asString())));

        assertThat(keys).containsExactly(
                "key/000000000", "key/000000001", "key/000000002", "key/000000003", "key/000000004");
    }

    @Test
    void crashMidFetchLeavesCursorValidAndResumeIsByteExact() throws Exception {
        // 10 keys, maxKeys=1 ⇒ 10 pages. The fetch of the page whose start_after=key4 (i.e.
        // key5's page) throws — a crash mid-fetch, AFTER key4's page was committed+emitted.
        // key5's page was never committed nor emitted, so on resume it is simply re-fetched
        // from the last committed cursor (key4) — no double-count, no skip.
        List<byte[]> keyspace = Keyspaces.exactly(10);
        byte[] key4 = utf8("key/000000004");
        byte[] key5 = utf8("key/000000005");

        MockPageFetcher crashing = MockPageFetcher.builder()
                .keys(keyspace)
                .maxKeysCap(1)
                .interceptor((req, idx, page) -> {
                    if (req.startAfter() != null && Arrays.equals(req.startAfter(), key4)) {
                        throw new ListingException("injected crash mid-fetch of page 5");
                    }
                    return page;
                })
                .build();

        List<byte[]> firstRun = new ArrayList<>();
        byte[][] lastCommitted = {null};
        assertThatThrownBy(() -> RangeScanners.of(crashing).runRange(
                new byte[0], ListingMode.OBJECTS, null, (byte[]) null, null,
                (batch, lastKey, completed) -> {
                    batch.forEach(e -> firstRun.add(e.key().raw()));
                    if (lastKey != null) {
                        lastCommitted[0] = lastKey;
                    }
                }))
                .isInstanceOf(ListingException.class)
                .hasMessageContaining("mid-fetch");

        assertThat(firstRun).doesNotHaveDuplicates();
        assertThat(Arrays.compareUnsigned(lastCommitted[0], key5))
                .as("durable cursor never advanced onto or past the un-committed crashing page").isNegative();

        // Resume from the last committed cursor with a clean fetcher: byte-exact, no gap/overlap.
        MockPageFetcher clean = MockPageFetcher.builder().keys(keyspace).maxKeysCap(1).build();
        List<byte[]> resumeRun = new ArrayList<>();
        RangeScanners.of(clean).runRange(new byte[0], ListingMode.OBJECTS, lastCommitted[0], (byte[]) null, null,
                (batch, lastKey, completed) -> batch.forEach(e -> resumeRun.add(e.key().raw())));

        List<byte[]> union = new ArrayList<>(firstRun);
        union.addAll(resumeRun);
        union.sort(Arrays::compareUnsigned);
        List<byte[]> expected = new ArrayList<>(keyspace);
        expected.sort(Arrays::compareUnsigned);
        assertThat(union).as("first-run ∪ resume = full keyspace, byte-exact").hasSize(10);
        for (int i = 0; i < expected.size(); i++) {
            assertThat(union.get(i)).isEqualTo(expected.get(i));
        }
        // No key straddles the crash boundary: the pre-crash and resume runs are disjoint.
        for (byte[] x : firstRun) {
            for (byte[] y : resumeRun) {
                assertThat(Arrays.equals(x, y))
                        .as("no key emitted in both runs (no double-count)").isFalse();
            }
        }
    }

    @Test
    void postNarrowEmptyBatchCompletesWithoutErrorOrCursorAdvance() throws Exception {
        // A TRUNCATED page whose keys are ALL above the just-narrowed bound m: the in-range
        // batch is empty yet reachedBound=true ⇒ the node COMPLETES (no I9 "no keys" error)
        // and the cursor does NOT advance (lastKey == null) — §4.2.
        ObjectEntry b0 = ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8("b/0"), 1, 0, "e", "STANDARD", null, true, null, null);
        ObjectEntry b1 = ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8("b/1"), 1, 0, "e", "STANDARD", null, true, null, null);
        MockPageFetcher f = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(5))
                .interceptor((req, idx, computed) -> new ListPage(
                        List.<ListEntry>of(b0, b1), List.of(), true, null, null, null, 200, Duration.ZERO))
                .build();
        Supplier<byte[]> hi = () -> utf8("a");   // below every "b/..." key

        List<byte[]> lastKeys = new ArrayList<>();
        List<Boolean> completedFlags = new ArrayList<>();
        long emitted = RangeScanners.of(f).runRange(new byte[0], ListingMode.OBJECTS, null, hi, null,
                (batch, lastKey, completed) -> {
                    lastKeys.add(lastKey);
                    completedFlags.add(completed);
                });

        assertThat(emitted).isZero();
        assertThat(completedFlags).containsExactly(true);     // single page, node completed
        assertThat(lastKeys).containsExactly((byte[]) null);  // cursor left unchanged
    }
}
