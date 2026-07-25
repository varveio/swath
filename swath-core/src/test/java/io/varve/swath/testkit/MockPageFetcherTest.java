/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.error.ListingException;
import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Self-test of the {@link MockPageFetcher} S3 fidelity: start_after exclusivity,
 * prefix filtering, {@code IsTruncated} (not count==maxKeys), delimiter rollup,
 * and the fault interceptor. The mock must be trustworthy before the engine is
 * tested against it.
 */
class MockPageFetcherTest {

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Drain a flat OBJECTS listing by paginating on the last emitted key. */
    private static List<String> drainFlat(MockPageFetcher f, byte[] prefix) throws Exception {
        List<String> keys = new ArrayList<>();
        byte[] startAfter = null;
        while (true) {
            ListPage page = f.fetchPage(PageRequest.objects(prefix, startAfter, 1000));
            for (ListEntry e : page.entries()) {
                keys.add(e.key().asString());
                startAfter = e.key().raw();
            }
            if (!page.truncated()) {
                break;
            }
        }
        return keys;
    }

    @Test
    void paginatesAcrossPagesInByteOrderNoDuplicates() throws Exception {
        MockPageFetcher f = MockPageFetcher.builder().keys(Keyspaces.exactly(2500)).build();

        List<String> keys = drainFlat(f, new byte[0]);

        assertThat(keys).hasSize(2500);
        assertThat(keys).isSorted();
        assertThat(keys).doesNotHaveDuplicates();
        assertThat(f.apiCalls()).isEqualTo(3);   // ceil(2500/1000)
    }

    @Test
    void truncatedUsesIsTruncatedNotCount() throws Exception {
        // Exactly 1000 keys: the first page has 1000 entries but is NOT truncated.
        MockPageFetcher exactly1000 = MockPageFetcher.builder().keys(Keyspaces.exactly(1000)).build();
        ListPage page = exactly1000.fetchPage(PageRequest.objects(new byte[0], null, 1000));
        assertThat(page.entries()).hasSize(1000);
        assertThat(page.truncated()).isFalse();
        assertThat(exactly1000.apiCalls()).isEqualTo(1);

        // 1001 keys: first page 1000 + truncated, second page 1.
        MockPageFetcher exactly1001 = MockPageFetcher.builder().keys(Keyspaces.exactly(1001)).build();
        ListPage first = exactly1001.fetchPage(PageRequest.objects(new byte[0], null, 1000));
        assertThat(first.truncated()).isTrue();
        byte[] last = first.entries().getLast().key().raw();
        ListPage second = exactly1001.fetchPage(PageRequest.objects(new byte[0], last, 1000));
        assertThat(second.entries()).hasSize(1);
        assertThat(second.truncated()).isFalse();
    }

    @Test
    void startAfterIsExclusive() throws Exception {
        MockPageFetcher f = MockPageFetcher.builder().keys(Keyspaces.exactly(10)).build();
        byte[] boundary = utf8("key/000000004");
        ListPage page = f.fetchPage(PageRequest.objects(new byte[0], boundary, 1000));
        for (ListEntry e : page.entries()) {
            assertThat(KeyBytes.compareUnsigned(e.key().raw(), boundary)).isGreaterThan(0);
        }
        assertThat(page.entries().getFirst().key().asString()).isEqualTo("key/000000005");
    }

    @Test
    void prefixFiltersTheKeyspace() throws Exception {
        MockPageFetcher f = MockPageFetcher.builder()
                .keys(List.of(utf8("a/1"), utf8("a/2"), utf8("b/1"), utf8("c")))
                .build();
        List<String> aKeys = drainFlat(f, utf8("a/"));
        assertThat(aKeys).containsExactly("a/1", "a/2");
    }

    @Test
    void delimiterRollsUpCommonPrefixes() throws Exception {
        MockPageFetcher f = MockPageFetcher.builder()
                .keys(List.of(utf8("a/1"), utf8("a/2"), utf8("b/1"), utf8("c")))
                .build();
        ListPage page = f.fetchPage(PageRequest.objectsDelimited(new byte[0], utf8("/"), null, 1000));

        List<String> prefixes = page.commonPrefixes().stream().map(KeyBytes::asString).toList();
        List<String> objects = page.entries().stream()
                .map(e -> ((ObjectEntry) e).key().asString()).toList();

        assertThat(prefixes).containsExactly("a/", "b/");
        assertThat(objects).containsExactly("c");
        assertThat(page.entries()).allMatch(e -> !(e instanceof CommonPrefixEntry));
    }

    @Test
    void interceptorCanInjectAStuckPage() {
        MockPageFetcher f = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(50))
                .interceptor((req, idx, computed) -> {
                    throw new ListingException("injected fault at call " + idx);
                })
                .build();
        assertThat(catchListing(() -> f.fetchPage(PageRequest.objects(new byte[0], null, 1000))))
                .isInstanceOf(ListingException.class);
    }

    private static Throwable catchListing(ThrowingCall c) {
        try {
            c.run();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }

    @FunctionalInterface
    private interface ThrowingCall {
        void run() throws Exception;
    }
}
