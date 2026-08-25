/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.ListingException;
import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** UNIT-filter: each v1.0 Filter variant + chain ordering. */
class FilterTest {

    private static ObjectEntry obj(String key, long size, long mtimeMicros, String storageClass) {
        return ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8(key), size, mtimeMicros, "etag", storageClass,
                null, true, null, null);
    }

    @Test
    void includeRegexKeepsOnlyMatches() throws Exception {
        IncludeRegexFilter f = IncludeRegexFilter.of("\\.parquet$");
        assertThat(f.matches(obj("a/x.parquet", 1, 0, "STANDARD"))).isTrue();
        assertThat(f.matches(obj("a/x.tmp", 1, 0, "STANDARD"))).isFalse();
    }

    @Test
    void excludeRegexDropsMatches() throws Exception {
        ExcludeRegexFilter f = ExcludeRegexFilter.of("\\.tmp$");
        assertThat(f.matches(obj("a/x.tmp", 1, 0, "STANDARD"))).isFalse();
        assertThat(f.matches(obj("a/x.parquet", 1, 0, "STANDARD"))).isTrue();
    }

    @Test
    void sizeFilterIsInclusiveRangeAndPassesNonObjects() throws Exception {
        SizeFilter f = new SizeFilter(100, 1000);
        assertThat(f.matches(obj("k", 100, 0, "STANDARD"))).isTrue();
        assertThat(f.matches(obj("k", 1000, 0, "STANDARD"))).isTrue();
        assertThat(f.matches(obj("k", 99, 0, "STANDARD"))).isFalse();
        assertThat(f.matches(obj("k", 1001, 0, "STANDARD"))).isFalse();
        assertThat(f.matches(new CommonPrefixEntry(KeyBytes.ofUtf8("p/")))).isTrue();  // no size → passes
    }

    @Test
    void mtimeFilterIsInclusiveRange() throws Exception {
        MtimeFilter f = new MtimeFilter(1000, 2000);
        assertThat(f.matches(obj("k", 1, 1000, "STANDARD"))).isTrue();
        assertThat(f.matches(obj("k", 1, 2000, "STANDARD"))).isTrue();
        assertThat(f.matches(obj("k", 1, 999, "STANDARD"))).isFalse();
        assertThat(f.matches(obj("k", 1, 2001, "STANDARD"))).isFalse();
    }

    @Test
    void storageClassFilterIsCaseInsensitive() throws Exception {
        StorageClassFilter f = new StorageClassFilter(Set.of("glacier"));
        assertThat(f.matches(obj("k", 1, 0, "GLACIER"))).isTrue();
        assertThat(f.matches(obj("k", 1, 0, "STANDARD"))).isFalse();
    }

    @Test
    void chainOrdersCheapFiltersBeforeRegexAndAnds() throws Exception {
        // Insertion order: regex (cost 10) then size (cost 0). The chain must reorder
        // size-first so the cheap predicate short-circuits before the regex.
        FilterChain chain = FilterChain.of(List.of(
                IncludeRegexFilter.of("\\.parquet$"), SizeFilter.atLeast(1000)));
        var order = chain.orderedForTest();
        assertThat(order.get(0)).isInstanceOf(SizeFilter.class);
        assertThat(order.get(1)).isInstanceOf(IncludeRegexFilter.class);

        // AND semantics: must satisfy both.
        assertThat(chain.matches(obj("x.parquet", 5000, 0, "STANDARD"))).isTrue();
        assertThat(chain.matches(obj("x.parquet", 10, 0, "STANDARD"))).isFalse();   // too small
        assertThat(chain.matches(obj("x.txt", 5000, 0, "STANDARD"))).isFalse();     // wrong ext
    }

    @Test
    void emptyChainKeepsEverything() throws Exception {
        assertThat(FilterChain.EMPTY.isEmpty()).isTrue();
        assertThat(FilterChain.EMPTY.matches(obj("anything", 1, 0, "X"))).isTrue();
    }

    @Test
    void mtimeFilterMapsUnsupportedWireTextToAListingFailure() {
        MtimeFilter filter = new MtimeFilter(Long.MIN_VALUE, Long.MAX_VALUE);
        ObjectEntry entry = new ObjectEntry(KeyBytes.ofUtf8("bad-time"), 1L, "not-a-timestamp",
                "etag", "STANDARD", null, true, null, null, null, null);

        assertThatThrownBy(() -> filter.matches(entry))
                .isInstanceOf(ListingException.class)
                .hasMessage("invalid last-modified timestamp in object-store response")
                .hasRootCauseInstanceOf(java.time.format.DateTimeParseException.class);
    }
}
