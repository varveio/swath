/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.filter.ExcludeRegexFilter;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.filter.IncludeRegexFilter;
import io.varve.swath.filter.SizeFilter;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.testkit.MockObject;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * INT-filter: combined {@code --exclude '\.tmp$' --include
 * '\.parquet$'} + a size bound filters correctly end-to-end through
 * {@link ListRunner} (deterministic MockPageFetcher).
 */
class FilterE2ETest {

    private static MockObject obj(String key, long size) {
        return new MockObject(key.getBytes(StandardCharsets.UTF_8), size, 0L, "etag", "STANDARD");
    }

    @Test
    void combinedExcludeIncludeAndSizeFilter() throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .object(obj("data/a.parquet", 5000))
                .object(obj("data/b.parquet", 50))      // too small
                .object(obj("data/c.tmp", 5000))        // excluded by .tmp$
                .object(obj("data/d.txt", 5000))        // not included by .parquet$
                .object(obj("data/e.parquet", 5000))
                .build();

        FilterChain chain = FilterChain.of(List.of(
                ExcludeRegexFilter.of("\\.tmp$"),
                IncludeRegexFilter.of("\\.parquet$"),
                SizeFilter.atLeast(100)));

        StringWriter out = new StringWriter();
        var stats = new ListRunner().run(RunContext.create(), fetcher, out,
                new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8, 1000, FilterChain.EMPTY, null, null)
                        .withFilters(chain));

        String text = out.toString();
        assertThat(stats.objects()).isEqualTo(2);
        assertThat(text.lines().count()).isEqualTo(2);
        assertThat(text).contains("\"key\":\"data/a.parquet\"");
        assertThat(text).contains("\"key\":\"data/e.parquet\"");
        assertThat(text).doesNotContain("b.parquet").doesNotContain(".tmp").doesNotContain(".txt");
    }
}
