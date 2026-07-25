/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.filter.TimeParser;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.MockObject;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * A combined {@code --include} /
 * {@code --exclude} <b>plus</b> size <b>and</b> mtime bound filters correctly
 * end-to-end through the CLI's own {@link ListCommand#buildFilters} → {@link
 * ListRunner} path — not just the regex/size subset. Mtime is exercised through
 * the same {@code --modified-since/until} → {@link TimeParser} → filter wiring
 * the CLI uses, closing a gap where mtime was previously unit-only.
 */
class ListCommandFilterE2ETest {

    private static long micros(String date) throws Exception {
        return TimeParser.parseToMicros(date);
    }

    private static MockObject obj(String key, long size, String date) throws Exception {
        return new MockObject(key.getBytes(StandardCharsets.UTF_8), size, micros(date), "etag", "STANDARD");
    }

    @Test
    void combinedIncludeExcludeSizeAndMtimeThroughTheCliPath() throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .object(obj("data/keep.parquet", 5_000, "2024-06-01"))     // passes all four
                .object(obj("data/small.parquet", 50, "2024-06-01"))       // dropped: below --min-size
                .object(obj("data/old.parquet", 5_000, "2023-01-01"))      // dropped: before --modified-since
                .object(obj("data/new.parquet", 5_000, "2025-01-01"))      // dropped: after --modified-until
                .object(obj("data/scratch_tmp.parquet", 5_000, "2024-06-01")) // dropped: --exclude _tmp
                .object(obj("data/notes.txt", 5_000, "2024-06-01"))        // dropped: not --include .parquet$
                .object(obj("data/also.parquet", 5_000, "2024-07-15"))     // passes all four
                .build();

        // Drive the CLI's own field set → buildFilters(): exactly the flags a user types.
        ListCommand cmd = new ListCommand();
        cmd.filters.include = "\\.parquet$";
        cmd.filters.exclude = "_tmp";
        cmd.filters.minSize = "1k";
        cmd.filters.maxSize = "1mb";
        cmd.filters.modifiedAfter = "2024-01-01";
        cmd.filters.modifiedBefore = "2024-12-31";

        FilterChain chain = cmd.filters.chain();

        StringWriter out = new StringWriter();
        var stats = new ListRunner().run(RunContext.create(), fetcher, out,
                new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8, 1000, FilterChain.EMPTY, null, null)
                        .withFilters(chain));

        // Only the two rows that satisfy include ∧ exclude ∧ size ∧ mtime survive.
        assertThat(stats.objects()).isEqualTo(2);
        String text = out.toString();
        assertThat(text.lines().count()).isEqualTo(2);
        assertThat(text).contains("data/keep.parquet").contains("data/also.parquet");
        // Each exclusion reason is individually exercised (esp. mtime: old + new dropped).
        assertThat(text)
                .doesNotContain("small.parquet")
                .doesNotContain("old.parquet")
                .doesNotContain("new.parquet")
                .doesNotContain("scratch_tmp.parquet")
                .doesNotContain("notes.txt");
    }

    @Test
    void invertedSizeRangeIsRejectedAsInvalidArgsExitTwo() {
        ListCommand cmd = new ListCommand();
        cmd.filters.minSize = "2k";
        cmd.filters.maxSize = "1k";
        assertThatThrownBy(cmd.filters::chain)
                .isInstanceOf(InvalidArgsException.class)
                .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(2));
    }

    @Test
    void invertedModifiedRangeIsRejectedAsInvalidArgsExitTwo() {
        ListCommand cmd = new ListCommand();
        cmd.filters.modifiedAfter = "2026-01-02";
        cmd.filters.modifiedBefore = "2026-01-01";
        assertThatThrownBy(cmd.filters::chain)
                .isInstanceOf(InvalidArgsException.class)
                .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(2));
    }
}
