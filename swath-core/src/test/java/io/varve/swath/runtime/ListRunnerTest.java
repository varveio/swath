/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.varve.swath.filter.FilterChain;
import io.varve.swath.output.ListingStatistics;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import org.junit.jupiter.api.Test;

/**
 * INT-2 logic (jsonl line count == bucket size) and INT-12 (broken pipe → clean
 * stop, no exception) over the deterministic MockPageFetcher; the LocalStack
 * variants live in {@code ListRunnerIT}.
 */
class ListRunnerTest {

    private ListRunner.Spec jsonl() {
        return new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8, 1000, FilterChain.EMPTY, null, null);
    }

    @Test
    void jsonlEmitsExactlyOneLinePerObject() throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(2500)).build();
        StringWriter out = new StringWriter();

        ListingStatistics stats = new ListRunner().run(RunContext.create(), fetcher, out, jsonl());

        String text = out.toString();
        assertThat(text.split("\n")).hasSize(2500);   // jq -s length == bucket size
        assertThat(stats.objects()).isEqualTo(2500);
        assertThat(text.lines()).allMatch(l -> l.startsWith("{") && l.endsWith("}"));
    }

    @Test
    void brokenPipeStopsCleanlyWithoutThrowing() {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(5000)).build();
        Writer brokenPipe = new Writer() {
            @Override
            public void write(char[] cbuf, int off, int len) throws IOException {
                throw new IOException("Broken pipe");
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };

        // Downstream closed → the run must complete cleanly (exit 0), no stack trace.
        assertThatCode(() -> new ListRunner().run(RunContext.create(), fetcher, brokenPipe, jsonl()))
                .doesNotThrowAnyException();
    }
}
