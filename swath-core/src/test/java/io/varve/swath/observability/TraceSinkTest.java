/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit coverage for the {@link TraceSink} seam itself (docs/internals/metrics-internals.md §7) —
 * separate from {@code WorkStealingScanTraceTest}, which covers the engine-level event sequence.
 */
final class TraceSinkTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void noop_isAlwaysDisabled_andEveryMethodIsAFreeNoOp() {
        assertThat(TraceSink.NONE.enabled()).isFalse();
        assertThatCode(() -> {
            TraceSink.NONE.seeded(1L, null, null);
            TraceSink.NONE.claimed(0L, 1L, null, null, null);
            TraceSink.NONE.pageCommitted(0L, 1L, 10, null, false);
            TraceSink.NONE.stealAttempt(0L, "RETRY", "cursor_passed_pivot");
            TraceSink.NONE.split(0L, 1L, 2L, "midpoint", null, null);
            TraceSink.NONE.ownerSplit(0L, 1L, 2L, "self_published", null, null);
            TraceSink.NONE.completed(0L, 1L);
            TraceSink.NONE.failed(0L, 1L, "boom");
            TraceSink.NONE.close();
        }).doesNotThrowAnyException();
    }

    @Test
    void jsonl_writesSchemaVersionedParseableLines_withDeterministicNanoClock(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("run.trace.jsonl");
        AtomicLong fakeNanos = new AtomicLong(1_000L);
        TraceSink sink = TraceSink.jsonl(file, fakeNanos::incrementAndGet);
        try {
            sink.seeded(1L, null, "hi".getBytes(StandardCharsets.UTF_8));
            sink.claimed(0L, 1L, null, "cur".getBytes(StandardCharsets.UTF_8), "hi".getBytes(StandardCharsets.UTF_8));
        } finally {
            sink.close();
        }

        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        assertThat(lines).hasSize(2);
        JsonNode seeded = MAPPER.readTree(lines.get(0));
        assertThat(seeded.get("v").asInt()).isEqualTo(1);
        assertThat(seeded.get("event").asText()).isEqualTo("seeded");
        assertThat(seeded.get("node_id").asLong()).isEqualTo(1L);
        assertThat(seeded.get("hi").asText()).isEqualTo("hi");
        assertThat(seeded.get("lo").isNull()).as("null key fields serialize as JSON null, not absent").isTrue();

        JsonNode claimed = MAPPER.readTree(lines.get(1));
        assertThat(claimed.get("event").asText()).isEqualTo("claimed");
        assertThat(claimed.get("cursor").asText()).isEqualTo("cur");
        // ts_ns is strictly increasing under the injected nanoClock (deterministic ordering, §
        // "reuse the nanoClock seam" — mirrors RunMetrics's avg-in-flight test seam).
        assertThat(claimed.get("ts_ns").asLong()).isGreaterThan(seeded.get("ts_ns").asLong());
    }

    @Test
    void jsonl_flushesOnlyOnCompletedOrFailed_soAnUnflushedTailNeverReachesDiskBeforeClose(
            @TempDir Path dir) throws Exception {
        // The "crash mid-run" durability contract (buffered writer, flush on completed/failed only —
        // not every page): simulate a process kill by NEVER calling close(). Whatever the sink
        // explicitly flushed (via `completed`) must already be a complete, parseable prefix on disk;
        // whatever was written AFTER that flush point (still only buffered) must NOT have reached
        // disk at all — proving a real kill -9 at that instant would lose it cleanly (not corrupt it).
        Path file = dir.resolve("crash.trace.jsonl");
        TraceSink sink = TraceSink.jsonl(file);
        sink.seeded(1L, null, null);
        sink.claimed(0L, 1L, null, null, null);
        sink.pageCommitted(0L, 1L, 10, "k1".getBytes(StandardCharsets.UTF_8), false);
        sink.pageCommitted(0L, 1L, 10, "k2".getBytes(StandardCharsets.UTF_8), false);
        sink.completed(0L, 1L);   // the only flush point so far

        // NEVER call sink.close() — this stands in for the process dying right here.
        List<String> flushedLines = Files.readAllLines(file, StandardCharsets.UTF_8);
        assertThat(flushedLines).as("everything up to and including the flush point survived")
                .hasSize(5);
        for (String line : flushedLines) {
            JsonNode node = MAPPER.readTree(line);   // must be whole, parseable JSON — never a torn line
            assertThat(node.get("v").asInt()).isEqualTo(1);
        }
        assertThat(flushedLines.getLast()).contains("\"event\":\"completed\"");

        // A write AFTER the flush point, still un-closed: must NOT be visible on disk yet.
        sink.pageCommitted(0L, 1L, 5, "k3".getBytes(StandardCharsets.UTF_8), true);
        List<String> stillOnDisk = Files.readAllLines(file, StandardCharsets.UTF_8);
        assertThat(stillOnDisk).as("the post-flush write is only buffered, not yet durable")
                .hasSize(5);

        sink.close();   // real cleanup for this test's own temp file, not part of the "crash" assertion
        List<String> afterClose = Files.readAllLines(file, StandardCharsets.UTF_8);
        assertThat(afterClose).hasSize(6);
    }

    @Test
    void jsonl_closeIsIdempotentAndSwallowsRepeatedCalls(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("idempotent.trace.jsonl");
        TraceSink sink = TraceSink.jsonl(file);
        sink.seeded(1L, null, null);
        sink.close();
        assertThatCode(sink::close).doesNotThrowAnyException();
        // A write after close is a silent no-op (never throws mid-run over a closed sink).
        assertThatCode(() -> sink.completed(0L, 1L)).doesNotThrowAnyException();
    }
}
