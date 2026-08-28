/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Adversarial guards for the shared final-file roll loop used by serial and parallel merges. */
class RolledPartWriterTest {

    private final ListEntryComparator comparator = new ListEntryComparator();

    @Test
    void byteThresholdNeverSplitsAnEqualKeyGroupAndSignalsOncePerDeferredGroup() throws IOException {
        List<ListEntry> entries = new ArrayList<>();
        addVersions(entries, "a", 50);
        addVersions(entries, "b", 3);
        addVersions(entries, "c", 1);
        entries.sort(comparator);

        List<RecordingWriter> files = new ArrayList<>();
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        List<Long> progress = new ArrayList<>();
        long rows;
        try (SortedCursor cursor = new InMemoryCursor(entries, comparator, DuplicateHook.NO_OP)) {
            rows = RolledPartWriter.drain(cursor, 1L, () -> {
                RecordingWriter writer = new RecordingWriter();
                files.add(writer);
                return writer;
            }, true, progress::add, metrics, EqualKeyPolicy.ALLOW, comparator);
        }

        assertThat(rows).isEqualTo(entries.size());
        assertThat(progress.stream().mapToLong(Long::longValue).sum()).isEqualTo(entries.size());
        assertThat(files).hasSize(3);
        assertThat(keys(files.get(0))).containsOnly("a").hasSize(50);
        assertThat(keys(files.get(1))).containsOnly("b").hasSize(3);
        assertThat(keys(files.get(2))).containsOnly("c").hasSize(1);
        assertStrictlyDisjoint(files);
        assertThat(metrics.count("SORT.final_roll_equal_key_deferred"))
                .as("one bounded signal for each oversized equal-key group, never one per row")
                .isEqualTo(2);
        assertThat(files).allMatch(RecordingWriter::closed);
        assertThat(files.subList(0, files.size() - 1)).noneMatch(RecordingWriter::finalFile);
        assertThat(files.getLast().finalFile()).isTrue();
    }

    @Test
    void rejectPolicyFailsAtTheFinalDrainAndSignalsExactlyOnce() throws IOException {
        List<ListEntry> entries = List.of(
                SortTestSupport.object("a"), SortTestSupport.object("a"), SortTestSupport.object("a"));
        List<RecordingWriter> files = new ArrayList<>();
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        try (SortedCursor cursor = new InMemoryCursor(entries, comparator, DuplicateHook.NO_OP)) {
            assertThatThrownBy(() -> RolledPartWriter.drain(cursor, Long.MAX_VALUE, () -> {
                RecordingWriter writer = new RecordingWriter();
                files.add(writer);
                return writer;
            }, true, ignored -> { }, metrics, EqualKeyPolicy.REJECT, comparator))
                    .isInstanceOf(DuplicateKeyException.class)
                    .hasMessage("sort-fixture found a duplicate key "
                            + "(adjacent-equal under the sort order): 'a'");
        }

        assertThat(metrics.count("SORT.equal_key_rejected")).isEqualTo(1);
        assertThat(files).singleElement().satisfies(writer -> {
            assertThat(writer.entries).hasSize(1);
            assertThat(writer.closed()).isTrue();
            assertThat(writer.finalFile()).isFalse();
        });
    }

    private static void addVersions(List<ListEntry> entries, String key, int count) {
        for (int i = 0; i < count; i++) {
            entries.add(new ObjectEntry(KeyBytes.ofUtf8(key), i, 0L, null, null,
                    String.format("v%04d", i), i == count - 1, null, null, null, null));
        }
    }

    private static List<String> keys(RecordingWriter writer) {
        return writer.entries.stream().map(e -> e.key().asString()).toList();
    }

    private static void assertStrictlyDisjoint(List<RecordingWriter> files) {
        for (int i = 1; i < files.size(); i++) {
            byte[] previousMax = files.get(i - 1).entries.getLast().key().rawUnsafe();
            byte[] nextMin = files.get(i).entries.getFirst().key().rawUnsafe();
            assertThat(KeyBytes.compareUnsigned(previousMax, nextMin))
                    .as("adjacent file %d maxKey < file %d minKey", i - 1, i)
                    .isNegative();
        }
    }

    /** Deterministic one-byte-per-row writer: a threshold of one is reached after the first row. */
    private static final class RecordingWriter implements SortedFileWriter {
        private final List<ListEntry> entries = new ArrayList<>();
        private boolean finalFile;
        private boolean closed;

        @Override
        public void write(ListEntry entry) {
            entries.add(entry);
        }

        @Override
        public long rows() {
            return entries.size();
        }

        @Override
        public long dataSize() {
            return entries.size();
        }

        @Override
        public void markFinal() {
            finalFile = true;
        }

        @Override
        public void setFileIndex(int ignored) {
        }

        @Override
        public void close() {
            closed = true;
        }

        boolean finalFile() {
            return finalFile;
        }

        boolean closed() {
            return closed;
        }
    }
}
