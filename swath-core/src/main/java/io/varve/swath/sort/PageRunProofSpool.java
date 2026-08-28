/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ByteMidpoint;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/** Fixed-slot temporary storage for exact variable-length physical-zone proof keys. */
final class PageRunProofSpool {

    private static final int OPEN = 1;
    private static final int FINISHED = 2;
    private static final int FIXED_BYTES = 56;
    private static final int KEY_SLOT_BYTES = Short.BYTES + ByteMidpoint.MAX_KEY_LEN;
    private static final int SLOT_BYTES = FIXED_BYTES + KeyField.values().length * KEY_SLOT_BYTES;
    private static final int PREALLOCATE_BUFFER_BYTES = 64 * 1024;
    private static final ValueLayout.OfShort SHORT =
            ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfInt INT =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfLong LONG =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

    enum KeyField {
        FIRST_MIN,
        LAST_MIN,
        ZONE_MAX,
        FIRST_SAMPLE_PREFIX,
        FIRST_SAMPLE_PAGE_MAX,
        ROLLING_SAMPLE_PREFIX
    }

    record Summary(long pages, long entries, long framedBytes, long firstFrameOffset,
                   long endFrameOffset, int verifiedSamples, boolean sampleMismatch,
                   byte[] firstMin, byte[] lastMin, byte[] zoneMax,
                   byte[] firstSamplePrefix, byte[] firstSamplePageMax) {
    }

    record Snapshot(long operations, long bytes, long nanos) {
    }

    /** One merge-local aggregate shared by the writer, verifier reader, and delete step. */
    static final class Stats {
        private final SortMetrics metrics;
        private final LongAdder operations = new LongAdder();
        private final LongAdder bytes = new LongAdder();
        private final LongAdder nanos = new LongAdder();

        Stats(SortMetrics metrics) {
            this.metrics = metrics;
        }

        void record(long operationCount, long transferredBytes, long serviceNanos) {
            long safeOperations = Math.max(0L, operationCount);
            long safeBytes = Math.max(0L, transferredBytes);
            long safeNanos = Math.max(0L, serviceNanos);
            operations.add(safeOperations);
            bytes.add(safeBytes);
            nanos.add(safeNanos);
            metrics.recordProofSpool(safeOperations, safeBytes, safeNanos);
        }

        Snapshot snapshot() {
            return new Snapshot(operations.sum(), bytes.sum(), nanos.sum());
        }
    }

    private PageRunProofSpool() {
    }

    static long logicalBytes(int slots) {
        return Math.multiplyExact((long) slots, SLOT_BYTES);
    }

    static int slotBytes() {
        return SLOT_BYTES;
    }

    /**
     * Concurrent fixed-slot writer. The file mapping is the bounded backing store for inactive
     * segment/range keys: range workers update disjoint absolute slots without a positional
     * read/write syscall on every page-source switch. The shared arena permits concurrent access;
     * every operation uses an absolute long offset, so no mutable buffer position is shared.
     */
    static final class Writer implements AutoCloseable {
        private final Path path;
        private final FileChannel channel;
        private final Arena arena;
        private final MemorySegment storage;
        private final Stats stats;
        private final LongAdder writeCombineNanos = new LongAdder();
        private final AtomicBoolean closed = new AtomicBoolean();

        Writer(Path path, int slots, Stats stats) throws IOException {
            if (slots < 0) {
                throw new IllegalArgumentException("proof spool slots must not be negative");
            }
            this.path = path;
            this.stats = stats;
            long started = System.nanoTime();
            FileChannel opened = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            Arena mappingArena = Arena.ofShared();
            long bytes = logicalBytes(slots);
            try {
                preallocate(opened, bytes);
                this.storage = bytes == 0
                        ? MemorySegment.ofArray(new byte[0])
                        : opened.map(FileChannel.MapMode.READ_WRITE, 0, bytes, mappingArena);
            } catch (IOException | RuntimeException e) {
                mappingArena.close();
                try {
                    opened.close();
                } catch (IOException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
                try {
                    Files.deleteIfExists(path);
                } catch (IOException deleteFailure) {
                    e.addSuppressed(deleteFailure);
                }
                throw e;
            }
            this.channel = opened;
            this.arena = mappingArena;
            stats.record(1, bytes, System.nanoTime() - started);
        }

        void markOpen(int segment) {
            long started = System.nanoTime();
            storage.set(INT, slotOffset(segment), OPEN);
            writeCombineNanos.add(System.nanoTime() - started);
        }

        void writeKey(int segment, KeyField field, byte[] key) throws IOException {
            writeKey(segment, field, key, key.length);
        }

        void writeKey(int segment, KeyField field, byte[] key, int length) throws IOException {
            if (length < 0 || length > ByteMidpoint.MAX_KEY_LEN || length > key.length) {
                throw new IOException("page-run proof key exceeds the S3 key limit: " + length);
            }
            long started = System.nanoTime();
            long offset = keyOffset(segment, field);
            storage.set(SHORT, offset, (short) length);
            MemorySegment.copy(key, 0, storage, ValueLayout.JAVA_BYTE,
                    offset + Short.BYTES, length);
            writeCombineNanos.add(System.nanoTime() - started);
        }

        int readKey(int segment, KeyField field, byte[] target) throws IOException {
            long started = System.nanoTime();
            long offset = keyOffset(segment, field);
            int length = storage.get(SHORT, offset) & 0xffff;
            if (length > ByteMidpoint.MAX_KEY_LEN || length > target.length) {
                throw new IOException("page-run proof spool key length out of bounds in " + path);
            }
            MemorySegment.copy(storage, ValueLayout.JAVA_BYTE, offset + Short.BYTES,
                    target, 0, length);
            writeCombineNanos.add(System.nanoTime() - started);
            return length;
        }

        void finish(int segment, long pages, long entries, long framedBytes,
                    long firstFrameOffset, long endFrameOffset, int verifiedSamples,
                    boolean sampleMismatch) {
            long started = System.nanoTime();
            long offset = slotOffset(segment);
            storage.set(INT, offset + Integer.BYTES, sampleMismatch ? 1 : 0);
            storage.set(LONG, offset + 8, pages);
            storage.set(LONG, offset + 16, entries);
            storage.set(LONG, offset + 24, framedBytes);
            storage.set(LONG, offset + 32, firstFrameOffset);
            storage.set(LONG, offset + 40, endFrameOffset);
            storage.set(INT, offset + 48, verifiedSamples);
            storage.set(INT, offset + 52, 0);
            // Commit state last. Future completion supplies the happens-before edge before the
            // coordinator maps the same file read-only.
            storage.set(INT, offset, FINISHED);
            writeCombineNanos.add(System.nanoTime() - started);
            stats.record(1, SLOT_BYTES, 0);
        }

        @Override
        public void close() throws IOException {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            long started = System.nanoTime();
            arena.close();
            channel.close();
            stats.record(1, 0,
                    writeCombineNanos.sum() + System.nanoTime() - started);
        }
    }

    /** Sequential coordinator reader: one complete fixed slot per logical read operation. */
    static final class Reader implements AutoCloseable {
        private final Path path;
        private final FileChannel channel;
        private final Arena arena;
        private final MemorySegment storage;
        private final Stats stats;
        private final AtomicBoolean closed = new AtomicBoolean();

        Reader(Path path, Stats stats) throws IOException {
            this.path = path;
            this.stats = stats;
            // The coordinator polls cancellation immediately after its injected/open seam. Preserve
            // the pre-existing contract that merely constructing the read-only proof view does not
            // turn an already-latched interrupt into ClosedByInterruptException before that typed
            // poll can run.
            boolean interrupted = Thread.interrupted();
            long started = System.nanoTime();
            try {
                FileChannel opened = FileChannel.open(path, StandardOpenOption.READ);
                Arena mappingArena = Arena.ofConfined();
                try {
                    long bytes = opened.size();
                    this.storage = bytes == 0
                            ? MemorySegment.ofArray(new byte[0])
                            : opened.map(FileChannel.MapMode.READ_ONLY, 0, bytes, mappingArena);
                } catch (IOException | RuntimeException e) {
                    mappingArena.close();
                    try {
                        opened.close();
                    } catch (IOException closeFailure) {
                        e.addSuppressed(closeFailure);
                    }
                    throw e;
                }
                this.channel = opened;
                this.arena = mappingArena;
                stats.record(1, 0, System.nanoTime() - started);
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        Reader(Path path) throws IOException {
            this(path, new Stats(SortMetrics.NO_OP));
        }

        Summary read(int segment, boolean hasPages, boolean hasSamples) throws IOException {
            long started = System.nanoTime();
            long offset = slotOffset(segment);
            int state = storage.get(INT, offset);
            if (state != FINISHED) {
                throw new IOException("page-run proof spool has incomplete segment summary "
                        + segment + " in " + path);
            }
            boolean mismatch = storage.get(INT, offset + Integer.BYTES) != 0;
            long pages = storage.get(LONG, offset + 8);
            long entries = storage.get(LONG, offset + 16);
            long framedBytes = storage.get(LONG, offset + 24);
            long firstFrameOffset = storage.get(LONG, offset + 32);
            long endFrameOffset = storage.get(LONG, offset + 40);
            int verifiedSamples = storage.get(INT, offset + 48);
            byte[] firstMin = hasPages ? readKey(segment, KeyField.FIRST_MIN) : null;
            byte[] lastMin = hasPages ? readKey(segment, KeyField.LAST_MIN) : null;
            byte[] zoneMax = hasPages ? readKey(segment, KeyField.ZONE_MAX) : null;
            byte[] firstSamplePrefix = hasSamples
                    ? readKey(segment, KeyField.FIRST_SAMPLE_PREFIX) : null;
            byte[] firstSamplePageMax = hasSamples
                    ? readKey(segment, KeyField.FIRST_SAMPLE_PAGE_MAX) : null;
            stats.record(1, SLOT_BYTES, System.nanoTime() - started);
            return new Summary(pages, entries, framedBytes, firstFrameOffset, endFrameOffset,
                    verifiedSamples, mismatch, firstMin, lastMin, zoneMax,
                    firstSamplePrefix, firstSamplePageMax);
        }

        private byte[] readKey(int segment, KeyField field) throws IOException {
            long offset = keyOffset(segment, field);
            int size = storage.get(SHORT, offset) & 0xffff;
            if (size > ByteMidpoint.MAX_KEY_LEN) {
                throw new IOException("page-run proof spool key length out of bounds in " + path);
            }
            byte[] key = new byte[size];
            MemorySegment.copy(storage, ValueLayout.JAVA_BYTE, offset + Short.BYTES,
                    key, 0, size);
            return key;
        }

        @Override
        public void close() throws IOException {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            long started = System.nanoTime();
            arena.close();
            channel.close();
            stats.record(1, 0, System.nanoTime() - started);
        }
    }

    static void delete(Path path, Stats stats) throws IOException {
        long started = System.nanoTime();
        Files.deleteIfExists(path);
        stats.record(1, 0, System.nanoTime() - started);
    }

    /**
     * Materialize the complete fixed-slot extent before mapping it writable. Besides producing
     * sequential rather than page-switch-shaped I/O, this makes ENOSPC an ordinary constructor
     * {@link IOException}; a first-touch mapped write must never discover missing backing space as
     * a process-level SIGBUS.
     */
    private static void preallocate(FileChannel channel, long bytes) throws IOException {
        if (bytes == 0) {
            return;
        }
        ByteBuffer zeros = ByteBuffer.allocateDirect(
                Math.toIntExact(Math.min(PREALLOCATE_BUFFER_BYTES, bytes)));
        long position = 0;
        while (position < bytes) {
            zeros.clear();
            zeros.limit(Math.toIntExact(Math.min(zeros.capacity(), bytes - position)));
            while (zeros.hasRemaining()) {
                position += channel.write(zeros, position);
            }
        }
        channel.force(false);
    }

    private static long slotOffset(int segment) {
        return Math.multiplyExact((long) segment, SLOT_BYTES);
    }

    private static long keyOffset(int segment, KeyField field) {
        return Math.addExact(slotOffset(segment),
                FIXED_BYTES + (long) field.ordinal() * KEY_SLOT_BYTES);
    }
}
