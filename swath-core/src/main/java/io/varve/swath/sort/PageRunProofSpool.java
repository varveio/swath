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
import java.nio.channels.ClosedByInterruptException;
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

    record Snapshot(long logicalExtentBytes,
                    long preallocationOperations,
                    long preallocationAttemptedBytes,
                    long mappedOperations,
                    long mappedBytes,
                    long serviceNanos) {
    }

    /** One merge-local aggregate shared by the writer, verifier reader, and delete step. */
    static final class Stats {
        private final SortMetrics metrics;
        private final LongAdder logicalExtentBytes = new LongAdder();
        private final LongAdder preallocationOperations = new LongAdder();
        private final LongAdder preallocationAttemptedBytes = new LongAdder();
        private final LongAdder mappedOperations = new LongAdder();
        private final LongAdder mappedBytes = new LongAdder();
        private final LongAdder serviceNanos = new LongAdder();
        private final AtomicBoolean allocationFailureRecorded = new AtomicBoolean();
        private long publishedLogicalExtentBytes;
        private long publishedPreallocationOperations;
        private long publishedPreallocationAttemptedBytes;
        private long publishedMappedOperations;
        private long publishedMappedBytes;
        private long publishedServiceNanos;

        Stats(SortMetrics metrics) {
            this.metrics = metrics;
        }

        void recordLogicalExtent(long bytes) {
            logicalExtentBytes.add(Math.max(0L, bytes));
        }

        void recordPreallocation(long operations, long attemptedBytes, long nanos) {
            preallocationOperations.add(Math.max(0L, operations));
            preallocationAttemptedBytes.add(Math.max(0L, attemptedBytes));
            serviceNanos.add(Math.max(0L, nanos));
        }

        void recordMapped(long operations, long bytes, long nanos) {
            mappedOperations.add(Math.max(0L, operations));
            mappedBytes.add(Math.max(0L, bytes));
            serviceNanos.add(Math.max(0L, nanos));
        }

        void recordService(long nanos) {
            serviceNanos.add(Math.max(0L, nanos));
        }

        void markProgress() {
            metrics.markProgress();
        }

        void recordAllocationFailure() {
            if (allocationFailureRecorded.compareAndSet(false, true)) {
                metrics.recordStealReason("SORT", "proof_spool_allocation_failed");
            }
        }

        synchronized void publish() {
            Snapshot current = snapshot();
            long extentDelta = current.logicalExtentBytes() - publishedLogicalExtentBytes;
            long preallocationOperationsDelta = current.preallocationOperations()
                    - publishedPreallocationOperations;
            long preallocationBytesDelta = current.preallocationAttemptedBytes()
                    - publishedPreallocationAttemptedBytes;
            long mappedOperationsDelta = current.mappedOperations() - publishedMappedOperations;
            long mappedBytesDelta = current.mappedBytes() - publishedMappedBytes;
            long serviceNanosDelta = current.serviceNanos() - publishedServiceNanos;
            if (extentDelta != 0 || preallocationOperationsDelta != 0
                    || preallocationBytesDelta != 0 || mappedOperationsDelta != 0
                    || mappedBytesDelta != 0 || serviceNanosDelta != 0) {
                metrics.recordProofSpool(extentDelta, preallocationOperationsDelta,
                        preallocationBytesDelta, mappedOperationsDelta, mappedBytesDelta,
                        serviceNanosDelta);
                publishedLogicalExtentBytes = current.logicalExtentBytes();
                publishedPreallocationOperations = current.preallocationOperations();
                publishedPreallocationAttemptedBytes = current.preallocationAttemptedBytes();
                publishedMappedOperations = current.mappedOperations();
                publishedMappedBytes = current.mappedBytes();
                publishedServiceNanos = current.serviceNanos();
            }
        }

        Snapshot snapshot() {
            return new Snapshot(logicalExtentBytes.sum(), preallocationOperations.sum(),
                    preallocationAttemptedBytes.sum(), mappedOperations.sum(), mappedBytes.sum(),
                    serviceNanos.sum());
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

    @FunctionalInterface
    interface Preallocator {
        void preallocate(FileChannel channel, long bytes, Stats stats) throws IOException;
    }

    @FunctionalInterface
    interface Mapper {
        MemorySegment map(FileChannel channel, FileChannel.MapMode mode,
                          long bytes, Arena arena) throws IOException;
    }

    interface AllocationIo {
        int write(ByteBuffer source, long position) throws IOException;

        void force() throws IOException;
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
        private final AtomicBoolean closed = new AtomicBoolean();

        Writer(Path path, int slots, Stats stats) throws IOException {
            this(path, slots, stats, PageRunProofSpool::preallocate,
                    (channel, mode, bytes, arena) -> channel.map(mode, 0, bytes, arena));
        }

        Writer(Path path, int slots, Stats stats, Preallocator preallocator, Mapper mapper)
                throws IOException {
            if (slots < 0) {
                throw new IllegalArgumentException("proof spool slots must not be negative");
            }
            this.path = path;
            this.stats = stats;
            long bytes = logicalBytes(slots);
            stats.recordLogicalExtent(bytes);
            FileChannel opened = null;
            Arena mappingArena = null;
            MemorySegment mapped = null;
            try {
                long openStarted = System.nanoTime();
                try {
                    opened = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.READ, StandardOpenOption.WRITE);
                } finally {
                    stats.recordService(System.nanoTime() - openStarted);
                }
                long arenaStarted = System.nanoTime();
                try {
                    mappingArena = Arena.ofShared();
                } finally {
                    stats.recordService(System.nanoTime() - arenaStarted);
                }
                preallocator.preallocate(opened, bytes, stats);
                long mapStarted = System.nanoTime();
                try {
                    mapped = bytes == 0
                            ? MemorySegment.ofArray(new byte[0])
                            : mapper.map(opened, FileChannel.MapMode.READ_WRITE, bytes, mappingArena);
                } finally {
                    stats.recordService(System.nanoTime() - mapStarted);
                }
            } catch (MergeCancellation.Cancelled cancelled) {
                stats.publish();
                cleanFailedAllocation(path, opened, mappingArena, stats, cancelled);
                stats.publish();
                throw cancelled;
            } catch (IOException | RuntimeException failure) {
                if (isInterrupted(failure)) {
                    Thread.currentThread().interrupt();
                    stats.publish();
                    cleanFailedAllocation(path, opened, mappingArena, stats, failure);
                    stats.publish();
                    MergeCancellation.Cancelled cancelled = new MergeCancellation.Cancelled();
                    cancelled.initCause(failure);
                    throw cancelled;
                }
                stats.recordAllocationFailure();
                stats.publish();
                cleanFailedAllocation(path, opened, mappingArena, stats, failure);
                stats.publish();
                if (failure instanceof ProofSpoolAllocationException classified) {
                    throw classified;
                }
                throw new ProofSpoolAllocationException(path, failure);
            }
            this.channel = opened;
            this.arena = mappingArena;
            this.storage = mapped;
        }

        void markOpen(int segment) {
            long started = System.nanoTime();
            storage.set(INT, slotOffset(segment), OPEN);
            stats.recordMapped(1, Integer.BYTES, System.nanoTime() - started);
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
            stats.recordMapped(1, Short.BYTES + length, System.nanoTime() - started);
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
            stats.recordMapped(1, Short.BYTES + length, System.nanoTime() - started);
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
            stats.recordMapped(1, FIXED_BYTES, System.nanoTime() - started);
        }

        @Override
        public void close() throws IOException {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            long started = System.nanoTime();
            try {
                arena.close();
            } finally {
                try {
                    channel.close();
                } finally {
                    stats.recordService(System.nanoTime() - started);
                    stats.publish();
                }
            }
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
            try {
                FileChannel opened;
                long openStarted = System.nanoTime();
                try {
                    opened = FileChannel.open(path, StandardOpenOption.READ);
                } finally {
                    stats.recordService(System.nanoTime() - openStarted);
                }
                Arena mappingArena;
                long arenaStarted = System.nanoTime();
                try {
                    mappingArena = Arena.ofConfined();
                } finally {
                    stats.recordService(System.nanoTime() - arenaStarted);
                }
                try {
                    long bytes = opened.size();
                    long mapStarted = System.nanoTime();
                    try {
                        this.storage = bytes == 0
                                ? MemorySegment.ofArray(new byte[0])
                                : opened.map(FileChannel.MapMode.READ_ONLY, 0, bytes, mappingArena);
                    } finally {
                        stats.recordService(System.nanoTime() - mapStarted);
                    }
                } catch (IOException | RuntimeException e) {
                    long cleanupStarted = System.nanoTime();
                    mappingArena.close();
                    try {
                        opened.close();
                    } catch (IOException closeFailure) {
                        e.addSuppressed(closeFailure);
                    } finally {
                        stats.recordService(System.nanoTime() - cleanupStarted);
                        stats.publish();
                    }
                    throw e;
                }
                this.channel = opened;
                this.arena = mappingArena;
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
                stats.recordMapped(1, Integer.BYTES, System.nanoTime() - started);
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
            int reserved = storage.get(INT, offset + 52);
            stats.recordMapped(1, FIXED_BYTES, System.nanoTime() - started);
            if (reserved != 0) {
                throw new IOException("page-run proof spool reserved field is non-zero in " + path);
            }
            byte[] firstMin = hasPages ? readKey(segment, KeyField.FIRST_MIN) : null;
            byte[] lastMin = hasPages ? readKey(segment, KeyField.LAST_MIN) : null;
            byte[] zoneMax = hasPages ? readKey(segment, KeyField.ZONE_MAX) : null;
            byte[] firstSamplePrefix = hasSamples
                    ? readKey(segment, KeyField.FIRST_SAMPLE_PREFIX) : null;
            byte[] firstSamplePageMax = hasSamples
                    ? readKey(segment, KeyField.FIRST_SAMPLE_PAGE_MAX) : null;
            return new Summary(pages, entries, framedBytes, firstFrameOffset, endFrameOffset,
                    verifiedSamples, mismatch, firstMin, lastMin, zoneMax,
                    firstSamplePrefix, firstSamplePageMax);
        }

        private byte[] readKey(int segment, KeyField field) throws IOException {
            long started = System.nanoTime();
            long offset = keyOffset(segment, field);
            int size = storage.get(SHORT, offset) & 0xffff;
            if (size > ByteMidpoint.MAX_KEY_LEN) {
                stats.recordMapped(1, Short.BYTES, System.nanoTime() - started);
                throw new IOException("page-run proof spool key length out of bounds in " + path);
            }
            byte[] key = new byte[size];
            MemorySegment.copy(storage, ValueLayout.JAVA_BYTE, offset + Short.BYTES,
                    key, 0, size);
            stats.recordMapped(1, Short.BYTES + size, System.nanoTime() - started);
            return key;
        }

        @Override
        public void close() throws IOException {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            long started = System.nanoTime();
            try {
                arena.close();
            } finally {
                try {
                    channel.close();
                } finally {
                    stats.recordService(System.nanoTime() - started);
                    stats.publish();
                }
            }
        }
    }

    static void delete(Path path, Stats stats) throws IOException {
        long started = System.nanoTime();
        try {
            Files.deleteIfExists(path);
        } finally {
            stats.recordService(System.nanoTime() - started);
            stats.publish();
        }
    }

    /**
     * Materialize the complete fixed-slot extent before mapping it writable. Besides producing
     * sequential rather than page-switch-shaped I/O, this makes ENOSPC an ordinary constructor
     * {@link IOException}; a first-touch mapped write must never discover missing backing space as
     * a process-level SIGBUS.
     */
    static void preallocate(FileChannel channel, long bytes, Stats stats) throws IOException {
        preallocate(new AllocationIo() {
            @Override
            public int write(ByteBuffer source, long position) throws IOException {
                return channel.write(source, position);
            }

            @Override
            public void force() throws IOException {
                channel.force(false);
            }
        }, bytes, stats);
    }

    static void preallocate(AllocationIo io, long bytes, Stats stats) throws IOException {
        if (bytes == 0) {
            return;
        }
        long bufferStarted = System.nanoTime();
        ByteBuffer zeros = ByteBuffer.allocateDirect(
                Math.toIntExact(Math.min(PREALLOCATE_BUFFER_BYTES, bytes)));
        stats.recordService(System.nanoTime() - bufferStarted);
        long position = 0;
        while (position < bytes) {
            zeros.clear();
            zeros.limit(Math.toIntExact(Math.min(zeros.capacity(), bytes - position)));
            while (zeros.hasRemaining()) {
                MergeCancellation.check();
                int attemptedBytes = zeros.remaining();
                long writeStarted = System.nanoTime();
                int written;
                try {
                    written = io.write(zeros, position);
                } catch (IOException | RuntimeException failure) {
                    stats.recordPreallocation(
                            1, attemptedBytes, System.nanoTime() - writeStarted);
                    throw failure;
                }
                stats.recordPreallocation(1, attemptedBytes, System.nanoTime() - writeStarted);
                if (written <= 0) {
                    throw new IOException("proof spool preallocation made no forward progress");
                }
                position += written;
            }
            stats.markProgress();
        }
        MergeCancellation.check();
        long forceStarted = System.nanoTime();
        try {
            io.force();
        } catch (IOException | RuntimeException failure) {
            stats.recordPreallocation(1, 0, System.nanoTime() - forceStarted);
            throw failure;
        }
        stats.recordPreallocation(1, 0, System.nanoTime() - forceStarted);
        stats.markProgress();
    }

    private static void cleanFailedAllocation(Path path, FileChannel channel, Arena arena,
                                              Stats stats, Throwable primary) {
        long started = System.nanoTime();
        if (arena != null) {
            try {
                arena.close();
            } catch (RuntimeException closeFailure) {
                primary.addSuppressed(closeFailure);
            }
        }
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                primary.addSuppressed(closeFailure);
            }
        }
        try {
            Files.deleteIfExists(path);
        } catch (IOException deleteFailure) {
            primary.addSuppressed(deleteFailure);
        } finally {
            stats.recordService(System.nanoTime() - started);
        }
    }

    private static boolean isInterrupted(Throwable failure) {
        if (Thread.currentThread().isInterrupted()) {
            return true;
        }
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof ClosedByInterruptException) {
                return true;
            }
        }
        return false;
    }

    /** Package-private corruption-fixture seam for the fixed reserved-zero field. */
    static long reservedFieldOffset(int segment) {
        return slotOffset(segment) + 52;
    }

    private static long slotOffset(int segment) {
        return Math.multiplyExact((long) segment, SLOT_BYTES);
    }

    private static long keyOffset(int segment, KeyField field) {
        return Math.addExact(slotOffset(segment),
                FIXED_BYTES + (long) field.ordinal() * KEY_SLOT_BYTES);
    }
}
