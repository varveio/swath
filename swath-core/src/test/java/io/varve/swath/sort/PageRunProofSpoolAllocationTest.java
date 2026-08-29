/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PageRunProofSpoolAllocationTest {

    private static final long CHUNK = 64L * 1024;

    @Test
    void preallocationPollsAndMarksProgressAtBoundedByteCadence(@TempDir Path root)
            throws IOException {
        Path path = root.resolve("allocation.tmp");
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        PageRunProofSpool.Stats stats = new PageRunProofSpool.Stats(metrics);
        long bytes = 2 * CHUNK + 17;

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            PageRunProofSpool.preallocate(channel, bytes, stats);
        }
        stats.publish();

        assertThat(metrics.proofSpoolPreallocationOperations.sum()).isEqualTo(4); // 3 writes + force
        assertThat(metrics.proofSpoolPreallocationAttemptedBytes.sum()).isEqualTo(bytes);
        assertThat(metrics.progress.sum()).isEqualTo(4); // each chunk plus force
        assertThat(Files.size(path)).isEqualTo(bytes);
    }

    @Test
    void preallocationCancellationPreservesInterruptAndAttemptsNoIo(@TempDir Path root)
            throws IOException {
        Path path = root.resolve("cancelled.tmp");
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        PageRunProofSpool.Stats stats = new PageRunProofSpool.Stats(metrics);

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            try {
                Thread.currentThread().interrupt();
                assertThatThrownBy(() -> PageRunProofSpool.preallocate(channel, CHUNK, stats))
                        .isInstanceOf(MergeCancellation.Cancelled.class);
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
            } finally {
                Thread.interrupted();
            }
        }
        stats.publish();

        assertThat(metrics.proofSpoolPreallocationOperations.sum()).isZero();
        assertThat(metrics.proofSpoolPreallocationAttemptedBytes.sum()).isZero();
        assertThat(metrics.count("SORT.proof_spool_allocation_failed")).isZero();
    }

    @Test
    void enospcPublishesAttemptedWorkAndStableClassificationBeforeCleanup(@TempDir Path root) {
        Path path = root.resolve("enospc.tmp");
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        PageRunProofSpool.Stats stats = new PageRunProofSpool.Stats(metrics);
        AtomicInteger writes = new AtomicInteger();
        PageRunProofSpool.AllocationIo allocation = new PageRunProofSpool.AllocationIo() {
            @Override
            public int write(ByteBuffer source, long position) throws IOException {
                if (writes.incrementAndGet() == 2) {
                    throw new IOException("ENOSPC injected");
                }
                int written = source.remaining();
                source.position(source.limit());
                return written;
            }

            @Override
            public void force() {
            }
        };
        PageRunProofSpool.Preallocator preallocator =
                (channel, bytes, observed) -> PageRunProofSpool.preallocate(
                        allocation, bytes, observed);
        PageRunProofSpool.Mapper unreachable =
                (channel, mode, bytes, arena) -> MemorySegment.NULL;
        int slots = 11; // 68,332 bytes: two preallocation attempts at the 64 KiB cadence

        assertThatThrownBy(() -> new PageRunProofSpool.Writer(
                path, slots, stats, preallocator, unreachable))
                .isInstanceOf(ProofSpoolAllocationException.class)
                .extracting(error -> ((ProofSpoolAllocationException) error).errorClass())
                .isEqualTo(ProofSpoolAllocationException.ERROR_CLASS);

        long extent = PageRunProofSpool.logicalBytes(slots);
        assertThat(metrics.proofSpoolLogicalExtentBytes.sum()).isEqualTo(extent);
        assertThat(metrics.proofSpoolPreallocationOperations.sum()).isEqualTo(2);
        assertThat(metrics.proofSpoolPreallocationAttemptedBytes.sum()).isEqualTo(extent);
        assertThat(metrics.proofSpoolMappedOperations.sum()).isZero();
        assertThat(metrics.count("SORT.proof_spool_allocation_failed")).isEqualTo(1);
        assertThat(metrics.progress.sum()).isEqualTo(1);
        assertThat(path).doesNotExist();
    }

    @Test
    void mapFailureRetainsPhysicalAllocationMetricsAndCleans(@TempDir Path root) {
        Path path = root.resolve("map-failure.tmp");
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        PageRunProofSpool.Stats stats = new PageRunProofSpool.Stats(metrics);
        PageRunProofSpool.Mapper failing = (channel, mode, bytes, arena) -> {
            throw new IOException("map injected");
        };

        assertThatThrownBy(() -> new PageRunProofSpool.Writer(
                path, 1, stats, PageRunProofSpool::preallocate, failing))
                .isInstanceOf(ProofSpoolAllocationException.class)
                .hasRootCauseMessage("map injected");

        assertThat(metrics.proofSpoolLogicalExtentBytes.sum())
                .isEqualTo(PageRunProofSpool.slotBytes());
        assertThat(metrics.proofSpoolPreallocationOperations.sum()).isEqualTo(2); // write + force
        assertThat(metrics.proofSpoolPreallocationAttemptedBytes.sum())
                .isEqualTo(PageRunProofSpool.slotBytes());
        assertThat(metrics.count("SORT.proof_spool_allocation_failed")).isEqualTo(1);
        assertThat(path).doesNotExist();
    }

    @Test
    void interruptInsidePreallocationWriteIsCancellationWithAttemptMetrics(@TempDir Path root) {
        assertInterruptedAllocation(root.resolve("write-interrupt.tmp"), new PageRunProofSpool.AllocationIo() {
            @Override
            public int write(ByteBuffer source, long position) throws IOException {
                throw new ClosedByInterruptException();
            }

            @Override
            public void force() {
            }
        }, 1, PageRunProofSpool.slotBytes(), 0);
    }

    @Test
    void interruptInsidePreallocationForceIsCancellationWithCompletedWriteMetrics(
            @TempDir Path root) {
        assertInterruptedAllocation(root.resolve("force-interrupt.tmp"),
                new PageRunProofSpool.AllocationIo() {
                    @Override
                    public int write(ByteBuffer source, long position) {
                        int written = source.remaining();
                        source.position(source.limit());
                        return written;
                    }

                    @Override
                    public void force() throws IOException {
                        throw new ClosedByInterruptException();
                    }
                }, 2, PageRunProofSpool.slotBytes(), 1);
    }

    private static void assertInterruptedAllocation(Path path, PageRunProofSpool.AllocationIo io,
                                                    long operations, long attemptedBytes,
                                                    long progress) {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        PageRunProofSpool.Stats stats = new PageRunProofSpool.Stats(metrics);
        PageRunProofSpool.Preallocator preallocator =
                (channel, bytes, observed) -> PageRunProofSpool.preallocate(io, bytes, observed);
        try {
            assertThatThrownBy(() -> new PageRunProofSpool.Writer(path, 1, stats, preallocator,
                    (channel, mode, bytes, arena) -> MemorySegment.NULL))
                    .isInstanceOf(MergeCancellation.Cancelled.class)
                    .hasCauseInstanceOf(ClosedByInterruptException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
        assertThat(metrics.proofSpoolLogicalExtentBytes.sum())
                .isEqualTo(PageRunProofSpool.slotBytes());
        assertThat(metrics.proofSpoolPreallocationOperations.sum()).isEqualTo(operations);
        assertThat(metrics.proofSpoolPreallocationAttemptedBytes.sum()).isEqualTo(attemptedBytes);
        assertThat(metrics.progress.sum()).isEqualTo(progress);
        assertThat(metrics.count("SORT.proof_spool_allocation_failed")).isZero();
        assertThat(path).doesNotExist();
    }
}
