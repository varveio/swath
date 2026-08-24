/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SortedRangeReaderTest {

    @Test
    void pooledReaderCanSwitchFromNoOwnerToOwnerProjection(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("part-00001.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(file, SortConfigs.base(), SortMode.OBJECTS, 1)) {
            for (int i = 0; i < 5_000; i++) {
                writer.write(object(String.format("key-%05d", i)));
            }
        }

        try (SortedRangeReader reader = new SortedRangeReader(file, 1)) {
            byte[] lower = KeyBytes.ofUtf8("key-02000").raw();
            assertThat(reader.range(0, lower, true, null, 10, false))
                    .hasSize(10)
                    .allSatisfy(row -> assertThat(row.ownerId()).isNull());

            assertThat(reader.range(0, lower, true, null, 10, true))
                    .hasSize(10)
                    .allSatisfy(row -> {
                        assertThat(row.ownerId()).isEqualTo("owner-id");
                        assertThat(row.ownerDisplayName()).isEqualTo("owner-display");
                    });
        }
    }

    @Test
    void poolContentionIsExcludedFromTheReaderLeaseDuration(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("part-00001.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(file, SortConfigs.base(), SortMode.OBJECTS, 1)) {
            for (int i = 0; i < 20; i++) {
                writer.write(object(String.format("key-%03d", i)));
            }
        }

        ThreadLocal<String> caller = new ThreadLocal<>();
        CountDownLatch firstAcquired = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch secondAcquired = new CountDownLatch(1);
        Map<String, Long> leaseNanos = new ConcurrentHashMap<>();
        AtomicLong nanoClock = new AtomicLong(100L);
        AtomicInteger secondClockReads = new AtomicInteger();

        try (SortedRangeReader reader = new SortedRangeReader(file, 1, () -> {
            if ("first".equals(caller.get())) {
                firstAcquired.countDown();
                await(releaseFirst);
            } else if ("second".equals(caller.get())) {
                secondAcquired.countDown();
            }
        }, elapsed -> leaseNanos.put(caller.get(), elapsed), () -> {
            if ("second".equals(caller.get())) {
                secondClockReads.incrementAndGet();
            }
            return nanoClock.get();
        })) {
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                try {
                    var first = executor.submit(() -> {
                        caller.set("first");
                        return reader.range(0, null, true, null, 1, false);
                    });
                    assertThat(firstAcquired.await(5, TimeUnit.SECONDS)).isTrue();

                    var second = executor.submit(() -> {
                        caller.set("second");
                        secondStarted.countDown();
                        return reader.range(0, null, true, null, 1, false);
                    });
                    assertThat(secondStarted.await(5, TimeUnit.SECONDS)).isTrue();

                    // The only pooled reader stays held through this bounded negative assertion. A
                    // pre-borrow timer would read 100 now; the production post-borrow timer reads
                    // only after the synthetic clock advances to 1,000 below.
                    assertThat(secondAcquired.await(1, TimeUnit.SECONDS)).isFalse();
                    assertThat(secondClockReads).hasValue(0);
                    nanoClock.set(1_000L);
                    releaseFirst.countDown();

                    assertThat(first.get(5, TimeUnit.SECONDS)).hasSize(1);
                    assertThat(second.get(5, TimeUnit.SECONDS)).hasSize(1);
                } finally {
                    // Must precede executor.close(): a failed assertion must not leave its task
                    // parked on the latch while close waits for that same task to terminate.
                    releaseFirst.countDown();
                }
            }
        }

        assertThat(leaseNanos).containsKeys("first", "second");
        assertThat(leaseNanos.get("second"))
                .as("the second sample starts after the synthetic pool-wait interval")
                .isZero();
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted test reader", e);
        }
    }

    private static ObjectEntry object(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 11L, 0L, "etag-" + key, "STANDARD", null, true,
                "owner-id", "owner-display", "CRC32", "FULL_OBJECT");
    }
}
