/* SPDX-License-Identifier: Apache-2.0 */
package io.varve.swath.spike.bench;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

/** Streamed deterministic object listings; bounded state independent of corpus length. */
public final class Corpus {
    private Corpus() {}
    public static Object[] row(long i) {
        String key = "objects/" + Long.toHexString(i / 1000) + "/" + Long.toHexString(i)
                + (i % 257 == 0 ? "/中é/" + "x".repeat(900) : "/" + "key".repeat((int)(i % 23)));
        boolean version = i % 3 != 0;
        return new Object[] {key, i * 104729, 1_700_000_000_000_000L + i * 1001,
                Long.toHexString(i * 0x123456789abcdefL), i % 5 == 0 ? "GLACIER" : "STANDARD",
                version ? "v" + (i % 10007) : null, version ? i % 2 == 0 : null, false,
                i % 7 == 0 ? null : "owner-" + i % 11, i % 7 == 0 ? null : "display 中",
                i % 11 == 0 ? "CRC32C,SHA256" : null, i % 11 == 0 ? "FULL_OBJECT" : null, "OBJECT"};
    }
    public static void equal(Object[] got, long i) {
        Object[] expected = row(i);
        if (!java.util.Arrays.equals(got, expected)) throw new AssertionError("row mismatch " + i + " got=" + java.util.Arrays.toString(got));
    }
    public static final class Heap implements AutoCloseable {
        private final AtomicLong peak = new AtomicLong();
        private final Thread sampler;
        public Heap() {
            sampler = Thread.ofPlatform().daemon().start(() -> {
                while (!Thread.interrupted()) {
                    peak.accumulateAndGet(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(), Math::max);
                    try { Thread.sleep(10); } catch (InterruptedException e) { break; }
                }
            });
        }
        public long peak() { return peak.get(); }
        public void close() { sampler.interrupt(); }
    }
    public static void report(String engine, String mode, long rows, long start, long bytes, Heap heap, long target) {
        System.out.printf(java.util.Locale.ROOT,
                "{\"engine\":\"%s\",\"mode\":\"%s\",\"rows\":%d,\"operation_seconds\":%.6f,\"file_bytes\":%d,\"sampled_peak_heap_bytes\":%d,\"row_group_target_bytes\":%d}%n",
                engine, mode, rows, (System.nanoTime()-start)/1e9, bytes, heap.peak(), target);
    }
}
