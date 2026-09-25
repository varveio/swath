/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import com.sun.management.ThreadMXBean;
import java.io.ByteArrayInputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/** Isolated parser ceiling for a real captured 1k response body; no HTTP/server work is included. */
public final class ReplayClientParseBench {
    private ReplayClientParseBench() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException("usage: ReplayClientParseBench s3|gcs|azure BODY THREADS SECONDS EXPECTED_ROWS");
        }
        String protocol = args[0];
        if (!List.of("s3", "gcs", "azure").contains(protocol))
            throw new IllegalArgumentException("protocol");
        byte[] body = Files.readAllBytes(Path.of(args[1]));
        int threads = Integer.parseInt(args[2]);
        int seconds = Integer.parseInt(args[3]);
        int expectedRows = Integer.parseInt(args[4]);
        if (body.length < 1 || threads < 1 || threads > 128 || seconds < 1 || seconds > 60
                || expectedRows < 1 || expectedRows > 5000) {
            throw new IllegalArgumentException("invalid parser benchmark settings");
        }
        byte[] expectedDigest = parse(protocol, body, expectedRows);
        ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!bean.isThreadCpuTimeSupported() || !bean.isThreadAllocatedMemorySupported())
            throw new IllegalStateException("thread CPU/allocation counters unavailable");
        bean.setThreadCpuTimeEnabled(true);
        bean.setThreadAllocatedMemoryEnabled(true);

        AtomicLong startNanos = new AtomicLong();
        AtomicLong deadlineNanos = new AtomicLong();
        CyclicBarrier start = new CyclicBarrier(threads, () -> {
            long now = System.nanoTime();
            startNanos.set(now);
            deadlineNanos.set(now + seconds * 1_000_000_000L);
        });
        List<Future<WorkerResult>> results = new ArrayList<>(threads);
        try (var workers = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < threads; i++) {
                results.add(workers.submit((Callable<WorkerResult>) () -> {
                    for (int warmup = 0; warmup < 100; warmup++) {
                        if (!Arrays.equals(expectedDigest, parse(protocol, body, expectedRows)))
                            throw new IllegalStateException("warmup response digest changed");
                    }
                    long tid = Thread.currentThread().threadId();
                    start.await();
                    long cpuBefore = bean.getThreadCpuTime(tid);
                    long allocatedBefore = bean.getThreadAllocatedBytes(tid);
                    long pages = 0;
                    while (System.nanoTime() < deadlineNanos.get()) {
                        if (!Arrays.equals(expectedDigest, parse(protocol, body, expectedRows)))
                            throw new IllegalStateException("response digest changed");
                        pages++;
                    }
                    return new WorkerResult(pages, bean.getThreadCpuTime(tid) - cpuBefore,
                            bean.getThreadAllocatedBytes(tid) - allocatedBefore, System.nanoTime());
                }));
            }
            long pages = 0, cpuNanos = 0, allocatedBytes = 0, ended = 0, maxThreadCpu = 0;
            for (Future<WorkerResult> future : results) {
                WorkerResult result = future.get();
                pages += result.pages();
                cpuNanos += result.cpuNanos();
                allocatedBytes += result.allocatedBytes();
                ended = Math.max(ended, result.endedNanos());
                maxThreadCpu = Math.max(maxThreadCpu, result.cpuNanos());
            }
            long elapsed = ended - startNanos.get();
            long objects = pages * expectedRows;
            if (objects <= 0 || elapsed <= 0) throw new IllegalStateException("no measured parse work");
            System.out.printf(Locale.ROOT,
                    "{\"kind\":\"parser_only\",\"protocol\":\"%s\",\"body_bytes\":%d,"
                            + "\"page_objects\":%d,\"threads\":%d,\"pages\":%d,\"objects\":%d,"
                            + "\"elapsed_ns\":%d,\"objects_per_s\":%.3f,"
                            + "\"thread_cpu_ns_per_object\":%.3f,\"thread_alloc_bytes_per_object\":%.3f,"
                            + "\"max_thread_utilization\":%.6f}%n",
                    protocol, body.length, expectedRows, threads, pages, objects, elapsed,
                    objects * 1e9 / elapsed, (double) cpuNanos / objects,
                    (double) allocatedBytes / objects, (double) maxThreadCpu / elapsed);
        }
    }

    private static byte[] parse(String protocol, byte[] body, int expectedRows) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        ByteArrayInputStream stream = new ByteArrayInputStream(body);
        ReplayHttpBench.Page page = protocol.equals("gcs")
                ? ReplayHttpBench.parseGcs(stream, digest, null, null)
                : ReplayHttpBench.parseXml(stream, protocol, digest, null, null);
        if (page.count() != expectedRows || page.owned() != expectedRows)
            throw new IllegalStateException("canned page row count changed");
        return digest.digest();
    }

    private record WorkerResult(long pages, long cpuNanos, long allocatedBytes, long endedNanos) { }
}
