/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import com.sun.management.ThreadMXBean;
import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.protocol.S3Xml;
import io.varve.swath.replay.protocol.azure.AzureListRequest;
import io.varve.swath.replay.protocol.azure.AzureListResult;
import io.varve.swath.replay.protocol.azure.AzureXml;
import io.varve.swath.replay.protocol.gcs.GcsJson;
import io.varve.swath.replay.protocol.gcs.GcsListRequest;
import io.varve.swath.replay.protocol.gcs.GcsPage;
import io.varve.swath.replay.store.DuckDbListingStore;
import io.varve.swath.replay.store.Projection;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Fixed offline chunk-size panel; results are diagnostics until paired server gates run. */
public final class OutputChunkBench {
    private static final int WARMUP = 200;
    private static final int ITERATIONS = 1000;
    private static volatile long sink;

    private OutputChunkBench() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("usage: OutputChunkBench FIXTURE");
        Path fixture = Path.of(args[0]);
        ReplayMetrics metrics = new ReplayMetrics();
        List<ListedObject> rows;
        try (DuckDbListingStore store = new DuckDbListingStore(fixture, metrics, 1)) {
            rows = store.rows(null, true, null, 1000, Projection.KEYS_ONLY);
        } finally {
            metrics.registry().close();
        }
        if (rows.size() != 1000) throw new IllegalArgumentException("fixture must supply 1000 objects");
        long keyBytes = rows.stream().mapToLong(row -> row.key().length).sum();
        System.out.printf(Locale.ROOT,
                "{\"event\":\"INPUT\",\"fixture\":\"%s\",\"rows\":1000,\"key_bytes\":%d,"
                        + "\"warmup\":%d,\"iterations\":%d}%n",
                fixture, keyBytes, WARMUP, ITERATIONS);
        List<S3ResultEntry> s3Entries = rows.stream().map(S3ResultEntry.ObjectResult::new)
                .map(entry -> (S3ResultEntry) entry).toList();
        S3ListResult s3 = new S3ListResult(new S3ListRequest("bench", null, null, null, null,
                1000, true, false), s3Entries, false, null);
        GcsListRequest gcsRequest = new GcsListRequest("bench", null, null, null, null,
                1000, null, false);
        GcsPage gcs = new GcsPage(rows, List.of(), null);
        List<AzureListResult.Entry> azureEntries = new ArrayList<>(rows.size());
        for (ListedObject row : rows) azureEntries.add(new AzureListResult.Entry.Blob(row));
        AzureListRequest azureRequest = new AzureListRequest("replay", "bench", "2026-06-06",
                null, null, null, null, 1000, "1000", false, false, false, true, null);
        AzureListResult azure = new AzureListResult(azureRequest, azureEntries, null);
        ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        bean.setThreadAllocatedMemoryEnabled(true);
        bean.setThreadCpuTimeEnabled(true);
        long thread = Thread.currentThread().threadId();
        for (String protocol : List.of("s3", "gcs", "azure")) {
            for (int chunk : new int[] {64 * 1024, 128 * 1024, 256 * 1024}) {
                for (int i = 0; i < WARMUP; i++) render(protocol, chunk, s3, gcsRequest, gcs, azure);
                long allocBefore = bean.getThreadAllocatedBytes(thread);
                long cpuBefore = bean.getThreadCpuTime(thread);
                long wallBefore = System.nanoTime();
                long encoded = 0, allocatedCapacity = 0, views = 0;
                for (int i = 0; i < ITERATIONS; i++) {
                    Measurement result = render(protocol, chunk, s3, gcsRequest, gcs, azure);
                    encoded += result.encodedBytes();
                    allocatedCapacity += result.allocatedCapacity();
                    views += result.views();
                }
                long wall = System.nanoTime() - wallBefore;
                long cpu = bean.getThreadCpuTime(thread) - cpuBefore;
                long allocated = bean.getThreadAllocatedBytes(thread) - allocBefore;
                System.out.printf(Locale.ROOT,
                        "{\"protocol\":\"%s\",\"chunk_bytes\":%d,\"warmup\":%d,"
                                + "\"iterations\":%d,\"objects\":%d,\"encoded_bytes\":%d,"
                                + "\"allocated_chunk_capacity\":%d,\"views\":%d,"
                                + "\"thread_allocated_bytes\":%d,\"thread_cpu_ns\":%d,"
                                + "\"wall_ns\":%d,\"sink\":%d}%n",
                        protocol, chunk, WARMUP, ITERATIONS, 1000L * ITERATIONS,
                        encoded, allocatedCapacity, views, allocated, cpu, wall, sink);
            }
        }
    }

    private static Measurement render(String protocol, int chunk, S3ListResult s3,
                                      GcsListRequest gcsRequest, GcsPage gcs,
                                      AzureListResult azure) {
        ResponseByteBudget budget = new ResponseByteBudget(64L * 1024 * 1024);
        int initial = switch (protocol) {
            case "s3" -> 512 + 1000 * 320;
            case "gcs" -> 1024 + 1000 * 384;
            case "azure" -> 2048 + 1000 * 512;
            default -> throw new IllegalArgumentException(protocol);
        };
        try (BudgetedOutput output = new BudgetedOutput(budget, initial, 64 * 1024 * 1024, chunk)) {
            if (output.chunkBytes() != chunk) throw new IllegalStateException("chunk arm mismatch");
            OwnedBody body = switch (protocol) {
                case "s3" -> S3Xml.listBucketBody(s3, output);
                case "gcs" -> {
                    GcsJson.write(gcsRequest, gcs, output);
                    yield output.body();
                }
                case "azure" -> {
                    AzureXml.write(azure, "http://127.0.0.1:19090/replay/", output);
                    yield output.body();
                }
                default -> throw new IllegalArgumentException(protocol);
            };
            for (ByteBuffer view : body.views()) {
                if (view.hasRemaining()) sink += view.get(view.position()) & 0xff;
            }
            return new Measurement(body.length(), output.capacity(), body.views().size());
        }
    }

    private record Measurement(int encodedBytes, int allocatedCapacity, int views) { }
}
