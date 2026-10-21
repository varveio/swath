/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import com.sun.management.ThreadMXBean;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.gcs.GcsJson;
import io.varve.swath.replay.protocol.gcs.GcsListRequest;
import io.varve.swath.replay.protocol.gcs.GcsPage;
import io.varve.swath.replay.server.BudgetedOutput;
import java.lang.management.ManagementFactory;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/** Isolates GCS JSON encoder allocation/CPU on 1,000 real fixture names. */
public final class GcsJsonRenderBench {
    private static final int PAGE_SIZE = 1000;
    private static final int INITIAL_BYTES = 1 << 20;

    private GcsJsonRenderBench() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("usage: GcsJsonRenderBench FIXTURE_GLOB ITERATIONS");
        int iterations = Integer.parseInt(args[1]);
        if (iterations < 1 || iterations > 1000) throw new IllegalArgumentException("iterations out of range");
        List<byte[]> keys = keys(args[0]);
        if (keys.size() != PAGE_SIZE) throw new IllegalArgumentException("fixture needs 1,000 keys");
        measure(keys, iterations, "same-second");
        measure(keys, iterations, "varied-second");
        measure(keys, iterations, "varied-day");
    }

    private static List<byte[]> keys(String fixture) throws Exception {
        String escaped = fixture.replace("'", "''");
        Properties properties = new Properties();
        properties.setProperty("jdbc_stream_results", "true");
        try (var connection = DriverManager.getConnection("jdbc:duckdb:", properties);
             var statement = connection.createStatement()) {
            String type;
            try (var rows = statement.executeQuery(
                    "SELECT typeof(key) FROM read_parquet('" + escaped + "') LIMIT 1")) {
                type = rows.next() ? rows.getString(1) : "BLOB";
            }
            String expression = switch (type.toUpperCase(Locale.ROOT)) {
                case "BLOB" -> "key";
                case "VARCHAR" -> "encode(key)";
                default -> throw new IllegalStateException("unsupported key type " + type);
            };
            List<byte[]> keys = new ArrayList<>(PAGE_SIZE);
            try (var rows = statement.executeQuery("SELECT " + expression + " FROM read_parquet('"
                    + escaped + "') LIMIT " + PAGE_SIZE)) {
                while (rows.next()) keys.add(rows.getBytes(1));
            }
            return keys;
        }
    }

    private static void measure(List<byte[]> keys, int iterations, String mode) throws Exception {
        List<ListedObject> objects = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            long micros = 1_700_000_000_000_000L + switch (mode) {
                case "varied-second" -> i * 1_000_000L;
                case "varied-day" -> i * 86_400_000_000L;
                default -> 0L;
            };
            objects.add(new ListedObject(keys.get(i), 123, micros,
                    null, null, null, null, null, null));
        }
        GcsListRequest request = new GcsListRequest("bucket", null, null, null, null,
                PAGE_SIZE, null, true);
        GcsPage page = new GcsPage(objects, List.of(), null);
        for (int i = 0; i < 20; i++) {
            try (BudgetedOutput out = BudgetedOutput.standalone(INITIAL_BYTES)) {
                GcsJson.write(request, page, out);
            }
        }
        ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!bean.isThreadAllocatedMemorySupported()) throw new IllegalStateException("allocation counters unavailable");
        bean.setThreadAllocatedMemoryEnabled(true);
        if (!bean.isThreadCpuTimeEnabled()) bean.setThreadCpuTimeEnabled(true);
        long thread = Thread.currentThread().threadId();
        long allocated = 0;
        long cpu = 0;
        long bytes = 0;
        String wireSha256 = null;
        for (int i = 0; i < iterations; i++) {
            try (BudgetedOutput out = BudgetedOutput.standalone(INITIAL_BYTES)) {
                long beforeBytes = bean.getThreadAllocatedBytes(thread);
                long beforeCpu = bean.getThreadCpuTime(thread);
                GcsJson.write(request, page, out);
                cpu += bean.getThreadCpuTime(thread) - beforeCpu;
                allocated += bean.getThreadAllocatedBytes(thread) - beforeBytes;
                bytes += out.size();
                if (wireSha256 == null) {
                    wireSha256 = RenderBenchWire.sha256(out);
                }
            }
        }
        long count = (long) iterations * keys.size();
        System.out.printf(Locale.ROOT,
                "{\"mode\":\"%s\",\"objects\":%d,\"encoded_bytes\":%d,\"wire_sha256\":\"%s\","
                + "\"allocated_bytes\":%d,\"allocated_bytes_per_object\":%.3f,"
                + "\"cpu_ns\":%d,\"cpu_ns_per_object\":%.3f}%n",
                mode, count, bytes, wireSha256,
                allocated, (double) allocated / count, cpu, (double) cpu / count);
    }
}
