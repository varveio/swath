/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import com.sun.management.ThreadMXBean;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.azure.AzureListRequest;
import io.varve.swath.replay.protocol.azure.AzureListResult;
import io.varve.swath.replay.protocol.azure.AzureXml;
import io.varve.swath.replay.server.BudgetedOutput;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/** Isolates Azure XML encoder allocation/CPU on 1,000 real fixture names. */
public final class AzureXmlRenderBench {
    private static final int PAGE_SIZE = 1000;
    private static final int INITIAL_BYTES = 1 << 20;

    private AzureXmlRenderBench() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: AzureXmlRenderBench FIXTURE_GLOB ITERATIONS");
        }
        int iterations = Integer.parseInt(args[1]);
        if (iterations < 1 || iterations > 1000) throw new IllegalArgumentException("iterations out of range");
        List<byte[]> keys = fixtureKeys(args[0]);
        if (keys.size() != PAGE_SIZE) throw new IllegalArgumentException("fixture needs at least 1,000 keys");
        measure(keys, iterations, false);
        measure(keys, iterations, true);
    }

    private static List<byte[]> fixtureKeys(String fixture) throws Exception {
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

    private static void measure(List<byte[]> keys, int iterations, boolean variedSeconds) throws Exception {
        List<AzureListResult.Entry> entries = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            long micros = 1_700_000_000_000_000L + (variedSeconds ? i * 1_000_000L : 0);
            ListedObject object = new ListedObject(keys.get(i), 123, micros,
                    null, null, null, null, null, null);
            entries.add(new AzureListResult.Entry.Blob(object));
        }
        AzureListRequest request = new AzureListRequest("replay", "bucket", "2026-06-06", null,
                null, null, null, PAGE_SIZE, null, false, false, false, false, null);
        AzureListResult page = new AzureListResult(request, entries, null);
        String endpoint = "http://127.0.0.1:19090/replay/";
        for (int i = 0; i < 20; i++) {
            try (BudgetedOutput out = BudgetedOutput.standalone(INITIAL_BYTES)) {
                AzureXml.write(page, endpoint, out);
            }
        }
        ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!bean.isThreadAllocatedMemorySupported()) {
            throw new IllegalStateException("thread allocation counters unavailable");
        }
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
                AzureXml.write(page, endpoint, out);
                cpu += bean.getThreadCpuTime(thread) - beforeCpu;
                allocated += bean.getThreadAllocatedBytes(thread) - beforeBytes;
                bytes += out.size();
                if (wireSha256 == null) {
                    wireSha256 = RenderBenchWire.sha256(out);
                }
            }
        }
        long objects = (long) iterations * keys.size();
        System.out.printf(Locale.ROOT,
                "{\"mode\":\"%s\",\"iterations\":%d,\"objects\":%d,\"encoded_bytes\":%d,"
                + "\"wire_sha256\":\"%s\",\"allocated_bytes\":%d,"
                + "\"allocated_bytes_per_object\":%.3f,"
                + "\"cpu_ns\":%d,\"cpu_ns_per_object\":%.3f}%n",
                variedSeconds ? "varied-second" : "same-second", iterations, objects, bytes, wireSha256,
                allocated, (double) allocated / objects, cpu, (double) cpu / objects);
    }
}
