/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** Four-key fixture and real HTTP parser check for both endOffset arms. */
public final class ReplayGcsNarrowBenchSelfTest {
    private static final List<String> KEYS = List.of("a", "b", "c", "d");

    private ReplayGcsNarrowBenchSelfTest() { }

    public static void main(String[] args) throws Exception {
        Path fixture = Files.createTempFile("gcs-narrow-selftest-", ".parquet");
        Files.delete(fixture);
        try {
            try (var connection = DriverManager.getConnection("jdbc:duckdb:");
                 var statement = connection.createStatement()) {
                statement.execute("CREATE TABLE fixture(key BLOB, size BIGINT, last_modified TIMESTAMPTZ)");
                for (String key : KEYS) {
                    statement.execute("INSERT INTO fixture VALUES (encode('" + key
                            + "'), 1, TIMESTAMPTZ '2020-01-01 00:00:00+00')");
                }
                statement.execute("COPY fixture TO '" + fixture.toString().replace("'", "''")
                        + "' (FORMAT PARQUET)");
            }
            ReplayHttpBench.Inventory inventory = ReplayHttpBench.fixtureInventory(fixture.toString(), 2);
            List<byte[]> midpoints = ReplayGcsNarrowBench.midpointKeys(fixture.toString(), inventory);
            if (!"b".equals(new String(midpoints.get(0), StandardCharsets.UTF_8))
                    || !"d".equals(new String(midpoints.get(1), StandardCharsets.UTF_8))) {
                throw new AssertionError("fixture midpoint selection changed");
            }
            URI encoded = ReplayGcsNarrowBench.requestUri(URI.create("http://localhost:80"), "bucket",
                    2, "a b".getBytes(StandardCharsets.UTF_8), "c".getBytes(StandardCharsets.UTF_8), "token+");
            if (!encoded.getRawQuery().contains("startOffset=a%20b")
                    || !encoded.getRawQuery().contains("endOffset=c")
                    || !encoded.getRawQuery().contains("pageToken=token%2B")) {
                throw new AssertionError("native range/token query encoding changed");
            }
            HttpServer endpoint = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            AtomicBoolean ignoreEndOffset = new AtomicBoolean();
            AtomicBoolean repeatToken = new AtomicBoolean();
            endpoint.createContext("/", exchange -> {
                Map<String, String> query = query(exchange.getRequestURI().getRawQuery());
                String body = page(query, ignoreEndOffset.get(), repeatToken.get());
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                try (var output = exchange.getResponseBody()) { output.write(bytes); }
            });
            endpoint.start();
            try {
                String base = "http://127.0.0.1:" + endpoint.getAddress().getPort();
                String declared = inventory.count() + ":" + inventory.digest();
                String unchanged = run(base, fixture, declared, "unchanged");
                String narrowed = run(base, fixture, declared, "narrowed");
                if (!unchanged.contains("\"overshoot_objects\":2")
                        || !unchanged.contains("\"successful_requests\":3")
                        || !narrowed.contains("\"overshoot_objects\":0")
                        || !narrowed.contains("\"successful_requests\":4")) {
                    throw new AssertionError("expected client-side overshoot only: "
                            + unchanged + " / " + narrowed);
                }
                for (String result : List.of(unchanged, narrowed)) {
                    if (!result.contains("\"objects\":4")
                            || !result.contains("\"warmup_metadata_verified_objects\":4")) {
                        throw new AssertionError("inventory, metadata, or pagination lost: " + result);
                    }
                }
                ignoreEndOffset.set(true);
                expectFailure("narrowed walk accepted a server that ignored endOffset",
                        () -> run(base, fixture, declared, "narrowed"));
                ignoreEndOffset.set(false);
                repeatToken.set(true);
                expectFailure("unchanged walk accepted a repeated pageToken",
                        () -> run(base, fixture, declared, "unchanged"));
            } finally { endpoint.stop(0); }
        } finally { Files.deleteIfExists(fixture); }
        System.out.println("ReplayGcsNarrowBenchSelfTest passed");
    }

    private static String run(String endpoint, Path fixture, String declared, String mode) throws Exception {
        var originalIn = System.in;
        var originalOut = System.out;
        ByteArrayOutputStream capture = new ByteArrayOutputStream();
        try (PrintStream output = new PrintStream(capture, true, StandardCharsets.UTF_8)) {
            System.setIn(new ByteArrayInputStream(new byte[] {'a'}));
            System.setOut(output);
            ReplayGcsNarrowBench.main(new String[] {endpoint, "gcs", "bucket", fixture.toString(),
                    "2", "2", "1", declared, "bracket", mode, "1"});
        } finally {
            System.setIn(originalIn);
            System.setOut(originalOut);
        }
        return capture.toString(StandardCharsets.UTF_8);
    }

    private static void expectFailure(String label, ThrowingCall call) throws Exception {
        try { call.run(); }
        catch (Exception expected) { return; }
        throw new AssertionError(label);
    }

    @FunctionalInterface
    private interface ThrowingCall { void run() throws Exception; }

    private static Map<String, String> query(String raw) {
        Map<String, String> result = new HashMap<>();
        for (String part : raw.split("&")) {
            String[] pair = part.split("=", 2);
            result.put(URLDecoder.decode(pair[0], StandardCharsets.UTF_8),
                    pair.length == 2 ? URLDecoder.decode(pair[1], StandardCharsets.UTF_8) : "");
        }
        return result;
    }

    private static String page(Map<String, String> query, boolean ignoreEndOffset,
                               boolean repeatToken) {
        String start = query.getOrDefault("startOffset", "");
        String end = ignoreEndOffset ? null : query.get("endOffset");
        int offset = Integer.parseInt(query.getOrDefault("pageToken", "0"));
        int limit = Integer.parseInt(query.get("maxResults"));
        List<String> selected = new ArrayList<>();
        for (String key : KEYS) {
            if (key.compareTo(start) >= 0 && (end == null || key.compareTo(end) < 0)) selected.add(key);
        }
        int next = Math.min(selected.size(), offset + limit);
        StringBuilder json = new StringBuilder("{\"items\":[");
        for (int i = offset; i < next; i++) {
            if (i != offset) json.append(',');
            json.append("{\"name\":\"").append(selected.get(i))
                    .append("\",\"size\":\"1\",\"updated\":\"2020-01-01T00:00:00Z\"}");
        }
        json.append(']');
        if (next < selected.size()) json.append(",\"nextPageToken\":\"")
                .append(repeatToken ? 0 : next).append('"');
        return json.append('}').toString();
    }
}
