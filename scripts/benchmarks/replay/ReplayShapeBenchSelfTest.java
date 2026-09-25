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

/** Tiny fixture-backed HTTP smoke for each seek and delimiter route. */
public final class ReplayShapeBenchSelfTest {
    private static final List<String> KEYS = List.of("a/1", "a/2", "a/b/1", "a/c", "c");

    private ReplayShapeBenchSelfTest() { }

    public static void main(String[] args) throws Exception {
        Path fixture = Files.createTempFile("swath-shape-selftest-", ".parquet");
        Files.delete(fixture);
        try {
            rejectsMalformedShapePages();
            try (var conn = DriverManager.getConnection("jdbc:duckdb:");
                 var statement = conn.createStatement()) {
                statement.execute("CREATE TABLE fixture(key BLOB, size BIGINT, last_modified TIMESTAMPTZ)");
                for (String key : KEYS) {
                    statement.execute("INSERT INTO fixture VALUES (encode('" + key
                            + "'), 1, TIMESTAMPTZ '2020-01-01 00:00:00+00')");
                }
                statement.execute("COPY fixture TO '" + fixture.toString().replace("'", "''")
                        + "' (FORMAT PARQUET)");
            }
            ReplayHttpBench.Inventory inventory = ReplayHttpBench.fixtureInventory(fixture.toString(), 0);
            String declared = inventory.count() + ":" + inventory.digest();
            AtomicBoolean corruptS3Size = new AtomicBoolean();
            HttpServer endpoint = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            endpoint.createContext("/", exchange -> {
                String path = exchange.getRequestURI().getPath();
                String protocol = path.startsWith("/storage/v1/") ? "gcs"
                        : path.startsWith("/replay/") ? "azure" : "s3";
                Map<String, String> query = query(exchange.getRequestURI().getRawQuery());
                String body = response(protocol, query);
                if (corruptS3Size.get() && protocol.equals("s3"))
                    body = body.replace("<Size>1</Size>", "<Size>2</Size>");
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", protocol.equals("gcs")
                        ? "application/json" : "application/xml");
                exchange.sendResponseHeaders(200, bytes.length);
                try (var output = exchange.getResponseBody()) { output.write(bytes); }
            });
            endpoint.start();
            try {
                String base = "http://127.0.0.1:" + endpoint.getAddress().getPort();
                for (String protocol : List.of("s3", "gcs", "azure")) {
                    run(base, protocol, fixture, declared, "seek", null);
                    run(base, protocol, fixture, declared, "delimiter", "a/");
                }
                corruptS3Size.set(true);
                expectFailure("seek warmup rejects wrong fixture-backed size",
                        () -> run(base, "s3", fixture, declared, "seek", null));
            } finally { endpoint.stop(0); }
        } finally { Files.deleteIfExists(fixture); }
        System.out.println("ReplayShapeBenchSelfTest passed");
    }

    private static void rejectsMalformedShapePages() throws Exception {
        expectFailure("out-of-order GCS items", () -> ReplayShapeBench.parseGcs(bytes("""
                {"items":[{"name":"b"},{"name":"a"}]}
                """)));
        expectFailure("duplicate GCS prefixes", () -> ReplayShapeBench.parseGcs(bytes("""
                {"prefixes":["a/","a/"]}
                """)));
        expectFailure("empty GCS token", () -> ReplayShapeBench.parseGcs(bytes("""
                {"nextPageToken":""}
                """)));
        expectFailure("truncated Azure XML", () -> ReplayShapeBench.parseXml(bytes("""
                <EnumerationResults><Blobs><Blob><Name>a</Name></Blob>
                """), "azure"));
        expectFailure("S3 truncated without token", () -> ReplayShapeBench.parseXml(bytes("""
                <ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>
                """), "s3"));
        ReplayShapeBench.Response grouped = ReplayShapeBench.parseXml(bytes("""
                <ListBucketResult><Contents><Key>a/c</Key></Contents>
                <CommonPrefixes><Prefix>a/b/</Prefix></CommonPrefixes>
                <IsTruncated>false</IsTruncated></ListBucketResult>
                """), "s3");
        if (grouped.entries().size() != 2 || !grouped.entries().get(0).prefix()
                || !"a/b/".equals(new String(grouped.entries().get(0).name(), StandardCharsets.UTF_8))
                || !"a/c".equals(new String(grouped.entries().get(1).name(), StandardCharsets.UTF_8))) {
            throw new AssertionError("S3 grouped XML was not merged into unsigned key order");
        }
        expectFailure("S3 merged duplicate object/prefix", () -> ReplayShapeBench.parseXml(bytes("""
                <ListBucketResult><Contents><Key>a/b/</Key></Contents>
                <CommonPrefixes><Prefix>a/b/</Prefix></CommonPrefixes>
                <IsTruncated>false</IsTruncated></ListBucketResult>
                """), "s3"));
    }

    private static ByteArrayInputStream bytes(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }

    private static void expectFailure(String label, ThrowingCall call) throws Exception {
        try { call.run(); }
        catch (Exception expected) { return; }
        throw new AssertionError(label + " was accepted");
    }

    @FunctionalInterface
    private interface ThrowingCall { void run() throws Exception; }

    private static void run(String base, String protocol, Path fixture, String declared,
                            String mode, String prefix) throws Exception {
        var originalIn = System.in;
        var originalOut = System.out;
        ByteArrayOutputStream capture = new ByteArrayOutputStream();
        try (PrintStream output = new PrintStream(capture, true, StandardCharsets.UTF_8)) {
            System.setIn(new ByteArrayInputStream(new byte[] {'a'}));
            System.setOut(output);
            List<String> argv = new ArrayList<>(List.of(base, protocol, "bucket", fixture.toString(),
                    "1", mode.equals("seek") ? "1" : "2", "1", declared, "bracket", mode, "2"));
            if (prefix != null) argv.add(prefix);
            ReplayShapeBench.main(argv.toArray(String[]::new));
        } finally {
            System.setIn(originalIn);
            System.setOut(originalOut);
        }
        String observed = capture.toString(StandardCharsets.UTF_8);
        if (!observed.contains("\"event\":\"MEASURE_START\"")
                || !observed.contains("\"event\":\"MEASURE_END\"")
                || !observed.contains("\"workload\":\"" + mode + "\"")) {
            throw new AssertionError(protocol + " " + mode + " shape result missing: " + observed);
        }
    }

    private static Map<String, String> query(String raw) {
        Map<String, String> result = new HashMap<>();
        for (String part : raw.split("&")) {
            String[] pair = part.split("=", 2);
            result.put(URLDecoder.decode(pair[0], StandardCharsets.UTF_8),
                    pair.length == 2 ? URLDecoder.decode(pair[1], StandardCharsets.UTF_8) : "");
        }
        return result;
    }

    private static String response(String protocol, Map<String, String> query) {
        boolean delimiter = query.containsKey("delimiter");
        int pageSize = Integer.parseInt(query.getOrDefault("max-keys",
                query.getOrDefault("maxResults", query.getOrDefault("maxresults", "1"))));
        String token = query.getOrDefault("continuation-token",
                query.getOrDefault("pageToken", query.get("marker")));
        List<LogicalEntry> entries = new ArrayList<>();
        if (delimiter) {
            entries.add(new LogicalEntry(false, "a/1"));
            entries.add(new LogicalEntry(false, "a/2"));
            entries.add(new LogicalEntry(true, "a/b/"));
            entries.add(new LogicalEntry(false, "a/c"));
        } else {
            String floor = query.getOrDefault("startOffset", query.getOrDefault("startFrom", null));
            String after = query.get("start-after");
            for (String key : KEYS) {
                if (floor != null && key.compareTo(floor) < 0 || after != null && key.compareTo(after) <= 0) {
                    continue;
                }
                entries.add(new LogicalEntry(false, key));
            }
        }
        int start = token == null ? 0 : 2;
        int end = Math.min(entries.size(), start + pageSize);
        List<LogicalEntry> page = entries.subList(start, end);
        String next = delimiter && end < entries.size() ? "t1" : null;
        return switch (protocol) {
            case "gcs" -> gcs(page, next);
            case "azure" -> azure(page, next);
            default -> s3(page, next);
        };
    }

    private static String gcs(List<LogicalEntry> entries, String next) {
        List<String> objects = new ArrayList<>();
        List<String> prefixes = new ArrayList<>();
        for (LogicalEntry entry : entries) {
            if (entry.prefix()) prefixes.add("\"" + entry.name() + "\"");
            else objects.add("{\"name\":\"" + entry.name()
                    + "\",\"size\":\"1\",\"updated\":\"2020-01-01T00:00:00.000000Z\"}");
        }
        return "{\"kind\":\"storage#objects\",\"items\":[" + String.join(",", objects)
                + "],\"prefixes\":[" + String.join(",", prefixes) + "]"
                + (next == null ? "}" : ",\"nextPageToken\":\"" + next + "\"}");
    }

    private static String azure(List<LogicalEntry> entries, String next) {
        StringBuilder xml = new StringBuilder("<?xml version=\"1.0\"?><EnumerationResults><Blobs>");
        for (LogicalEntry entry : entries) {
            xml.append(entry.prefix() ? "<BlobPrefix><Name>" : "<Blob><Name>")
                    .append(entry.name()).append(entry.prefix() ? "</Name></BlobPrefix>"
                            : "</Name><Properties><Last-Modified>Wed, 01 Jan 2020 00:00:00 GMT"
                            + "</Last-Modified><Content-Length>1</Content-Length></Properties></Blob>");
        }
        return xml.append("</Blobs><NextMarker>").append(next == null ? "" : next)
                .append("</NextMarker></EnumerationResults>").toString();
    }

    private static String s3(List<LogicalEntry> entries, String next) {
        StringBuilder xml = new StringBuilder("<ListBucketResult>");
        StringBuilder objects = new StringBuilder();
        StringBuilder prefixes = new StringBuilder();
        for (LogicalEntry entry : entries) {
            StringBuilder section = entry.prefix() ? prefixes : objects;
            section.append(entry.prefix() ? "<CommonPrefixes><Prefix>" : "<Contents><Key>")
                    .append(entry.name()).append(entry.prefix()
                            ? "</Prefix></CommonPrefixes>"
                            : "</Key><LastModified>2020-01-01T00:00:00.000Z</LastModified>"
                            + "<Size>1</Size></Contents>");
        }
        xml.append(objects).append(prefixes);
        xml.append("<IsTruncated>").append(next != null).append("</IsTruncated>");
        if (next != null) xml.append("<NextContinuationToken>").append(next).append("</NextContinuationToken>");
        return xml.append("</ListBucketResult>").toString();
    }

    private record LogicalEntry(boolean prefix, String name) { }
}
