/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

/** Short nonfinal pages, real opaque native tokens, exact fixture oracle and open-loop counts. */
public final class ReplayMixedOpenLoopBenchSelfTest {
    private static final AtomicBoolean MEASURED = new AtomicBoolean();
    private static final AtomicBoolean RATE_WARMUP = new AtomicBoolean();
    private ReplayMixedOpenLoopBenchSelfTest() { }

    public static void main(String[] args) throws Exception {
        Path fixture = Files.createTempFile("swath-mixed-selftest-", ".parquet");
        Path emptyFinalFixture = Files.createTempFile("swath-mixed-empty-final-", ".parquet");
        Files.delete(fixture);
        Files.delete(emptyFinalFixture);
        try {
            try (var connection = DriverManager.getConnection("jdbc:duckdb:");
                 var statement = connection.createStatement()) {
                statement.execute("CREATE TABLE fixture(key BLOB, size BIGINT, last_modified TIMESTAMPTZ)");
                statement.execute("INSERT INTO fixture SELECT encode(printf('k%04d', i)), 1, "
                        + "TIMESTAMPTZ '2020-01-01 00:00:00+00' FROM range(1001) t(i) ORDER BY i");
                statement.execute("COPY fixture TO '" + fixture.toString().replace("'", "''")
                        + "' (FORMAT PARQUET)");
                statement.execute("COPY (SELECT * FROM fixture WHERE key < encode('k1000')) TO '"
                        + emptyFinalFixture.toString().replace("'", "''") + "' (FORMAT PARQUET)");
            }
            ReplayHttpBench.Inventory inventory = ReplayHttpBench.fixtureInventory(fixture.toString(), 0);
            String declared = inventory.count() + ":" + inventory.digest();
            AtomicBoolean corruptToken = new AtomicBoolean();
            AtomicBoolean dropMeasuredToken = new AtomicBoolean();
            AtomicBoolean mutateMeasuredName = new AtomicBoolean();
            AtomicBoolean refuseMeasured = new AtomicBoolean();
            AtomicBoolean slowMeasured = new AtomicBoolean();
            AtomicBoolean slowTargetWarmup = new AtomicBoolean();
            AtomicBoolean emptyFinal = new AtomicBoolean();
            AtomicInteger availableKeys = new AtomicInteger(1001);
            Map<String, AtomicLong> requestCounts = new ConcurrentHashMap<>();
            AtomicLong tokenRequests = new AtomicLong();
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            var workers = Executors.newVirtualThreadPerTaskExecutor();
            server.setExecutor(workers);
            server.createContext("/", exchange -> {
                String path = exchange.getRequestURI().getPath();
                String protocol = path.startsWith("/storage/v1/") ? "gcs"
                        : path.startsWith("/replay/") ? "azure" : "s3";
                Map<String, String> query = query(exchange.getRequestURI().getRawQuery());
                int requested = Integer.parseInt(query.getOrDefault("max-keys",
                        query.getOrDefault("maxResults", query.getOrDefault("maxresults", "-1"))));
                String token = query.getOrDefault("continuation-token",
                        query.getOrDefault("pageToken", query.get("marker")));
                long ordinal = requestCounts.computeIfAbsent(protocol, ignored -> new AtomicLong())
                        .incrementAndGet();
                boolean measured = MEASURED.get();
                if (token != null) tokenRequests.incrementAndGet();
                if (slowMeasured.get() && measured || slowTargetWarmup.get() && RATE_WARMUP.get()) {
                    try { Thread.sleep(250); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                }
                int start = token == null ? 0
                        : token.equals("tok-500") ? 500 : token.equals("tok-1000") ? 1000 : -1;
                String body;
                int status;
                if (refuseMeasured.get() && measured && "tok-500".equals(token)) {
                    status = 503;
                    exchange.getResponseHeaders().set("x-swath-replay-error", "benchmark-refusal");
                    body = "refused";
                } else if (start < 0 || requested != 1000 || protocol.equals("azure")
                        && !"2026-06-06".equals(exchange.getRequestHeaders().getFirst("x-ms-version"))) {
                    status = 400;
                    body = "invalid native token";
                } else {
                    status = 200;
                    int end = Math.min(availableKeys.get(), start + 500);
                    String next = end < availableKeys.get() || emptyFinal.get()
                            && end == availableKeys.get() && start < end ? "tok-" + end : null;
                    if (corruptToken.get() && start == 500) next = "tok-500";
                    if (dropMeasuredToken.get() && measured && start == 500) next = null;
                    body = switch (protocol) {
                        case "gcs" -> gcs(start, end, next);
                        case "azure" -> azure(start, end, next);
                        default -> s3(start, end, next);
                    };
                    if (mutateMeasuredName.get() && measured && protocol.equals("s3")
                            && start == 500) {
                        body = body.replace("<Key>k0501</Key>", "<Key>k0501x</Key>");
                    }
                }
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status, bytes.length);
                try (var output = exchange.getResponseBody()) { output.write(bytes); }
            });
            server.start();
            try {
                String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
                String preflight = inspect(endpoint, fixture, declared);
                if (!preflight.contains("\"purpose\":\"native_page_plan_preflight\"")
                        || !preflight.contains("\"native_pages\":3")) {
                    throw new AssertionError("native page-plan preflight missing: " + preflight);
                }
                tokenRequests.set(0);
                requestCounts.clear();
                String mixed = run(endpoint, fixture, declared);
                if (!mixed.contains("\"native_pages\":3")
                        || !mixed.contains("\"RATE_WARMUP_START\"")
                        || !mixed.contains("\"RATE_WARMUP_END\"")
                        || !mixed.contains("\"complete_inventory_cycles\":1")
                        || !mixed.contains("\"partial_tail_pages\":0")
                        || !mixed.contains("\"page_plan_sha256\":\"")
                        || !mixed.contains("\"worker_dispatch_p99_lag_ns\":")
                        || !mixed.contains("\"client_send_p99_lag_ns\":")
                        || !mixed.contains("\"offered_requests_each\":3")
                        || !mixed.contains("\"attempted_requests\":9")
                        || !mixed.contains("\"successful_requests\":9")
                        || !mixed.contains("\"unsent_requests\":0")
                        || tokenRequests.get() < 12) {
                    throw new AssertionError("mixed native-token result missing: " + mixed);
                }
                requestCounts.clear();
                RunOutcome isolatedGcs = capture(endpoint, fixture, declared,
                        "gcs", "3", "64");
                if (isolatedGcs.failure() != null) throw isolatedGcs.failure();
                if (!isolatedGcs.output().contains("\"phase_offset_pages\":1")
                        || !isolatedGcs.output().contains("\"time_phase_fraction\":0.333333")) {
                    throw new AssertionError("isolated GCS phase differs from mixed plan");
                }
                corruptToken.set(true);
                requestCounts.clear();
                try {
                    run(endpoint, fixture, declared);
                    throw new AssertionError("repeating native token was accepted");
                } catch (IllegalStateException expected) {
                    if (!expected.getMessage().contains("repeated native token")) throw expected;
                }
                corruptToken.set(false);
                dropMeasuredToken.set(true);
                requestCounts.clear();
                expectFailure(endpoint, fixture, declared, "measured continuation token");
                dropMeasuredToken.set(false);
                mutateMeasuredName.set(true);
                requestCounts.clear();
                expectFailure(endpoint, fixture, declared, "page inventory mismatch");
                mutateMeasuredName.set(false);
                refuseMeasured.set(true);
                requestCounts.clear();
                expectFailure(endpoint, fixture, declared, "benchmark-refusal");
                refuseMeasured.set(false);
                slowMeasured.set(true);
                requestCounts.clear();
                RunOutcome slow = capture(endpoint, fixture, declared, "3", "64");
                if (slow.failure() != null) throw slow.failure();
                var latency = java.util.regex.Pattern.compile("\\\"p50_ns\\\":(\\d+)")
                        .matcher(slow.output());
                if (!latency.find() || Long.parseLong(latency.group(1)) < 200_000_000L) {
                    throw new AssertionError("scheduled-send latency omitted stalled responses");
                }
                requestCounts.clear();
                RunOutcome overloaded = capture(endpoint, fixture, declared, "15", "4");
                var unsent = java.util.regex.Pattern.compile("\\\"unsent_requests\\\":([1-9]\\d*)")
                        .matcher(overloaded.output());
                if (overloaded.failure() == null || !unsent.find()
                        || !overloaded.output().contains("\"status\":\"failed\"")) {
                    throw new AssertionError("bounded outstanding overflow lacked a failed receipt: "
                            + overloaded.output(), overloaded.failure());
                }
                slowMeasured.set(false);
                slowTargetWarmup.set(true);
                requestCounts.clear();
                RunOutcome warmupLimited = capture(endpoint, fixture, declared, "15", "4");
                if (warmupLimited.failure() == null
                        || !warmupLimited.output().contains("\"phase\":\"target_rate_warmup\"")
                        || !warmupLimited.output().contains(
                                "\"event\":\"RATE_WARMUP_END\",\"status\":\"failed\"")
                        || warmupLimited.output().contains("\"MEASURE_START\"")) {
                    throw new AssertionError("target-rate warmup failure phase was not preserved: "
                            + warmupLimited.output(), warmupLimited.failure());
                }
                long scheduled = number(warmupLimited.output(), "target_warmup_scheduled");
                long admitted = number(warmupLimited.output(), "target_warmup_admitted");
                long completed = number(warmupLimited.output(), "target_warmup_completed");
                long missed = number(warmupLimited.output(), "target_warmup_unsent");
                if (scheduled != 9 || missed < 1 || admitted + missed != scheduled
                        || completed != admitted
                        || number(warmupLimited.output(), "offered_requests") != scheduled
                        || number(warmupLimited.output(), "successful_requests") != completed
                        || number(warmupLimited.output(), "measured_scheduled") != 0) {
                    throw new AssertionError("target-rate warmup counters were mislabeled: "
                            + warmupLimited.output());
                }
                slowTargetWarmup.set(false);
                emptyFinal.set(true);
                availableKeys.set(1000);
                requestCounts.clear();
                ReplayHttpBench.Inventory exactlyThousand = ReplayHttpBench.fixtureInventory(
                        emptyFinalFixture.toString(), 0);
                String emptyDeclared = exactlyThousand.count() + ":" + exactlyThousand.digest();
                String emptyResult = run(endpoint, emptyFinalFixture, emptyDeclared);
                if (!emptyResult.contains("\"native_pages\":3")
                        || !emptyResult.contains("\"objects\":3000")) {
                    throw new AssertionError("empty final native page was rejected: " + emptyResult);
                }
            } finally {
                server.stop(0);
                workers.shutdownNow();
            }
        } finally {
            Files.deleteIfExists(fixture);
            Files.deleteIfExists(emptyFinalFixture);
        }
        System.out.println("ReplayMixedOpenLoopBenchSelfTest passed");
    }

    private static String run(String endpoint, Path fixture, String declared) throws Exception {
        RunOutcome outcome = capture(endpoint, fixture, declared, "3", "64");
        if (outcome.failure() != null) throw outcome.failure();
        return outcome.output();
    }

    private static String inspect(String endpoint, Path fixture, String declared) throws Exception {
        PrintStream original = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        MEASURED.set(false);
        try (PrintStream capture = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            System.setOut(capture);
            ReplayMixedOpenLoopBench.main(new String[] {"--page-plan", endpoint, "bench",
                    fixture.toString(), declared});
        } finally {
            System.setOut(original);
        }
        return output.toString(StandardCharsets.UTF_8);
    }

    private static RunOutcome capture(String endpoint, Path fixture, String declared,
                                      String rate, String maxOutstanding) throws Exception {
        return capture(endpoint, fixture, declared, "s3,gcs,azure", rate, rate, maxOutstanding);
    }

    private static RunOutcome capture(String endpoint, Path fixture, String declared,
                                      String protocols, String rate, String maxOutstanding) throws Exception {
        return capture(endpoint, fixture, declared, protocols, rate, rate, maxOutstanding);
    }

    private static RunOutcome capture(String endpoint, Path fixture, String declared,
                                      String protocols, String rate, String offeredEach,
                                      String maxOutstanding) throws Exception {
        var input = System.in;
        var output = System.out;
        ByteArrayOutputStream capture = new ByteArrayOutputStream();
        Exception failure = null;
        MEASURED.set(false);
        RATE_WARMUP.set(false);
        OutputStream marked = new OutputStream() {
            @Override public void write(int value) throws IOException { capture.write(value); }
            @Override public void write(byte[] bytes, int offset, int length) throws IOException {
                capture.write(bytes, offset, length);
                String text = capture.toString(StandardCharsets.UTF_8);
                if (text.contains("\"RATE_WARMUP_START\"")) RATE_WARMUP.set(true);
                if (text.contains("\"RATE_WARMUP_END\"")) RATE_WARMUP.set(false);
                if (text.contains("\"MEASURE_START\"")) {
                    MEASURED.set(true);
                }
            }
        };
        try (PrintStream stream = new PrintStream(marked, true, StandardCharsets.UTF_8)) {
            System.setIn(new ByteArrayInputStream(new byte[] {'\n', '\n', '\n', '\n'}));
            System.setOut(stream);
            try {
                ReplayMixedOpenLoopBench.main(new String[] {endpoint, "bench", fixture.toString(),
                        declared, protocols, rate, offeredEach, "1000", "1", "1",
                        maxOutstanding, "bracket", "end_ack"});
            } catch (Exception error) {
                failure = error;
            }
        } finally {
            System.setIn(input);
            System.setOut(output);
        }
        return new RunOutcome(capture.toString(StandardCharsets.UTF_8), failure);
    }

    private static void expectFailure(String endpoint, Path fixture, String declared,
                                      String expected) throws Exception {
        RunOutcome outcome = capture(endpoint, fixture, declared, "3", "64");
        if (outcome.failure() == null) throw new AssertionError(expected + " failure was accepted");
        String detail = outcome.failure().toString()
                + (outcome.failure().getCause() == null ? "" : outcome.failure().getCause());
        if (!detail.contains(expected) || !outcome.output().contains("\"status\":\"failed\"")) {
            throw new AssertionError("failure receipt omitted " + expected + ": " + outcome.output(),
                    outcome.failure());
        }
    }

    private record RunOutcome(String output, Exception failure) { }

    private static long number(String output, String field) {
        var match = java.util.regex.Pattern.compile("\\\"" + field + "\\\":(\\d+)")
                .matcher(output);
        if (!match.find()) throw new AssertionError("missing " + field + " in " + output);
        return Long.parseLong(match.group(1));
    }

    private static Map<String, String> query(String raw) {
        Map<String, String> result = new HashMap<>();
        if (raw == null) return result;
        for (String part : raw.split("&")) {
            String[] fields = part.split("=", 2);
            result.put(URLDecoder.decode(fields[0], StandardCharsets.UTF_8),
                    fields.length == 2 ? URLDecoder.decode(fields[1], StandardCharsets.UTF_8) : "");
        }
        return result;
    }

    private static String s3(int start, int end, String next) {
        StringBuilder out = new StringBuilder("<ListBucketResult><KeyCount>").append(end - start)
                .append("</KeyCount><IsTruncated>").append(next != null).append("</IsTruncated>");
        for (int i = start; i < end; i++) {
            out.append("<Contents><Key>k").append(String.format("%04d", i))
                    .append("</Key><Size>1</Size><LastModified>2020-01-01T00:00:00.000Z</LastModified></Contents>");
        }
        if (next != null) out.append("<NextContinuationToken>").append(next)
                .append("</NextContinuationToken>");
        return out.append("</ListBucketResult>").toString();
    }

    private static String gcs(int start, int end, String next) {
        StringBuilder out = new StringBuilder("{\"items\":[");
        for (int i = start; i < end; i++) {
            if (i > start) out.append(',');
            out.append("{\"name\":\"k").append(String.format("%04d", i))
                    .append("\",\"size\":\"1\",\"updated\":\"2020-01-01T00:00:00.000000Z\"}");
        }
        out.append(']');
        if (next != null) out.append(",\"nextPageToken\":\"").append(next).append('"');
        return out.append('}').toString();
    }

    private static String azure(int start, int end, String next) {
        StringBuilder out = new StringBuilder("<EnumerationResults><Blobs>");
        for (int i = start; i < end; i++) {
            out.append("<Blob><Name>k").append(String.format("%04d", i))
                    .append("</Name><Properties><Content-Length>1</Content-Length>"
                            + "<Last-Modified>Wed, 01 Jan 2020 00:00:00 GMT</Last-Modified>"
                            + "</Properties></Blob>");
        }
        out.append("</Blobs>");
        if (next != null) out.append("<NextMarker>").append(next).append("</NextMarker>");
        return out.append("</EnumerationResults>").toString();
    }
}
