/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Adversarial self-test for the standalone benchmark's independent response parsers. */
public final class ReplayHttpBenchSelfTest {
    private ReplayHttpBenchSelfTest() { }

    public static void main(String[] args) throws Exception {
        parsesGcsInventoryAndUpperBoundary();
        refusesMalformedGcsPages();
        parsesAzureEncodedNamesAndMarker();
        refusesMalformedXmlAndWrongInventory();
        rejectsHttpFailuresAndTokenLoops();
        timesOutAStalledBody();
        System.out.println("ReplayHttpBenchSelfTest passed");
    }

    private static void parsesGcsInventoryAndUpperBoundary() throws Exception {
        MessageDigest digest = sha256();
        var page = ReplayHttpBench.parseGcs(bytes("""
                {"kind":"storage#objects","items":[{"name":"a"},{"name":"c"}],
                 "nextPageToken":"opaque"}
                """), digest, null, raw("b"));
        require(page.count() == 2 && page.owned() == 1 && page.crossedUpper(), "GCS upper-bound accounting");
        require("opaque".equals(page.nextToken()), "GCS continuation token");
        require(Arrays.equals(page.lastKey(), raw("c")), "GCS last key");
        require(HexFormat.of().formatHex(digest.digest()).equals(digestOf("a")), "GCS owned digest");
    }

    private static void refusesMalformedGcsPages() throws Exception {
        expectFailure("GCS duplicate name", () -> gcs("""
                {"items":[{"name":"a"},{"name":"a"}]}
                """));
        expectFailure("GCS reversed order", () -> gcs("""
                {"items":[{"name":"b"},{"name":"a"}]}
                """));
        expectFailure("GCS missing name", () -> gcs("""
                {"items":[{"size":"1"}]}
                """));
        expectFailure("GCS truncated JSON", () -> gcs("{\"items\":[{\"name\":\"a\"}"));
        expectFailure("GCS trailing payload", () -> gcs("{\"items\":[]}{}"));
        expectFailure("GCS prior-page duplicate", () -> ReplayHttpBench.parseGcs(bytes("""
                {"items":[{"name":"a"}]}
                """), sha256(), raw("a"), null));
    }

    private static void parsesAzureEncodedNamesAndMarker() throws Exception {
        MessageDigest digest = sha256();
        var page = ReplayHttpBench.parseXml(bytes("""
                <?xml version="1.0"?><EnumerationResults><Blobs>
                  <Blob><Name Encoded="true">x%EF%BF%BE</Name></Blob>
                </Blobs><NextMarker>opaque</NextMarker></EnumerationResults>
                """), "azure", digest, null, null);
        require(page.count() == 1 && page.owned() == 1, "Azure encoded-name count");
        require("opaque".equals(page.nextToken()), "Azure marker");
        require(Arrays.equals(page.lastKey(), raw("x\uFFFE")), "Azure encoded-name bytes");
        require(HexFormat.of().formatHex(digest.digest()).equals(digestOf("x\uFFFE")), "Azure encoded digest");
    }

    private static void refusesMalformedXmlAndWrongInventory() throws Exception {
        expectFailure("Azure bad encoded name", () -> ReplayHttpBench.parseXml(bytes("""
                <EnumerationResults><Blobs><Blob><Name Encoded="true">a%GG</Name></Blob></Blobs>
                <NextMarker></NextMarker></EnumerationResults>
                """), "azure", sha256(), null, null));
        expectFailure("Azure reversed order", () -> ReplayHttpBench.parseXml(bytes("""
                <EnumerationResults><Blobs><Blob><Name>b</Name></Blob><Blob><Name>a</Name></Blob></Blobs>
                <NextMarker></NextMarker></EnumerationResults>
                """), "azure", sha256(), null, null));
        expectFailure("Azure truncated XML", () -> ReplayHttpBench.parseXml(bytes("""
                <EnumerationResults><Blobs><Blob><Name>a</Name></Blob>
                """), "azure", sha256(), null, null));
        expectFailure("S3 declared count differs", () -> ReplayHttpBench.parseXml(bytes("""
                <ListBucketResult><KeyCount>2</KeyCount><Contents><Key>a</Key></Contents>
                <IsTruncated>false</IsTruncated></ListBucketResult>
                """), "s3", sha256(), null, null));
    }

    private static void rejectsHttpFailuresAndTokenLoops() throws Exception {
        HttpServer failure = server(exchange -> {
            byte[] body = raw("fail");
            exchange.sendResponseHeaders(503, body.length);
            try (var out = exchange.getResponseBody()) { out.write(body); }
        });
        try {
            expectFailure("HTTP 503", () -> walk(failure, Duration.ofSeconds(1)));
        } finally { failure.stop(0); }

        AtomicInteger emptyRequests = new AtomicInteger();
        HttpServer rotating = server(exchange -> {
            String token = "token-" + emptyRequests.incrementAndGet();
            byte[] body = raw("{\"kind\":\"storage#objects\",\"nextPageToken\":\"" + token + "\"}");
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var out = exchange.getResponseBody()) { out.write(body); }
        });
        try {
            expectFailure("fresh empty tokens", () -> walk(rotating, Duration.ofSeconds(1)));
            require(emptyRequests.get() == 1, "empty nonfinal page refused on first request");
        } finally { rotating.stop(0); }

        AtomicInteger repeatedRequests = new AtomicInteger();
        HttpServer repeated = server(exchange -> {
            char name = (char) ('a' + repeatedRequests.incrementAndGet() - 1);
            byte[] body = raw("{\"items\":[{\"name\":\"" + name
                    + "\"}],\"nextPageToken\":\"same\"}");
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var out = exchange.getResponseBody()) { out.write(body); }
        });
        try {
            try {
                walk(repeated, Duration.ofSeconds(1));
                throw new AssertionError("repeated token was accepted");
            } catch (Exception expected) {
                require(expected.getMessage().contains("repeated continuation token"),
                        "repeated token classified, saw: " + expected);
            }
            require(repeatedRequests.get() == 2,
                    "repeated token bounded at second request, saw " + repeatedRequests.get());
        } finally { repeated.stop(0); }
    }

    private static void timesOutAStalledBody() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        HttpServer stalled = server(exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, 1_000_000);
            try (var out = exchange.getResponseBody()) {
                out.write(raw("{"));
                out.flush();
                try { release.await(3, TimeUnit.SECONDS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        });
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var future = executor.submit(() -> walk(stalled, Duration.ofMillis(100)));
            try {
                future.get(3, TimeUnit.SECONDS);
                throw new AssertionError("stalled body was accepted");
            } catch (java.util.concurrent.ExecutionException expected) {
                require(expected.getCause() instanceof IllegalStateException
                                && expected.getCause().getMessage().contains("body_timeout"),
                        "stalled body classified body_timeout");
            } finally {
                release.countDown();
                future.cancel(true);
            }
        } finally { stalled.stop(0); }
    }

    private static ReplayHttpBench.Walk walk(HttpServer server, Duration deadline) throws Exception {
        return ReplayHttpBench.walkForTest(
                URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
                "gcs", "bucket", 1, 2, deadline);
    }

    private static HttpServer server(com.sun.net.httpserver.HttpHandler handler) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", handler);
        server.start();
        return server;
    }

    private static void gcs(String json) throws Exception {
        ReplayHttpBench.parseGcs(bytes(json), sha256(), null, null);
    }

    private static MessageDigest sha256() throws Exception { return MessageDigest.getInstance("SHA-256"); }

    private static String digestOf(String key) throws Exception {
        byte[] value = raw(key);
        MessageDigest digest = sha256();
        digest.update(new byte[] {(byte) (value.length >>> 24), (byte) (value.length >>> 16),
                (byte) (value.length >>> 8), (byte) value.length});
        digest.update(value);
        return HexFormat.of().formatHex(digest.digest());
    }

    private static ByteArrayInputStream bytes(String value) {
        return new ByteArrayInputStream(raw(value));
    }

    private static byte[] raw(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    private static void expectFailure(String label, ThrowingCall call) throws Exception {
        try {
            call.run();
        } catch (Exception expected) {
            return;
        }
        throw new AssertionError(label + " was accepted");
    }

    private static void require(boolean condition, String label) {
        if (!condition) throw new AssertionError(label);
    }

    @FunctionalInterface
    private interface ThrowingCall { void run() throws Exception; }
}
