/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/** Isolated real-HTTP streaming parser control using immutable captured 1k response bodies. */
public final class ReplayCannedHttpBench {
    private ReplayCannedHttpBench() { }

    public static void main(String[] args) throws Exception {
        if (args.length == 5 && "server".equals(args[0])) {
            serve(Integer.parseInt(args[1]), Files.readAllBytes(Path.of(args[2])),
                    Files.readAllBytes(Path.of(args[3])), Files.readAllBytes(Path.of(args[4])));
            return;
        }
        if (args.length == 7 && "client".equals(args[0])) {
            drive(URI.create(args[1]), args[2], Files.readAllBytes(Path.of(args[3])),
                    Integer.parseInt(args[4]), Integer.parseInt(args[5]), Integer.parseInt(args[6]));
            return;
        }
        throw new IllegalArgumentException("usage: server PORT S3_BODY GCS_BODY AZURE_BODY | "
                + "client ENDPOINT PROTOCOL BODY CLIENTS SECONDS EXPECTED_ROWS");
    }

    private static void serve(int port, byte[] s3, byte[] gcs, byte[] azure) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 1024);
        var workers = Executors.newFixedThreadPool(64);
        server.setExecutor(workers);
        AtomicLong requests = new AtomicLong();
        server.createContext("/bench", exchange -> respond(exchange, s3, "application/xml", requests));
        server.createContext("/storage/v1/b/bench/o",
                exchange -> respond(exchange, gcs, "application/json", requests));
        server.createContext("/replay/bench",
                exchange -> respond(exchange, azure, "application/xml", requests));
        server.createContext("/counters", exchange -> {
            byte[] body = Long.toString(requests.get()).getBytes(StandardCharsets.US_ASCII);
            exchange.sendResponseHeaders(200, body.length);
            try (var out = exchange.getResponseBody()) { out.write(body); }
        });
        server.start();
        System.out.println("canned_http_endpoint=http://127.0.0.1:" + port);
        System.out.flush();
        try {
            new java.util.concurrent.CountDownLatch(1).await();
        } finally {
            server.stop(0);
            workers.shutdownNow();
        }
    }

    private static void respond(HttpExchange exchange, byte[] body, String contentType,
                                AtomicLong requests) throws IOException {
        requests.incrementAndGet();
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(200, body.length);
        try (var out = exchange.getResponseBody()) { out.write(body); }
    }

    private static void drive(URI endpoint, String protocol, byte[] expectedBody,
                              int clients, int seconds, int expectedRows) throws Exception {
        if (!List.of("s3", "gcs", "azure").contains(protocol) || clients < 1 || clients > 512
                || seconds < 1 || seconds > 60 || expectedRows < 1 || expectedRows > 5000) {
            throw new IllegalArgumentException("invalid client settings");
        }
        byte[] expectedDigest = parse(protocol, new ByteArrayInputStream(expectedBody), expectedRows);
        HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10)).build();
        URI uri = URI.create(endpoint.toString().replaceAll("/$", "") + switch (protocol) {
            case "s3" -> "/bench?list-type=2&encoding-type=url&max-keys=" + expectedRows;
            case "gcs" -> "/storage/v1/b/bench/o?maxResults=" + expectedRows;
            case "azure" -> "/replay/bench?restype=container&comp=list&maxresults=" + expectedRows;
            default -> throw new IllegalStateException();
        });
        AtomicLong startNanos = new AtomicLong();
        AtomicLong deadlineNanos = new AtomicLong();
        CyclicBarrier barrier = new CyclicBarrier(clients, () -> {
            System.out.println("{\"event\":\"MEASURE_START\"}");
            System.out.flush();
            try {
                if (System.in.read() < 0) throw new IllegalStateException("start ACK missing");
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            long now = System.nanoTime();
            startNanos.set(now);
            deadlineNanos.set(now + seconds * 1_000_000_000L);
        });
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<WorkerResult>> futures = new ArrayList<>(clients);
            for (int i = 0; i < clients; i++) {
                futures.add(workers.submit((Callable<WorkerResult>) () -> {
                    for (int warmup = 0; warmup < 100; warmup++)
                        one(client, uri, protocol, expectedRows, expectedDigest);
                    barrier.await();
                    long requests = 0, bytes = 0, ended = 0;
                    while (System.nanoTime() < deadlineNanos.get()) {
                        bytes += one(client, uri, protocol, expectedRows, expectedDigest);
                        requests++;
                    }
                    ended = System.nanoTime();
                    return new WorkerResult(requests, bytes, ended);
                }));
            }
            long requests = 0, bytes = 0, ended = 0;
            for (Future<WorkerResult> future : futures) {
                WorkerResult result = future.get();
                requests += result.requests();
                bytes += result.bytes();
                ended = Math.max(ended, result.endedNanos());
            }
            long elapsed = ended - startNanos.get();
            System.out.println("{\"event\":\"MEASURE_END\"}");
            System.out.flush();
            if (System.in.read() < 0) throw new IllegalStateException("end ACK missing");
            long objects = requests * expectedRows;
            System.out.printf(Locale.ROOT,
                    "{\"kind\":\"canned_http\",\"protocol\":\"%s\",\"clients\":%d,"
                            + "\"body_bytes\":%d,\"page_objects\":%d,\"requests\":%d,\"objects\":%d,"
                            + "\"bytes\":%d,\"elapsed_ns\":%d,\"objects_per_s\":%.3f}%n",
                    protocol, clients, expectedBody.length, expectedRows, requests, objects, bytes, elapsed,
                    objects * 1e9 / elapsed);
        } finally {
            client.shutdownNow();
        }
    }

    private static long one(HttpClient client, URI uri, String protocol, int expectedRows,
                            byte[] expectedDigest) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri).GET().timeout(Duration.ofSeconds(30));
        if (protocol.equals("azure")) builder.header("x-ms-version", "2026-06-06");
        HttpResponse<InputStream> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() != 200) throw new IllegalStateException("canned HTTP " + response.statusCode());
        try (CountingInputStream body = new CountingInputStream(response.body())) {
            if (!Arrays.equals(expectedDigest, parse(protocol, body, expectedRows)))
                throw new IllegalStateException("canned HTTP response digest changed");
            byte[] tail = new byte[8192];
            int count;
            while ((count = body.read(tail)) >= 0) {
                for (int i = 0; i < count; i++) {
                    if (tail[i] != ' ' && tail[i] != '\r' && tail[i] != '\n' && tail[i] != '\t')
                        throw new IllegalStateException("trailing body bytes");
                }
            }
            return body.bytes;
        }
    }

    private static byte[] parse(String protocol, InputStream input, int expectedRows) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        ReplayHttpBench.Page page = protocol.equals("gcs")
                ? ReplayHttpBench.parseGcs(input, digest, null, null)
                : ReplayHttpBench.parseXml(input, protocol, digest, null, null);
        if (page.count() != expectedRows || page.owned() != expectedRows)
            throw new IllegalStateException("canned page row count changed");
        return digest.digest();
    }

    private record WorkerResult(long requests, long bytes, long endedNanos) { }

    private static final class CountingInputStream extends FilterInputStream {
        long bytes;
        CountingInputStream(InputStream input) { super(input); }
        @Override public int read() throws IOException {
            int value = in.read();
            if (value >= 0) bytes++;
            return value;
        }
        @Override public int read(byte[] dst, int off, int len) throws IOException {
            int read = in.read(dst, off, len);
            if (read > 0) bytes += read;
            return read;
        }
    }
}
