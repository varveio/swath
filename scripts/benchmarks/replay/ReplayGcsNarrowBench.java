/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import java.io.FilterInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Isolated GCS replay comparison: existing client stop vs native narrowed endOffset. */
public final class ReplayGcsNarrowBench {
    private static final Duration BODY_DEADLINE = Duration.ofSeconds(30);
    private static final AtomicLong ATTEMPTED = new AtomicLong();
    private static final AtomicLong SUCCEEDED = new AtomicLong();

    private ReplayGcsNarrowBench() { }

    public static void main(String[] argv) throws Exception {
        boolean endAck = argv.length > 0 && "end_ack".equals(argv[argv.length - 1]);
        int fields = argv.length - (endAck ? 1 : 0);
        if (fields != 11 || !"gcs".equals(argv[1]) || !"bracket".equals(argv[8])
                || !List.of("unchanged", "narrowed").contains(argv[9])) {
            throw new IllegalArgumentException("usage: ReplayGcsNarrowBench ENDPOINT gcs BUCKET "
                    + "FIXTURE_GLOB CLIENTS PAGE_SIZE WARMUP_WALKS COUNT:DIGEST bracket "
                    + "unchanged|narrowed REPETITIONS [end_ack]");
        }
        URI endpoint = URI.create(argv[0]);
        String bucket = argv[2];
        String fixture = argv[3];
        int clients = Integer.parseInt(argv[4]);
        int pageSize = Integer.parseInt(argv[5]);
        int warmup = Integer.parseInt(argv[6]);
        String mode = argv[9];
        int repetitions = Integer.parseInt(argv[10]);
        if (clients < 1 || clients > 512 || pageSize < 1 || pageSize > 1000
                || warmup < 0 || warmup > 10 || repetitions < 1) {
            throw new IllegalArgumentException("clients/page size/warmup/repetitions out of range");
        }
        ReplayHttpBench.Inventory inventory = ReplayHttpBench.fixtureInventory(fixture, clients);
        if (!strippedInventory(argv[7]).equals(inventory.count() + ":" + inventory.digest())) {
            throw new IllegalStateException("fixture changed since declared inventory");
        }
        List<byte[]> midpoints = midpointKeys(fixture, inventory);
        FixtureMetadataOracle.Plan metadataPlan = warmup == 0 ? null
                : FixtureMetadataOracle.plan(fixture, "gcs", clients);
        if (metadataPlan != null && metadataPlan.total().count() != inventory.count()) {
            throw new IllegalStateException("metadata oracle disagrees with key inventory");
        }
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
                .version(HttpClient.Version.HTTP_1_1).build();
        var workers = Executors.newVirtualThreadPerTaskExecutor();
        ScheduledThreadPoolExecutor watchdog = new ScheduledThreadPoolExecutor(1);
        watchdog.setRemoveOnCancelPolicy(true);
        List<Walk> measured;
        long elapsed;
        try {
            for (int w = 0; w < warmup; w++) {
                runBatch(client, workers, watchdog, endpoint, bucket, pageSize, inventory,
                        midpoints, mode, repetitions, metadataPlan);
            }
            ATTEMPTED.set(0);
            SUCCEEDED.set(0);
            System.out.println("{\"event\":\"MEASURE_START\"}");
            System.out.flush();
            if (System.in.read() < 0) {
                throw new IllegalStateException("measurement controller did not acknowledge start");
            }
            long begun = System.nanoTime();
            try {
                measured = runBatch(client, workers, watchdog, endpoint, bucket, pageSize,
                        inventory, midpoints, mode, repetitions, null);
            } catch (Exception failure) {
                workers.shutdownNow();
                client.shutdownNow();
                System.out.printf("{\"status\":\"failed\",\"attempted_requests\":%d,"
                        + "\"successful_requests\":%d}%n", ATTEMPTED.get(), SUCCEEDED.get());
                throw failure;
            }
            elapsed = System.nanoTime() - begun;
            System.out.println("{\"event\":\"MEASURE_END\"}");
            System.out.flush();
            if (endAck && System.in.read() < 0) {
                throw new IllegalStateException("measurement controller did not acknowledge end");
            }
        } finally {
            watchdog.shutdownNow();
            workers.shutdownNow();
            client.shutdownNow();
        }
        long objects = 0, emitted = 0, requests = 0, bytes = 0, maxBody = 0, activeWall = 0;
        Histogram latencies = new Histogram();
        for (Walk walk : measured) {
            objects += walk.objects();
            emitted += walk.emitted();
            requests += walk.requests();
            bytes += walk.bytes();
            maxBody = Math.max(maxBody, walk.maxBody());
            activeWall += walk.activeWallNanos();
            latencies.addAll(walk.latencies());
        }
        if (objects != inventory.count() * repetitions || requests != ATTEMPTED.get()
                || requests != SUCCEEDED.get()) {
            throw new IllegalStateException("measured inventory or request accounting changed");
        }
        if (mode.equals("narrowed") && emitted != objects) {
            throw new IllegalStateException("narrowed endOffset emitted keys outside requested intervals");
        }
        System.out.printf(Locale.ROOT,
                "{\"protocol\":\"gcs\",\"mode\":\"%s\",\"clients\":%d,"
                + "\"page_size\":%d,\"fixture_count\":%d,\"fixture_digest\":\"%s\","
                + "\"partitioned\":true,\"repetitions\":%d,\"objects\":%d,"
                + "\"emitted_objects\":%d,\"overshoot_objects\":%d,\"requests\":%d,"
                + "\"bytes\":%d,\"max_response_bytes\":%d,\"elapsed_ns\":%d,"
                + "\"objects_per_s\":%.3f,\"requests_per_s\":%.3f,\"bytes_per_s\":%.3f,"
                + "\"p50_ns\":%d,\"p95_ns\":%d,\"p99_ns\":%d,"
                + "\"latency_histogram_relative_error_max\":0.016,"
                + "\"client_active_wall_ns\":%d,\"tail_dilution_ratio\":%.6f,"
                + "\"metadata_profile\":\"%s\",\"warmup_metadata_verified_objects\":%d,"
                + "\"attempted_requests\":%d,\"successful_requests\":%d}%n",
                mode, clients, pageSize, inventory.count(), inventory.digest(), repetitions,
                objects, emitted, emitted - objects, requests, bytes, maxBody, elapsed,
                objects * 1e9 / elapsed, requests * 1e9 / elapsed, bytes * 1e9 / elapsed,
                latencies.percentile(.50), latencies.percentile(.95), latencies.percentile(.99),
                activeWall, (double) activeWall / (clients * elapsed),
                metadataPlan == null ? "none" : "fixture_name_size_time",
                metadataPlan == null ? 0 : inventory.count() * repetitions * warmup,
                ATTEMPTED.get(), SUCCEEDED.get());
    }

    private static String strippedInventory(String supplied) {
        if (!supplied.matches("[0-9]+:[0-9a-f]{64}")) {
            throw new IllegalArgumentException("expected COUNT:DIGEST");
        }
        return supplied;
    }

    /** One streaming key scan yields deterministic first keys of the upper half. */
    static List<byte[]> midpointKeys(String fixture, ReplayHttpBench.Inventory inventory) throws Exception {
        long[] targets = new long[inventory.partitions().size()];
        long offset = 0;
        for (int i = 0; i < targets.length; i++) {
            ReplayHttpBench.Partition part = inventory.partitions().get(i);
            if (part.count() < 2) throw new IllegalArgumentException("narrowed arm needs two keys per client");
            targets[i] = offset + part.count() / 2;
            offset += part.count();
        }
        if (offset != inventory.count()) throw new IllegalStateException("partition counts disagree with inventory");
        String quoted = fixture.replace("'", "''");
        Properties properties = new Properties();
        properties.setProperty("jdbc_stream_results", "true");
        List<byte[]> midpoints = new ArrayList<>(targets.length);
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:", properties);
             Statement statement = connection.createStatement()) {
            String type;
            try (ResultSet result = statement.executeQuery(
                    "SELECT typeof(key) FROM read_parquet('" + quoted + "') LIMIT 1")) {
                type = result.next() ? result.getString(1) : "BLOB";
            }
            String expression = switch (type.toUpperCase(Locale.ROOT)) {
                case "BLOB" -> "key";
                case "VARCHAR" -> "encode(key)";
                default -> throw new IllegalStateException("unsupported fixture key type " + type);
            };
            long row = 0;
            int next = 0;
            try (ResultSet result = statement.executeQuery(
                    "SELECT " + expression + " FROM read_parquet('" + quoted + "')")) {
                while (result.next()) {
                    if (next < targets.length && row == targets[next]) {
                        byte[] midpoint = result.getBytes(1);
                        ReplayHttpBench.Partition part = inventory.partitions().get(next);
                        if (midpoint == null || Arrays.compareUnsigned(part.first(), midpoint) >= 0
                                || part.upper() != null && Arrays.compareUnsigned(midpoint, part.upper()) >= 0) {
                            throw new IllegalStateException("invalid fixture midpoint for partition " + next);
                        }
                        midpoints.add(midpoint);
                        next++;
                    }
                    row++;
                }
            }
            if (row != inventory.count() || next != targets.length) {
                throw new IllegalStateException("fixture changed while selecting narrowed midpoints");
            }
        }
        return List.copyOf(midpoints);
    }

    private static List<Walk> runBatch(HttpClient client, java.util.concurrent.ExecutorService workers,
                                       ScheduledThreadPoolExecutor watchdog, URI endpoint, String bucket,
                                       int pageSize, ReplayHttpBench.Inventory inventory,
                                       List<byte[]> midpoints, String mode, int repetitions,
                                       FixtureMetadataOracle.Plan metadataPlan) throws Exception {
        ExecutorCompletionService<Walk> completion = new ExecutorCompletionService<>(workers);
        List<Future<Walk>> futures = new ArrayList<>(inventory.partitions().size());
        try {
            for (int i = 0; i < inventory.partitions().size(); i++) {
                ReplayHttpBench.Partition part = inventory.partitions().get(i);
                byte[] midpoint = midpoints.get(i);
                FixtureMetadataOracle.Digest expectedMetadata = metadataPlan == null ? null
                        : metadataPlan.partitions().get(i);
                futures.add(completion.submit((Callable<Walk>) () -> {
                    long activeStart = System.nanoTime();
                    Counters counters = new Counters();
                    for (int repetition = 0; repetition < repetitions; repetition++) {
                        MessageDigest digest = MessageDigest.getInstance("SHA-256");
                        MetadataVerifier metadata = expectedMetadata == null ? null : new MetadataVerifier();
                        long before = counters.objects;
                        if (mode.equals("unchanged")) {
                            walkRange(client, watchdog, endpoint, bucket, pageSize, part.first(), null,
                                    part.upper(), part.count(), digest, metadata, counters);
                        } else {
                            walkRange(client, watchdog, endpoint, bucket, pageSize, part.first(), midpoint,
                                    midpoint, part.count() / 2, digest, metadata, counters);
                            walkRange(client, watchdog, endpoint, bucket, pageSize, midpoint, part.upper(),
                                    part.upper(), part.count() - part.count() / 2,
                                    digest, metadata, counters);
                        }
                        if (counters.objects - before != part.count()
                                || !HexFormat.of().formatHex(digest.digest()).equals(part.digest())) {
                            throw new IllegalStateException("partition inventory mismatch");
                        }
                        if (metadata != null && !metadata.finish().equals(expectedMetadata)) {
                            throw new IllegalStateException("partition metadata mismatch");
                        }
                    }
                    return counters.result(System.nanoTime() - activeStart);
                }));
            }
            List<Walk> walks = new ArrayList<>(futures.size());
            for (int i = 0; i < futures.size(); i++) walks.add(completion.take().get());
            return walks;
        } catch (Exception failure) {
            for (Future<Walk> future : futures) future.cancel(true);
            throw failure;
        }
    }

    private static void walkRange(HttpClient client, ScheduledThreadPoolExecutor watchdog,
                                  URI endpoint, String bucket, int pageSize, byte[] start,
                                  byte[] serverEnd, byte[] ownedUpper, long expectedCount,
                                  MessageDigest digest, MetadataVerifier metadata,
                                  Counters counters) throws Exception {
        String token = null;
        byte[] prior = null;
        long owned = 0;
        long requests = 0;
        while (true) {
            if (++requests > Math.min(1_000_000L, Math.max(2, expectedCount + 1))) {
                throw new IllegalStateException("GCS walk exceeded inventory-derived request bound");
            }
            URI uri = requestUri(endpoint, bucket, pageSize, start, serverEnd, token);
            HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(60)).GET().build();
            long begun = System.nanoTime();
            ATTEMPTED.incrementAndGet();
            HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() != 200) {
                response.body().close();
                throw new IllegalStateException("HTTP " + response.statusCode() + " from GCS listing");
            }
            CountingInputStream body = new CountingInputStream(response.body());
            AtomicBoolean timedOut = new AtomicBoolean();
            ScheduledFuture<?> deadline = watchdog.schedule(() -> {
                timedOut.set(true);
                try { body.close(); } catch (Exception ignored) { }
            }, BODY_DEADLINE.toNanos(), TimeUnit.NANOSECONDS);
            ReplayHttpBench.Page page;
            try (body) {
                try {
                    page = ReplayHttpBench.parseGcs(body, digest, prior, ownedUpper, metadata);
                    byte[] rest = new byte[8192];
                    int n;
                    while ((n = body.read(rest)) >= 0) {
                        for (int i = 0; i < n; i++) {
                            if (rest[i] != ' ' && rest[i] != '\n' && rest[i] != '\r' && rest[i] != '\t') {
                                throw new IllegalStateException("non-whitespace GCS response suffix");
                            }
                        }
                    }
                } catch (Exception failure) {
                    if (timedOut.get()) throw new IllegalStateException("body_timeout", failure);
                    throw failure;
                }
            } finally {
                deadline.cancel(false);
            }
            if (timedOut.get()) throw new IllegalStateException("body_timeout");
            if (response.headers().firstValueAsLong("Content-Length").orElse(body.bytes) != body.bytes) {
                throw new IllegalStateException("GCS response length mismatch");
            }
            if (page.count() > pageSize || page.nextToken() != null && page.count() == 0) {
                throw new IllegalStateException("GCS page violated limit or forward progress");
            }
            if (serverEnd != null && page.crossedUpper()) {
                throw new IllegalStateException("native endOffset emitted a key outside its range");
            }
            SUCCEEDED.incrementAndGet();
            counters.requests++;
            counters.objects += page.owned();
            counters.emitted += page.count();
            counters.bytes += body.bytes;
            counters.maxBody = Math.max(counters.maxBody, body.bytes);
            counters.latencies.add(System.nanoTime() - begun);
            owned += page.owned();
            if (page.crossedUpper() || page.nextToken() == null) break;
            if (page.nextToken().equals(token)) {
                throw new IllegalStateException("GCS continuation token repeated");
            }
            token = page.nextToken();
            prior = page.lastKey();
        }
        if (owned != expectedCount) {
            throw new IllegalStateException("GCS interval count mismatch");
        }
    }

    static URI requestUri(URI endpoint, String bucket, int limit, byte[] start, byte[] end,
                          String token) throws CharacterCodingException {
        String base = endpoint.toString().replaceAll("/$", "");
        String path = "/storage/v1/b/" + encode(bucket) + "/o?maxResults=" + limit;
        if (start != null) path += "&startOffset=" + encode(strictUtf8(start));
        if (end != null) path += "&endOffset=" + encode(strictUtf8(end));
        if (token != null) path += "&pageToken=" + encode(token);
        return URI.create(base + path);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String strictUtf8(byte[] value) throws CharacterCodingException {
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(value)).toString();
    }

    private record Walk(long objects, long emitted, long requests, long bytes, long maxBody,
                        Histogram latencies, long activeWallNanos) { }

    private static final class Counters {
        long objects;
        long emitted;
        long requests;
        long bytes;
        long maxBody;
        final Histogram latencies = new Histogram();
        Walk result(long activeWallNanos) {
            return new Walk(objects, emitted, requests, bytes, maxBody, latencies, activeWallNanos);
        }
    }

    private static final class CountingInputStream extends FilterInputStream {
        long bytes;
        CountingInputStream(InputStream input) { super(input); }
        @Override public int read() throws java.io.IOException {
            int result = super.read();
            if (result >= 0) bytes++;
            return result;
        }
        @Override public int read(byte[] buffer, int offset, int length) throws java.io.IOException {
            int result = in.read(buffer, offset, length);
            if (result > 0) bytes += result;
            return result;
        }
    }

    /** Same fixed relative-error latency bins as the flat HTTP benchmark. */
    private static final class Histogram {
        private final long[] bins = new long[4096];
        private long count;
        void add(long value) {
            int exponent = Math.max(0, 63 - Long.numberOfLeadingZeros(Math.max(1, value)));
            long floor = 1L << exponent;
            int sub = (int) Math.min(63, ((Math.max(1, value) - floor) * 64) / floor);
            bins[Math.min(bins.length - 1, exponent * 64 + sub)]++;
            count++;
        }
        void addAll(Histogram other) {
            for (int i = 0; i < bins.length; i++) bins[i] += other.bins[i];
            count += other.count;
        }
        long percentile(double fraction) {
            if (count == 0) throw new IllegalStateException("no latency samples");
            long target = (long) Math.ceil(count * fraction);
            long seen = 0;
            for (int i = 0; i < bins.length; i++) {
                seen += bins[i];
                if (seen >= target) {
                    long floor = 1L << (i / 64);
                    return floor + (floor * (i % 64 + 1)) / 64;
                }
            }
            throw new IllegalStateException("latency histogram omitted its target");
        }
    }
}
