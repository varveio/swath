/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadFeature;
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
import java.util.Properties;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

/** Standalone, out-of-process replay driver. Compile against the frozen replay distribution. */
public final class ReplayHttpBench {
    private static final int MAX_REQUESTS_PER_WALK = 1_000_000;
    private static final JsonFactory JSON = JsonFactory.builder()
            .disable(StreamReadFeature.AUTO_CLOSE_SOURCE).build();
    private static final AtomicLong ATTEMPTED = new AtomicLong();
    private static final AtomicLong SUCCEEDED = new AtomicLong();
    private static final AtomicLong OUTSTANDING = new AtomicLong();
    private static final AtomicLong PEAK_OUTSTANDING = new AtomicLong();
    private static final Duration BODY_DEADLINE = Duration.ofSeconds(30);

    private ReplayHttpBench() {}

    public static void main(String[] argv) throws Exception {
        if (argv.length == 3 && "--inventory".equals(argv[0])) {
            Inventory inventory = fixtureInventory(argv[1], Integer.parseInt(argv[2]));
            StringBuilder out = new StringBuilder("{\"fixture_count\":").append(inventory.count())
                    .append(",\"fixture_key_bytes\":").append(inventory.keyBytes())
                    .append(",\"fixture_digest\":\"").append(inventory.digest()).append("\",\"partitions\":[");
            for (Partition part : inventory.partitions()) {
                if (out.charAt(out.length() - 1) != '[') out.append(',');
                out.append("{\"first_hex\":\"").append(hex(part.first()))
                        .append("\",\"predecessor_hex\":\"").append(hex(part.predecessor()))
                        .append("\",\"upper_hex\":\"").append(hex(part.upper()))
                        .append("\",\"count\":").append(part.count())
                        .append(",\"digest\":\"").append(part.digest()).append("\"}");
            }
            System.out.println(out.append("]}").toString());
            return;
        }
        boolean endAck = argv.length > 0 && "end_ack".equals(argv[argv.length - 1]);
        int fields = argv.length - (endAck ? 1 : 0);
        if (fields < 7 || fields > 11 || fields >= 9 && !"bracket".equals(argv[8])
                || fields == 11 && !"partitioned".equals(argv[9])) {
            System.err.println("usage: ReplayHttpBench ENDPOINT s3|gcs|azure BUCKET FIXTURE_GLOB CLIENTS PAGE_SIZE WARMUP_WALKS [COUNT:DIGEST [bracket [partitioned REPS] [end_ack]]]");
            System.exit(2);
        }
        URI endpoint = URI.create(argv[0]);
        String protocol = argv[1].toLowerCase(Locale.ROOT);
        if (!List.of("s3", "gcs", "azure").contains(protocol)) throw new IllegalArgumentException("protocol");
        String bucket = argv[2];
        String fixture = argv[3];
        int clients = Integer.parseInt(argv[4]);
        int pageSize = Integer.parseInt(argv[5]);
        int warmup = Integer.parseInt(argv[6]);
        boolean partitioned = fields == 11;
        int repetitions = partitioned ? Integer.parseInt(argv[10]) : 1;
        if (clients < 1 || clients > 512 || pageSize < 1 || pageSize > 5000 || warmup < 0 || warmup > 10)
            throw new IllegalArgumentException("clients/page size/warmup out of range");
        if (repetitions < 1) throw new IllegalArgumentException("repetitions must be positive");

        Inventory expected = partitioned ? fixtureInventory(fixture, clients)
                : fields >= 8 ? parseInventory(argv[7]) : fixtureInventory(fixture, 0);
        if (partitioned && fields >= 8) {
            Inventory supplied = parseInventory(argv[7]);
            if (supplied.count() != expected.count() || !supplied.digest().equals(expected.digest()))
                throw new IllegalStateException("fixture changed since declared inventory");
        }
        FixtureMetadataOracle.Plan metadataPlan = warmup == 0 ? null
                : FixtureMetadataOracle.plan(fixture, protocol, partitioned ? clients : 0);
        if (metadataPlan != null && metadataPlan.total().count() != expected.count())
            throw new IllegalStateException("fixture metadata oracle count differs from key inventory");
        long start;
        long elapsed;
        List<Walk> walks = new ArrayList<>();
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
                .version(HttpClient.Version.HTTP_1_1).build();
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        ScheduledThreadPoolExecutor watchdog = new ScheduledThreadPoolExecutor(1);
        watchdog.setRemoveOnCancelPolicy(true);
        long warmupAttempted;
        long warmupSuccessful;
        try {
            for (int w = 0; w < warmup; w++)
                runBatch(client, watchdog, executor, endpoint, protocol, bucket, pageSize, clients, expected,
                        partitioned, repetitions, metadataPlan);
            warmupAttempted = ATTEMPTED.get();
            warmupSuccessful = SUCCEEDED.get();
            if (warmupAttempted != warmupSuccessful)
                throw new IllegalStateException("warmup request outcomes disagree");
            ATTEMPTED.set(0);
            SUCCEEDED.set(0);
            if (OUTSTANDING.get() != 0) throw new IllegalStateException("warmup left an outstanding request");
            PEAK_OUTSTANDING.set(0);
            System.out.println("{\"event\":\"MEASURE_START\"}");
            System.out.flush();
            if (fields >= 9 && System.in.read() < 0)
                throw new IllegalStateException("measurement controller did not acknowledge start");
            start = System.nanoTime();
            try {
                walks.addAll(runBatch(client, watchdog, executor, endpoint, protocol, bucket, pageSize, clients,
                        expected, partitioned, repetitions, null));
            } catch (Exception failure) {
                executor.shutdownNow();
                client.shutdownNow();
                System.out.printf("{\"status\":\"failed\",\"attempted_requests\":%d,"
                        + "\"successful_requests\":%d}%n", ATTEMPTED.get(), SUCCEEDED.get());
                throw failure;
            }
            elapsed = System.nanoTime() - start;
            System.out.println("{\"event\":\"MEASURE_END\"}");
            System.out.flush();
            if (endAck && System.in.read() < 0)
                throw new IllegalStateException("measurement controller did not acknowledge end");
        } finally {
            watchdog.shutdownNow();
            executor.shutdownNow();
            client.shutdownNow();
        }
        long objects = 0, emitted = 0, requests = 0, bytes = 0, maxBody = 0, activeWallNanos = 0;
        LatencyHistogram latencies = new LatencyHistogram();
        for (Walk walk : walks) {
            objects += walk.count();
            emitted += walk.emitted();
            requests += walk.requests();
            bytes += walk.bytes();
            maxBody = Math.max(maxBody, walk.maxResponseBytes());
            activeWallNanos += walk.activeWallNanos();
            latencies.addAll(walk.latencies());
        }
        System.out.printf(Locale.ROOT,
                "{\"protocol\":\"%s\",\"clients\":%d,\"page_size\":%d,\"fixture_count\":%d,"
                + "\"fixture_digest\":\"%s\",\"partitioned\":%s,\"repetitions\":%d,"
                + "\"objects\":%d,\"emitted_objects\":%d,\"requests\":%d,\"bytes\":%d,"
                + "\"max_response_bytes\":%d,"
                + "\"elapsed_ns\":%d,\"objects_per_s\":%.3f,\"requests_per_s\":%.3f,"
                + "\"bytes_per_s\":%.3f,\"p50_ns\":%d,\"p95_ns\":%d,\"p99_ns\":%d,"
                + "\"latency_histogram_relative_error_max\":0.016,"
                + "\"client_active_wall_ns\":%d,\"tail_dilution_ratio\":%.6f,"
                + "\"metadata_profile\":\"%s\",\"warmup_metadata_verified_objects\":%d,"
                + "\"warmup_attempted_requests\":%d,\"warmup_successful_requests\":%d,"
                + "\"attempted_requests\":%d,\"successful_requests\":%d,"
                + "\"peak_outstanding_requests\":%d}%n",
                protocol, clients, pageSize, expected.count(), expected.digest(), partitioned, repetitions,
                objects, emitted, requests, bytes, maxBody,
                elapsed, objects * 1e9 / elapsed, requests * 1e9 / elapsed, bytes * 1e9 / elapsed,
                latencies.percentile(.50), latencies.percentile(.95), latencies.percentile(.99),
                activeWallNanos, (double) activeWallNanos / (clients * elapsed),
                metadataPlan == null ? "none" : "fixture_name_size_time",
                metadataPlan == null ? 0 : expected.count() * repetitions * warmup
                        * (partitioned ? 1 : clients),
                warmupAttempted, warmupSuccessful,
                ATTEMPTED.get(), SUCCEEDED.get(), PEAK_OUTSTANDING.get());
    }

    private static List<Walk> runBatch(HttpClient client, ScheduledThreadPoolExecutor watchdog,
                                       java.util.concurrent.ExecutorService executor, URI endpoint,
                                       String protocol, String bucket, int pageSize, int clients, Inventory expected,
                                       boolean partitioned, int repetitions,
                                       FixtureMetadataOracle.Plan metadataPlan) throws Exception {
        if (partitioned && expected.partitions().size() != clients)
            throw new IllegalStateException("partition plan does not match client count");
        ExecutorCompletionService<Walk> completion = new ExecutorCompletionService<>(executor);
        List<Future<Walk>> futures = new ArrayList<>(clients);
        try {
            for (int i = 0; i < clients; i++) {
                final Partition partition = partitioned ? expected.partitions().get(i) : null;
                final FixtureMetadataOracle.Digest expectedMetadata = metadataPlan == null ? null
                        : partitioned ? metadataPlan.partitions().get(i) : metadataPlan.total();
                futures.add(completion.submit((Callable<Walk>) () -> {
                    long activeStart = System.nanoTime();
                    List<Walk> repeated = new ArrayList<>(repetitions);
                    for (int repetition = 0; repetition < repetitions; repetition++) {
                        MetadataVerifier metadata = expectedMetadata == null ? null : new MetadataVerifier();
                        Walk result = walk(client, watchdog, endpoint, protocol, bucket, pageSize,
                                partition == null ? expected.count() : partition.count(), partition, BODY_DEADLINE,
                                metadata);
                        check(partition == null ? expected : partition.inventory(), result);
                        if (metadata != null && !metadata.finish().equals(expectedMetadata))
                            throw new IllegalStateException("fixture-backed metadata inventory mismatch");
                        repeated.add(result);
                    }
                    Walk combined = combine(repeated);
                    return new Walk(combined.count(), combined.emitted(), combined.requests(),
                            combined.bytes(), combined.maxResponseBytes(), combined.digest(),
                            combined.latencies(), System.nanoTime() - activeStart);
                }));
            }
            List<Walk> walks = new ArrayList<>(clients);
            for (int i = 0; i < clients; i++) walks.add(completion.take().get());
            return walks;
        } catch (Exception failure) {
            for (Future<Walk> future : futures) future.cancel(true);
            throw failure;
        }
    }

    private static Walk combine(List<Walk> walks) {
        long count = 0, emitted = 0, requests = 0, bytes = 0, maxBody = 0;
        LatencyHistogram latencies = new LatencyHistogram();
        for (Walk walk : walks) {
            count += walk.count();
            emitted += walk.emitted();
            requests += walk.requests();
            bytes += walk.bytes();
            maxBody = Math.max(maxBody, walk.maxResponseBytes());
            latencies.addAll(walk.latencies());
        }
        return new Walk(count, emitted, requests, bytes, maxBody, "repeated", latencies, 0);
    }

    static Inventory fixtureInventory(String fixture, int partitionCount) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        long count = 0, keyBytes = 0;
        String escaped = fixture.replace("'", "''");
        Properties config = new Properties();
        config.setProperty("jdbc_stream_results", "true");
        List<PartitionBuilder> builders = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:", config);
             Statement stmt = conn.createStatement()) {
            long total = 0;
            if (partitionCount > 0) {
                try (ResultSet totalRows = stmt.executeQuery(
                        "SELECT count(*) FROM read_parquet('" + escaped + "')")) {
                    totalRows.next();
                    total = totalRows.getLong(1);
                }
                if (total < partitionCount)
                    throw new IllegalArgumentException("partitioned run needs at least one key per client");
                for (int i = 0; i < partitionCount; i++) builders.add(new PartitionBuilder());
            }
            String type;
            try (ResultSet types = stmt.executeQuery(
                    "SELECT typeof(key) FROM read_parquet('" + escaped + "') LIMIT 1")) {
                type = types.next() ? types.getString(1) : "BLOB";
            }
            String expression = switch (type.toUpperCase(Locale.ROOT)) {
                case "BLOB" -> "key";
                case "VARCHAR" -> "encode(key)";
                default -> throw new IllegalStateException("unsupported fixture key type " + type);
            };
            try (ResultSet rows = stmt.executeQuery(
                    "SELECT " + expression + " FROM read_parquet('" + escaped + "')")) {
            byte[] prior = null;
            while (rows.next()) {
                byte[] key = rows.getBytes(1);
                if (key == null || prior != null && compareUnsigned(prior, key) >= 0)
                    throw new IllegalStateException("fixture is not strictly byte-sorted at row " + count);
                digestKey(digest, key);
                keyBytes += key.length;
                if (partitionCount > 0) {
                    int index = (int) Math.min(partitionCount - 1, count * partitionCount / total);
                    PartitionBuilder builder = builders.get(index);
                    if (builder.first == null) {
                        builder.first = key.clone();
                        builder.predecessor = prior == null ? null : prior.clone();
                        if (index > 0) builders.get(index - 1).upper = key.clone();
                    }
                    digestKey(builder.digest, key);
                    builder.count++;
                }
                prior = key;
                count++;
            }
            }
        }
        List<Partition> partitions = new ArrayList<>(partitionCount);
        for (PartitionBuilder builder : builders) {
            partitions.add(new Partition(builder.first, builder.predecessor, builder.upper,
                    builder.count, HexFormat.of().formatHex(builder.digest.digest())));
        }
        return new Inventory(count, HexFormat.of().formatHex(digest.digest()), partitions, partitionCount, keyBytes);
    }

    private static Inventory parseInventory(String value) {
        String[] fields = value.split(":", -1);
        if (fields.length != 2 || !fields[1].matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("expected COUNT:DIGEST");
        return new Inventory(Long.parseLong(fields[0]), fields[1], List.of(), 0, -1);
    }

    private static void check(Inventory expected, Walk walk) {
        if (expected.count() != walk.count() || !expected.digest().equals(walk.digest()))
            throw new IllegalStateException("inventory mismatch: expected " + expected + " observed " + walk);
    }

    private static Walk walk(HttpClient client, ScheduledThreadPoolExecutor watchdog, URI endpoint, String protocol,
                             String bucket, int pageSize, long expectedCount, Partition partition,
                             Duration bodyDeadline, MetadataVerifier metadata) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        long count = 0, emitted = 0, bytes = 0, requests = 0, maxBody = 0;
        String continuation = null;
        byte[] prior = null;
        LatencyHistogram latencies = new LatencyHistogram();
        while (true) {
            if (++requests > Math.min(MAX_REQUESTS_PER_WALK, Math.max(2, expectedCount + 1)))
                throw new IllegalStateException("pagination exceeded inventory-derived request bound");
            URI uri = requestUri(endpoint, protocol, bucket, pageSize, continuation, partition);
            HttpRequest.Builder builder = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(60)).GET();
            if (protocol.equals("azure")) builder.header("x-ms-version", "2026-06-06");
            long begun = System.nanoTime();
            ATTEMPTED.incrementAndGet();
            long outstanding = OUTSTANDING.incrementAndGet();
            PEAK_OUTSTANDING.accumulateAndGet(outstanding, Math::max);
            try {
            HttpResponse<InputStream> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() != 200) {
                response.body().close();
                throw new IllegalStateException("HTTP " + response.statusCode() + " from " + uri);
            }
            CountingInputStream body = new CountingInputStream(response.body());
            Page page;
            AtomicBoolean timedOut = new AtomicBoolean();
            ScheduledFuture<?> deadline = watchdog.schedule(() -> {
                timedOut.set(true);
                try { body.close(); } catch (Exception ignored) { }
            }, bodyDeadline.toNanos(), TimeUnit.NANOSECONDS);
            try (body) {
                try {
                    byte[] upper = partition == null ? null : partition.upper();
                    page = protocol.equals("gcs") ? parseGcs(body, digest, prior, upper, metadata)
                            : parseXml(body, protocol, digest, prior, upper, metadata);
                    byte[] rest = new byte[8192];
                    int n;
                    while ((n = body.read(rest)) >= 0) {
                        for (int j = 0; j < n; j++) {
                            if (rest[j] != ' ' && rest[j] != '\n' && rest[j] != '\r' && rest[j] != '\t')
                                throw new IllegalStateException("non-whitespace trailing response bytes");
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
            if (page.count() > pageSize) throw new IllegalStateException("page exceeded declared limit");
            if (page.nextToken() != null && page.count() == 0)
                throw new IllegalStateException("empty nonfinal page cannot make progress in flat walk");
            latencies.add(System.nanoTime() - begun);
            SUCCEEDED.incrementAndGet();
            count += page.owned();
            emitted += page.count();
            bytes += body.bytes;
            maxBody = Math.max(maxBody, body.bytes);
            prior = page.lastKey() == null ? prior : page.lastKey();
            if (page.crossedUpper()) break;
            if (page.nextToken() == null) break;
            if (page.nextToken().equals(continuation)) throw new IllegalStateException("repeated continuation token");
            continuation = page.nextToken();
            } finally {
                OUTSTANDING.decrementAndGet();
            }
        }
        return new Walk(count, emitted, requests, bytes, maxBody,
                HexFormat.of().formatHex(digest.digest()), latencies, 0);
    }

    /** Small stub-server seam for adversarial parser, pagination, and body-deadline tests. */
    static Walk walkForTest(URI endpoint, String protocol, String bucket, int pageSize,
                            long expectedCount, Duration bodyDeadline) throws Exception {
        HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        ScheduledThreadPoolExecutor watchdog = new ScheduledThreadPoolExecutor(1);
        watchdog.setRemoveOnCancelPolicy(true);
        try {
            return walk(client, watchdog, endpoint, protocol, bucket, pageSize,
                    expectedCount, null, bodyDeadline, null);
        } finally {
            watchdog.shutdownNow();
            client.shutdownNow();
        }
    }

    static URI requestUri(URI endpoint, String protocol, String bucket, int limit, String token,
                          Partition partition) throws CharacterCodingException {
        String base = endpoint.toString().replaceAll("/$", "");
        String path;
        if (protocol.equals("s3")) {
            path = "/" + encode(bucket) + "?list-type=2&encoding-type=url&max-keys=" + limit;
            if (token != null) path += "&continuation-token=" + encode(token);
            else if (partition != null && partition.predecessor() != null)
                path += "&start-after=" + encode(strictUtf8(partition.predecessor()));
        } else if (protocol.equals("gcs")) {
            path = "/storage/v1/b/" + encode(bucket) + "/o?maxResults=" + limit;
            if (token != null) path += "&pageToken=" + encode(token);
            if (partition != null && partition.first() != null)
                path += "&startOffset=" + encode(strictUtf8(partition.first()));
        } else {
            path = "/replay/" + encode(bucket) + "?restype=container&comp=list&maxresults=" + limit;
            if (token != null) path += "&marker=" + encode(token);
            if (partition != null && partition.first() != null)
                path += "&startFrom=" + encode(strictUtf8(partition.first()));
        }
        return URI.create(base + path);
    }

    private static String encode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String strictUtf8(byte[] key) throws CharacterCodingException {
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(key)).toString();
    }

    private static String hex(byte[] key) {
        return key == null ? "" : HexFormat.of().formatHex(key);
    }

    static Page parseXml(InputStream body, String protocol, MessageDigest digest, byte[] prior,
                         byte[] upper) throws Exception {
        return parseXml(body, protocol, digest, prior, upper, null);
    }

    static Page parseXml(InputStream body, String protocol, MessageDigest digest, byte[] prior,
                         byte[] upper, MetadataVerifier metadata) throws Exception {
        XMLInputFactory factory = XMLInputFactory.newFactory();
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty("javax.xml.stream.isSupportingExternalEntities", false);
        XMLStreamReader xml = factory.createXMLStreamReader(body);
        String item = protocol.equals("s3") ? "Contents" : "Blob";
        String name = protocol.equals("s3") ? "Key" : "Name";
        String tokenTag = protocol.equals("s3") ? "NextContinuationToken" : "NextMarker";
        int depth = 0;
        long count = 0, owned = 0;
        boolean crossedUpper = false;
        boolean truncated = false;
        String token = null;
        byte[] last = null;
        byte[] itemKey = null;
        long itemSize = 0, itemTime = 0;
        boolean itemOwned = false, hasSize = false, hasTime = false;
        long declaredCount = -1;
        String root = null;
        try {
            while (xml.hasNext()) {
                int event = xml.next();
                if (event == XMLStreamConstants.START_ELEMENT) {
                    String tag = xml.getLocalName();
                    if (root == null) root = tag;
                    if (tag.equals(item)) {
                        depth++;
                        if (metadata != null) {
                            itemKey = null;
                            itemOwned = false;
                            hasSize = hasTime = false;
                        }
                    }
                    else if (depth == 1 && tag.equals(name)) {
                        boolean encoded = protocol.equals("s3") || protocol.equals("azure")
                                && "true".equalsIgnoreCase(xml.getAttributeValue(null, "Encoded"));
                        String text = xml.getElementText();
                        byte[] key = encoded ? percentDecode(text) : text.getBytes(StandardCharsets.UTF_8);
                        if (compareUnsigned(last == null ? prior : last, key) >= 0)
                            throw new IllegalStateException("duplicate/out-of-order key");
                        if (upper == null || compareUnsigned(key, upper) < 0) {
                            digestKey(digest, key);
                            owned++;
                            if (metadata != null) itemOwned = true;
                        } else crossedUpper = true;
                        if (metadata != null) itemKey = key;
                        last = key;
                        count++;
                    } else if (depth == 1 && metadata != null
                            && tag.equals(protocol.equals("s3") ? "Size" : "Content-Length")) {
                        itemSize = Long.parseLong(xml.getElementText().trim());
                        hasSize = true;
                    } else if (depth == 1 && metadata != null
                            && tag.equals(protocol.equals("s3") ? "LastModified" : "Last-Modified")) {
                        String timestamp = xml.getElementText();
                        itemTime = protocol.equals("s3") ? Instant.parse(timestamp).toEpochMilli()
                                : ZonedDateTime.parse(timestamp, DateTimeFormatter.RFC_1123_DATE_TIME)
                                        .toEpochSecond();
                        hasTime = true;
                    } else if (tag.equals(tokenTag)) token = xml.getElementText();
                    else if (tag.equals("IsTruncated")) truncated = Boolean.parseBoolean(xml.getElementText());
                    else if (protocol.equals("s3") && tag.equals("KeyCount"))
                        declaredCount = Long.parseLong(xml.getElementText());
                } else if (event == XMLStreamConstants.END_ELEMENT && xml.getLocalName().equals(item)) {
                    if (metadata != null && itemOwned) {
                        if (itemKey == null || !hasSize || !hasTime)
                            throw new IllegalStateException("listed object omitted fixture-backed metadata");
                        metadata.accept(itemKey, itemSize, itemTime);
                    }
                    depth--;
                }
            }
        } finally {
            xml.close();
        }
        if (!root.equals(protocol.equals("s3") ? "ListBucketResult" : "EnumerationResults"))
            throw new IllegalStateException("unexpected XML response root: " + root);
        if (protocol.equals("s3") && declaredCount != count)
            throw new IllegalStateException("S3 KeyCount differs from content entries");
        if (protocol.equals("s3")) {
            if (truncated && (token == null || token.isEmpty())) throw new IllegalStateException("truncated without token");
            if (!truncated) token = null;
        } else if (token != null && token.isEmpty()) token = null;
        return new Page(count, owned, token, last, crossedUpper);
    }

    static Page parseGcs(InputStream body, MessageDigest digest, byte[] prior, byte[] upper) throws Exception {
        return parseGcs(body, digest, prior, upper, null);
    }

    static Page parseGcs(InputStream body, MessageDigest digest, byte[] prior, byte[] upper,
                         MetadataVerifier metadata) throws Exception {
        long count = 0, owned = 0;
        boolean crossedUpper = false;
        String token = null;
        byte[] last = null;
        try (JsonParser json = JSON.createParser(body)) {
            if (json.nextToken() != JsonToken.START_OBJECT) throw new IllegalStateException("GCS root is not object");
            while (next(json) != JsonToken.END_OBJECT) {
                String field = json.currentName();
                JsonToken value = next(json);
                if ("nextPageToken".equals(field)) token = json.getValueAsString();
                else if ("items".equals(field) && value == JsonToken.START_ARRAY) {
                    while (next(json) != JsonToken.END_ARRAY) {
                        if (json.currentToken() != JsonToken.START_OBJECT) throw new IllegalStateException("bad item");
                        String name = null;
                        long size = 0, updatedMicros = 0;
                        boolean hasSize = false, hasUpdated = false;
                        while (next(json) != JsonToken.END_OBJECT) {
                            String itemField = json.currentName();
                            next(json);
                            if ("name".equals(itemField)) {
                                if (json.currentToken() != JsonToken.VALUE_STRING || name != null)
                                    throw new IllegalStateException("duplicate or non-string GCS name");
                                name = json.getText();
                            } else if (metadata != null && "size".equals(itemField)) {
                                if (json.currentToken() != JsonToken.VALUE_STRING || hasSize)
                                    throw new IllegalStateException("missing/duplicate GCS size");
                                size = Long.parseLong(json.getText());
                                hasSize = true;
                            } else if (metadata != null && "updated".equals(itemField)) {
                                if (json.currentToken() != JsonToken.VALUE_STRING || hasUpdated)
                                    throw new IllegalStateException("missing/duplicate GCS updated");
                                Instant time = Instant.parse(json.getText());
                                updatedMicros = Math.addExact(Math.multiplyExact(time.getEpochSecond(), 1_000_000L),
                                        time.getNano() / 1_000L);
                                hasUpdated = true;
                            } else json.skipChildren();
                        }
                        if (name == null) throw new IllegalStateException("item without name");
                        byte[] key = name.getBytes(StandardCharsets.UTF_8);
                        if (compareUnsigned(last == null ? prior : last, key) >= 0)
                            throw new IllegalStateException("duplicate/out-of-order key");
                        if (upper == null || compareUnsigned(key, upper) < 0) {
                            digestKey(digest, key);
                            owned++;
                            if (metadata != null) {
                                if (!hasSize || !hasUpdated)
                                    throw new IllegalStateException("listed GCS object omitted fixture-backed metadata");
                                metadata.accept(key, size, updatedMicros);
                            }
                        } else crossedUpper = true;
                        last = key;
                        count++;
                    }
                } else json.skipChildren();
            }
            if (json.nextToken() != null) throw new IllegalStateException("trailing GCS JSON tokens");
        }
        return new Page(count, owned, token == null || token.isEmpty() ? null : token, last, crossedUpper);
    }

    private static void digestKey(MessageDigest digest, byte[] key) {
        digest.update((byte) (key.length >>> 24));
        digest.update((byte) (key.length >>> 16));
        digest.update((byte) (key.length >>> 8));
        digest.update((byte) key.length);
        digest.update(key);
    }

    private static JsonToken next(JsonParser parser) throws Exception {
        JsonToken token = parser.nextToken();
        if (token == null) throw new IllegalStateException("truncated GCS JSON");
        return token;
    }

    static byte[] percentDecode(String text) {
        long byteCount = 0;
        for (int i = 0; i < text.length();) {
            char c = text.charAt(i);
            if (c == '%') {
                if (i + 2 >= text.length() || asciiHex(text.charAt(i + 1)) < 0
                        || asciiHex(text.charAt(i + 2)) < 0) {
                    throw new IllegalStateException("bad percent escape");
                }
                byteCount++;
                i += 3;
            } else if (c < 0x80) {
                byteCount++;
                i++;
            } else if (c < 0x800) {
                byteCount += 2;
                i++;
            } else if (Character.isHighSurrogate(c)) {
                if (i + 1 >= text.length() || !Character.isLowSurrogate(text.charAt(i + 1)))
                    throw new IllegalStateException("unpaired UTF-16 surrogate");
                byteCount += 4;
                i += 2;
            } else if (Character.isLowSurrogate(c)) {
                throw new IllegalStateException("unpaired UTF-16 surrogate");
            } else {
                byteCount += 3;
                i++;
            }
            if (byteCount > Integer.MAX_VALUE) throw new IllegalStateException("decoded name too long");
        }
        byte[] out = new byte[(int) byteCount];
        int at = 0;
        for (int i = 0; i < text.length();) {
            char c = text.charAt(i);
            if (c == '%') {
                out[at++] = (byte) ((asciiHex(text.charAt(i + 1)) << 4)
                        | asciiHex(text.charAt(i + 2)));
                i += 3;
            } else if (c < 0x80) {
                out[at++] = (byte) c;
                i++;
            } else if (c < 0x800) {
                out[at++] = (byte) (0xc0 | c >>> 6);
                out[at++] = (byte) (0x80 | c & 0x3f);
                i++;
            } else if (Character.isHighSurrogate(c)) {
                int codePoint = Character.toCodePoint(c, text.charAt(i + 1));
                out[at++] = (byte) (0xf0 | codePoint >>> 18);
                out[at++] = (byte) (0x80 | codePoint >>> 12 & 0x3f);
                out[at++] = (byte) (0x80 | codePoint >>> 6 & 0x3f);
                out[at++] = (byte) (0x80 | codePoint & 0x3f);
                i += 2;
            } else {
                out[at++] = (byte) (0xe0 | c >>> 12);
                out[at++] = (byte) (0x80 | c >>> 6 & 0x3f);
                out[at++] = (byte) (0x80 | c & 0x3f);
                i++;
            }
        }
        return out;
    }

    private static int asciiHex(char value) {
        if (value >= '0' && value <= '9') return value - '0';
        if (value >= 'a' && value <= 'f') return value - 'a' + 10;
        if (value >= 'A' && value <= 'F') return value - 'A' + 10;
        return -1;
    }

    private static int compareUnsigned(byte[] a, byte[] b) {
        return a == null ? -1 : Arrays.compareUnsigned(a, b);
    }

    record Inventory(long count, String digest, List<Partition> partitions, int clients, long keyBytes) {}
    record Partition(byte[] first, byte[] predecessor, byte[] upper, long count, String digest) {
        Inventory inventory() { return new Inventory(count, digest, List.of(), 0, -1); }
    }
    private static final class PartitionBuilder {
        byte[] first;
        byte[] predecessor;
        byte[] upper;
        long count;
        final MessageDigest digest;
        PartitionBuilder() throws Exception { digest = MessageDigest.getInstance("SHA-256"); }
    }
    record Page(long count, long owned, String nextToken, byte[] lastKey, boolean crossedUpper) {}
    record Walk(long count, long emitted, long requests, long bytes, long maxResponseBytes, String digest,
                LatencyHistogram latencies, long activeWallNanos) {}

    private static final class CountingInputStream extends FilterInputStream {
        long bytes;
        CountingInputStream(InputStream in) { super(in); }
        @Override public int read() throws java.io.IOException {
            int value = super.read();
            if (value >= 0) bytes++;
            return value;
        }
        @Override public int read(byte[] b, int off, int len) throws java.io.IOException {
            int n = in.read(b, off, len);
            if (n > 0) bytes += n;
            return n;
        }
    }

    /** Fixed 4096-bin, base-2 histogram; each bucket is at most 1/64 of its range wide. */
    private static final class LatencyHistogram {
        private final long[] bins = new long[4096];
        private long count;
        void add(long value) {
            int exponent = Math.max(0, 63 - Long.numberOfLeadingZeros(Math.max(1, value)));
            long floor = 1L << exponent;
            int sub = (int) Math.min(63, ((Math.max(1, value) - floor) * 64) / floor);
            bins[Math.min(bins.length - 1, exponent * 64 + sub)]++;
            count++;
        }
        void addAll(LatencyHistogram other) {
            for (int i = 0; i < bins.length; i++) bins[i] += other.bins[i];
            count += other.count;
        }
        long percentile(double p) {
            if (count == 0) throw new IllegalStateException("no latency samples");
            long target = (long) Math.ceil(count * p);
            long observed = 0;
            for (int i = 0; i < bins.length; i++) {
                observed += bins[i];
                if (observed >= target) {
                    long floor = 1L << (i / 64);
                    return floor + (floor * (i % 64 + 1)) / 64;
                }
            }
            return 0;
        }
    }
}
