/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

/** Deterministic one-key seek and delimiter probes against provider-native listing routes. */
public final class ReplayShapeBench {
    private static final Duration BODY_DEADLINE = Duration.ofSeconds(30);
    private static final int MAX_BODY_BYTES = 64 * 1024 * 1024;
    private static final JsonFactory JSON = new JsonFactory();
    private static final AtomicLong ATTEMPTED = new AtomicLong();
    private static final AtomicLong SUCCEEDED = new AtomicLong();

    private ReplayShapeBench() { }

    public static void main(String[] argv) throws Exception {
        boolean endAck = argv.length > 0 && "end_ack".equals(argv[argv.length - 1]);
        int fields = argv.length - (endAck ? 1 : 0);
        if ((fields != 11 && fields != 12) || !"bracket".equals(argv[8])) {
            throw new IllegalArgumentException("usage: ReplayShapeBench ENDPOINT PROTOCOL BUCKET FIXTURE_GLOB "
                    + "CLIENTS PAGE_SIZE WARMUP_WALKS COUNT:DIGEST bracket seek|delimiter REPETITIONS [PREFIX] [end_ack]");
        }
        URI endpoint = URI.create(argv[0]);
        String protocol = argv[1].toLowerCase(Locale.ROOT);
        if (!Set.of("s3", "gcs", "azure").contains(protocol)) throw new IllegalArgumentException("protocol");
        String bucket = argv[2];
        String fixture = argv[3];
        int clients = Integer.parseInt(argv[4]);
        int pageSize = Integer.parseInt(argv[5]);
        int warmups = Integer.parseInt(argv[6]);
        String[] declared = argv[7].split(":", -1);
        String mode = argv[9];
        int repetitions = Integer.parseInt(argv[10]);
        String prefix = fields == 12 ? argv[11] : null;
        if (clients < 1 || clients > 512 || pageSize < 1 || pageSize > 5000
                || warmups < 0 || warmups > 10 || repetitions < 1 || repetitions > 1_000_000
                || Math.multiplyExact(clients, repetitions) > 1_000_000
                || !Set.of("seek", "delimiter").contains(mode)
                || mode.equals("seek") && (pageSize != 1 || prefix != null)
                || mode.equals("delimiter") && (prefix == null || prefix.isEmpty())) {
            throw new IllegalArgumentException("invalid shape run settings");
        }
        if (declared.length != 2 || !declared[1].matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("expected COUNT:DIGEST");
        }
        ReplayHttpBench.Inventory fixtureInventory = ReplayHttpBench.fixtureInventory(fixture, 0);
        if (fixtureInventory.count() != Long.parseLong(declared[0])
                || !fixtureInventory.digest().equals(declared[1])) {
            throw new IllegalStateException("fixture changed since declared inventory");
        }
        Plan plan = mode.equals("seek")
                ? seekPlan(fixture, clients * repetitions, fixtureInventory.count())
                : delimiterPlan(fixture, prefix);
        FixtureMetadataOracle.Digest delimiterMetadata = warmups > 0 && mode.equals("delimiter")
                ? FixtureMetadataOracle.planDelimiter(fixture, protocol, prefix, "/") : null;
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
                .version(HttpClient.Version.HTTP_1_1).build();
        ScheduledThreadPoolExecutor watchdog = new ScheduledThreadPoolExecutor(1);
        watchdog.setRemoveOnCancelPolicy(true);
        long started;
        long elapsed;
        Batch measured;
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                for (int i = 0; i < warmups; i++) {
                    runBatch(workers, watchdog, client, endpoint, protocol, bucket, mode,
                            pageSize, clients, repetitions, prefix, plan, true, delimiterMetadata);
                }
                ATTEMPTED.set(0);
                SUCCEEDED.set(0);
                System.out.println("{\"event\":\"MEASURE_START\"}");
                System.out.flush();
                if (System.in.read() < 0) throw new IllegalStateException("measurement ACK missing");
                started = System.nanoTime();
                try {
                    measured = runBatch(workers, watchdog, client, endpoint, protocol, bucket, mode,
                            pageSize, clients, repetitions, prefix, plan, false, null);
                } catch (Exception e) {
                    System.out.printf("{\"status\":\"failed\",\"attempted_requests\":%d,"
                            + "\"successful_requests\":%d}%n", ATTEMPTED.get(), SUCCEEDED.get());
                    throw e;
                }
                elapsed = System.nanoTime() - started;
                System.out.println("{\"event\":\"MEASURE_END\"}");
                System.out.flush();
                if (endAck && System.in.read() < 0)
                    throw new IllegalStateException("measurement end ACK missing");
            } finally {
                watchdog.shutdownNow();
                client.shutdownNow();
            }
        }
        System.out.printf(Locale.ROOT,
                "{\"protocol\":\"%s\",\"workload\":\"%s\",\"clients\":%d,\"page_size\":%d,"
                + "\"fixture_count\":%d,\"fixture_digest\":\"%s\",\"objects\":%d,"
                + "\"emitted_objects\":%d,\"common_prefixes\":%d,\"requests\":%d,\"bytes\":%d,"
                + "\"elapsed_ns\":%d,\"objects_per_s\":%.3f,\"requests_per_s\":%.3f,"
                + "\"bytes_per_s\":%.3f,\"p50_ns\":%d,\"p95_ns\":%d,\"p99_ns\":%d,"
                + "\"metadata_profile\":\"%s\",\"warmup_metadata_verified_objects\":%d,"
                + "\"attempted_requests\":%d,\"successful_requests\":%d}%n",
                protocol, mode, clients, pageSize, fixtureInventory.count(), fixtureInventory.digest(),
                measured.entries(), measured.entries(), measured.commonPrefixes(), measured.requests(),
                measured.bytes(), elapsed, measured.entries() * 1e9 / elapsed,
                measured.requests() * 1e9 / elapsed, measured.bytes() * 1e9 / elapsed,
                measured.histogram().percentile(.50), measured.histogram().percentile(.95),
                measured.histogram().percentile(.99),
                mode.equals("seek") ? "fixture_seek_name_size_time"
                        : delimiterMetadata == null ? "none" : "fixture_delimiter_direct_name_size_time",
                mode.equals("seek") ? (long) clients * repetitions * warmups
                        : delimiterMetadata == null ? 0
                        : delimiterMetadata.count() * clients * repetitions * warmups,
                ATTEMPTED.get(), SUCCEEDED.get());
    }

    private static Batch runBatch(java.util.concurrent.ExecutorService workers,
                                  ScheduledThreadPoolExecutor watchdog, HttpClient client,
                                  URI endpoint, String protocol, String bucket, String mode,
                                  int pageSize, int clients, int repetitions, String prefix,
                                  Plan plan, boolean verifyMetadata,
                                  FixtureMetadataOracle.Digest delimiterMetadata) throws Exception {
        List<Future<Batch>> futures = new ArrayList<>(clients);
        try {
            for (int clientIndex = 0; clientIndex < clients; clientIndex++) {
                final int owner = clientIndex;
                futures.add(workers.submit(() -> {
                    Batch batch = new Batch();
                    for (int repetition = 0; repetition < repetitions; repetition++) {
                        if (mode.equals("seek")) {
                            SeekTarget target = plan.seeks().get(owner * repetitions + repetition);
                            Response page = fetch(client, watchdog, endpoint, protocol, bucket,
                                    pageSize, null, null, target, verifyMetadata);
                            if (page.entries().size() != 1 || page.entries().getFirst().prefix()
                                    || !Arrays.equals(page.entries().getFirst().name(), target.key())) {
                                throw new IllegalStateException("one-key seek inventory mismatch");
                            }
                            if (verifyMetadata) {
                                Entry item = page.entries().getFirst();
                                if (!item.hasMetadata() || item.size() != target.size()
                                        || item.wireEpochUnit() != FixtureMetadataOracle.wireEpochUnit(
                                                protocol, target.epochMicros())) {
                                    throw new IllegalStateException("one-key seek fixture-backed metadata mismatch");
                                }
                            }
                            batch.add(page);
                        } else {
                            walkDelimiter(client, watchdog, endpoint, protocol, bucket,
                                    pageSize, prefix, plan.delimiter(), batch, verifyMetadata,
                                    delimiterMetadata);
                        }
                    }
                    return batch;
                }));
            }
            Batch total = new Batch();
            for (Future<Batch> future : futures) total.merge(future.get());
            return total;
        } catch (Exception e) {
            for (Future<Batch> future : futures) future.cancel(true);
            throw e;
        }
    }

    private static void walkDelimiter(HttpClient client, ScheduledThreadPoolExecutor watchdog,
                                      URI endpoint, String protocol, String bucket, int pageSize,
                                      String prefix, DelimiterExpected expected, Batch batch,
                                      boolean verifyMetadata,
                                      FixtureMetadataOracle.Digest expectedMetadata) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        MetadataVerifier metadata = verifyMetadata ? new MetadataVerifier() : null;
        String token = null;
        byte[] prior = null;
        Set<String> seen = new HashSet<>();
        long count = 0;
        long commonPrefixes = 0;
        for (long guard = 0; guard < Math.min(1_000_000L, expected.count() + 2); guard++) {
            Response page = fetch(client, watchdog, endpoint, protocol, bucket, pageSize, prefix, token, null,
                    verifyMetadata);
            if (page.entries().size() > pageSize || page.entries().isEmpty() && page.token() != null) {
                throw new IllegalStateException("invalid delimiter page size/progress");
            }
            for (Entry entry : page.entries()) {
                if (prior != null && Arrays.compareUnsigned(prior, entry.name()) >= 0) {
                    throw new IllegalStateException("duplicate/out-of-order delimiter entry");
                }
                digestEntry(digest, entry);
                if (metadata != null && !entry.prefix()) {
                    if (!entry.hasMetadata())
                        throw new IllegalStateException("delimiter object omitted fixture-backed metadata");
                    metadata.accept(entry.name(), entry.size(), entry.wireEpochUnit());
                }
                prior = entry.name();
                count++;
                if (entry.prefix()) commonPrefixes++;
            }
            batch.add(page);
            if (page.token() == null) {
                if (count != expected.count() || commonPrefixes != expected.commonPrefixes()
                        || !HexFormat.of().formatHex(digest.digest()).equals(expected.digest())) {
                    throw new IllegalStateException("delimiter inventory differs from fixture oracle");
                }
                if (metadata != null && !metadata.finish().equals(expectedMetadata))
                    throw new IllegalStateException("delimiter fixture-backed metadata differs from oracle");
                return;
            }
            if (!seen.add(page.token())) throw new IllegalStateException("repeated delimiter token");
            token = page.token();
        }
        throw new IllegalStateException("delimiter walk exceeded fixture-derived request bound");
    }

    private static Response fetch(HttpClient client, ScheduledThreadPoolExecutor watchdog,
                                  URI endpoint, String protocol, String bucket, int pageSize,
                                  String prefix, String token, SeekTarget seek,
                                  boolean verifyMetadata) throws Exception {
        URI uri = requestUri(endpoint, protocol, bucket, pageSize, prefix, token, seek);
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(60)).GET();
        if (protocol.equals("azure")) builder.header("x-ms-version", "2026-06-06");
        ATTEMPTED.incrementAndGet();
        long started = System.nanoTime();
        HttpResponse<InputStream> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() != 200) {
            response.body().close();
            throw new IllegalStateException("HTTP " + response.statusCode() + " on shape request");
        }
        AtomicBoolean timedOut = new AtomicBoolean();
        try (CountingInputStream body = new CountingInputStream(response.body())) {
            var timeout = watchdog.schedule(() -> {
                timedOut.set(true);
                try { body.close(); } catch (IOException ignored) { }
            }, BODY_DEADLINE.toNanos(), TimeUnit.NANOSECONDS);
            try {
                Response parsed = protocol.equals("gcs") ? parseGcs(body, verifyMetadata)
                        : parseXml(body, protocol, verifyMetadata);
                byte[] extra = new byte[8192];
                int n;
                while ((n = body.read(extra)) >= 0) {
                    for (int i = 0; i < n; i++) {
                        if (extra[i] != ' ' && extra[i] != '\n' && extra[i] != '\r' && extra[i] != '\t') {
                            throw new IllegalStateException("trailing response bytes");
                        }
                    }
                }
                if (timedOut.get()) throw new IllegalStateException("body_timeout");
                SUCCEEDED.incrementAndGet();
                return new Response(parsed.entries(), parsed.token(), body.bytes(), System.nanoTime() - started);
            } catch (Exception e) {
                if (timedOut.get()) throw new IllegalStateException("body_timeout", e);
                throw e;
            } finally { timeout.cancel(false); }
        }
    }

    private static URI requestUri(URI endpoint, String protocol, String bucket, int pageSize,
                                  String prefix, String token, SeekTarget seek) throws Exception {
        String base = endpoint.toString().replaceAll("/$", "");
        String path;
        if (protocol.equals("s3")) {
            path = "/" + enc(bucket) + "?list-type=2&encoding-type=url&max-keys=" + pageSize;
            if (prefix != null) path += "&delimiter=%2F&prefix=" + enc(prefix);
            if (token != null) path += "&continuation-token=" + enc(token);
            else if (seek != null && seek.predecessor() != null)
                path += "&start-after=" + enc(strictUtf8(seek.predecessor()));
        } else if (protocol.equals("gcs")) {
            path = "/storage/v1/b/" + enc(bucket) + "/o?maxResults=" + pageSize;
            if (prefix != null) path += "&delimiter=%2F&prefix=" + enc(prefix);
            if (token != null) path += "&pageToken=" + enc(token);
            else if (seek != null) path += "&startOffset=" + enc(strictUtf8(seek.key()));
        } else {
            path = "/replay/" + enc(bucket) + "?restype=container&comp=list&maxresults=" + pageSize;
            if (prefix != null) path += "&delimiter=%2F&prefix=" + enc(prefix);
            if (token != null) path += "&marker=" + enc(token);
            else if (seek != null) path += "&startFrom=" + enc(strictUtf8(seek.key()));
        }
        return URI.create(base + path);
    }

    static Response parseGcs(InputStream body) throws Exception {
        return parseGcs(body, false);
    }

    static Response parseGcs(InputStream body, boolean verifyMetadata) throws Exception {
        List<Entry> objects = new ArrayList<>();
        List<Entry> prefixes = new ArrayList<>();
        String token = null;
        try (JsonParser json = JSON.createParser(body)) {
            json.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
            if (next(json) != JsonToken.START_OBJECT) throw new IllegalStateException("GCS root");
            while (next(json) != JsonToken.END_OBJECT) {
                String field = json.currentName();
                JsonToken value = next(json);
                if ("items".equals(field)) {
                    if (value != JsonToken.START_ARRAY) throw new IllegalStateException("GCS items type");
                    while (next(json) != JsonToken.END_ARRAY) {
                        if (json.currentToken() != JsonToken.START_OBJECT) throw new IllegalStateException("GCS item");
                        String name = null;
                        long size = 0, updatedMicros = 0;
                        boolean hasSize = false, hasUpdated = false;
                        while (next(json) != JsonToken.END_OBJECT) {
                            String itemField = json.currentName();
                            JsonToken itemValue = next(json);
                            if ("name".equals(itemField)) {
                                if (itemValue != JsonToken.VALUE_STRING || name != null)
                                    throw new IllegalStateException("GCS item name");
                                name = json.getText();
                            } else if (verifyMetadata && "size".equals(itemField)) {
                                if (itemValue != JsonToken.VALUE_STRING || hasSize)
                                    throw new IllegalStateException("GCS item size");
                                size = Long.parseLong(json.getText());
                                hasSize = true;
                            } else if (verifyMetadata && "updated".equals(itemField)) {
                                if (itemValue != JsonToken.VALUE_STRING || hasUpdated)
                                    throw new IllegalStateException("GCS item updated");
                                Instant time = Instant.parse(json.getText());
                                updatedMicros = Math.addExact(Math.multiplyExact(time.getEpochSecond(), 1_000_000L),
                                        time.getNano() / 1_000L);
                                hasUpdated = true;
                            } else json.skipChildren();
                        }
                        if (name == null) throw new IllegalStateException("GCS item without name");
                        if (verifyMetadata && (!hasSize || !hasUpdated))
                            throw new IllegalStateException("GCS item omitted fixture-backed metadata");
                        objects.add(new Entry(false, name.getBytes(StandardCharsets.UTF_8), size,
                                updatedMicros, verifyMetadata));
                    }
                } else if ("prefixes".equals(field)) {
                    if (value != JsonToken.START_ARRAY) throw new IllegalStateException("GCS prefixes type");
                    while (next(json) != JsonToken.END_ARRAY) {
                        if (json.currentToken() != JsonToken.VALUE_STRING)
                            throw new IllegalStateException("GCS prefix type");
                        prefixes.add(new Entry(true, json.getText().getBytes(StandardCharsets.UTF_8)));
                    }
                } else if ("nextPageToken".equals(field)) {
                    if (value != JsonToken.VALUE_STRING) throw new IllegalStateException("GCS token type");
                    token = json.getText();
                } else json.skipChildren();
            }
            if (json.nextToken() != null) throw new IllegalStateException("trailing GCS JSON");
        }
        List<Entry> merged = new ArrayList<>(objects.size() + prefixes.size());
        requireIncreasing(objects, "GCS items");
        requireIncreasing(prefixes, "GCS prefixes");
        merged.addAll(objects);
        merged.addAll(prefixes);
        merged.sort((a, b) -> Arrays.compareUnsigned(a.name(), b.name()));
        requireIncreasing(merged, "GCS merged page");
        if (token != null && token.isEmpty()) throw new IllegalStateException("empty GCS page token");
        return new Response(merged, token, 0, 0);
    }

    private static void requireIncreasing(List<Entry> entries, String label) {
        byte[] prior = null;
        for (Entry entry : entries) {
            if (prior != null && Arrays.compareUnsigned(prior, entry.name()) >= 0) {
                throw new IllegalStateException(label + " duplicate/out-of-order entry");
            }
            prior = entry.name();
        }
    }

    private static JsonToken next(JsonParser json) throws IOException {
        JsonToken token = json.nextToken();
        if (token == null) throw new IllegalStateException("truncated GCS JSON");
        return token;
    }

    static Response parseXml(InputStream body, String protocol) throws Exception {
        return parseXml(body, protocol, false);
    }

    static Response parseXml(InputStream body, String protocol, boolean verifyMetadata) throws Exception {
        XMLInputFactory factory = XMLInputFactory.newFactory();
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty("javax.xml.stream.isSupportingExternalEntities", false);
        XMLStreamReader xml = factory.createXMLStreamReader(body);
        List<Entry> entries = new ArrayList<>();
        List<Entry> s3Objects = protocol.equals("s3") ? new ArrayList<>() : null;
        List<Entry> s3Prefixes = protocol.equals("s3") ? new ArrayList<>() : null;
        String token = null;
        boolean truncated = false;
        boolean entryPrefix = false;
        boolean insideEntry = false;
        byte[] entryName = null;
        long entrySize = 0, entryTime = 0;
        boolean hasSize = false, hasTime = false;
        String itemTag = protocol.equals("s3") ? "Contents" : "Blob";
        String prefixTag = protocol.equals("s3") ? "CommonPrefixes" : "BlobPrefix";
        String nameTag = protocol.equals("s3") ? "Key" : "Name";
        String tokenTag = protocol.equals("s3") ? "NextContinuationToken" : "NextMarker";
        String root = null;
        try {
            while (xml.hasNext()) {
                int event = xml.next();
                if (event == XMLStreamConstants.START_ELEMENT) {
                    String tag = xml.getLocalName();
                    if (root == null) root = tag;
                    if (tag.equals(itemTag) || tag.equals(prefixTag)) {
                        insideEntry = true;
                        entryPrefix = tag.equals(prefixTag);
                        entryName = null;
                        hasSize = hasTime = false;
                    } else if (insideEntry && (tag.equals(nameTag)
                            || protocol.equals("s3") && entryPrefix && tag.equals("Prefix"))) {
                        boolean encoded = protocol.equals("s3") || protocol.equals("azure")
                                && "true".equalsIgnoreCase(xml.getAttributeValue(null, "Encoded"));
                        String value = xml.getElementText();
                        entryName = encoded ? ReplayHttpBench.percentDecode(value)
                                : value.getBytes(StandardCharsets.UTF_8);
                    } else if (verifyMetadata && insideEntry && !entryPrefix
                            && tag.equals(protocol.equals("s3") ? "Size" : "Content-Length")) {
                        entrySize = Long.parseLong(xml.getElementText().trim());
                        hasSize = true;
                    } else if (verifyMetadata && insideEntry && !entryPrefix
                            && tag.equals(protocol.equals("s3") ? "LastModified" : "Last-Modified")) {
                        String timestamp = xml.getElementText();
                        entryTime = protocol.equals("s3") ? Instant.parse(timestamp).toEpochMilli()
                                : ZonedDateTime.parse(timestamp, DateTimeFormatter.RFC_1123_DATE_TIME)
                                        .toEpochSecond();
                        hasTime = true;
                    } else if (tag.equals(tokenTag)) token = xml.getElementText();
                    else if (tag.equals("IsTruncated")) truncated = Boolean.parseBoolean(xml.getElementText());
                } else if (event == XMLStreamConstants.END_ELEMENT) {
                    String tag = xml.getLocalName();
                    if (insideEntry && (tag.equals(itemTag) || tag.equals(prefixTag))) {
                        if (entryName == null) throw new IllegalStateException("XML entry missing name");
                        if (verifyMetadata && !entryPrefix && (!hasSize || !hasTime))
                            throw new IllegalStateException("XML item omitted fixture-backed metadata");
                        Entry entry = new Entry(entryPrefix, entryName, entrySize, entryTime,
                                verifyMetadata && !entryPrefix);
                        if (protocol.equals("s3")) {
                            (entryPrefix ? s3Prefixes : s3Objects).add(entry);
                        } else {
                            entries.add(entry);
                        }
                        insideEntry = false;
                    }
                }
            }
        } finally { xml.close(); }
        if (!java.util.Objects.equals(root, protocol.equals("s3") ? "ListBucketResult" : "EnumerationResults")) {
            throw new IllegalStateException("unexpected XML listing root");
        }
        if (protocol.equals("s3")) {
            if (truncated && (token == null || token.isEmpty()))
                throw new IllegalStateException("S3 truncated without token");
            if (!truncated) token = null;
            requireIncreasing(s3Objects, "S3 Contents");
            requireIncreasing(s3Prefixes, "S3 CommonPrefixes");
            entries.addAll(s3Objects);
            entries.addAll(s3Prefixes);
            entries.sort((a, b) -> Arrays.compareUnsigned(a.name(), b.name()));
            requireIncreasing(entries, "S3 merged page");
        } else if (token != null && token.isEmpty()) token = null;
        return new Response(entries, token, 0, 0);
    }

    private static Plan seekPlan(String fixture, int targets, long fixtureCount) throws Exception {
        if (fixtureCount < 1) throw new IllegalArgumentException("seek fixture is empty");
        List<SeekTarget> result = new ArrayList<>(targets);
        try (Connection conn = connection(); Statement stmt = conn.createStatement();
             ResultSet rows = stmt.executeQuery(keyQuery(conn, fixture))) {
            long index = 0;
            byte[] prior = null;
            while (rows.next() && result.size() < targets) {
                byte[] key = rows.getBytes(1);
                while (result.size() < targets
                        && result.size() * fixtureCount / targets == index) {
                    result.add(new SeekTarget(key.clone(), prior == null ? null : prior.clone(),
                            rows.getLong(2), rows.getLong(3)));
                }
                prior = key;
                index++;
            }
        }
        if (result.size() != targets) throw new IllegalStateException("seek roster incomplete");
        return new Plan(List.copyOf(result), null);
    }

    private static Plan delimiterPlan(String fixture, String prefixText) throws Exception {
        byte[] prefix = prefixText.getBytes(StandardCharsets.UTF_8);
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        long count = 0;
        long commonPrefixes = 0;
        byte[] previousPrefix = null;
        try (Connection conn = connection(); Statement stmt = conn.createStatement();
             ResultSet rows = stmt.executeQuery(keyQuery(conn, fixture))) {
            while (rows.next()) {
                byte[] key = rows.getBytes(1);
                if (!startsWith(key, prefix)) continue;
                int slash = -1;
                for (int i = prefix.length; i < key.length; i++) {
                    if (key[i] == '/') { slash = i; break; }
                }
                Entry entry;
                if (slash >= 0) {
                    byte[] rolled = Arrays.copyOf(key, slash + 1);
                    if (Arrays.equals(rolled, previousPrefix)) continue;
                    previousPrefix = rolled;
                    entry = new Entry(true, rolled);
                    commonPrefixes++;
                } else {
                    previousPrefix = null;
                    entry = new Entry(false, key);
                }
                digestEntry(digest, entry);
                count++;
            }
        }
        if (count == 0) throw new IllegalArgumentException("delimiter prefix matches no fixture entries");
        return new Plan(List.of(), new DelimiterExpected(count, commonPrefixes,
                HexFormat.of().formatHex(digest.digest())));
    }

    private static Connection connection() throws Exception {
        Properties props = new Properties();
        props.setProperty("jdbc_stream_results", "true");
        return DriverManager.getConnection("jdbc:duckdb:", props);
    }

    private static String keyQuery(Connection conn, String fixture) throws Exception {
        String path = fixture.replace("'", "''");
        String type;
        try (Statement probe = conn.createStatement();
             ResultSet result = probe.executeQuery("SELECT typeof(key) FROM read_parquet('" + path + "') LIMIT 1")) {
            type = result.next() ? result.getString(1) : "BLOB";
        }
        String key = switch (type.toUpperCase(Locale.ROOT)) {
            case "BLOB" -> "key";
            case "VARCHAR" -> "encode(key)";
            default -> throw new IllegalStateException("unsupported fixture key type " + type);
        };
        return "SELECT " + key + ", size, epoch_us(last_modified) FROM read_parquet('" + path + "')";
    }

    private static void digestEntry(MessageDigest digest, Entry entry) {
        digest.update((byte) (entry.prefix() ? 'P' : 'O'));
        byte[] name = entry.name();
        digest.update(ByteBuffer.allocate(4).putInt(name.length).array());
        digest.update(name);
    }

    private static boolean startsWith(byte[] value, byte[] prefix) {
        if (value.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) if (value[i] != prefix[i]) return false;
        return true;
    }

    private static String strictUtf8(byte[] raw) throws Exception {
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(raw)).toString();
    }

    private static String enc(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    record Entry(boolean prefix, byte[] name, long size, long wireEpochUnit, boolean hasMetadata) {
        Entry(boolean prefix, byte[] name) { this(prefix, name, 0, 0, false); }
    }
    record Response(List<Entry> entries, String token, long bytes, long latencyNanos) { }
    private record SeekTarget(byte[] key, byte[] predecessor, long size, long epochMicros) { }
    private record DelimiterExpected(long count, long commonPrefixes, String digest) { }
    private record Plan(List<SeekTarget> seeks, DelimiterExpected delimiter) { }

    private static final class Batch {
        private long entries;
        private long commonPrefixes;
        private long requests;
        private long bytes;
        private final Histogram histogram = new Histogram();

        void add(Response page) {
            entries += page.entries().size();
            for (Entry entry : page.entries()) if (entry.prefix()) commonPrefixes++;
            requests++;
            bytes += page.bytes();
            histogram.add(page.latencyNanos());
        }

        void merge(Batch other) {
            entries += other.entries;
            commonPrefixes += other.commonPrefixes;
            requests += other.requests;
            bytes += other.bytes;
            histogram.merge(other.histogram);
        }

        long entries() { return entries; }
        long commonPrefixes() { return commonPrefixes; }
        long requests() { return requests; }
        long bytes() { return bytes; }
        Histogram histogram() { return histogram; }
    }

    private static final class CountingInputStream extends FilterInputStream {
        private long count;

        CountingInputStream(InputStream in) { super(in); }
        long bytes() { return count; }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value >= 0) charge(1);
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int n = in.read(bytes, offset, length);
            if (n > 0) charge(n);
            return n;
        }

        private void charge(int n) {
            count += n;
            if (count > MAX_BODY_BYTES) throw new IllegalStateException("shape response body exceeds cap");
        }
    }

    /** Fixed 4096-bin, base-2 histogram; relative bucket width at most 1/64. */
    private static final class Histogram {
        private final long[] bins = new long[4096];
        private long count;

        void add(long value) {
            long positive = Math.max(1, value);
            int exponent = 63 - Long.numberOfLeadingZeros(positive);
            long floor = 1L << exponent;
            int sub = (int) Math.min(63, ((positive - floor) * 64) / floor);
            bins[Math.min(4095, exponent * 64 + sub)]++;
            count++;
        }

        void merge(Histogram other) {
            for (int i = 0; i < bins.length; i++) bins[i] += other.bins[i];
            count += other.count;
        }

        long percentile(double ratio) {
            if (count == 0) throw new IllegalStateException("no shape latency samples");
            long target = (long) Math.ceil(count * ratio);
            long seen = 0;
            for (int i = 0; i < bins.length; i++) {
                seen += bins[i];
                if (seen >= target) {
                    long floor = 1L << (i / 64);
                    return floor + floor * (i % 64 + 1) / 64;
                }
            }
            throw new IllegalStateException("histogram count mismatch");
        }
    }
}
