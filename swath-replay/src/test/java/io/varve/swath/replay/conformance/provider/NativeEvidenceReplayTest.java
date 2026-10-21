/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.replay.provider.ProviderSdkCapture;
import io.varve.swath.replay.server.Protocol;
import io.varve.swath.replay.server.ReplayServer;
import io.varve.swath.replay.server.ServeConfig;
import io.varve.swath.replay.server.ServingMode;
import io.varve.swath.replay.testkit.NativeSdkFixtures;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.RawHttp;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Replays every committed native GCS, Azure and S3 capture against replay serving the same corpus,
 * walking replay with its own tokens. Each capture's outcome must equal its committed
 * expectation: {@code match}, or a pinned discrepancy. With {@code swath.provider.receipts}
 * and {@code swath.provider.distribution} set on a clean tree, matches also write receipts.
 */
class NativeEvidenceReplayTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path REPO = Path.of("..").toAbsolutePath().normalize();
    private static final Path EVIDENCE = REPO.resolve("docs/replay-provider-evidence");
    private static final Path SOURCE = REPO.resolve(
            "swath-replay/src/test/java/io/varve/swath/replay/conformance/provider/NativeEvidenceReplayTest.java");
    private static final String REPLAY_BUCKET = "replay-bucket";
    private static final String REPLAY_ACCOUNT = "replay";
    private static final String TAMPERED_TOKEN = "CONFORMANCE-TAMPERED-TOKEN";
    private static final String GCS_NATIVE_PATH = "/storage/v1/b/{bucket}/o";
    private static final String AZURE_NATIVE_ENDPOINT = "https://{account}.blob.core.windows.net/";
    private static final int MAX_WALK_PAGES = 64;
    private static final int SORTED_KEY_LIMIT = 1024;

    @Test
    void gcsNativeCapturesMatchReplay(@TempDir Path dir) throws Exception {
        replayProvider("gcs", "gcsNativeCapturesMatchReplay", dir);
    }

    @Test
    void azureNativeCapturesMatchReplay(@TempDir Path dir) throws Exception {
        replayProvider("azure", "azureNativeCapturesMatchReplay", dir);
    }

    @Test
    void s3NativeCapturesMatchReplay(@TempDir Path dir) throws Exception {
        replayProvider("s3", "s3NativeCapturesMatchReplay", dir);
    }

    private void replayProvider(String provider, String testName, Path dir) throws Exception {
        Path root = EVIDENCE.resolve(provider);
        assumeTrue(Files.isRegularFile(root.resolve("index.json")), "no committed " + provider + " evidence");
        JsonNode index = JSON.readTree(root.resolve("index.json").toFile());
        JsonNode expectations = JSON.readTree(root.resolve("replay-expectations.json").toFile());
        Map<String, JsonNode> cases = new HashMap<>();
        index.path("cases").forEach(node -> cases.put(node.path("id").asText(), node));
        String firstManifest = index.path("manifests").fieldNames().next();
        JsonNode manifest = JSON.readTree(root.resolve(index.path("manifests").path(firstManifest)
                .path("file").asText()).toFile());
        ObjectEntry[] objects = entries(provider, manifest);
        // The sorted fixture format caps keys at the S3 limit of 1,024 bytes, while Azure accepts
        // 1,024 UTF-16 units (up to about 3 KiB of UTF-8). Such a corpus is served by the DuckDB
        // mode; a recorded limitation of the sorted path, not a comparison shortcut.
        boolean longKeys = java.util.Arrays.stream(objects).anyMatch(o -> o.key().length() > SORTED_KEY_LIMIT);
        Path fixture;
        if (longKeys) {
            fixture = dir.resolve("part.parquet");
            io.varve.swath.replay.testkit.ParquetFixtures.write(fixture, objects);
        } else {
            fixture = NativeSdkFixtures.sorted(dir, objects);
        }
        boolean s3 = provider.equals("s3");
        // The S3 route applies its latency function to every page and has no null default.
        ServeConfig config = new ServeConfig(fixture, "127.0.0.1", 0, REPLAY_BUCKET,
                longKeys ? ServingMode.DUCKDB : ServingMode.SORTED,
                2, 16, s3 ? Set.of(Protocol.S3) : Set.of(Protocol.GCS, Protocol.AZURE), s3 ? null : REPLAY_ACCOUNT,
                256L * 1024 * 1024, 64 * 1024 * 1024, Duration.ofSeconds(10), Duration.ofSeconds(30),
                Duration.ofSeconds(30), null, s3 ? (request, result) -> Duration.ZERO : null);
        String receipts = System.getProperty("swath.provider.receipts");
        String distribution = System.getProperty("swath.provider.distribution");
        Map<String, String> outcomes = new TreeMap<>();
        try (ReplayServer server = ReplayServer.open(config)) {
            server.start();
            Endpoint endpoint = new Endpoint(provider, server.port(), cases);
            for (var capture : index.path("captures").properties()) {
                String key = capture.getKey();
                Path file = root.resolve(capture.getValue().path("file").asText());
                String outcome;
                try {
                    endpoint.verify(key, file, capture.getValue());
                    outcome = "match";
                } catch (IllegalArgumentException | IllegalStateException | IOException error) {
                    outcome = "mismatch: " + error.getMessage();
                    if (error.getMessage() != null && error.getMessage().startsWith("Azure BOM/prolog")) {
                        String rest;
                        try {
                            new Endpoint(provider, server.port(), cases, true).verify(key, file, capture.getValue());
                            rest = "nothing else";
                        } catch (IllegalArgumentException | IllegalStateException | IOException other) {
                            rest = other.getMessage();
                        }
                        outcome += " | with the BOM restored: " + rest;
                    }
                }
                outcomes.put(key, outcome);
                if (outcome.equals("match") && receipts != null) {
                    ProviderMatchReceipt.writeReceipt(file, REPO, Path.of(distribution), SOURCE, testName,
                            Path.of(receipts, provider, "receipts", key + ".json"));
                }
            }
        }
        String report = System.getProperty("swath.provider.report");
        if (report != null) {
            Files.createDirectories(Path.of(report));
            Files.writeString(Path.of(report, provider + "-outcomes.json"),
                    JSON.writerWithDefaultPrettyPrinter().writeValueAsString(outcomes) + "\n");
        }
        Map<String, String> unexpected = new TreeMap<>();
        outcomes.forEach((key, outcome) -> {
            JsonNode expected = expectations.path(key);
            boolean ok = expected.isTextual() && expected.textValue().equals("match")
                    ? outcome.equals("match")
                    : expected.has("mismatch") && outcome.startsWith("mismatch: ")
                            && outcome.contains(expected.path("mismatch").asText());
            if (!ok) unexpected.put(key, outcome);
        });
        expectations.fieldNames().forEachRemaining(key -> {
            if (!outcomes.containsKey(key)) unexpected.put(key, "expectation without a capture");
        });
        assertThat(unexpected).as("replay outcomes differing from committed expectations").isEmpty();
    }

    private static ObjectEntry[] entries(String provider, JsonNode manifest) {
        List<ObjectEntry> entries = new ArrayList<>();
        for (JsonNode object : manifest.path("objects")) {
            String updated = object.path("updated").asText();
            Instant instant = provider.equals("azure")
                    ? ZonedDateTime.parse(updated, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant()
                    : Instant.parse(updated);
            ObjectEntries.Builder builder = ObjectEntries.key(object.path("name").asText())
                    .size(object.path("size").asLong())
                    .lastModifiedEpochMicros(ChronoUnit.MICROS.between(Instant.EPOCH, instant)).isLatest(true);
            JsonNode attributes = object.path("attributes");
            // swath captures store the ETag without its quotes (S3PageFetcher.stripEtagQuotes).
            if (attributes.has("ETag")) builder.etag(attributes.path("ETag").asText().replaceAll("^\"|\"$", ""));
            if (attributes.has("StorageClass")) builder.storageClass(attributes.path("StorageClass").asText());
            if (attributes.has("ChecksumAlgorithm") || attributes.has("ChecksumType")) {
                builder.checksum(attributes.path("ChecksumAlgorithm").asText(null),
                        attributes.path("ChecksumType").asText(null));
            }
            entries.add(builder.build());
        }
        return entries.toArray(ObjectEntry[]::new);
    }

    /** Issues the replay side of one capture with replay's own tokens and compares it. */
    /**
     * {@code restoreBom} is a diagnostic only: after the pinned "replay omits the UTF-8 BOM" Azure
     * mismatch, it re-compares with the BOM restored to report whatever else differs. It never
     * produces a match verdict or a receipt.
     */
    private record Endpoint(String provider, int port, Map<String, JsonNode> cases, boolean restoreBom) {
        Endpoint(String provider, int port, Map<String, JsonNode> cases) {
            this(provider, port, cases, false);
        }

        ProviderExchangeComparator.Exchange replayExchange(int status, Map<String, List<String>> headers,
                                                           byte[] body) {
            if (!restoreBom || body.length == 0 || (body.length >= 3 && (body[0] & 0xff) == 0xef)) {
                return new ProviderExchangeComparator.Exchange(status, headers, body);
            }
            byte[] withBom = new byte[body.length + 3];
            withBom[0] = (byte) 0xef;
            withBom[1] = (byte) 0xbb;
            withBom[2] = (byte) 0xbf;
            System.arraycopy(body, 0, withBom, 3, body.length);
            Map<String, List<String>> adjusted = new LinkedHashMap<>(headers);
            if (adjusted.containsKey("content-length")) {
                adjusted.put("content-length", List.of(Integer.toString(withBom.length)));
            }
            return new ProviderExchangeComparator.Exchange(status, adjusted, withBom);
        }

        boolean gcs() { return provider.equals("gcs"); }

        boolean s3() { return provider.equals("s3"); }

        String tokenField() { return gcs() ? "pageToken" : s3() ? "continuation-token" : "marker"; }

        String replayPath() {
            if (s3()) return "/" + REPLAY_BUCKET;
            return gcs() ? "/storage/v1/b/" + REPLAY_BUCKET + "/o" : "/" + REPLAY_ACCOUNT + "/" + REPLAY_BUCKET;
        }

        String replayEndpoint() { return "http://127.0.0.1:" + port + "/" + REPLAY_ACCOUNT + "/"; }

        AzureNamespaceMapping mapping() {
            return new AzureNamespaceMapping("{account}", "{container}", REPLAY_ACCOUNT, REPLAY_BUCKET);
        }

        void verify(String key, Path file, JsonNode entry) throws IOException {
            JsonNode capture = JSON.readTree(ProviderMatchReceipt.evidenceBytes(file));
            String caseId = key.split("~", 2)[0];
            JsonNode record = cases.get(caseId);
            if (record == null) throw new IllegalStateException("capture has no case record");
            if (capture.path("schema_version").asText().equals("provider-capture-v1")) {
                verifyExchange(file, capture, record);
            } else if (capture.path("evidence_kind").asText().equals("sdk_run")) {
                verifySdk(file, capture, entry.path("sdk_options"));
            } else {
                verifyWalk(file, capture);
            }
        }

        private void verifyExchange(Path file, JsonNode capture, JsonNode record) throws IOException {
            JsonNode request = capture.path("request");
            String nativeQuery = request.path("query").asText();
            Map<String, String> tokens = new HashMap<>();
            String query = nativeQuery;
            String nativeToken = queryValue(nativeQuery, tokenField());
            if (nativeToken != null && !nativeToken.isEmpty()) {
                JsonNode source = record.path("token");
                String mutation = source.path("mutation").isTextual() ? source.path("mutation").asText() : null;
                String replayToken = "tampered".equals(mutation) ? TAMPERED_TOKEN
                        : sourceToken(source, versionHeader(request));
                if (replayToken == null) throw new IllegalStateException("replay source page is terminal");
                if (mutation != null && !"tampered".equals(mutation)) replayToken = mutate(replayToken, mutation);
                String encoded = encode(replayToken);
                tokens.put(encoded, nativeToken);
                query = nativeQuery.replace(tokenField() + "=" + nativeToken, tokenField() + "=" + encoded);
            }
            List<Map.Entry<String, String>> headers = replayHeaders(request, record);
            RawHttp.Response response = send(request.path("method").asText(), query, headers);
            var replayRequest = new ProviderMatchReceipt.Request(request.path("method").asText(), replayPath(),
                    query, headerMap(headers));
            var exchange = replayExchange(response.status(), response.headers(), response.body());
            if (s3()) {
                ProviderMatchReceipt.verifyS3(file, replayRequest, exchange, tokens, REPLAY_BUCKET);
            } else if (gcs()) {
                ProviderMatchReceipt.verifyGcs(file, replayRequest, exchange, tokens, REPLAY_BUCKET,
                        "full".equals(queryValue(nativeQuery, "projection")));
            } else {
                ProviderMatchReceipt.verifyAzure(file, replayRequest, exchange, tokens, mapping(),
                        AZURE_NATIVE_ENDPOINT, replayEndpoint());
            }
        }

        private void verifyWalk(Path file, JsonNode capture) throws IOException {
            JsonNode steps = capture.path("steps");
            JsonNode first = steps.get(0).path("exchange").path("request");
            String base = first.path("query").asText();
            List<Map.Entry<String, String>> headers = replayHeaders(first, null);
            List<ProviderMatchReceipt.ReplayStep> replay = new ArrayList<>();
            Map<String, String> tokens = new HashMap<>();
            String token = null;
            for (int page = 0; page < MAX_WALK_PAGES; page++) {
                String query = token == null ? base : base + "&" + tokenField() + "=" + encode(token);
                if (token != null && page < steps.size()) {
                    String nativeToken = queryValue(steps.get(page).path("exchange").path("request")
                            .path("query").asText(), tokenField());
                    tokens.put(encode(token), nativeToken);
                }
                RawHttp.Response response = send("GET", query, headers);
                replay.add(new ProviderMatchReceipt.ReplayStep("walk",
                        new ProviderMatchReceipt.Request("GET", replayPath(), query, headerMap(headers)),
                        replayExchange(response.status(), response.headers(), response.body())));
                token = response.status() == 200 ? nextToken(response.body()) : null;
                if (token == null) break;
            }
            if (s3()) {
                ProviderMatchReceipt.verifyS3Run(file, replay, tokens, REPLAY_BUCKET);
            } else if (gcs()) {
                ProviderMatchReceipt.verifyGcsRun(file, replay, tokens, REPLAY_BUCKET,
                        "full".equals(queryValue(base, "projection")));
            } else {
                ProviderMatchReceipt.verifyAzureRun(file, replay, tokens, mapping(), AZURE_NATIVE_ENDPOINT,
                        replayEndpoint());
            }
        }

        private void verifySdk(Path file, JsonNode capture, JsonNode portable) throws IOException {
            ObjectNode options = portable.deepCopy();
            options.put("endpoint", gcs() || s3() ? "http://127.0.0.1:" + port : "http://127.0.0.1:" + port + "/"
                    + REPLAY_ACCOUNT);
            options.put("credentials", "none");
            options.put("maxAttempts", MAX_WALK_PAGES);
            options.put("deadlineEpochMs", System.currentTimeMillis() + 60_000);
            if (gcs()) {
                options.put("bucket", REPLAY_BUCKET);
                options.put("projectId", "replay-test");
            } else if (s3()) {
                options.put("bucket", REPLAY_BUCKET);
            } else {
                options.put("container", REPLAY_BUCKET);
            }
            var result = ProviderSdkCapture.run(ProviderSdkCapture.parseOptions(options));
            if (result.status() != ProviderSdkCapture.Status.COMPLETE) {
                throw new IllegalStateException("replay SDK walk incomplete: " + result.reason());
            }
            List<ProviderMatchReceipt.ReplayStep> replay = new ArrayList<>();
            Map<String, String> tokens = new HashMap<>();
            JsonNode nativeSteps = capture.path("steps");
            for (int i = 0; i < result.steps().size(); i++) {
                JsonNode exchange = JSON.valueToTree(result.steps().get(i)).path("exchange");
                String url = exchange.path("request").path("url").asText();
                int start = url.indexOf('/', url.indexOf("//") + 2);
                String target = url.substring(start);
                String path = target.contains("?") ? target.substring(0, target.indexOf('?')) : target;
                String query = target.contains("?") ? target.substring(target.indexOf('?') + 1) : "";
                String replayToken = queryValue(query, tokenField());
                if (replayToken != null && i < nativeSteps.size()) {
                    tokens.put(replayToken, queryValue(nativeSteps.get(i).path("exchange").path("request")
                            .path("query").asText(), tokenField()));
                }
                Map<String, List<String>> responseHeaders = new LinkedHashMap<>();
                exchange.path("response").path("headers").forEach(h -> responseHeaders.computeIfAbsent(
                        h.path("name").asText().toLowerCase(Locale.ROOT), ignored -> new ArrayList<>())
                        .add(h.path("value").asText()));
                // The SDK reads replay's transport framing itself; only semantic headers are compared.
                responseHeaders.remove("transfer-encoding");
                Map<String, List<String>> requestHeaders = new LinkedHashMap<>();
                exchange.path("request").path("headers").forEach(h -> requestHeaders.computeIfAbsent(
                        h.path("name").asText().toLowerCase(Locale.ROOT), ignored -> new ArrayList<>())
                        .add(h.path("value").asText()));
                replay.add(new ProviderMatchReceipt.ReplayStep("walk",
                        new ProviderMatchReceipt.Request(exchange.path("request").path("method").asText(),
                                path, query, requestHeaders),
                        replayExchange(exchange.path("response").path("status").asInt(),
                                responseHeaders, Base64.getDecoder().decode(
                                        exchange.path("response").path("body_base64").asText()))));
            }
            if (s3()) {
                ProviderMatchReceipt.verifyS3Run(file, replay, tokens, REPLAY_BUCKET);
            } else if (gcs()) {
                ProviderMatchReceipt.verifyGcsRun(file, replay, tokens, REPLAY_BUCKET, true);
            } else {
                ProviderMatchReceipt.verifyAzureRun(file, replay, tokens, mapping(), AZURE_NATIVE_ENDPOINT,
                        replayEndpoint());
            }
        }

        /** Replay's continuation token at the named page of the source case's own walk. */
        private String sourceToken(JsonNode source, String version) throws IOException {
            JsonNode record = cases.get(source.path("case_id").asText());
            String base = record.path("query").asText();
            if (!gcs() && !s3()) base = "restype=container&comp=list" + (base.isEmpty() ? "" : "&" + base);
            List<Map.Entry<String, String>> headers = s3() ? List.of()
                    : gcs() ? List.of(Map.entry("Accept", "application/json"))
                    : List.of(Map.entry("x-ms-version", version));
            String token = null;
            for (int page = 0; page <= source.path("page").asInt(); page++) {
                String query = token == null ? base : base + "&" + tokenField() + "=" + encode(token);
                RawHttp.Response response = send("GET", query, headers);
                if (response.status() != 200) return null;
                token = nextToken(response.body());
                if (token == null) return null;
            }
            return token;
        }

        private List<Map.Entry<String, String>> replayHeaders(JsonNode request, JsonNode record) {
            Map<String, String> raw = new LinkedHashMap<>();
            if (record != null) {
                record.path("headers").forEach(pair -> raw.put(pair.get(0).asText().toLowerCase(Locale.ROOT),
                        pair.get(1).asText()));
            }
            List<Map.Entry<String, String>> headers = new ArrayList<>();
            for (JsonNode header : request.path("headers")) {
                String name = header.path("name").asText();
                String value = header.path("value").asText();
                // Client request IDs are digests in captures; the case record holds the raw value.
                if (name.equals("x-ms-client-request-id")) {
                    value = raw.get(name);
                    if (value == null || !opaqueDigest(value).equals(header.path("value").asText())) {
                        throw new IllegalStateException("captured client request ID has no raw case value");
                    }
                }
                headers.add(Map.entry(name, value));
            }
            return headers;
        }

        private RawHttp.Response send(String method, String query, List<Map.Entry<String, String>> headers)
                throws IOException {
            RawHttp.Response response = RawHttp.exchange("127.0.0.1", port, method,
                    replayPath() + (query.isEmpty() ? "" : "?" + query), headers);
            // The test client asks for Connection: close; replay's echo of that hop-by-hop header is
            // an artifact of this client, not of the listing response under comparison.
            Map<String, List<String>> semantic = new LinkedHashMap<>(response.headers());
            semantic.remove("connection");
            return new RawHttp.Response(response.status(), semantic, response.body());
        }

        private String nextToken(byte[] body) throws IOException {
            if (s3()) return S3ProfileComparator.nextContinuationToken(body);
            if (gcs()) {
                JsonNode page = JSON.readTree(body);
                return page.hasNonNull("nextPageToken") ? page.path("nextPageToken").asText() : null;
            }
            String text = new String(body, StandardCharsets.UTF_8);
            int start = text.indexOf("<NextMarker>");
            if (start < 0) return null;
            String marker = text.substring(start + "<NextMarker>".length(), text.indexOf("</NextMarker>", start));
            return marker.isEmpty() ? null : marker;
        }
    }

    /** The same deterministic alterations the native probe runner applied (matrix.mutate_token). */
    static String mutate(String token, String mutation) {
        switch (mutation) {
            case "flip" -> {
                char last = token.charAt(token.length() - 1);
                return token.substring(0, token.length() - 1) + (last == 'A' ? 'B' : 'A');
            }
            case "truncate" -> {
                return token.length() > 4 ? token.substring(0, token.length() - 4) : token.substring(0, 1);
            }
            case "flip-decoded" -> {
                boolean urlSafe = token.indexOf('-') >= 0 || token.indexOf('_') >= 0;
                String padded = token + "=".repeat((4 - token.length() % 4) % 4);
                byte[] raw;
                try {
                    raw = (urlSafe ? Base64.getUrlDecoder() : Base64.getDecoder()).decode(padded);
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("replay token is not base64; the native mutation has no analogue");
                }
                if (raw.length == 0) throw new IllegalStateException("replay token decodes to no bytes");
                raw[raw.length - 1] ^= 1;
                String encoded = (urlSafe ? Base64.getUrlEncoder() : Base64.getEncoder()).encodeToString(raw);
                return token.endsWith("=") ? encoded : encoded.replaceAll("=+$", "");
            }
            default -> throw new IllegalArgumentException("unknown token mutation " + mutation);
        }
    }

    private static String versionHeader(JsonNode request) {
        for (JsonNode header : request.path("headers")) {
            if (header.path("name").asText().equals("x-ms-version")) return header.path("value").asText();
        }
        return null;
    }

    private static Map<String, List<String>> headerMap(List<Map.Entry<String, String>> headers) {
        Map<String, List<String>> map = new LinkedHashMap<>();
        headers.forEach(header -> map.computeIfAbsent(header.getKey().toLowerCase(Locale.ROOT),
                ignored -> new ArrayList<>()).add(header.getValue()));
        return map;
    }

    private static String queryValue(String query, String name) {
        String found = null;
        for (String part : query.split("&")) {
            if (part.startsWith(name + "=")) {
                if (found != null) throw new IllegalArgumentException("duplicate " + name + " in query");
                found = part.substring(name.length() + 1);
            }
        }
        return found;
    }

    /** RFC 3986 unreserved characters stay literal, as in the native probe runner. */
    private static String encode(String value) {
        StringBuilder out = new StringBuilder();
        for (byte b : value.getBytes(StandardCharsets.UTF_8)) {
            char c = (char) (b & 0xff);
            if (Character.isLetterOrDigit(c) && c < 0x80 || c == '-' || c == '.' || c == '_' || c == '~') {
                out.append(c);
            } else {
                out.append('%').append(String.format(Locale.ROOT, "%02X", b & 0xff));
            }
        }
        return out.toString();
    }

    private static String opaqueDigest(String value) {
        try {
            return "token-sha256-" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
