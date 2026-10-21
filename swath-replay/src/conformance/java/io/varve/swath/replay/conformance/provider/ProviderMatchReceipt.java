/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Emits a source- and distribution-bound receipt after checking the captured request and response. */
public final class ProviderMatchReceipt {
    private static final ObjectMapper JSON = new ObjectMapper();

    private ProviderMatchReceipt() { }

    public record Request(String method, String path, String query, Map<String, List<String>> headers) {
        public Request {
            if (method == null || path == null || query == null || headers == null) {
                throw new IllegalArgumentException("request scope is incomplete");
            }
            Map<String, List<String>> lower = new HashMap<>();
            headers.forEach((name, values) -> {
                if (lower.putIfAbsent(name.toLowerCase(java.util.Locale.ROOT), List.copyOf(values)) != null) {
                    throw new IllegalArgumentException("duplicate request header name");
                }
            });
            headers = Map.copyOf(lower);
        }
    }

    public record ReplayStep(String phase, Request request, ProviderExchangeComparator.Exchange exchange) { }

    public static void gcs(Path captureFile, Request replayRequest,
                           ProviderExchangeComparator.Exchange replayResponse,
                           Map<String, String> replayTokenToNativeToken, String replayBucket,
                           boolean fullProjection, Path repoRoot, Path distribution,
                           Path testSource, String testName, Path receipt) throws IOException {
        Capture capture = read(captureFile, "gcs");
        require(capture.kind().equals("exchange"), "GCS exchange receipt needs one exchange");
        Step nativeStep = capture.steps().getFirst();
        assertRequest(nativeStep.request(), replayRequest, "/storage/v1/b/" + replayBucket + "/o",
                replayTokenToNativeToken, "pageToken", false, capture.probeId());
        ProviderExchangeComparator.assertGcs(nativeStep.exchange(), replayResponse,
                "{bucket}", replayBucket, fullProjection);
        write(capture, captureFile, repoRoot, distribution, testSource, testName, receipt);
    }

    public static void azure(Path captureFile, Request replayRequest,
                             ProviderExchangeComparator.Exchange replayResponse,
                             Map<String, String> replayTokenToNativeToken,
                             AzureNamespaceMapping mapping, String nativeEndpoint,
                             String replayEndpoint, Path repoRoot, Path distribution,
                             Path testSource, String testName, Path receipt) throws IOException {
        Capture capture = read(captureFile, "azure");
        require(capture.kind().equals("exchange"), "Azure exchange receipt needs one exchange");
        Step nativeStep = capture.steps().getFirst();
        mapping.assertMapped(nativeStep.request().path(), replayRequest.path(),
                nativeEndpoint, replayEndpoint);
        assertRequest(nativeStep.request(), replayRequest, replayRequest.path(),
                replayTokenToNativeToken, "marker", true, capture.probeId());
        String version = header(nativeStep.request(), "x-ms-version");
        String clientId = header(nativeStep.request(), "x-ms-client-request-id");
        ProviderExchangeComparator.assertAzure(nativeStep.exchange(),
                normalizedAzureResponse(replayResponse, clientId), nativeEndpoint, replayEndpoint,
                mapping.nativeContainer(), mapping.replayContainer(), version, clientId,
                token(nativeStep.request().query(), "marker"), mappedToken(replayRequest.query(),
                        "marker", replayTokenToNativeToken), nativeStep.request().method());
        write(capture, captureFile, repoRoot, distribution, testSource, testName, receipt);
    }

    public static void gcsRun(Path captureFile, List<ReplayStep> replay,
                              Map<String, String> replayTokenToNativeToken, String replayBucket,
                              boolean fullProjection, Path repoRoot, Path distribution,
                              Path testSource, String testName, Path receipt) throws IOException {
        Capture capture = read(captureFile, "gcs");
        require(!capture.kind().equals("exchange"), "GCS run receipt needs typed run capture");
        assertRunRequests(capture, replay, "/storage/v1/b/" + replayBucket + "/o",
                replayTokenToNativeToken, "pageToken", false);
        if (capture.kind().equals("retry_run")) {
            for (int i = 0; i < replay.size(); i++) {
                ProviderExchangeComparator.assertGcs(capture.steps().get(i).exchange(),
                        replay.get(i).exchange(), "{bucket}", replayBucket, fullProjection);
            }
        } else {
            List<ProviderExchangeComparator.CapturedExchangePage> nativePages = new ArrayList<>();
            List<ProviderExchangeComparator.CapturedExchangePage> replayPages = new ArrayList<>();
            for (int i = 0; i < replay.size(); i++) {
                nativePages.add(new ProviderExchangeComparator.CapturedExchangePage(
                        token(capture.steps().get(i).request().query(), "pageToken"),
                        capture.steps().get(i).exchange()));
                replayPages.add(new ProviderExchangeComparator.CapturedExchangePage(
                        token(replay.get(i).request().query(), "pageToken"), replay.get(i).exchange()));
            }
            ProviderExchangeComparator.assertGcsWalk(nativePages, replayPages, "{bucket}",
                    replayBucket, fullProjection, 1024, 1_000_000);
        }
        write(capture, captureFile, repoRoot, distribution, testSource, testName, receipt);
    }

    public static void azureRun(Path captureFile, List<ReplayStep> replay,
                                Map<String, String> replayTokenToNativeToken,
                                AzureNamespaceMapping mapping, String nativeEndpoint,
                                String replayEndpoint, Path repoRoot, Path distribution,
                                Path testSource, String testName, Path receipt) throws IOException {
        Capture capture = read(captureFile, "azure");
        require(!capture.kind().equals("exchange"), "Azure run receipt needs typed run capture");
        require(!replay.isEmpty(), "Azure replay run is empty");
        for (int i = 0; i < replay.size(); i++) {
            mapping.assertMapped(capture.steps().get(i).request().path(), replay.get(i).request().path(),
                    nativeEndpoint, replayEndpoint);
        }
        assertRunRequests(capture, replay, replay.getFirst().request().path(),
                replayTokenToNativeToken, "marker", true);
        String version = header(capture.steps().getFirst().request(), "x-ms-version");
        if (capture.kind().equals("retry_run")) {
            for (int i = 0; i < replay.size(); i++) {
                Step nativeStep = capture.steps().get(i);
                ReplayStep replayStep = replay.get(i);
                String clientId = header(nativeStep.request(), "x-ms-client-request-id");
                ProviderExchangeComparator.assertAzure(nativeStep.exchange(),
                        normalizedAzureResponse(replayStep.exchange(), clientId), nativeEndpoint,
                        replayEndpoint, mapping.nativeContainer(), mapping.replayContainer(),
                        version, clientId, token(nativeStep.request().query(), "marker"),
                        mappedToken(replayStep.request().query(), "marker", replayTokenToNativeToken),
                        nativeStep.request().method());
            }
        } else {
            List<ProviderExchangeComparator.CapturedExchangePage> nativePages = new ArrayList<>();
            List<ProviderExchangeComparator.CapturedExchangePage> replayPages = new ArrayList<>();
            for (int i = 0; i < replay.size(); i++) {
                Step nativeStep = capture.steps().get(i);
                ReplayStep replayStep = replay.get(i);
                nativePages.add(azurePage(nativeStep.request(), nativeStep.exchange()));
                replayPages.add(azurePage(replayStep.request(), replayStep.exchange()));
            }
            ProviderExchangeComparator.assertAzureWalk(nativePages, replayPages, nativeEndpoint,
                    replayEndpoint, mapping.nativeContainer(), mapping.replayContainer(),
                    version, 1024, 1_000_000);
        }
        write(capture, captureFile, repoRoot, distribution, testSource, testName, receipt);
    }

    private static ProviderExchangeComparator.CapturedExchangePage azurePage(
            Request request, ProviderExchangeComparator.Exchange exchange) {
        return new ProviderExchangeComparator.CapturedExchangePage(token(request.query(), "marker"),
                decodedQuery(request.query(), "prefix"), decodedQuery(request.query(), "delimiter"),
                decodedQuery(request.query(), "maxresults"), exchange);
    }

    private static String decodedQuery(String query, String name) {
        String value = token(query, name);
        return value == null ? null : URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private static void assertRunRequests(Capture capture, List<ReplayStep> replay, String replayPath,
                                          Map<String, String> tokenMapping, String tokenName,
                                          boolean azure) {
        require(capture.steps().size() == replay.size(), "replay run exchange count differs");
        for (int i = 0; i < replay.size(); i++) {
            Step step = capture.steps().get(i);
            ReplayStep actual = replay.get(i);
            require(step.phase().equals(actual.phase()), "replay run phase differs");
            assertRequest(step.request(), actual.request(), replayPath, tokenMapping, tokenName,
                    azure, capture.probeId());
            if (capture.kind().equals("sdk_run") || capture.kind().equals("retry_run")) {
                assertSdkHeaders(step.request(), actual.request());
            }
            if (azure) {
                String clientId = header(step.request(), "x-ms-client-request-id");
                String nativeEcho = step.exchange().headers().containsKey("x-ms-client-request-id")
                        ? step.exchange().headers().get("x-ms-client-request-id").getFirst() : null;
                require(java.util.Objects.equals(clientId, nativeEcho),
                        "native response client ID differs from captured request");
                normalizedAzureResponse(actual.exchange(), clientId);
            }
        }
    }

    private static void assertRequest(Request nativeRequest, Request replay, String replayPath,
                                      Map<String, String> tokenMapping, String tokenName,
                                      boolean azure, String probeId) {
        require(nativeRequest.method().equals(replay.method()) && replay.path().equals(replayPath),
                "replay request method/path differs from captured probe");
        String expectedNativePath = azure ? nativeRequest.path() : "/storage/v1/b/{bucket}/o";
        require(nativeRequest.path().equals(expectedNativePath), "native request path differs");
        require(nativeRequest.query().equals(mappedQuery(replay.query(), tokenName, tokenMapping)),
                "replay request query differs from captured probe");
        boolean duplicateProbe = probeId.equals("gcs-05") || probeId.equals("azure-08");
        require(noUnexpectedDuplicates(nativeRequest.query(), duplicateProbe)
                && noUnexpectedDuplicates(replay.query(), duplicateProbe),
                "duplicate query field outside duplicate-parameter probe");
        if (azure) {
            require(header(nativeRequest, "x-ms-version") != null
                    && header(nativeRequest, "x-ms-version").equals(header(replay, "x-ms-version")),
                    "replay request service version differs");
            String nativeClientId = header(nativeRequest, "x-ms-client-request-id");
            String replayClientId = header(replay, "x-ms-client-request-id");
            require(java.util.Objects.equals(nativeClientId,
                    replayClientId == null ? null : opaqueDigest(replayClientId)),
                    "replay request client ID differs");
        }
    }

    private static String mappedQuery(String query, String tokenName, Map<String, String> mapping) {
        List<String> parts = new ArrayList<>();
        for (String part : query.split("&", -1)) {
            String prefix = tokenName + "=";
            if (part.startsWith(prefix) && part.length() > prefix.length()) {
                String mapped = mapping.get(part.substring(prefix.length()));
                require(mapped != null, "replay continuation token lacks explicit mapping");
                parts.add(prefix + mapped);
            } else parts.add(part);
        }
        return String.join("&", parts);
    }

    private static String mappedToken(String query, String tokenName, Map<String, String> mapping) {
        return token(mappedQuery(query, tokenName, mapping), tokenName);
    }

    private static boolean noUnexpectedDuplicates(String query, boolean duplicateProbe) {
        if (duplicateProbe) return true;
        java.util.Set<String> names = new java.util.HashSet<>();
        for (String part : query.split("&")) {
            if (!names.add(part.split("=", 2)[0])) return false;
        }
        return true;
    }

    private static String token(String query, String name) {
        String result = null;
        for (String part : query.split("&")) {
            if (part.startsWith(name + "=")) {
                require(result == null, "duplicate query field " + name);
                result = part.substring(name.length() + 1);
            }
        }
        return result;
    }

    private static String header(Request request, String name) {
        List<String> values = request.headers().get(name);
        require(values == null || values.size() == 1, "duplicate request header " + name);
        return values == null ? null : values.getFirst();
    }

    private static void assertSdkHeaders(Request nativeRequest, Request replayRequest) {
        int classified = 0;
        for (String name : List.of("user-agent", "x-goog-api-client")) {
            String nativeValue = header(nativeRequest, name);
            if (nativeValue == null) continue;
            classified++;
            String replayValue = header(replayRequest, name);
            require(replayValue != null, "replay SDK header " + name + " is missing");
            List<String> replayTokens = List.of(replayValue.split("\\s+"));
            for (String token : nativeValue.split("\\s+")) {
                require(replayTokens.contains(token), "replay SDK product/version token differs");
            }
        }
        require(classified > 0, "native SDK run lacks classified product/version header");
    }

    private static ProviderExchangeComparator.Exchange normalizedAzureResponse(
            ProviderExchangeComparator.Exchange response, String nativeClientId) {
        List<String> values = response.headers().get("x-ms-client-request-id");
        if (values == null) {
            require(nativeClientId == null, "replay response omitted captured client ID");
            return response;
        }
        require(values.size() == 1, "duplicate replay client ID response header");
        require(nativeClientId != null && nativeClientId.equals(opaqueDigest(values.getFirst())),
                "replay response client ID differs from captured request");
        Map<String, List<String>> normalized = new HashMap<>(response.headers());
        normalized.put("x-ms-client-request-id", List.of(nativeClientId));
        return new ProviderExchangeComparator.Exchange(response.status(), normalized, response.body());
    }

    private static Capture read(Path file, String provider) throws IOException {
        JsonNode root = JSON.readTree(Files.readAllBytes(file));
        require(provider.equals(root.path("provider").asText()), "capture provider differs");
        String schema = root.path("schema_version").asText();
        String kind = schema.equals("provider-capture-v1") ? "exchange"
                : schema.equals("provider-capture-run-v1") ? root.path("evidence_kind").asText() : "";
        require(List.of("exchange", "walk", "sdk_run", "retry_run").contains(kind),
                "capture schema/kind differs");
        List<Step> steps = new ArrayList<>();
        if (kind.equals("exchange")) steps.add(step(root, "exchange"));
        else {
            for (JsonNode entry : root.path("steps")) steps.add(step(entry.path("exchange"),
                    entry.path("phase").asText()));
            require(steps.size() >= 2 && steps.size() <= 1024, "run capture is incomplete/unbounded");
        }
        return new Capture(kind, root.path("api_version").asText(),
                root.path("probe_id").asText(), List.copyOf(steps));
    }

    private static Step step(JsonNode root, String phase) {
        JsonNode request = root.path("request");
        Map<String, List<String>> requestHeaders = new LinkedHashMap<>();
        for (JsonNode field : request.path("headers")) requestHeaders.computeIfAbsent(
                field.path("name").asText(), ignored -> new ArrayList<>()).add(field.path("value").asText());
        Request scope = new Request(request.path("method").asText(), request.path("path").asText(),
                request.path("query").asText(""), requestHeaders);
        JsonNode response = root.path("response");
        Map<String, List<String>> responseHeaders = new LinkedHashMap<>();
        for (JsonNode field : response.path("headers")) responseHeaders.computeIfAbsent(
                field.path("name").asText(), ignored -> new ArrayList<>()).add(field.path("value").asText());
        var exchange = new ProviderExchangeComparator.Exchange(response.path("status").asInt(),
                responseHeaders, Base64.getDecoder().decode(response.path("body_base64").asText()));
        return new Step(phase, scope, exchange);
    }

    private static void write(Capture capture, Path captureFile, Path repoRoot, Path distribution,
                              Path testSource, String testName, Path receipt) throws IOException {
        require(testName != null && !testName.isBlank(), "test method is required");
        String commit = git(repoRoot, "rev-parse", "HEAD");
        require(git(repoRoot, "status", "--porcelain", "--untracked-files=all").isEmpty(),
                "receipt requires a clean tested source tree");
        Path relative = repoRoot.toAbsolutePath().normalize().relativize(testSource.toAbsolutePath().normalize());
        require(!relative.startsWith("..") && Files.isRegularFile(testSource),
                "receipt test source must be inside repository");
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("schema_version", "provider-match-receipt-v2");
        fields.put("comparison", "ProviderExchangeComparator");
        fields.put("status", "PASS");
        fields.put("test", testName);
        fields.put("test_path", relative.toString().replace('\\', '/'));
        fields.put("test_source_sha256", sha256(testSource));
        fields.put("api_version", capture.apiVersion());
        fields.put("evidence_kind", capture.kind());
        fields.put("compared_steps", capture.steps().size());
        fields.put("capture_sha256", sha256(captureFile));
        fields.put("replay_commit", commit);
        fields.put("distribution_sha256", sha256(distribution));
        fields.put("distribution_name", distribution.getFileName().toString());
        Files.createDirectories(receipt.getParent());
        Files.writeString(receipt, JSON.writeValueAsString(fields) + "\n", StandardCharsets.UTF_8);
    }

    private static String git(Path repoRoot, String... arguments) throws IOException {
        List<String> command = new ArrayList<>(List.of("git", "-C", repoRoot.toString()));
        command.addAll(List.of(arguments));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        try {
            if (process.waitFor() != 0) throw new IOException("cannot inspect replay source: " + output);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while inspecting replay source", e);
        }
        return output;
    }

    private static String sha256(Path file) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(file)) {
                byte[] buffer = new byte[8192];
                int count;
                while ((count = input.read(buffer)) >= 0) digest.update(buffer, 0, count);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String opaqueDigest(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            return "token-sha256-" + HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }

    private record Step(String phase, Request request, ProviderExchangeComparator.Exchange exchange) { }
    private record Capture(String kind, String apiVersion, String probeId, List<Step> steps) { }
}
