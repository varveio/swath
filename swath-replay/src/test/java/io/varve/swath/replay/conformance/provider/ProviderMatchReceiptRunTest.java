/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.azure.AzureListRequest;
import io.varve.swath.replay.protocol.azure.AzureListResult;
import io.varve.swath.replay.protocol.azure.AzureXml;
import io.varve.swath.replay.server.BudgetedOutput;
import io.varve.swath.replay.testkit.OwnedBodyBytes;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Actual receipt paths: request binding, SDK headers, token maps, and clean build identity. */
class ProviderMatchReceiptRunTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String NATIVE_TOKEN = "token-sha256-" + "a".repeat(64);

    @Test
    void gcsSdkRunReceiptNeedsEveryRequestStepAndSdkToken(@TempDir Path dir) throws Exception {
        String nativeFirst = gcsBody("{bucket}", "a", NATIVE_TOKEN);
        String nativeLast = gcsBody("{bucket}", "b", null);
        Path capture = dir.resolve("capture.json");
        requestRun(capture, "gcs", "sdk_run", "gcs-09", List.of(
                step("walk", "/storage/v1/b/{bucket}/o", "prefix=a", "gcloud-java/2.73.0",
                        null, nativeFirst),
                step("walk", "/storage/v1/b/{bucket}/o", "prefix=a&pageToken=" + NATIVE_TOKEN,
                        "gcloud-java/2.73.0", null, nativeLast)));
        Path source = cleanFixtureRepo(dir);
        var replay = List.of(
                replay("walk", "/storage/v1/b/replay/o", "prefix=a", "gcloud-java/2.73.0 platform",
                        null, gcsBody("replay", "a", "local-token"), "application/json"),
                replay("walk", "/storage/v1/b/replay/o", "prefix=a&pageToken=local-token",
                        "gcloud-java/2.73.0 platform", null, gcsBody("replay", "b", null),
                        "application/json"));
        ProviderMatchReceipt.gcsRun(capture, replay, Map.of("local-token", NATIVE_TOKEN),
                "replay", false, dir, dir.resolve("dist.jar"), source, "exactTest",
                dir.resolve("receipt.json"));
        var receipt = JSON.readTree(dir.resolve("receipt.json").toFile());
        assertThat(receipt.path("evidence_kind").asText()).isEqualTo("sdk_run");
        assertThat(receipt.path("compared_steps").asInt()).isEqualTo(2);
        assertThatThrownBy(() -> ProviderMatchReceipt.gcs(capture, replay.getFirst().request(),
                replay.getFirst().exchange(), Map.of(), "replay", false, dir,
                dir.resolve("dist.jar"), source, "exactTest", dir.resolve("bad.json")))
                .hasMessageContaining("one exchange");
        assertThatThrownBy(() -> ProviderMatchReceipt.gcsRun(capture, replay.subList(0, 1),
                Map.of("local-token", NATIVE_TOKEN), "replay", false, dir,
                dir.resolve("dist.jar"), source, "exactTest", dir.resolve("bad.json")))
                .hasMessageContaining("exchange count");
        assertThatThrownBy(() -> ProviderMatchReceipt.gcsRun(capture, replay, Map.of(),
                "replay", false, dir, dir.resolve("dist.jar"), source, "exactTest",
                dir.resolve("bad.json"))).hasMessageContaining("explicit mapping");
        var wrongSdk = List.of(replay.getFirst(), replay("walk", "/storage/v1/b/replay/o",
                "prefix=a&pageToken=local-token", "gcloud-java/2.73.1", null,
                gcsBody("replay", "b", null), "application/json"));
        assertThatThrownBy(() -> ProviderMatchReceipt.gcsRun(capture, wrongSdk,
                Map.of("local-token", NATIVE_TOKEN), "replay", false, dir,
                dir.resolve("dist.jar"), source, "exactTest", dir.resolve("bad.json")))
                .hasMessageContaining("SDK product/version");
        var wrongPhase = List.of(replay.getFirst(), replay("normal_retry", "/storage/v1/b/replay/o",
                "prefix=a&pageToken=local-token", "gcloud-java/2.73.0", null,
                gcsBody("replay", "b", null), "application/json"));
        assertThatThrownBy(() -> ProviderMatchReceipt.gcsRun(capture, wrongPhase,
                Map.of("local-token", NATIVE_TOKEN), "replay", false, dir,
                dir.resolve("dist.jar"), source, "exactTest", dir.resolve("bad.json")))
                .hasMessageContaining("phase differs");
        Path exchange = dir.resolve("single.json");
        Files.writeString(exchange, JSON.writeValueAsString(Map.of("schema_version", "provider-capture-v1",
                "provider", "gcs", "api_version", "json-v1", "probe_id", "gcs-09",
                "request", step("walk", "/storage/v1/b/{bucket}/o", "prefix=a", null,
                        null, nativeFirst).get("request"),
                "response", step("walk", "/storage/v1/b/{bucket}/o", "prefix=a", null,
                        null, nativeFirst).get("response"))));
        assertThatThrownBy(() -> ProviderMatchReceipt.gcsRun(exchange, replay,
                Map.of("local-token", NATIVE_TOKEN), "replay", false, dir,
                dir.resolve("dist.jar"), source, "exactTest", dir.resolve("bad.json")))
                .hasMessageContaining("typed run");
    }

    @Test
    void azureWalkReceiptPreservesClientIdAndRejectsUnexpectedDuplicate(@TempDir Path dir) throws Exception {
        String rawClientId = "sdk-probe-1";
        String safeClientId = "token-sha256-" + hexSha(rawClientId);
        String endpoint = "http://127.0.0.1:1234/replay/";
        String nativeEndpoint = "https://{account}.blob.core.windows.net/";
        String firstReplay = azureBody("a", null, "local-token", endpoint);
        String lastReplay = azureBody("b", "local-token", null, endpoint);
        String firstNative = firstReplay.replace(endpoint, nativeEndpoint)
                .replace("ContainerName=\"bucket\"", "ContainerName=\"{container}\"")
                .replace("local-token", NATIVE_TOKEN);
        String lastNative = lastReplay.replace(endpoint, nativeEndpoint)
                .replace("ContainerName=\"bucket\"", "ContainerName=\"{container}\"")
                .replace("local-token", NATIVE_TOKEN);
        Path capture = dir.resolve("capture.json");
        requestRun(capture, "azure", "walk", "azure-09", List.of(
                step("walk", "/{container}", "restype=container&comp=list&maxresults=1",
                        null, safeClientId, firstNative),
                step("walk", "/{container}", "restype=container&comp=list&maxresults=1&marker="
                        + NATIVE_TOKEN, null, safeClientId, lastNative)));
        Path source = cleanFixtureRepo(dir);
        var replay = List.of(
                replay("walk", "/replay/bucket", "restype=container&comp=list&maxresults=1",
                        null, rawClientId, firstReplay, "application/xml"),
                replay("walk", "/replay/bucket", "restype=container&comp=list&maxresults=1&marker=local-token",
                        null, rawClientId, lastReplay, "application/xml"));
        ProviderMatchReceipt.azureRun(capture, replay, Map.of("local-token", NATIVE_TOKEN),
                new AzureNamespaceMapping("{account}", "{container}", "replay", "bucket"),
                nativeEndpoint, endpoint, dir, dir.resolve("dist.jar"), source, "exactTest",
                dir.resolve("receipt.json"));
        assertThat(JSON.readTree(dir.resolve("receipt.json").toFile())
                .path("compared_steps").asInt()).isEqualTo(2);
        Path duplicate = dir.resolve("duplicate.json");
        requestRun(duplicate, "azure", "walk", "azure-09", List.of(
                step("walk", "/{container}", "restype=container&comp=list&maxresults=1&prefix=a&prefix=a",
                        null, safeClientId, firstNative),
                step("walk", "/{container}", "restype=container&comp=list&maxresults=1&prefix=a&prefix=a&marker="
                        + NATIVE_TOKEN, null, safeClientId, lastNative)));
        var repeated = List.of(replay("walk", "/replay/bucket",
                        "restype=container&comp=list&maxresults=1&prefix=a&prefix=a", null,
                        rawClientId, firstReplay, "application/xml"),
                replay("walk", "/replay/bucket",
                        "restype=container&comp=list&maxresults=1&prefix=a&prefix=a&marker=local-token",
                        null, rawClientId, lastReplay, "application/xml"));
        assertThatThrownBy(() -> ProviderMatchReceipt.azureRun(duplicate, repeated,
                Map.of("local-token", NATIVE_TOKEN),
                new AzureNamespaceMapping("{account}", "{container}", "replay", "bucket"),
                nativeEndpoint, endpoint, dir, dir.resolve("dist.jar"), source, "exactTest",
                dir.resolve("bad.json"))).hasMessageContaining("duplicate query field");
    }

    private static Map<String, Object> step(String phase, String path, String query, String sdk,
                                            String clientId, String body) {
        List<Map<String, String>> requestHeaders = new ArrayList<>();
        List<Map<String, String>> responseHeaders = new ArrayList<>();
        if (path.startsWith("/{container}")) {
            requestHeaders.add(Map.of("name", "x-ms-version", "value", "2026-06-06"));
            responseHeaders.add(Map.of("name", "x-ms-version", "value", "2026-06-06"));
        }
        if (sdk != null) requestHeaders.add(Map.of("name", "user-agent", "value", sdk));
        if (clientId != null) {
            requestHeaders.add(Map.of("name", "x-ms-client-request-id", "value", clientId));
            responseHeaders.add(Map.of("name", "x-ms-client-request-id", "value", clientId));
        }
        responseHeaders.add(Map.of("name", "content-type", "value",
                path.startsWith("/{container}") ? "application/xml" : "application/json"));
        return Map.of("phase", phase, "request", Map.of("method", "GET", "path", path,
                "query", query, "headers", requestHeaders),
                "response", Map.of("status", 200, "headers", responseHeaders,
                        "body_base64", Base64.getEncoder().encodeToString(bytes(body))));
    }

    private static void requestRun(Path file, String provider, String kind, String probe,
                                   List<Map<String, Object>> steps) throws Exception {
        List<Map<String, Object>> entries = new ArrayList<>();
        for (var step : steps) {
            entries.add(Map.of("phase", step.get("phase"), "exchange", Map.of(
                    "request", step.get("request"), "response", step.get("response"))));
        }
        Files.writeString(file, JSON.writeValueAsString(Map.of("schema_version", "provider-capture-run-v1",
                "evidence_kind", kind, "provider", provider, "probe_id", probe,
                "api_version", provider.equals("gcs") ? "json-v1" : "2026-06-06",
                "steps", entries)));
    }

    private static ProviderMatchReceipt.ReplayStep replay(String phase, String path, String query,
                                                          String sdk, String clientId, String body,
                                                          String contentType) {
        Map<String, List<String>> requestHeaders = new java.util.HashMap<>();
        Map<String, List<String>> responseHeaders = new java.util.HashMap<>();
        responseHeaders.put("content-type", List.of(contentType));
        if (sdk != null) requestHeaders.put("user-agent", List.of(sdk));
        if (path.startsWith("/replay/")) {
            requestHeaders.put("x-ms-version", List.of("2026-06-06"));
            responseHeaders.put("x-ms-version", List.of("2026-06-06"));
        }
        if (clientId != null) {
            requestHeaders.put("x-ms-client-request-id", List.of(clientId));
            responseHeaders.put("x-ms-client-request-id", List.of(clientId));
        }
        return new ProviderMatchReceipt.ReplayStep(phase,
                new ProviderMatchReceipt.Request("GET", path, query, requestHeaders),
                new ProviderExchangeComparator.Exchange(200, responseHeaders, bytes(body)));
    }

    private static String gcsBody(String bucket, String name, String next) {
        String item = "{\"kind\":\"storage#object\",\"bucket\":\"" + bucket
                + "\",\"name\":\"" + name + "\",\"size\":\"1\","
                + "\"updated\":\"2026-01-01T00:00:00.000000Z\","
                + "\"etag\":\"swath-gcs-synthetic-v1\","
                + "\"generation\":\"1\",\"metageneration\":\"1\","
                + "\"storageClass\":\"STANDARD\",\"contentType\":\"application/octet-stream\"}";
        return "{\"kind\":\"storage#objects\",\"items\":[" + item + "]"
                + (next == null ? "}" : ",\"nextPageToken\":\"" + next + "\"}");
    }

    private static String azureBody(String name, String marker, String next, String endpoint) throws Exception {
        AzureListRequest request = new AzureListRequest("replay", "bucket", "2026-06-06", null,
                null, null, marker, 1, "1", false, false, marker != null, true, null);
        AzureListResult page = new AzureListResult(request, List.of(
                new AzureListResult.Entry.Blob(new ListedObject(bytes(name), 1,
                        1_767_225_600_000_000L, null, null, null, null, null, null))), next);
        try (BudgetedOutput output = BudgetedOutput.standalone(4096)) {
            AzureXml.write(page, endpoint, output);
            return new String(OwnedBodyBytes.copy(output.body()), StandardCharsets.UTF_8);
        }
    }

    private static Path cleanFixtureRepo(Path dir) throws Exception {
        Path source = dir.resolve("EvidenceTest.java");
        Files.writeString(source, "class EvidenceTest { @Test void exactTest() {} }");
        Files.writeString(dir.resolve("dist.jar"), "synthetic distribution");
        for (String[] args : List.of(new String[]{"init"}, new String[]{"config", "user.name", "Test"},
                new String[]{"config", "user.email", "test@example.invalid"}, new String[]{"add", "-A"},
                new String[]{"commit", "-m", "fixture"})) {
            List<String> command = new ArrayList<>(List.of("git", "-C", dir.toString()));
            command.addAll(List.of(args));
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertThat(process.waitFor()).as(output).isZero();
        }
        return source;
    }

    private static String hexSha(String value) throws Exception {
        return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes(value)));
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}
