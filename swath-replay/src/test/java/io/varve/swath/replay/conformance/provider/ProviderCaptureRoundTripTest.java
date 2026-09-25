/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Runs the actual sanitizer and then compares its output to a replay error envelope. */
class ProviderCaptureRoundTripTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void sanitizedGcsErrorStillHasTypedEnvelope(@TempDir Path dir) throws Exception {
        String nativeBody = """
                {"error":{"code":400,"message":"bucket live-bucket invalid",
                 "errors":[{"reason":"invalid","message":"live-bucket invalid"}]}}
                """;
        String raw = """
                {"captured_at":"2026-09-25T10:00:00Z",
                 "request":{"method":"GET","url":"https://storage.googleapis.com/storage/v1/b/live-bucket/o?maxResults=0",
                            "headers":[{"name":"Accept","value":"application/json"}]},
                 "response":{"status":400,"headers":[{"name":"Content-Type","value":"application/json"}],
                             "body_base64":"%s"}}
                """.formatted(Base64.getEncoder().encodeToString(bytes(nativeBody)));
        JsonNode safe = sanitize(dir, "gcs", "gcs-06", raw, "--bucket", "live-bucket", "json-v1");
        byte[] sanitizedBody = Base64.getDecoder().decode(safe.path("response").path("body_base64").asText());
        assertThat(new String(sanitizedBody, StandardCharsets.UTF_8)).doesNotContain("live-bucket");
        var nativeExchange = exchange(safe);
        var replayExchange = new ProviderExchangeComparator.Exchange(400,
                Map.of("content-type", List.of("application/json; charset=utf-8")),
                bytes("""
                        {"error":{"code":400,"message":"Invalid GCS listing request",
                         "errors":[{"reason":"invalid","message":"Invalid GCS listing request"}]}}
                        """));
        assertThatCode(() -> ProviderExchangeComparator.assertGcs(nativeExchange, replayExchange,
                "{bucket}", "replay", false)).doesNotThrowAnyException();
        Path distribution = dir.resolve("replay.jar");
        Path receipt = dir.resolve("match.json");
        Files.writeString(distribution, "synthetic replay distribution");
        Path testSource = dir.resolve("EvidenceTest.java");
        Files.writeString(testSource, "class EvidenceTest { @Test void exactTest() {} }");
        git(dir, "init");
        git(dir, "config", "user.name", "Replay Test");
        git(dir, "config", "user.email", "replay-test@example.invalid");
        git(dir, "add", "-A");
        git(dir, "commit", "-m", "fixture");
        var replayRequest = new ProviderMatchReceipt.Request("GET", "/storage/v1/b/replay/o",
                "maxResults=0", Map.of());
        var wrongRequest = new ProviderMatchReceipt.Request("GET", "/storage/v1/b/replay/o",
                "maxResults=1", Map.of());
        assertThatThrownBy(() -> ProviderMatchReceipt.gcs(dir.resolve("safe.json"), wrongRequest,
                replayExchange, Map.of(), "replay", false, dir, distribution,
                testSource, "exactTest", receipt)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("query differs");
        ProviderMatchReceipt.gcs(dir.resolve("safe.json"), replayRequest, replayExchange,
                Map.of(), "replay", false, dir, distribution, testSource, "exactTest", receipt);
        JsonNode proof = JSON.readTree(receipt.toFile());
        assertThat(proof.path("status").asText()).isEqualTo("PASS");
        assertThat(proof.path("replay_commit").asText()).matches("[0-9a-f]{40}");
        assertThat(proof.path("distribution_sha256").asText()).matches("[0-9a-f]{64}");
        assertThat(proof.path("test_source_sha256").asText()).matches("[0-9a-f]{64}");
    }

    @Test
    void sanitizedAzureErrorKeepsCodeAndRedactsNativeDetails(@TempDir Path dir) throws Exception {
        String nativeBody = """
                <?xml version="1.0" encoding="utf-8"?><Error><Code>OutOfRangeQueryParameterValue</Code>
                <Message>bad RequestId: secret-id</Message><QueryParameterName>maxresults</QueryParameterName>
                <QueryParameterValue>0</QueryParameterValue><RequestId>secret-id</RequestId></Error>
                """;
        String raw = """
                {"captured_at":"2026-09-25T10:00:00Z",
                 "request":{"method":"GET",
                            "url":"https://liveacct.blob.core.windows.net/livecontainer?restype=container&comp=list&maxresults=0",
                            "headers":[{"name":"x-ms-version","value":"2026-06-06"},
                                       {"name":"x-ms-client-request-id","value":"native-client"}]},
                 "response":{"status":400,"headers":[
                     {"name":"Content-Type","value":"application/xml"},
                     {"name":"x-ms-version","value":"2026-06-06"},
                     {"name":"x-ms-client-request-id","value":"native-client"},
                     {"name":"x-ms-error-code","value":"OutOfRangeQueryParameterValue"},
                     {"name":"x-ms-request-id","value":"secret-id"}],
                     "body_base64":"%s"}}
                """.formatted(Base64.getEncoder().encodeToString(bytes(nativeBody)));
        JsonNode safe = sanitize(dir, "azure", "azure-08", raw,
                "--account", "liveacct", "2026-06-06", "--container", "livecontainer");
        byte[] sanitizedBody = Base64.getDecoder().decode(safe.path("response").path("body_base64").asText());
        assertThat(new String(sanitizedBody, StandardCharsets.UTF_8)).doesNotContain("secret-id");
        var replayExchange = new ProviderExchangeComparator.Exchange(400,
                Map.of("content-type", List.of("application/xml"),
                        "x-ms-version", List.of("2026-06-06"),
                        "x-ms-error-code", List.of("OutOfRangeQueryParameterValue")),
                bytes("""
                        <?xml version="1.0" encoding="utf-8"?><Error><Code>OutOfRangeQueryParameterValue</Code>
                        <Message>OutOfRangeQueryParameterValue</Message></Error>
                        """));
        assertThatCode(() -> ProviderExchangeComparator.assertAzure(exchange(safe), replayExchange,
                "https://{account}.blob.core.windows.net/", "http://127.0.0.1/replay/",
                "{container}", "bucket", "2026-06-06", null, null, null))
                .doesNotThrowAnyException();
        var missingClientId = new ProviderMatchReceipt.Request("GET", "/replay/bucket",
                "restype=container&comp=list&maxresults=0",
                Map.of("x-ms-version", List.of("2026-06-06")));
        assertThatThrownBy(() -> ProviderMatchReceipt.azure(dir.resolve("safe.json"),
                missingClientId, replayExchange, Map.of(),
                new AzureNamespaceMapping("{account}", "{container}", "replay", "bucket"),
                "https://{account}.blob.core.windows.net/", "http://127.0.0.1/replay/",
                dir, dir.resolve("replay.jar"), dir.resolve("EvidenceTest.java"),
                "exactTest", dir.resolve("match.json")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("client ID differs");
        Path source = dir.resolve("EvidenceTest.java");
        Path distribution = dir.resolve("replay.jar");
        Files.writeString(source, "class EvidenceTest { @Test void exactTest() {} }");
        Files.writeString(distribution, "synthetic replay distribution");
        git(dir, "init");
        git(dir, "config", "user.name", "Replay Test");
        git(dir, "config", "user.email", "replay-test@example.invalid");
        git(dir, "add", "-A");
        git(dir, "commit", "-m", "fixture");
        var matchingClientId = new ProviderMatchReceipt.Request("GET", "/replay/bucket",
                "restype=container&comp=list&maxresults=0",
                Map.of("x-ms-version", List.of("2026-06-06"),
                        "x-ms-client-request-id", List.of("native-client")));
        var replayWithEcho = new ProviderExchangeComparator.Exchange(400,
                Map.of("content-type", List.of("application/xml"),
                        "x-ms-version", List.of("2026-06-06"),
                        "x-ms-error-code", List.of("OutOfRangeQueryParameterValue"),
                        "x-ms-client-request-id", List.of("native-client")), replayExchange.body());
        ProviderMatchReceipt.azure(dir.resolve("safe.json"), matchingClientId, replayWithEcho,
                Map.of(), new AzureNamespaceMapping("{account}", "{container}", "replay", "bucket"),
                "https://{account}.blob.core.windows.net/", "http://127.0.0.1/replay/",
                dir, distribution, source, "exactTest", dir.resolve("match.json"));
        assertThat(JSON.readTree(dir.resolve("match.json").toFile()).path("status").asText())
                .isEqualTo("PASS");
        assertThatThrownBy(() -> ProviderMatchReceipt.azureRun(dir.resolve("safe.json"),
                List.of(new ProviderMatchReceipt.ReplayStep("walk", matchingClientId, replayWithEcho)),
                Map.of(), new AzureNamespaceMapping("{account}", "{container}", "replay", "bucket"),
                "https://{account}.blob.core.windows.net/", "http://127.0.0.1/replay/",
                dir, distribution, source, "exactTest", dir.resolve("bad.json")))
                .hasMessageContaining("typed run");
        String otherPreamble = nativeBody.replace("encoding=\"utf-8\"", "encoding=\"UTF-8\"");
        JsonNode differentlyDeclared = sanitize(dir, "azure", "azure-08",
                raw.replace(Base64.getEncoder().encodeToString(bytes(nativeBody)),
                        Base64.getEncoder().encodeToString(bytes(otherPreamble))),
                "--account", "liveacct", "2026-06-06", "--container", "livecontainer");
        assertThatThrownBy(() -> ProviderExchangeComparator.assertAzure(exchange(differentlyDeclared),
                replayExchange, "https://{account}.blob.core.windows.net/",
                "http://127.0.0.1/replay/", "{container}", "bucket",
                "2026-06-06", null, null, null)).hasMessageContaining("BOM/prolog");
    }

    private static JsonNode sanitize(Path dir, String provider, String probeId, String raw,
                                     String nameFlag, String nameValue, String version,
                                     String... moreNames) throws Exception {
        Path script = Path.of("..", "scripts", "provider-conformance", "evidence.py").toAbsolutePath();
        Path manifest = dir.resolve("manifest.json");
        Path input = dir.resolve("raw.json");
        Path output = dir.resolve("safe.json");
        Files.writeString(manifest, """
                {"provider":"%s","namespace_mode":"%s","region":"test-region",
                 "api_version":"%s","sdk_product":"%s","sdk_version":"test-sdk",
                 "capture_date":"2026-09-25",
                 "objects":[{"name":"a","size":1,"sha256":"%s"}]}
                """.formatted(provider, provider.equals("gcs") ? "flat-ubla" : "flat-hns-off",
                version, provider.equals("gcs") ? "gcloud-java"
                        : "azsdk-java-azure-storage-blob", "a".repeat(64)));
        Files.writeString(input, raw);
        List<String> command = new ArrayList<>(List.of("python3", script.toString(), "sanitize",
                "--input", input.toString(), "--output", output.toString(), "--provider", provider,
                "--manifest", manifest.toString(), "--probe-id", probeId, nameFlag, nameValue));
        command.addAll(List.of(moreNames));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String report = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(process.waitFor()).as(report).isZero();
        return JSON.readTree(output.toFile());
    }

    private static ProviderExchangeComparator.Exchange exchange(JsonNode safe) {
        Map<String, List<String>> headers = new HashMap<>();
        for (JsonNode header : safe.path("response").path("headers")) {
            headers.computeIfAbsent(header.path("name").asText(), ignored -> new ArrayList<>())
                    .add(header.path("value").asText());
        }
        return new ProviderExchangeComparator.Exchange(safe.path("response").path("status").asInt(), headers,
                Base64.getDecoder().decode(safe.path("response").path("body_base64").asText()));
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    private static void git(Path root, String... arguments) throws Exception {
        List<String> command = new ArrayList<>(List.of("git", "-C", root.toString()));
        command.addAll(List.of(arguments));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(process.waitFor()).as(output).isZero();
    }
}
