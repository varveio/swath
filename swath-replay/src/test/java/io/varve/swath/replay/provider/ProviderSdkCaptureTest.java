/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.varve.swath.replay.server.Protocol;
import io.varve.swath.replay.server.ReplayServer;
import io.varve.swath.replay.server.ServeConfig;
import io.varve.swath.replay.server.ServingMode;
import io.varve.swath.replay.testkit.NativeSdkFixtures;
import io.varve.swath.replay.testkit.ObjectEntries;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Offline checks of the live-campaign SDK capture driver: bounds, recording and redaction. */
class ProviderSdkCaptureTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String GCS_LIST = "{\"kind\":\"storage#objects\",\"items\":[{\"kind\":\"storage#object\","
            + "\"bucket\":\"bucket\",\"name\":\"a\",\"size\":\"1\"}]}";

    @Test
    void gateRefusesTheAttemptBeyondTheAllowanceAcrossSdkRetries() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = server(hits, 503, "{\"error\":{\"code\":503,\"message\":\"busy\",\"errors\":"
                + "[{\"reason\":\"backendError\",\"message\":\"busy\"}]}}", false);
        try {
            var result = ProviderSdkCapture.run(gcs(server).maxAttempts(2).retryAttempts(3).build());
            assertThat(result.status()).isEqualTo(ProviderSdkCapture.Status.INCOMPLETE);
            assertThat(result.reason()).isEqualTo("bound:attempt_allowance");
            assertThat(hits).hasValue(2);
            assertThat(result.storageAttempts()).isEqualTo(2);
            assertThat(result.steps()).hasSize(2).allSatisfy(step -> assertThat(status(step)).isEqualTo(503));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void expiredDeadlineSendsNothingAndCliStillWritesIncompleteCapture(@TempDir Path dir) throws Exception {
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = server(hits, 200, GCS_LIST, false);
        try {
            Path options = dir.resolve("options.json");
            Files.writeString(options, JSON.writeValueAsString(Map.of("provider", "gcs",
                    "endpoint", "http://127.0.0.1:" + server.getAddress().getPort(), "bucket", "bucket",
                    "projectId", "replay-test", "credentials", "none", "maxAttempts", 5,
                    "deadlineEpochMs", System.currentTimeMillis() - 1)));
            Path output = dir.resolve("raw.json");
            int exit = ProviderSdkCapture.execute(new String[] {"--options", options.toString(),
                    "--output", output.toString()});
            assertThat(exit).isEqualTo(ProviderSdkCapture.EXIT_INCOMPLETE);
            assertThat(hits).hasValue(0);
            JsonNode raw = JSON.readTree(output.toFile());
            assertThat(raw.path("status").asText()).isEqualTo("INCOMPLETE");
            assertThat(raw.path("reason").asText()).isEqualTo("bound:deadline");
            assertThat(raw.path("storage_attempts").asInt()).isZero();
            assertThat(raw.path("steps")).isEmpty();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void optionsRejectUnknownFieldsAndCrossProviderOptions(@TempDir Path dir) throws Exception {
        Path options = dir.resolve("options.json");
        Files.writeString(options, "{\"provider\":\"gcs\",\"bucket\":\"b\",\"projectId\":\"p\","
                + "\"maxAttempts\":1,\"deadlineEpochMs\":1,\"sasToken\":\"x\"}");
        assertThat(ProviderSdkCapture.execute(new String[] {"--options", options.toString(),
                "--output", dir.resolve("out.json").toString()})).isEqualTo(ProviderSdkCapture.EXIT_USAGE);
        assertThatThrownBy(() -> ProviderSdkCapture.Options.builder(ProviderSdkCapture.Provider.GCS)
                .bucket("b").projectId("p").startFrom("x").build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Azure options");
        assertThatThrownBy(() -> ProviderSdkCapture.Options.builder(ProviderSdkCapture.Provider.GCS)
                .bucket("b").projectId("p").requestTimeoutSeconds(31).build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("requestTimeoutSeconds");
        assertThatThrownBy(() -> new AttemptGate(1, Long.MAX_VALUE, 31, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void gzipBodiesAreRecordedDecodedWhileTheSdkReadsTheWireEncoding() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = server(hits, 200, GCS_LIST, true);
        try {
            var result = ProviderSdkCapture.run(gcs(server).maxAttempts(3).build());
            assertThat(result.status()).as(result.reason()).isEqualTo(ProviderSdkCapture.Status.COMPLETE);
            assertThat(result.entries()).containsExactly(new ProviderSdkCapture.Entry("a", false));
            Map<String, Object> response = response(result.steps().getFirst());
            assertThat(new String(Base64.getDecoder().decode((String) response.get("body_base64")),
                    StandardCharsets.UTF_8)).isEqualTo(GCS_LIST);
            assertThat(headerValues(response, "content-encoding")).isEmpty();
            assertThat(headerValues(response, "content-length")).isEmpty();
            @SuppressWarnings("unchecked")
            Map<String, Object> transport = (Map<String, Object>) ((Map<String, Object>) result.steps().getFirst()
                    .get("exchange")).get("transport");
            assertThat(transport).containsEntry("content_encoding", "gzip");
            assertThat(headerValues(response, "connection")).isEmpty();
            assertThat((Integer) transport.get("wire_bytes")).isPositive();
            assertThat(result.responseBytes()).isEqualTo(((Integer) transport.get("wire_bytes")).longValue());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void gcsSdkWalkAgainstReplayIsRecordedWithoutCredentialsAndSanitizes(@TempDir Path dir) throws Exception {
        Path fixture = NativeSdkFixtures.sorted(dir.resolve("fixture"),
                ObjectEntries.key("a/1").size(1).isLatest(true).build(),
                ObjectEntries.key("a/2").size(2).isLatest(true).build(),
                ObjectEntries.key("b").size(3).isLatest(true).build());
        try (ReplayServer server = ReplayServer.open(config(fixture, Protocol.GCS))) {
            server.start();
            var result = ProviderSdkCapture.run(ProviderSdkCapture.Options.builder(ProviderSdkCapture.Provider.GCS)
                    .endpoint("http://127.0.0.1:" + server.port()).bucket("bucket").projectId("replay-test")
                    .pageSize(1).maxAttempts(10).deadlineEpochMs(System.currentTimeMillis() + 60_000).build());
            assertThat(result.status()).as(result.reason()).isEqualTo(ProviderSdkCapture.Status.COMPLETE);
            assertThat(result.entries()).extracting(ProviderSdkCapture.Entry::name).containsExactly("a/1", "a/2", "b");
            assertThat(result.steps()).hasSizeGreaterThanOrEqualTo(2);
            String version = ProviderSdkCapture.Provider.GCS.sdkVersion();
            for (var step : result.steps()) {
                assertThat(requestHeaderNames(step)).doesNotContain("authorization");
                assertThat(String.join(" ", requestHeaderValues(step, "user-agent"))).contains("gcloud-java/" + version);
            }
            Path manifest = dir.resolve("manifest.json");
            Files.writeString(manifest, JSON.writeValueAsString(Map.of("provider", "gcs",
                    "namespace_mode", "flat-ubla", "region", "local-test", "api_version", "json-v1",
                    "sdk_product", "gcloud-java", "sdk_version", version, "capture_date", "2026-10-21",
                    "objects", List.of(Map.of("name", "a/1", "size", 1, "sha256", "a".repeat(64))))));
            Path raw = dir.resolve("raw.json");
            Files.write(raw, JSON.writeValueAsBytes(Map.of("steps", result.steps())));
            Path safe = dir.resolve("safe.json");
            Path log = dir.resolve("sanitize.log");
            Process process = new ProcessBuilder("python3",
                    Path.of("..", "scripts", "provider-conformance", "evidence.py").toAbsolutePath().toString(),
                    "sanitize", "--kind", "sdk_run", "--input", raw.toString(), "--output", safe.toString(),
                    "--provider", "gcs", "--bucket", "bucket", "--manifest", manifest.toString(),
                    "--probe-id", "gcs-09").redirectErrorStream(true).redirectOutput(log.toFile()).start();
            if (!process.waitFor(30, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new AssertionError("provider capture sanitizer timed out");
            }
            assertThat(process.exitValue()).as(Files.readString(log)).isZero();
            assertThat(JSON.readTree(safe.toFile()).path("steps").size()).isEqualTo(result.steps().size());
        }
    }

    @Test
    void azureHierarchyWalkUsesExplicitVersionAndReproducibleClientIds(@TempDir Path dir) throws Exception {
        Path fixture = NativeSdkFixtures.sorted(dir.resolve("fixture"),
                ObjectEntries.key("a/1").size(1).isLatest(true).build(),
                ObjectEntries.key("a/2").size(2).isLatest(true).build(),
                ObjectEntries.key("b").size(3).isLatest(true).build());
        try (ReplayServer server = ReplayServer.open(config(fixture, Protocol.AZURE))) {
            server.start();
            var result = ProviderSdkCapture.run(azure(server.port()).walk(ProviderSdkCapture.Walk.HIERARCHY)
                    .serviceVersion("2026-06-06").build());
            assertThat(result.status()).as(result.reason()).isEqualTo(ProviderSdkCapture.Status.COMPLETE);
            assertThat(result.entries()).containsExactly(new ProviderSdkCapture.Entry("a/", true),
                    new ProviderSdkCapture.Entry("b", false));
            assertThat(result.steps()).hasSize(2);
            for (int i = 0; i < result.steps().size(); i++) {
                var step = result.steps().get(i);
                assertThat(requestHeaderValues(step, "x-ms-version")).containsExactly("2026-06-06");
                assertThat(requestHeaderValues(step, "x-ms-client-request-id")).containsExactly("probe-" + (i + 1));
                assertThat(headerValues(response(step), "x-ms-client-request-id")).containsExactly("probe-" + (i + 1));
                assertThat(requestHeaderNames(step)).doesNotContain("authorization");
                String url = (String) request(step).get("url");
                for (String part : url.substring(url.indexOf('?') + 1).split("&")) {
                    assertThat(AzureGatedHttpClient.LISTING_QUERY_FIELDS).contains(part.split("=", 2)[0]);
                }
                assertThat(status(step)).isEqualTo(200);
            }
        }
    }

    @Test
    void recordedAzureUrlsKeepOnlyListingFieldsInWireOrder() {
        assertThat(AzureGatedHttpClient.listingOnlyUrl("https://acct.blob.core.windows.net/c?restype=container"
                + "&sv=2025-01-05&comp=list&se=2026-10-24T15%3A36Z&sp=racwdl&prefix=a%2Bb+c&sig=abc%2B%3D"
                + "&marker=&spr=https&sr=c#frag"))
                .isEqualTo("https://acct.blob.core.windows.net/c?restype=container&comp=list&prefix=a%2Bb+c&marker=");
        assertThat(AzureGatedHttpClient.listingOnlyUrl("https://acct.blob.core.windows.net/c?sv=1&sig=2"))
                .isEqualTo("https://acct.blob.core.windows.net/c");
        assertThat(ExchangeRecorder.isCredential("Authorization")).isTrue();
    }

    @Test
    void unsupportedAzureServiceVersionFailsBeforeAnyAttempt() {
        assertThatThrownBy(() -> ProviderSdkCapture.run(azure(1).serviceVersion("2026-10-06").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not support service version 2026-10-06");
    }

    @Test
    void gateTimeoutIsShortenedByTheDeadlineAndBytesAreCappedInAggregate() {
        long[] now = {1_000};
        AttemptGate gate = new AttemptGate(5, 6_000, 30, 10, () -> now[0]);
        assertThat(gate.timeoutMillisNow()).isEqualTo(5_000);
        gate.chargeBytes(10);
        assertThatThrownBy(() -> gate.chargeBytes(1)).isInstanceOf(CaptureBoundException.class)
                .hasMessageContaining("response_bytes");
        now[0] = 6_000;
        assertThatThrownBy(gate::timeoutMillisNow).hasMessageContaining("deadline");
        now[0] = 6_001;
        assertThatThrownBy(gate::requireWithinDeadline).hasMessageContaining("deadline");
    }

    @Test
    void transportResendsBeyondCountedAttemptsAreGated() {
        AttemptGate gate = new AttemptGate(2, Long.MAX_VALUE, 30, Long.MAX_VALUE);
        gate.acquire(AttemptGate.Kind.STORAGE);
        gate.transmission(AttemptGate.Kind.STORAGE);
        gate.transmission(AttemptGate.Kind.STORAGE);
        assertThat(gate.resends()).isEqualTo(1);
        assertThat(gate.attempts(AttemptGate.Kind.STORAGE)).isEqualTo(2);
        assertThatThrownBy(() -> gate.transmission(AttemptGate.Kind.STORAGE))
                .hasMessageContaining("attempt_allowance");
    }

    @Test
    void aggregateResponseBytesIncludeErrorBodiesAndStopTheWalk() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        String error = "{\"error\":{\"code\":503,\"message\":\"busy\",\"errors\":"
                + "[{\"reason\":\"backendError\",\"message\":\"busy\"}]}}";
        HttpServer server = server(hits, 503, error, false);
        try {
            var result = ProviderSdkCapture.run(gcs(server).maxAttempts(3).maxTotalResponseBytes(
                    error.length() + 10L).build());
            assertThat(result.status()).isEqualTo(ProviderSdkCapture.Status.INCOMPLETE);
            assertThat(result.reason()).isEqualTo("bound:response_bytes");
            assertThat(hits).hasValue(2);
            assertThat(result.responseBytes()).isEqualTo(error.length() + 10L);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void privateIdentifiersAreRedactedBeforeRecordingInEveryForm() throws Exception {
        String secret = "Private-Bucket.Name";
        String body = "{\"kind\":\"storage#objects\",\"items\":[{\"kind\":\"storage#object\","
                + "\"bucket\":\"private-bucket.name\",\"name\":\"a\",\"size\":\"1\","
                + "\"selfLink\":\"https://x/b/Private%2DBucket.Name/o/a\"}]}";
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = server(hits, 200, body, false);
        try {
            var result = ProviderSdkCapture.run(ProviderSdkCapture.Options.builder(ProviderSdkCapture.Provider.GCS)
                    .endpoint("http://127.0.0.1:" + server.getAddress().getPort()).bucket(secret)
                    .projectId("replay-test").maxAttempts(3).deadlineEpochMs(System.currentTimeMillis() + 60_000)
                    .build(), Redactor.of(Map.of(secret, "{bucket}")));
            assertThat(result.status()).as(result.reason()).isEqualTo(ProviderSdkCapture.Status.COMPLETE);
            String recorded = JSON.writeValueAsString(result.toJson());
            String decodedBody = new String(Base64.getDecoder().decode((String) response(result.steps().getFirst())
                    .get("body_base64")), StandardCharsets.UTF_8);
            for (String text : List.of(recorded, decodedBody)) {
                assertThat(text.toLowerCase(java.util.Locale.ROOT)).doesNotContain("private-bucket.name")
                        .doesNotContain("private%2dbucket.name");
            }
            assertThat((String) request(result.steps().getFirst()).get("url")).contains("/b/{bucket}/o");
            assertThat(decodedBody).contains("\"bucket\":\"{bucket}\"").contains("/b/{bucket}/o/a");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void identityComesFromTheEnvironmentAndMustAgreeWithOptions() throws Exception {
        var options = (com.fasterxml.jackson.databind.node.ObjectNode) JSON.readTree(
                "{\"provider\":\"gcs\",\"bucket\":\"b\"}");
        ProviderSdkCapture.mergeIdentity(options, "{\"bucket\":\"b\",\"projectId\":\"p\"}");
        assertThat(options.path("projectId").asText()).isEqualTo("p");
        assertThatThrownBy(() -> ProviderSdkCapture.mergeIdentity(options, "{\"bucket\":\"other\"}"))
                .hasMessageContaining("disagree on bucket").hasMessageNotContaining("other");
        assertThatThrownBy(() -> ProviderSdkCapture.mergeIdentity(options, "{\"sasToken\":\"x\"}"))
                .hasMessageContaining("unknown field");
    }

    @Test
    void gcsResendsBelowTheSdkStayWithinTwiceTheReportedAttempts() throws Exception {
        try (DroppingServer server = new DroppingServer("application/json", GCS_LIST)) {
            var result = ProviderSdkCapture.run(gcs(server.port()).maxAttempts(6).retryAttempts(3).build());
            assertThat(result.status()).as(result.reason()).isEqualTo(ProviderSdkCapture.Status.COMPLETE);
            assertThat(server.hits()).isLessThanOrEqualTo(2 * result.storageAttempts());
        }
    }

    @Test
    void gcsAttemptCapCoversAPeerTricklingHeaders() throws Exception {
        // One header byte every 200 ms never trips the 1 s socket read timeout, so only a cap on
        // the whole attempt, started before the request is sent, ends it.
        try (var socket = new java.net.ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
            AtomicInteger hits = new AtomicInteger();
            Thread server = Thread.ofVirtual().start(() -> {
                while (!socket.isClosed()) {
                    try {
                        var client = socket.accept();
                        hits.incrementAndGet();
                        Thread.ofVirtual().start(() -> {
                            try (client) {
                                var output = client.getOutputStream();
                                output.write("HTTP/1.1 200 OK\r\nX-Pad: ".getBytes(StandardCharsets.ISO_8859_1));
                                for (int i = 0; i < 600; i++) {
                                    output.write('a');
                                    output.flush();
                                    Thread.sleep(200);
                                }
                            } catch (java.io.IOException | InterruptedException e) {
                                // the client closed the connection at its cap
                            }
                        });
                    } catch (java.io.IOException e) {
                        // closed
                    }
                }
            });
            long started = System.nanoTime();
            var result = ProviderSdkCapture.run(gcs(socket.getLocalPort()).requestTimeoutSeconds(1).maxAttempts(1)
                    .retryAttempts(1).build());
            long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
            assertThat(result.status()).isEqualTo(ProviderSdkCapture.Status.INCOMPLETE);
            assertThat(result.reason()).isEqualTo("bound:request_time");
            assertThat(elapsedMillis).isLessThan(10_000);
            assertThat(hits.get()).isGreaterThanOrEqualTo(1);
            socket.close();
            server.join(5_000);
        }
    }

    @Test
    void azureResendsBelowTheSdkStayWithinTwiceTheReportedAttempts() throws Exception {
        try (DroppingServer server = new DroppingServer("application/xml", null)) {
            var result = ProviderSdkCapture.run(azure(server.port()).retryAttempts(3).build());
            assertThat(result.status()).as(result.reason()).isEqualTo(ProviderSdkCapture.Status.COMPLETE);
            assertThat(result.entries()).containsExactly(new ProviderSdkCapture.Entry("a", false));
            // Plain HTTP has no TLS engine to count, so the JDK's one resend is not visible here.
            assertThat(server.hits()).isLessThanOrEqualTo(2 * result.storageAttempts());
        }
    }

    @Test
    void tlsConnectionsBeyondCountedAttemptsAreChargedAsResends() throws Exception {
        AttemptGate gate = new AttemptGate(2, Long.MAX_VALUE, 30, Long.MAX_VALUE);
        var context = CountingSslContext.create(gate, AttemptGate.Kind.STORAGE);
        gate.acquire(AttemptGate.Kind.STORAGE);
        context.createSSLEngine("example.invalid", 443);
        assertThat(gate.resends()).isZero();
        context.createSSLEngine("example.invalid", 443);
        assertThat(gate.resends()).isEqualTo(1);
        assertThatThrownBy(() -> context.createSSLEngine("example.invalid", 443))
                .hasMessageContaining("attempt_allowance");
        context.getDefaultSSLParameters();
        assertThat(gate.totalAttempts()).isEqualTo(2);
        AttemptGate socketGate = new AttemptGate(1, Long.MAX_VALUE, 30, Long.MAX_VALUE);
        var sockets = new CountingSslSocketFactory(socketGate, AttemptGate.Kind.AUTH);
        try (var unconnected = sockets.createSocket()) {
            assertThat(socketGate.resends()).isEqualTo(1);
        }
        assertThatThrownBy(sockets::createSocket).hasMessageContaining("attempt_allowance");
    }

    @Test
    void azureBodiesAreBoundedWhileStreaming() throws Exception {
        String big = "<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults>"
                + "x".repeat(64 * 1024) + "</EnumerationResults>";
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger hits = new AtomicInteger();
        server.createContext("/", exchange -> {
            hits.incrementAndGet();
            exchange.getResponseHeaders().set("Content-Type", "application/xml");
            exchange.sendResponseHeaders(200, 0); // chunked, no declared length
            try (var output = exchange.getResponseBody()) {
                output.write(big.getBytes(StandardCharsets.UTF_8));
            } catch (java.io.IOException ignored) {
                // the client cancels the read at its cap
            }
        });
        server.start();
        try {
            var result = ProviderSdkCapture.run(azure(server.getAddress().getPort()).maxResponseBytes(1024).build());
            assertThat(result.status()).isEqualTo(ProviderSdkCapture.Status.INCOMPLETE);
            assertThat(result.reason()).isEqualTo("bound:response_bytes");
            assertThat(result.responseBytes()).isLessThan(big.length());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void s3PaginatorWalksReplayWithOnlyListingFieldsAndClassifiedHeaders(@TempDir Path dir) throws Exception {
        String bucket = "private-bucket-1";
        Path fixture = NativeSdkFixtures.sorted(dir.resolve("fixture"), s3Object("a/1"), s3Object("a/2"),
                s3Object("b"));
        try (ReplayServer server = ReplayServer.open(config(fixture, Protocol.S3, bucket))) {
            server.start();
            var flat = ProviderSdkCapture.run(s3(server.port(), bucket).pageSize(1).build(),
                    Redactor.of(Map.of(bucket, "{bucket}")));
            assertThat(flat.status()).as(flat.reason()).isEqualTo(ProviderSdkCapture.Status.COMPLETE);
            assertThat(flat.entries()).extracting(ProviderSdkCapture.Entry::name).containsExactly("a/1", "a/2", "b");
            assertThat(flat.steps()).hasSize(3);
            String version = ProviderSdkCapture.Provider.S3.sdkVersion();
            for (var step : flat.steps()) {
                assertThat(S3GatedHttpClient.RECORDED_REQUEST_HEADERS).containsAll(requestHeaderNames(step));
                assertThat(String.join(" ", requestHeaderValues(step, "user-agent")))
                        .contains(ProviderSdkCapture.Provider.S3.product() + "/" + version);
                String url = (String) request(step).get("url");
                assertThat(url).startsWith("http://127.0.0.1:" + server.port() + "/{bucket}?");
                for (String part : url.substring(url.indexOf('?') + 1).split("&")) {
                    assertThat(S3GatedHttpClient.LISTING_QUERY_FIELDS).contains(part.split("=", 2)[0]);
                }
                String body = new String(Base64.getDecoder().decode((String) response(step).get("body_base64")),
                        StandardCharsets.UTF_8);
                assertThat(body).doesNotContain(bucket).contains("<Name>{bucket}</Name>");
                assertThat(status(step)).isEqualTo(200);
            }
            assertThat((String) request(flat.steps().get(1)).get("url")).contains("continuation-token=");
            var hierarchy = ProviderSdkCapture.run(s3(server.port(), bucket).walk(ProviderSdkCapture.Walk.HIERARCHY)
                    .startAfter("a").encodingType("url").fetchOwner(true).build());
            assertThat(hierarchy.status()).as(hierarchy.reason()).isEqualTo(ProviderSdkCapture.Status.COMPLETE);
            assertThat(hierarchy.entries()).containsExactly(new ProviderSdkCapture.Entry("b", false),
                    new ProviderSdkCapture.Entry("a/", true));
            assertThat((String) request(hierarchy.steps().getFirst()).get("url"))
                    .contains("delimiter=%2F").endsWith("&start-after=a").contains("encoding-type=url")
                    .contains("fetch-owner=true");
        }
    }

    @Test
    void s3SignedHeadersAndForeignQueryFieldsNeverReachTheRecord() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = server(hits, 200, "<ListBucketResult/>", false);
        try {
            AttemptGate gate = new AttemptGate(1, Long.MAX_VALUE, 30, Long.MAX_VALUE);
            ExchangeRecorder recorder = new ExchangeRecorder(4, 1024, Redactor.NONE);
            var request = software.amazon.awssdk.http.SdkHttpFullRequest.builder()
                    .method(software.amazon.awssdk.http.SdkHttpMethod.GET)
                    .uri(java.net.URI.create("http://127.0.0.1:" + server.getAddress().getPort()
                            + "/bucket?list-type=2&X-Amz-Signature=abc&prefix=a%2Bb&X-Amz-Credential=key"))
                    .putHeader("Authorization", "AWS4-HMAC-SHA256 Credential=key/x, Signature=abc")
                    .putHeader("X-Amz-Security-Token", "session").putHeader("X-Amz-Date", "20261021T000000Z")
                    .putHeader("amz-sdk-invocation-id", "id").putHeader("User-Agent", "aws-sdk-java/2.31.78")
                    .putHeader("x-amz-content-sha256", "UNSIGNED-PAYLOAD").build();
            try (var client = new S3GatedHttpClient(gate, recorder)) {
                var response = client.prepareRequest(software.amazon.awssdk.http.HttpExecuteRequest.builder()
                        .request(request).build()).call();
                assertThat(response.httpResponse().statusCode()).isEqualTo(200);
            }
            var step = recorder.steps().getFirst();
            assertThat(requestHeaderNames(step)).containsExactlyInAnyOrder("user-agent", "x-amz-content-sha256");
            assertThat((String) request(step).get("url")).endsWith("/bucket?list-type=2&prefix=a%2Bb");
            assertThat(JSON.writeValueAsString(step)).doesNotContain("Signature").doesNotContain("session")
                    .doesNotContain("20261021T000000Z").doesNotContain("Credential");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void s3GateRefusesTheAttemptBeyondTheAllowanceAcrossSdkRetries() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = server(hits, 503, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SlowDown"
                + "</Code><Message>busy</Message></Error>", false);
        try {
            var result = ProviderSdkCapture.run(s3(server.getAddress().getPort(), "bucket").maxAttempts(2)
                    .retryAttempts(3).build());
            assertThat(result.status()).isEqualTo(ProviderSdkCapture.Status.INCOMPLETE);
            assertThat(result.reason()).isEqualTo("bound:attempt_allowance");
            assertThat(hits).hasValue(2);
            assertThat(result.steps()).hasSize(2).allSatisfy(step -> assertThat(status(step)).isEqualTo(503));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void s3OptionsTakeIdentityFromTheEnvironmentAndRefuseForeignFields() throws Exception {
        var options = (com.fasterxml.jackson.databind.node.ObjectNode) JSON.readTree(
                "{\"provider\":\"s3\",\"credentials\":\"env\",\"walk\":\"hierarchy\",\"delimiter\":\"/\","
                + "\"startAfter\":\"a\",\"encodingType\":\"url\",\"fetchOwner\":true,\"pageSize\":2,"
                + "\"maxAttempts\":4,\"deadlineEpochMs\":1}");
        ProviderSdkCapture.mergeIdentity(options, "{\"bucket\":\"b\",\"endpoint\":\"http://127.0.0.1:1\"}");
        var parsed = ProviderSdkCapture.parseOptions(options);
        assertThat(parsed.bucket()).isEqualTo("b");
        assertThat(parsed.credentials()).isEqualTo(ProviderSdkCapture.Credentials.ENV);
        assertThat(parsed.fetchOwner()).isTrue();
        assertThatThrownBy(() -> ProviderSdkCapture.Options.builder(ProviderSdkCapture.Provider.S3).bucket("b")
                .encodingType("base64").build()).hasMessageContaining("encodingType");
        assertThatThrownBy(() -> ProviderSdkCapture.Options.builder(ProviderSdkCapture.Provider.S3).bucket("b")
                .startFrom("x").build()).hasMessageContaining("only S3 options");
        assertThatThrownBy(() -> ProviderSdkCapture.Options.builder(ProviderSdkCapture.Provider.GCS).bucket("b")
                .projectId("p").startAfter("x").build()).hasMessageContaining("S3 options");
        assertThatThrownBy(() -> ProviderSdkCapture.Options.builder(ProviderSdkCapture.Provider.S3).bucket("b")
                .credentials(ProviderSdkCapture.Credentials.ADC).build()).hasMessageContaining("env or none");
    }

    /** Closes the connection of every odd request without a response, answers even ones. */
    private static final class DroppingServer implements AutoCloseable {
        private final java.net.ServerSocket socket = new java.net.ServerSocket(0, 50,
                java.net.InetAddress.getLoopbackAddress());
        private final AtomicInteger hits = new AtomicInteger();
        private final Thread thread;

        DroppingServer(String contentType, String body) throws java.io.IOException {
            thread = Thread.ofVirtual().start(() -> {
                while (!socket.isClosed()) {
                    try (var client = socket.accept()) {
                        var input = client.getInputStream();
                        StringBuilder head = new StringBuilder();
                        int b;
                        while ((b = input.read()) >= 0) {
                            head.append((char) b);
                            if (head.toString().endsWith("\r\n\r\n")) break;
                        }
                        if (hits.incrementAndGet() % 2 == 1) continue;
                        String payload = body != null ? body : "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
                                + "<EnumerationResults ServiceEndpoint=\"http://127.0.0.1:" + port() + "/replay/\" "
                                + "ContainerName=\"bucket\"><MaxResults>1</MaxResults><Blobs><Blob><Name>a</Name>"
                                + "<Properties><Content-Length>1</Content-Length><BlobType>BlockBlob</BlobType>"
                                + "</Properties></Blob></Blobs><NextMarker /></EnumerationResults>";
                        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
                        String response = "HTTP/1.1 200 OK\r\nContent-Type: " + contentType
                                + "\r\nx-ms-version: 2026-06-06\r\nContent-Length: " + bytes.length
                                + "\r\nConnection: close\r\n\r\n";
                        client.getOutputStream().write(response.getBytes(StandardCharsets.ISO_8859_1));
                        client.getOutputStream().write(bytes);
                        client.getOutputStream().flush();
                    } catch (java.io.IOException e) {
                        // closed
                    }
                }
            });
        }

        int port() { return socket.getLocalPort(); }

        int hits() { return hits.get(); }

        @Override
        public void close() throws Exception {
            socket.close();
            thread.join(5_000);
        }
    }

    private static ProviderSdkCapture.Builder gcs(int port) {
        return ProviderSdkCapture.Options.builder(ProviderSdkCapture.Provider.GCS)
                .endpoint("http://127.0.0.1:" + port).bucket("bucket")
                .projectId("replay-test").deadlineEpochMs(System.currentTimeMillis() + 60_000);
    }

    private static ProviderSdkCapture.Builder gcs(HttpServer server) {
        return ProviderSdkCapture.Options.builder(ProviderSdkCapture.Provider.GCS)
                .endpoint("http://127.0.0.1:" + server.getAddress().getPort()).bucket("bucket")
                .projectId("replay-test").deadlineEpochMs(System.currentTimeMillis() + 60_000);
    }

    private static ProviderSdkCapture.Builder azure(int port) {
        return ProviderSdkCapture.Options.builder(ProviderSdkCapture.Provider.AZURE)
                .endpoint("http://127.0.0.1:" + port + "/replay").container("bucket").pageSize(1)
                .clientRequestIdPrefix("probe").serviceVersion("2026-06-06").maxAttempts(10).retryAttempts(1)
                .deadlineEpochMs(System.currentTimeMillis() + 60_000);
    }

    private static io.varve.swath.model.ObjectEntry s3Object(String key) {
        return ObjectEntries.key(key).size(1).lastModifiedEpochMicros(1_767_225_600_000_000L).etag("etag-" + key)
                .storageClass("STANDARD").isLatest(true).build();
    }

    private static ProviderSdkCapture.Builder s3(int port, String bucket) {
        return ProviderSdkCapture.Options.builder(ProviderSdkCapture.Provider.S3)
                .endpoint("http://127.0.0.1:" + port).bucket(bucket).maxAttempts(10).retryAttempts(1)
                .deadlineEpochMs(System.currentTimeMillis() + 60_000);
    }

    private static ServeConfig config(Path fixture, Protocol protocol) {
        return config(fixture, protocol, "bucket");
    }

    private static ServeConfig config(Path fixture, Protocol protocol, String bucket) {
        return new ServeConfig(fixture, "127.0.0.1", 0, bucket, ServingMode.SORTED, 2, 8, Set.of(protocol),
                protocol == Protocol.AZURE ? "replay" : null, 1024 * 1024, 1024 * 1024, Duration.ofSeconds(10), Duration.ofSeconds(30),
                // The S3 route always applies a latency function; this one injects none.
                Duration.ofSeconds(30), null, (request, result) -> Duration.ZERO);
    }

    private static HttpServer server(AtomicInteger hits, int status, String body, boolean gzip) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            hits.incrementAndGet();
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            if (gzip) {
                ByteArrayOutputStream encoded = new ByteArrayOutputStream();
                try (var output = new GZIPOutputStream(encoded)) {
                    output.write(bytes);
                }
                bytes = encoded.toByteArray();
                exchange.getResponseHeaders().set("Content-Encoding", "gzip");
            }
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
            exchange.sendResponseHeaders(status, bytes.length);
            try (var output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        });
        server.start();
        return server;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> request(Map<String, Object> step) {
        return (Map<String, Object>) ((Map<String, Object>) step.get("exchange")).get("request");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> response(Map<String, Object> step) {
        return (Map<String, Object>) ((Map<String, Object>) step.get("exchange")).get("response");
    }

    private static int status(Map<String, Object> step) {
        return (Integer) response(step).get("status");
    }

    @SuppressWarnings("unchecked")
    private static List<String> headerValues(Map<String, Object> side, String name) {
        List<String> values = new ArrayList<>();
        for (Map<String, String> header : (List<Map<String, String>>) side.get("headers")) {
            if (header.get("name").equalsIgnoreCase(name)) values.add(header.get("value"));
        }
        return values;
    }

    private static List<String> requestHeaderValues(Map<String, Object> step, String name) {
        return headerValues(request(step), name);
    }

    @SuppressWarnings("unchecked")
    private static List<String> requestHeaderNames(Map<String, Object> step) {
        List<String> names = new ArrayList<>();
        for (Map<String, String> header : (List<Map<String, String>>) request(step).get("headers")) {
            names.add(header.get("name").toLowerCase(java.util.Locale.ROOT));
        }
        return names;
    }
}
