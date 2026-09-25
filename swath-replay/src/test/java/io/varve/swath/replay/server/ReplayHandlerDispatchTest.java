/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.metrics.ReplayMetrics;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.jupiter.api.Test;

class ReplayHandlerDispatchTest {
    @Test
    void enabledNativeRoutesAndEncodedSelectorsAreClassifiedBeforeMethodChecks() throws Exception {
        try (Harness harness = new Harness(Map.of(Protocol.S3, fake(Protocol.S3),
                Protocol.GCS, fake(Protocol.GCS), Protocol.AZURE, fake(Protocol.AZURE)), "replay")) {
            assertThat(harness.get("/storage/v1/b/bucket/o").body()).isEqualTo("gcs");
            assertThat(harness.get("/replay/bucket?restype=container&comp=list").body()).isEqualTo("azure");
            assertThat(harness.get("/replay/bucket?re%73type=container&%63omp=list").body())
                    .isEqualTo("azure");
            assertThat(harness.get("/storage/v1/b/bucket/o?list%2Dtype=2").statusCode()).isEqualTo(400);
            assertThat(harness.get("/replay/bucket?restype=container&list%2Dtype=2").statusCode())
                    .isEqualTo(400);
            assertThat(harness.get("/bucket/trailing?list-type=2").body()).isEqualTo("s3");
            assertThat(harness.request("POST", "/storage/v1/b/bucket/o").body())
                    .isEqualTo("gcs:WRONG_METHOD");
        }
    }

    @Test
    void disabledRoutesStayInLegacyS3NamespaceAndNoS3LeavesRoutesUnclaimed() throws Exception {
        try (Harness s3 = new Harness(Map.of(Protocol.S3, fake(Protocol.S3)), "replay")) {
            assertThat(s3.get("/storage/v1/b/bucket/o").body()).isEqualTo("s3");
            assertThat(s3.get("/replay/bucket?restype=container&comp=list").body()).isEqualTo("s3");
        }
        try (Harness azure = new Harness(Map.of(Protocol.AZURE, fake(Protocol.AZURE)), "replay")) {
            assertThat(azure.get("/replay/replay?restype=container&comp=list").body()).isEqualTo("azure");
            assertThat(azure.get("/bucket?list-type=2").statusCode()).isEqualTo(404);
            assertThat(azure.metrics.snapshot().httpRequests()).isEqualTo(2);
        }
    }

    private static ListingProtocolHandler fake(Protocol protocol) {
        return new ListingProtocolHandler() {
            @Override public Protocol protocol() { return protocol; }

            @Override public ListingOperation parse(ListingHttpRequest request) {
                if (!"GET".equals(request.method())) {
                    throw new ReplayRequestException(new ReplayFailure(
                            ReplayFailure.Kind.WRONG_METHOD, "wrong_method", "wrong method"));
                }
                return new ListingOperation() {
                    @Override public int initialOutputBytes() { return 32; }
                    @Override public PreparedPage page() {
                        return output -> {
                            byte[] bytes = protocol.name().toLowerCase(java.util.Locale.ROOT)
                                    .getBytes(StandardCharsets.UTF_8);
                            output.write(bytes, 0, bytes.length);
                            return new RenderedResponse(200, "text/plain", Map.of(), output.buffer());
                        };
                    }
                };
            }

            @Override public RenderedResponse error(ReplayFailure failure, ListingHttpRequest request) {
                String body = protocol.name().toLowerCase(java.util.Locale.ROOT) + ':' + failure.kind();
                return new RenderedResponse(405, "text/plain", Map.of(),
                        ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8)));
            }
        };
    }

    private static final class Harness implements AutoCloseable {
        private final ReplayMetrics metrics = new ReplayMetrics();
        private final ListingRequestRunner runner = new ListingRequestRunner(metrics, 2, 8,
                1024, 256, Duration.ofSeconds(5));
        private final Server server = new Server(new QueuedThreadPool(8));
        private final HttpClient client = HttpClient.newHttpClient();
        private final ServerConnector connector = new ServerConnector(server);

        private Harness(Map<Protocol, ListingProtocolHandler> handlers, String account) throws Exception {
            connector.setHost("127.0.0.1");
            connector.setPort(0);
            server.addConnector(connector);
            server.setHandler(new ReplayHandler(handlers, account, runner));
            server.start();
        }

        HttpResponse<String> get(String path) throws Exception { return request("GET", path); }

        HttpResponse<String> request(String method, String path) throws Exception {
            HttpRequest request = HttpRequest.newBuilder(URI.create(
                    "http://127.0.0.1:" + connector.getLocalPort() + path))
                    .method(method, HttpRequest.BodyPublishers.noBody()).build();
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        }

        @Override public void close() throws Exception {
            runner.beginShutdown();
            server.stop();
            runner.stopDeadlines();
            client.close();
            metrics.registry().close();
        }
    }
}
