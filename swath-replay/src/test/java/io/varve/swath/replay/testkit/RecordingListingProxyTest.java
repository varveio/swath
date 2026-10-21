/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RecordingListingProxyTest {
    @Test
    void backendActuallyReceivesTheSemanticHeadersAndEmptyBodyIsNotChunked() throws Exception {
        AtomicReference<Map<String, List<String>>> backendHeaders = new AtomicReference<>();
        HttpServer backend = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        backend.createContext("/", request -> {
            backendHeaders.set(Map.copyOf(request.getRequestHeaders()));
            request.getResponseHeaders().set("Content-Type", "application/json");
            request.sendResponseHeaders(204, -1);
            request.close();
        });
        backend.start();
        try (RecordingListingProxy proxy = new RecordingListingProxy(
                URI.create("http://127.0.0.1:" + backend.getAddress().getPort()));
             HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            URI url = URI.create("http://127.0.0.1:" + proxy.port() + "/storage/v1/b/bucket/o?maxResults=1");
            HttpRequest request = HttpRequest.newBuilder(url).header("User-Agent", "gcloud-java/2.73.0")
                    .header("x-goog-api-client", "gl-java/25 gdcl/2.7.2")
                    .header("Accept", "application/json").GET().build();
            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            assertThat(response.statusCode()).isEqualTo(204);
            assertThat(response.body()).isEmpty();
            assertThat(response.headers().firstValue("transfer-encoding")).isEmpty();
            assertThat(proxy.exchanges()).hasSize(1);
            var capture = proxy.exchanges().getFirst();
            assertThat(capture.forwardedSemanticHeadersMatch()).isTrue();
            assertThat(RecordingListingProxy.Exchange.semantic(backendHeaders.get()))
                    .isEqualTo(RecordingListingProxy.Exchange.semantic(capture.requestHeaders()));
            assertThat(backendHeaders.get()).doesNotContainKeys("upgrade", "HTTP2-Settings");
        } finally {
            backend.stop(0);
        }
    }
}
