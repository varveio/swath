/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.testkit;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/** Transparent local SDK capture proxy: forwards semantic headers and records both sides. */
public final class RecordingListingProxy implements AutoCloseable {
    private static final Set<String> HOP_BY_HOP = Set.of("host", "connection", "content-length",
            "transfer-encoding", "upgrade", "keep-alive", "proxy-connection", "te", "trailer", "expect");

    private final HttpServer server;
    private final HttpClient forward = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1).build();
    private final URI target;
    private final List<Exchange> exchanges = new CopyOnWriteArrayList<>();

    public RecordingListingProxy(URI target) throws IOException {
        this.target = target;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", request -> {
            String pathAndQuery = request.getRequestURI().toString();
            URI backend = URI.create(this.target.toString().replaceAll("/$", "") + pathAndQuery);
            Map<String, List<String>> requestHeaders = Map.copyOf(request.getRequestHeaders());
            HttpRequest.Builder outgoing = HttpRequest.newBuilder(backend)
                    .method(request.getRequestMethod(), HttpRequest.BodyPublishers.noBody());
            requestHeaders.forEach((name, values) -> {
                if (!HOP_BY_HOP.contains(name.toLowerCase(java.util.Locale.ROOT))) {
                    values.forEach(value -> outgoing.header(name, value));
                }
            });
            try {
                HttpRequest forwarded = outgoing.build();
                HttpResponse<byte[]> response = forward.send(forwarded,
                        HttpResponse.BodyHandlers.ofByteArray());
                response.headers().map().forEach((name, values) -> {
                    if (!HOP_BY_HOP.contains(name.toLowerCase(java.util.Locale.ROOT))) {
                        request.getResponseHeaders().put(name, values);
                    }
                });
                byte[] body = response.body();
                exchanges.add(new Exchange(request.getRequestMethod(), pathAndQuery,
                        requestHeaders, forwarded.headers().map(), response.statusCode(),
                        response.headers().map(), body));
                request.sendResponseHeaders(response.statusCode(), body.length == 0 ? -1 : body.length);
                try (var output = request.getResponseBody()) {
                    output.write(body);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        });
        server.start();
    }

    public int port() { return server.getAddress().getPort(); }

    public List<Exchange> exchanges() { return List.copyOf(exchanges); }

    @Override
    public void close() {
        server.stop(0);
        forward.close();
    }

    public record Exchange(String method, String pathAndQuery, Map<String, List<String>> requestHeaders,
                           Map<String, List<String>> forwardedRequestHeaders, int status,
                           Map<String, List<String>> responseHeaders, byte[] responseBody) {
        public Exchange { responseBody = responseBody.clone(); }
        @Override
        public byte[] responseBody() { return responseBody.clone(); }
        public String requestHeader(String name) {
            return requestHeaders.entrySet().stream().filter(entry -> entry.getKey().equalsIgnoreCase(name))
                    .flatMap(entry -> entry.getValue().stream()).findFirst().orElse(null);
        }

        public String responseHeader(String name) {
            return responseHeaders.entrySet().stream().filter(entry -> entry.getKey().equalsIgnoreCase(name))
                    .flatMap(entry -> entry.getValue().stream()).findFirst().orElse(null);
        }

        public boolean forwardedSemanticHeadersMatch() {
            return semantic(requestHeaders).equals(semantic(forwardedRequestHeaders));
        }

        static Map<String, List<String>> semantic(Map<String, List<String>> headers) {
            Map<String, List<String>> result = new java.util.TreeMap<>();
            headers.forEach((name, values) -> {
                String lower = name.toLowerCase(java.util.Locale.ROOT);
                if (!HOP_BY_HOP.contains(lower)) result.put(lower, values);
            });
            return result;
        }
    }
}
