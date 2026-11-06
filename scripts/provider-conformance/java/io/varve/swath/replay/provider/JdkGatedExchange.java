/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.provider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLContext;

/**
 * One gated SDK attempt over java.net.http, shared by the SDK clients that hand the driver a
 * whole request. Each attempt uses a new HTTP/1.1 client, so no pooled connection is reused; the
 * JDK's connect retry is disabled at driver startup. The JDK still resends a GET once on a new
 * connection when the first ends before any response byte; over HTTPS that connection is counted
 * by {@link CountingSslContext}. The whole attempt, from connect to the last body byte, is bounded
 * by the request cap shortened by the deadline, and the body is streamed through the
 * per-response and aggregate byte caps.
 */
final class JdkGatedExchange {
    // java.net.http owns these headers and refuses them from callers.
    private static final Set<String> TRANSPORT_HEADERS = Set.of("connection", "content-length", "expect",
            "host", "upgrade");

    private final AttemptGate gate;
    private final ExchangeRecorder recorder;
    private final SSLContext tls;

    JdkGatedExchange(AttemptGate gate, ExchangeRecorder recorder) {
        this.gate = gate;
        this.recorder = recorder;
        this.tls = CountingSslContext.create(gate, AttemptGate.Kind.STORAGE);
    }

    /** A fully read response: headers in wire order and as a map for the SDK. */
    record Response(int status, List<ExchangeRecorder.Header> headers, Map<String, List<String>> headerMap,
                    byte[] body) { }

    /**
     * Counts, sends and reads one attempt. Reaching the per-request time cap or the deadline is
     * a {@link CaptureBoundException}, so the SDK retry policy cannot turn it into a success.
     */
    Response send(String method, URI uri, List<ExchangeRecorder.Header> headers, byte[] body) {
        recorder.reserve();
        gate.acquire(AttemptGate.Kind.STORAGE);
        int timeout = gate.timeoutMillisNow();
        java.net.http.HttpRequest.Builder outgoing = java.net.http.HttpRequest.newBuilder(uri)
                .timeout(Duration.ofMillis(timeout))
                .method(method, body == null || body.length == 0
                        ? java.net.http.HttpRequest.BodyPublishers.noBody()
                        : java.net.http.HttpRequest.BodyPublishers.ofByteArray(body));
        for (ExchangeRecorder.Header header : headers) {
            if (!TRANSPORT_HEADERS.contains(header.name().toLowerCase(Locale.ROOT))) {
                outgoing.header(header.name(), header.value());
            }
        }
        java.net.http.HttpResponse<byte[]> response;
        try (java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder()
                .version(java.net.http.HttpClient.Version.HTTP_1_1)
                .followRedirects(java.net.http.HttpClient.Redirect.NEVER)
                .sslContext(tls).connectTimeout(Duration.ofMillis(timeout)).build()) {
            CompletableFuture<java.net.http.HttpResponse<byte[]>> future = client.sendAsync(outgoing.build(),
                    info -> new BoundedBody(gate, recorder.maxResponseBytes()));
            try {
                response = future.get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                if (gate.pastDeadline()) throw new CaptureBoundException("deadline");
                // Reaching the per-request cap is an exhausted bound, never retried into success.
                throw new CaptureBoundException("request_time");
            } catch (ExecutionException e) {
                CaptureBoundException bound = CaptureBoundException.find(e);
                if (bound != null) throw bound;
                if (e.getCause() instanceof HttpTimeoutException) throw new CaptureBoundException("request_time");
                if (e.getCause() instanceof IOException io) throw new UncheckedIOException(io);
                throw new IllegalStateException("SDK attempt failed", e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                future.cancel(true);
                throw new IllegalStateException("interrupted during SDK attempt", e);
            }
        }
        List<ExchangeRecorder.Header> responseHeaders = new ArrayList<>();
        Map<String, List<String>> headerMap = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> header : response.headers().map().entrySet()) {
            if (header.getKey().startsWith(":")) continue;
            headerMap.put(header.getKey(), List.copyOf(header.getValue()));
            for (String value : header.getValue()) {
                responseHeaders.add(new ExchangeRecorder.Header(header.getKey(), value));
            }
        }
        return new Response(response.statusCode(), List.copyOf(responseHeaders), headerMap, response.body());
    }

    /** Records the exchange, then refuses late evidence: an attempt past the deadline stops the walk. */
    void record(String method, String recordedUrl, List<ExchangeRecorder.Header> recordedRequestHeaders,
                Response response) {
        try {
            recorder.record(method, recordedUrl, recordedRequestHeaders, response.status(), response.headers(),
                    response.body());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        gate.requireWithinDeadline();
    }

    /** Drops every query field outside {@code fields}, keeping the rest byte for byte in wire order. */
    static String keepQueryFields(String url, Set<String> fields) {
        int fragment = url.indexOf('#');
        String withoutFragment = fragment < 0 ? url : url.substring(0, fragment);
        int question = withoutFragment.indexOf('?');
        if (question < 0) {
            return withoutFragment;
        }
        List<String> kept = new ArrayList<>();
        for (String part : withoutFragment.substring(question + 1).split("&", -1)) {
            int equals = part.indexOf('=');
            if (fields.contains(equals < 0 ? part : part.substring(0, equals))) {
                kept.add(part);
            }
        }
        String base = withoutFragment.substring(0, question);
        return kept.isEmpty() ? base : base + "?" + String.join("&", kept);
    }

    /** Collects a body while charging the gate; a cap overrun cancels the read. */
    private static final class BoundedBody implements java.net.http.HttpResponse.BodySubscriber<byte[]> {
        private final AttemptGate gate;
        private final int maxBytes;
        private final ByteArrayOutputStream body = new ByteArrayOutputStream();
        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private Flow.Subscription subscription;

        BoundedBody(AttemptGate gate, int maxBytes) {
            this.gate = gate;
            this.maxBytes = maxBytes;
        }

        @Override public CompletionStage<byte[]> getBody() { return result; }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            if (result.isDone()) return;
            try {
                for (ByteBuffer buffer : buffers) {
                    int count = buffer.remaining();
                    gate.chargeBytes(count);
                    if (body.size() + count > maxBytes) throw new CaptureBoundException("response_bytes");
                    byte[] bytes = new byte[count];
                    buffer.get(bytes);
                    body.write(bytes, 0, count);
                }
            } catch (CaptureBoundException e) {
                subscription.cancel();
                result.completeExceptionally(e);
            }
        }

        @Override public void onError(Throwable error) { result.completeExceptionally(error); }

        @Override public void onComplete() { result.complete(body.toByteArray()); }
    }
}
