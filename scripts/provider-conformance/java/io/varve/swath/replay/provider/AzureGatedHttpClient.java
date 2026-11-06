/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.provider;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeader;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Sends, counts and records every Azure SDK attempt below the SDK retry policy through
 * {@link JdkGatedExchange}. Recorded URLs keep only listing query fields, so a SAS never reaches
 * a capture.
 */
final class AzureGatedHttpClient implements HttpClient {
    /** Listing fields retained in captures; mirrors AZURE_QUERY in evidence.py. */
    static final Set<String> LISTING_QUERY_FIELDS = Set.of("restype", "comp", "prefix", "delimiter",
            "marker", "maxresults", "startFrom", "timeout", "include", "showonly", "endBefore");

    private final JdkGatedExchange exchange;

    AzureGatedHttpClient(AttemptGate gate, ExchangeRecorder recorder) {
        this.exchange = new JdkGatedExchange(gate, recorder);
    }

    @Override
    public Mono<HttpResponse> send(HttpRequest request) {
        return send(request, Context.NONE);
    }

    @Override
    public Mono<HttpResponse> send(HttpRequest request, Context context) {
        return Mono.fromCallable(() -> sendSync(request, context));
    }

    @Override
    public HttpResponse sendSync(HttpRequest request, Context context) {
        List<ExchangeRecorder.Header> requestHeaders = new ArrayList<>();
        for (HttpHeader header : request.getHeaders()) {
            for (String value : header.getValuesList()) {
                requestHeaders.add(new ExchangeRecorder.Header(header.getName(), value));
            }
        }
        BinaryData body = request.getBodyAsBinaryData();
        String method = request.getHttpMethod().name();
        JdkGatedExchange.Response response = exchange.send(method, URI.create(request.getUrl().toString()),
                requestHeaders, body == null ? null : body.toBytes());
        exchange.record(method, listingOnlyUrl(request.getUrl().toString()), requestHeaders, response);
        HttpHeaders headers = new HttpHeaders();
        for (ExchangeRecorder.Header header : response.headers()) {
            headers.add(HttpHeaderName.fromString(header.name()), header.value());
        }
        return new CapturedResponse(request, response.status(), headers, response.body());
    }

    /** Drops every query field outside the listing allowlist, keeping the rest byte for byte. */
    static String listingOnlyUrl(String url) {
        return JdkGatedExchange.keepQueryFields(url, LISTING_QUERY_FIELDS);
    }

    /** A fully read response handed to the SDK as if it had streamed it. */
    private static final class CapturedResponse extends HttpResponse {
        private final int status;
        private final HttpHeaders headers;
        private final byte[] body;

        CapturedResponse(HttpRequest request, int status, HttpHeaders headers, byte[] body) {
            super(request);
            this.status = status;
            this.headers = headers;
            this.body = body;
        }

        @Override public int getStatusCode() { return status; }

        @Override
        @SuppressWarnings("deprecation")
        public String getHeaderValue(String name) { return headers.getValue(HttpHeaderName.fromString(name)); }

        @Override public String getHeaderValue(HttpHeaderName name) { return headers.getValue(name); }

        @Override public HttpHeaders getHeaders() { return headers; }

        @Override public Flux<ByteBuffer> getBody() { return body.length == 0 ? Flux.empty() : Flux.just(ByteBuffer.wrap(body)); }

        @Override public Mono<byte[]> getBodyAsByteArray() { return Mono.just(body); }

        @Override public Mono<String> getBodyAsString() { return Mono.just(new String(body, StandardCharsets.UTF_8)); }

        @Override public Mono<String> getBodyAsString(Charset charset) { return Mono.just(new String(body, charset)); }

        @Override public BinaryData getBodyAsBinaryData() { return BinaryData.fromBytes(body); }
    }
}
