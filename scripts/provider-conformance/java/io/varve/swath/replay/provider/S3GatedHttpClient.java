/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.provider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

/**
 * The AWS SDK's HTTP client for a capture: every attempt, below the SDK retry strategy, goes
 * through {@link JdkGatedExchange}. Captures keep only the listing query fields and a short list
 * of request headers, so the SigV4 signature, session token and signing date never reach one.
 */
final class S3GatedHttpClient implements SdkHttpClient {
    /** Listing fields retained in recorded URLs, in wire order. */
    static final Set<String> LISTING_QUERY_FIELDS = Set.of("list-type", "prefix", "delimiter", "max-keys",
            "continuation-token", "start-after", "encoding-type", "fetch-owner", "marker",
            "optional-object-attributes");
    /** The only request headers recorded; all others are sent but never captured. */
    static final Set<String> RECORDED_REQUEST_HEADERS = Set.of("user-agent", "x-amz-content-sha256", "accept",
            "amz-sdk-request");
    static final String CLIENT_NAME = "SwathGatedJdk";

    private final JdkGatedExchange exchange;

    S3GatedHttpClient(AttemptGate gate, ExchangeRecorder recorder) {
        this.exchange = new JdkGatedExchange(gate, recorder);
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
        return new ExecutableHttpRequest() {
            @Override
            public HttpExecuteResponse call() throws IOException {
                try {
                    return execute(request);
                } catch (UncheckedIOException e) {
                    throw e.getCause();
                }
            }

            @Override
            public void abort() {
                // Each attempt completes or fails within its own time cap.
            }
        };
    }

    private HttpExecuteResponse execute(HttpExecuteRequest request) throws IOException {
        SdkHttpRequest http = request.httpRequest();
        List<ExchangeRecorder.Header> sent = new ArrayList<>();
        List<ExchangeRecorder.Header> recorded = new ArrayList<>();
        for (Map.Entry<String, List<String>> header : http.headers().entrySet()) {
            for (String value : header.getValue()) {
                ExchangeRecorder.Header entry = new ExchangeRecorder.Header(header.getKey(), value);
                sent.add(entry);
                if (RECORDED_REQUEST_HEADERS.contains(header.getKey().toLowerCase(Locale.ROOT))) {
                    recorded.add(entry);
                }
            }
        }
        byte[] body = null;
        if (request.contentStreamProvider().isPresent()) {
            try (InputStream content = request.contentStreamProvider().get().newStream()) {
                body = content.readAllBytes();
            }
        }
        String method = http.method().name();
        JdkGatedExchange.Response response = exchange.send(method, http.getUri(), sent, body);
        exchange.record(method, listingOnlyUrl(http.getUri().toString()), recorded, response);
        return HttpExecuteResponse.builder()
                .response(SdkHttpResponse.builder().statusCode(response.status())
                        .headers(response.headerMap()).build())
                .responseBody(AbortableInputStream.create(new ByteArrayInputStream(response.body())))
                .build();
    }

    static String listingOnlyUrl(String url) {
        return JdkGatedExchange.keepQueryFields(url, LISTING_QUERY_FIELDS);
    }

    @Override
    public String clientName() {
        return CLIENT_NAME;
    }

    @Override
    public void close() {
        // Attempts own no pooled resources.
    }
}
