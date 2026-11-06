/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.provider;

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpPipelineNextSyncPolicy;
import com.azure.core.http.HttpPipelinePosition;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.core.publisher.Mono;

/**
 * Gives each SDK call an explicit, reproducible client request ID ({@code prefix-n}), reused by
 * that call's retries, so native and replay runs can send identical IDs.
 */
final class ClientRequestIdPolicy implements HttpPipelinePolicy {
    private final String prefix;
    private final AtomicInteger calls = new AtomicInteger();

    ClientRequestIdPolicy(String prefix) {
        if (prefix == null || prefix.isBlank() || prefix.indexOf('\r') >= 0 || prefix.indexOf('\n') >= 0) {
            throw new IllegalArgumentException("client request ID prefix must be a nonblank header value");
        }
        this.prefix = prefix;
    }

    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        assign(context);
        return next.process();
    }

    @Override
    public HttpResponse processSync(HttpPipelineCallContext context, HttpPipelineNextSyncPolicy next) {
        assign(context);
        return next.processSync();
    }

    @Override
    public HttpPipelinePosition getPipelinePosition() {
        return HttpPipelinePosition.PER_CALL;
    }

    private void assign(HttpPipelineCallContext context) {
        context.getHttpRequest().setHeader(HttpHeaderName.X_MS_CLIENT_REQUEST_ID,
                prefix + "-" + calls.incrementAndGet());
    }
}
