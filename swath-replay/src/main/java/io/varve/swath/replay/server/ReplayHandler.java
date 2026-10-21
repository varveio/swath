/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.protocol.ByteKeys;
import io.varve.swath.replay.protocol.ListingFixture;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;

/** Classifies native routes before validating a provider request or HTTP method. */
final class ReplayHandler extends Handler.Abstract {
    private final Map<Protocol, ListingProtocolHandler> handlers;
    private final String azureAccount;
    private final ListingRequestRunner runner;

    ReplayHandler(String bucket, ListingFixture fixture, ReplayMetrics metrics, int maxConcurrentReads,
                  BiFunction<S3ListRequest, S3ListResult, Duration> latency, int maxConcurrentRequests) {
        this(Map.of(Protocol.S3, new S3ListingProtocolHandler(bucket, fixture, metrics, latency)),
                "replay", new ListingRequestRunner(metrics, maxConcurrentReads,
                        2 * maxConcurrentRequests, ServeConfig.DEFAULT_RESPONSE_BUFFER_BUDGET,
                        ServeConfig.DEFAULT_MAX_RESPONSE_BYTES, Duration.ofSeconds(30)));
    }

    ReplayHandler(Map<Protocol, ListingProtocolHandler> handlers, String azureAccount,
                  ListingRequestRunner runner) {
        this.handlers = new EnumMap<>(Protocol.class);
        this.handlers.putAll(handlers);
        this.azureAccount = azureAccount;
        this.runner = runner;
    }

    ListingRequestRunner runner() { return runner; }

    @Override
    public boolean handle(Request request, Response response, Callback callback) {
        Map<String, String> headers = new HashMap<>();
        Set<String> duplicates = new HashSet<>();
        request.getHeaders().forEach(field -> {
            String name = field.getName().toLowerCase(java.util.Locale.ROOT);
            if (headers.put(name, field.getValue()) != null) {
                duplicates.add(name);
            }
        });
        ListingHttpRequest http = new ListingHttpRequest(request.getMethod(),
                request.getHttpURI().getPath(), request.getHttpURI().getQuery(), headers, duplicates);
        Protocol protocol = classify(http);
        if (protocol == null) {
            runner.serve(http, request, response, callback,
                    ambiguous(http) ? ReplayEnvelopeHandler.AMBIGUOUS : ReplayEnvelopeHandler.NOT_FOUND);
            return true;
        }
        runner.serve(http, request, response, callback, handlers.get(protocol));
        return true;
    }

    private Protocol classify(ListingHttpRequest request) {
        if (handlers.size() == 1 && handlers.containsKey(Protocol.S3)) {
            return Protocol.S3;
        }
        String path = request.path();
        String query = request.query();
        boolean s3Selector = hasQueryKey(query, "list-type");
        if (handlers.containsKey(Protocol.GCS) && path != null
                && path.startsWith("/storage/v1/b/")) {
            return s3Selector ? null : Protocol.GCS;
        }
        if (handlers.containsKey(Protocol.AZURE) && azurePath(path)
                && (hasQueryKey(query, "restype") || hasQueryKey(query, "comp"))) {
            return s3Selector ? null : Protocol.AZURE;
        }
        return handlers.containsKey(Protocol.S3) ? Protocol.S3 : null;
    }

    private boolean ambiguous(ListingHttpRequest request) {
        if (!hasQueryKey(request.query(), "list-type")) {
            return false;
        }
        return (handlers.containsKey(Protocol.GCS) && request.path() != null
                && request.path().startsWith("/storage/v1/b/"))
                || (handlers.containsKey(Protocol.AZURE) && azurePath(request.path())
                && (hasQueryKey(request.query(), "restype") || hasQueryKey(request.query(), "comp")));
    }

    private boolean azurePath(String path) {
        if (path == null || !path.startsWith("/" + azureAccount + "/")) {
            return false;
        }
        String container = path.substring(azureAccount.length() + 2);
        return !container.isEmpty() && !container.contains("/");
    }

    private static boolean hasQueryKey(String query, String key) {
        if (query == null) {
            return false;
        }
        for (String field : query.split("&", -1)) {
            int equals = field.indexOf('=');
            String name = equals < 0 ? field : field.substring(0, equals);
            try {
                name = new String(ByteKeys.percentDecode(name), StandardCharsets.UTF_8);
            } catch (IllegalArgumentException malformed) {
                continue;
            }
            if (key.equals(name)) {
                return true;
            }
        }
        return false;
    }
}
