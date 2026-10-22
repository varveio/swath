/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.provider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * Records storage exchanges in the raw capture shape consumed by
 * {@code scripts/provider-conformance/evidence.py sanitize}. Credentials never reach a record,
 * and private identifiers are redacted before an exchange is kept in memory.
 */
public final class ExchangeRecorder {
    /** Default per-response byte cap; a listing page is far below it. */
    public static final int DEFAULT_MAX_RESPONSE_BYTES = 8 * 1024 * 1024;
    private static final Set<String> CREDENTIAL_HEADERS = Set.of("authorization", "proxy-authorization",
            "cookie", "set-cookie", "x-goog-iam-authorization-token");
    // Describe the wire framing of a body the recorder has decoded, so they are not kept with it.
    private static final Set<String> WIRE_FRAMING_HEADERS = Set.of("content-encoding", "content-length");
    // Hop-by-hop connection management; the driver turns keep-alive off, so these describe the
    // driver's own transport choice rather than the listing response.
    private static final Set<String> HOP_BY_HOP_HEADERS = Set.of("connection", "keep-alive");
    private static final String GZIP = "gzip";

    /** One header as captured; names keep their wire spelling. */
    public record Header(String name, String value) { }

    private final int maxExchanges;
    private final int maxResponseBytes;
    private final Redactor redactor;
    private final List<Map<String, Object>> steps = new ArrayList<>();
    private int reserved;

    public ExchangeRecorder(int maxExchanges, int maxResponseBytes, Redactor redactor) {
        if (maxExchanges < 1 || maxResponseBytes < 1) {
            throw new IllegalArgumentException("recorder bounds must be positive");
        }
        this.maxExchanges = maxExchanges;
        this.maxResponseBytes = maxResponseBytes;
        this.redactor = redactor;
    }

    /** Reserves a slot for the next exchange; refuses one past the page bound. */
    synchronized void reserve() {
        if (reserved >= maxExchanges) {
            throw new CaptureBoundException("page_bound");
        }
        reserved++;
    }

    /**
     * Records one storage exchange from its wire body. A gzip body is kept decoded, without the
     * headers that described its wire framing; hop-by-hop connection headers are not kept either.
     * Both are noted under {@code transport}.
     */
    synchronized void record(String method, String url, List<Header> requestHeaders, int status,
                             List<Header> responseHeaders, byte[] wire) throws IOException {
        String encoding = null;
        for (Header header : responseHeaders) {
            if (header.name() != null && header.name().equalsIgnoreCase("content-encoding")) {
                encoding = header.value();
            }
        }
        boolean gzip = encoding != null && GZIP.equalsIgnoreCase(encoding.trim()) && wire.length > 0;
        byte[] body = gzip ? gunzip(wire) : wire;
        List<Header> kept = new ArrayList<>();
        Map<String, Object> transport = new LinkedHashMap<>();
        for (Header header : responseHeaders) {
            String name = header.name() == null ? null : header.name().toLowerCase(Locale.ROOT);
            if (name != null && HOP_BY_HOP_HEADERS.contains(name)) {
                transport.put(name, header.value());
            } else if (!gzip || name == null || !WIRE_FRAMING_HEADERS.contains(name)) {
                kept.add(header);
            }
        }
        if (gzip) {
            transport.put("content_encoding", GZIP);
            transport.put("wire_bytes", wire.length);
        }
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("method", method);
        request.put("url", redactor.apply(url));
        request.put("headers", headerList(requestHeaders));
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", status);
        response.put("headers", headerList(kept));
        response.put("body_base64", Base64.getEncoder().encodeToString(redactor.apply(body)));
        Map<String, Object> exchange = new LinkedHashMap<>();
        exchange.put("captured_at", Instant.now().truncatedTo(ChronoUnit.MILLIS).toString());
        exchange.put("request", request);
        exchange.put("response", response);
        if (!transport.isEmpty()) {
            exchange.put("transport", transport);
        }
        Map<String, Object> step = new LinkedHashMap<>();
        step.put("phase", "walk");
        step.put("exchange", exchange);
        steps.add(step);
    }

    public synchronized List<Map<String, Object>> steps() {
        return List.copyOf(steps);
    }

    int maxResponseBytes() {
        return maxResponseBytes;
    }

    /**
     * Reads a response body, charging every byte to the gate as it arrives. A body over the
     * per-response cap is a bound, not a truncation.
     */
    byte[] readBounded(InputStream input, AttemptGate gate) throws IOException {
        if (input == null) {
            return new byte[0];
        }
        try (input) {
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            byte[] buffer = new byte[16 * 1024];
            int count;
            while ((count = input.read(buffer)) >= 0) {
                gate.chargeBytes(count);
                if (body.size() + count > maxResponseBytes) {
                    throw new CaptureBoundException("response_bytes");
                }
                body.write(buffer, 0, count);
            }
            return body.toByteArray();
        }
    }

    private byte[] gunzip(byte[] wire) throws IOException {
        try (var input = new GZIPInputStream(new ByteArrayInputStream(wire))) {
            ByteArrayOutputStream decoded = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int count;
            while ((count = input.read(buffer)) >= 0) {
                decoded.write(buffer, 0, count);
                if (decoded.size() > maxResponseBytes) {
                    throw new CaptureBoundException("response_bytes");
                }
            }
            return decoded.toByteArray();
        }
    }

    static boolean isCredential(String headerName) {
        return CREDENTIAL_HEADERS.contains(headerName.toLowerCase(Locale.ROOT));
    }

    private List<Map<String, String>> headerList(List<Header> headers) {
        List<Map<String, String>> result = new ArrayList<>();
        for (Header header : headers) {
            if (header.name() == null || isCredential(header.name())) {
                continue;
            }
            Map<String, String> entry = new LinkedHashMap<>();
            entry.put("name", header.name());
            entry.put("value", redactor.apply(header.value()));
            result.add(entry);
        }
        return result;
    }
}
