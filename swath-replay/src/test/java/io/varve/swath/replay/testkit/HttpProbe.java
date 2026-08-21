/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.server.ReplayServer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

/**
 * Shared HTTP + XML probing support for replay tests: issuing GET requests against a running
 * {@link ReplayServer}, pulling a single tag's text out of an S3 XML response, and percent-encoding
 * raw key bytes into a query string. Consolidates copies that had drifted across the seven test
 * classes that hand-rolled their own {@code get}/{@code extract}/percent-encoder.
 */
public final class HttpProbe {

    private HttpProbe() {
    }

    /** Sends a GET, asserts the response is a 200, and returns the body — the differential walkers' shape. */
    public static String body(ReplayServer server, HttpClient client, String pathAndQuery) throws Exception {
        HttpResponse<String> response = response(server, client, pathAndQuery);
        assertThat(response.statusCode()).isEqualTo(200);
        return response.body();
    }

    /** Same as {@link #body(ReplayServer, HttpClient, String)}, with a fresh client per call. */
    public static String body(ReplayServer server, String pathAndQuery) throws Exception {
        return body(server, HttpClient.newHttpClient(), pathAndQuery);
    }

    /** Sends a GET and returns the raw response — no status assertion; the caller's choice. */
    public static HttpResponse<String> response(ReplayServer server, String pathAndQuery) throws Exception {
        return response(server, HttpClient.newHttpClient(), pathAndQuery);
    }

    public static HttpResponse<String> response(ReplayServer server, HttpClient client, String pathAndQuery)
            throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + server.port() + pathAndQuery))
                .GET()
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /** Pulls the text content of the first {@code <tag>...</tag>} occurrence out of an S3 XML body. */
    public static String extractTag(String xml, String tag) {
        String open = "<" + tag + ">";
        int start = xml.indexOf(open);
        if (start < 0) {
            return null;
        }
        int end = xml.indexOf("</" + tag + ">", start);
        return xml.substring(start + open.length(), end);
    }

    /** Percent-encodes every byte as {@code %XX} so raw key bytes (including 0xFF) survive a query verbatim. */
    public static String percentEncode(byte[] raw) {
        StringBuilder out = new StringBuilder(raw.length * 3);
        for (byte b : raw) {
            out.append('%');
            out.append(Character.forDigit((b >>> 4) & 0xF, 16));
            out.append(Character.forDigit(b & 0xF, 16));
        }
        return out.toString();
    }

    public static String percentEncode(String value) {
        return percentEncode(value.getBytes(StandardCharsets.UTF_8));
    }
}
