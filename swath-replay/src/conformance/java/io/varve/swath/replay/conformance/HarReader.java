/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

final class HarReader {

    private static final ObjectMapper JSON = new ObjectMapper();

    private HarReader() {
    }

    static List<RecordedExchange> read(Path har) throws IOException {
        JsonNode entries = JSON.readTree(har.toFile()).path("log").path("entries");
        if (!entries.isArray()) {
            throw new IOException("HAR file has no log.entries array: " + har);
        }

        List<RecordedExchange> out = new ArrayList<>();
        for (int i = 0; i < entries.size(); i++) {
            JsonNode entry = entries.get(i);
            JsonNode request = entry.path("request");
            JsonNode response = entry.path("response");
            String method = request.path("method").asText();
            String url = request.path("url").asText();
            int status = response.path("status").asInt(-1);
            String contentType = header(response.path("headers"), "content-type");
            byte[] body = body(response.path("content"));
            out.add(new RecordedExchange(i, method, URI.create(url), status, contentType, body));
        }
        return out;
    }

    private static byte[] body(JsonNode content) {
        String text = content.path("text").asText("");
        if ("base64".equalsIgnoreCase(content.path("encoding").asText())) {
            return Base64.getDecoder().decode(text);
        }
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String header(JsonNode headers, String name) {
        if (!headers.isArray()) {
            return null;
        }
        for (JsonNode header : headers) {
            if (name.equalsIgnoreCase(header.path("name").asText())) {
                return header.path("value").asText();
            }
        }
        return null;
    }

    record RecordedExchange(int index, String method, URI uri, int status, String contentType, byte[] body) {
    }
}
