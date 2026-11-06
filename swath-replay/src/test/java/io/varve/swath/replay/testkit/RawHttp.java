/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.testkit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * One HTTP/1.1 exchange with the request target sent byte for byte. java.net.http validates
 * percent escapes, so it cannot send the malformed spellings that native probes exercise.
 */
public final class RawHttp {
    private static final int TIMEOUT_MILLIS = 30_000;
    private static final int MAX_BYTES = 64 * 1024 * 1024;

    private RawHttp() {
    }

    public record Response(int status, Map<String, List<String>> headers, byte[] body) {
        public Response {
            body = body.clone();
        }

        @Override
        public byte[] body() { return body.clone(); }
    }

    public static Response exchange(String host, int port, String method, String target,
                                    List<Map.Entry<String, String>> headers) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), TIMEOUT_MILLIS);
            socket.setSoTimeout(TIMEOUT_MILLIS);
            StringBuilder request = new StringBuilder(method).append(' ').append(target)
                    .append(" HTTP/1.1\r\nHost: ").append(host).append(':').append(port)
                    .append("\r\nConnection: close\r\n");
            for (var header : headers) {
                request.append(header.getKey()).append(": ").append(header.getValue()).append("\r\n");
            }
            request.append("\r\n");
            socket.getOutputStream().write(request.toString().getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();
            return parse(socket.getInputStream(), method.equals("HEAD"));
        }
    }

    private static Response parse(InputStream input, boolean head) throws IOException {
        String statusLine = line(input);
        String[] status = statusLine.split(" ", 3);
        if (status.length < 2 || !status[0].startsWith("HTTP/1.")) {
            throw new IOException("malformed status line");
        }
        Map<String, List<String>> headers = new LinkedHashMap<>();
        for (String header = line(input); !header.isEmpty(); header = line(input)) {
            int colon = header.indexOf(':');
            if (colon <= 0) throw new IOException("malformed header line");
            headers.computeIfAbsent(header.substring(0, colon).trim().toLowerCase(Locale.ROOT),
                    ignored -> new ArrayList<>()).add(header.substring(colon + 1).trim());
        }
        byte[] body;
        if (head) {
            body = new byte[0];
        } else if (headers.getOrDefault("transfer-encoding", List.of()).stream()
                .anyMatch(value -> value.equalsIgnoreCase("chunked"))) {
            body = chunked(input);
        } else if (headers.containsKey("content-length")) {
            body = input.readNBytes(Math.toIntExact(Long.parseLong(headers.get("content-length").getFirst())));
        } else {
            body = input.readNBytes(MAX_BYTES);
        }
        return new Response(Integer.parseInt(status[1]), headers, body);
    }

    private static byte[] chunked(InputStream input) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        while (true) {
            String size = line(input).split(";", 2)[0].trim();
            int length = Integer.parseInt(size, 16);
            if (length == 0) {
                while (!line(input).isEmpty()) {
                    // trailers are not used by replay
                }
                return body.toByteArray();
            }
            if (body.size() + length > MAX_BYTES) throw new IOException("response too large");
            body.write(input.readNBytes(length));
            line(input);
        }
    }

    private static String line(InputStream input) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int previous = -1;
        while (true) {
            int next = input.read();
            if (next < 0) throw new IOException("connection closed mid-line");
            if (previous == '\r' && next == '\n') {
                byte[] raw = bytes.toByteArray();
                return new String(raw, 0, raw.length - 1, StandardCharsets.ISO_8859_1);
            }
            bytes.write(next);
            if (bytes.size() > 65_536) throw new IOException("header line too long");
            previous = next;
        }
    }
}
