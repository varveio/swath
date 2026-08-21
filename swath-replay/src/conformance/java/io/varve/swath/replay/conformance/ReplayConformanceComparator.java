/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance;

import io.varve.swath.replay.server.ReplayServer;
import io.varve.swath.replay.server.ServingMode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public final class ReplayConformanceComparator {

    private ReplayConformanceComparator() {
    }

    public static Summary compare(Options options) throws Exception {
        List<HarReader.RecordedExchange> exchanges = HarReader.read(options.har());
        List<HarReader.RecordedExchange> comparable = new ArrayList<>();
        List<String> failures = new ArrayList<>();
        int candidates = 0;
        int eligible = 0;
        int skipped = 0;

        if (options.mismatchDir() != null) {
            Files.createDirectories(options.mismatchDir());
        }

        for (HarReader.RecordedExchange exchange : exchanges) {
            if (!isCandidate(exchange, options.bucket())) {
                continue;
            }
            candidates++;
            if (hasQueryParam(exchange.uri(), "continuation-token")) {
                skipped++;
                continue;
            }
            comparable.add(exchange);
        }
        eligible = comparable.size();
        comparable = chooseComparable(comparable, options);
        skipped += eligible - comparable.size();

        String replayMetrics;
        try (ReplayServer server = new ReplayServer(
                options.host(), 0, options.bucket(), options.fixture(), options.replayParquetConnections(),
                options.servingMode())) {
            server.start();
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(options.timeout())
                    .followRedirects(HttpClient.Redirect.NEVER)
                    .build();

            failures.addAll(compareAll(options, server.port(), client, comparable));
            replayMetrics = server.metricsSummary();
        }

        return new Summary(exchanges.size(), candidates, eligible, comparable.size(), skipped, failures, replayMetrics);
    }

    private static List<HarReader.RecordedExchange> chooseComparable(List<HarReader.RecordedExchange> exchanges,
                                                                     Options options) {
        int limit = exchanges.size();
        if (options.maxEntries() > 0) {
            limit = Math.min(limit, options.maxEntries());
        }
        if (options.sampleEntries() > 0) {
            limit = Math.min(limit, options.sampleEntries());
            return sampleEvenly(exchanges, limit);
        }
        return limit == exchanges.size() ? exchanges : new ArrayList<>(exchanges.subList(0, limit));
    }

    private static List<HarReader.RecordedExchange> sampleEvenly(List<HarReader.RecordedExchange> exchanges, int n) {
        if (n >= exchanges.size()) {
            return exchanges;
        }
        if (n <= 0) {
            return List.of();
        }
        if (n == 1) {
            return List.of(exchanges.get(0));
        }
        List<HarReader.RecordedExchange> out = new ArrayList<>(n);
        long lastIndex = exchanges.size() - 1L;
        long lastSlot = n - 1L;
        int previous = -1;
        for (int slot = 0; slot < n; slot++) {
            int index = Math.toIntExact(Math.round((double) slot * lastIndex / lastSlot));
            if (index <= previous) {
                index = previous + 1;
            }
            out.add(exchanges.get(index));
            previous = index;
        }
        return out;
    }

    private static List<String> compareAll(Options options, int port, HttpClient client,
                                           List<HarReader.RecordedExchange> exchanges) throws Exception {
        if (exchanges.isEmpty()) {
            return List.of();
        }
        int parallelism = Math.max(1, options.parallelism());
        if (parallelism == 1 || exchanges.size() == 1) {
            List<String> failures = new ArrayList<>();
            for (HarReader.RecordedExchange exchange : exchanges) {
                String failure = compareOne(options, port, client, exchange);
                if (failure != null) {
                    failures.add(failure);
                }
            }
            return failures;
        }

        List<Callable<String>> calls = exchanges.stream()
                .<Callable<String>>map(exchange -> () -> compareOne(options, port, client, exchange))
                .toList();
        List<String> failures = new ArrayList<>();
        try (var executor = Executors.newFixedThreadPool(parallelism)) {
            for (var future : executor.invokeAll(calls)) {
                try {
                    String failure = future.get();
                    if (failure != null) {
                        failures.add(failure);
                    }
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof Exception exception) {
                        throw exception;
                    }
                    throw new RuntimeException(cause);
                }
            }
        }
        return failures;
    }

    private static String compareOne(Options options, int port, HttpClient client,
                                     HarReader.RecordedExchange expected) throws IOException, InterruptedException {
        URI actualUri = replayUri(options.host(), port, expected.uri());
        HttpRequest request = HttpRequest.newBuilder(actualUri)
                .timeout(options.timeout())
                .GET()
                .build();
        HttpResponse<byte[]> actual = client.send(request, HttpResponse.BodyHandlers.ofByteArray());

        StringBuilder failure = new StringBuilder();
        if (actual.statusCode() != expected.status()) {
            failure.append("status expected=").append(expected.status())
                    .append(" actual=").append(actual.statusCode()).append('\n');
        }
        String actualContentType = actual.headers().firstValue("content-type").orElse("");
        if (!actualContentType.toLowerCase(Locale.ROOT).contains("application/xml")) {
            failure.append("actual content-type is not application/xml: ").append(actualContentType).append('\n');
        }

        String expectedXml;
        String actualXml;
        try {
            expectedXml = XmlCanonicalizer.canonicalize(expected.body());
            actualXml = XmlCanonicalizer.canonicalize(actual.body());
            if (!expectedXml.equals(actualXml)) {
                failure.append("XML differs");
            }
        } catch (IllegalArgumentException e) {
            failure.append(e.getMessage());
            expectedXml = new String(expected.body(), StandardCharsets.UTF_8);
            actualXml = new String(actual.body(), StandardCharsets.UTF_8);
        }

        if (failure.length() == 0) {
            return null;
        }
        writeMismatch(options.mismatchDir(), expected.index(), expected, actual.body(), expectedXml, actualXml,
                failure.toString());
        return "HAR entry " + expected.index() + " " + expected.uri() + " -> " + failure;
    }

    private static boolean isCandidate(HarReader.RecordedExchange exchange, String bucket) {
        if (!"GET".equalsIgnoreCase(exchange.method())) {
            return false;
        }
        if (!bucket.equals(bucketFromPath(exchange.uri().getPath()))) {
            return false;
        }
        return "2".equals(queryParam(exchange.uri(), "list-type"));
    }

    private static boolean hasQueryParam(URI uri, String name) {
        return queryParam(uri, name) != null;
    }

    private static String queryParam(URI uri, String name) {
        String query = uri.getRawQuery();
        if (query == null || query.isEmpty()) {
            return null;
        }
        for (String part : query.split("&", -1)) {
            int eq = part.indexOf('=');
            String rawName = eq >= 0 ? part.substring(0, eq) : part;
            if (name.equals(rawName)) {
                return eq >= 0 ? part.substring(eq + 1) : "";
            }
        }
        return null;
    }

    private static String bucketFromPath(String path) {
        if (path == null || path.isBlank() || "/".equals(path)) {
            return "";
        }
        String withoutSlash = path.charAt(0) == '/' ? path.substring(1) : path;
        int slash = withoutSlash.indexOf('/');
        return slash >= 0 ? withoutSlash.substring(0, slash) : withoutSlash;
    }

    private static URI replayUri(String host, int port, URI original) {
        StringBuilder uri = new StringBuilder("http://").append(host).append(':').append(port);
        String rawPath = original.getRawPath();
        uri.append(rawPath == null || rawPath.isBlank() ? "/" : rawPath);
        if (original.getRawQuery() != null) {
            uri.append('?').append(original.getRawQuery());
        }
        return URI.create(uri.toString());
    }

    private static void writeMismatch(Path dir, int index, HarReader.RecordedExchange expected, byte[] actualBody,
                                      String expectedXml, String actualXml, String reason) throws IOException {
        if (dir == null) {
            return;
        }
        String prefix = "%05d".formatted(index);
        Files.writeString(dir.resolve(prefix + ".request.txt"), expected.method() + " " + expected.uri() + "\n"
                + reason + "\n", StandardCharsets.UTF_8);
        Files.write(dir.resolve(prefix + ".expected.xml"), expected.body());
        Files.write(dir.resolve(prefix + ".actual.xml"), actualBody);
        Files.writeString(dir.resolve(prefix + ".expected.canonical.txt"), expectedXml, StandardCharsets.UTF_8);
        Files.writeString(dir.resolve(prefix + ".actual.canonical.txt"), actualXml, StandardCharsets.UTF_8);
    }

    static int defaultParallelism() {
        return Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors()));
    }

    public record Options(Path har, Path fixture, String bucket, String host, Path mismatchDir,
                          int maxEntries, int sampleEntries, Duration timeout, int parallelism,
                          int replayParquetConnections, ServingMode servingMode) {
        public Options {
            parallelism = parallelism > 0 ? parallelism : defaultParallelism();
            replayParquetConnections = replayParquetConnections > 0
                    ? replayParquetConnections : defaultParallelism();
            servingMode = servingMode == null ? ServingMode.DUCKDB : servingMode;
        }
    }

    public record Summary(int harEntries, int candidates, int eligible, int compared, int skipped,
                          List<String> failures, String replayMetrics) {

        public int exitCode() {
            if (candidates == 0 || compared == 0) {
                return 2;
            }
            return failures.isEmpty() ? 0 : 1;
        }

        public String report() {
            StringBuilder out = new StringBuilder();
            out.append("replay_conformance har_entries=").append(harEntries)
                    .append(" candidates=").append(candidates)
                    .append(" eligible=").append(eligible)
                    .append(" compared=").append(compared)
                    .append(" skipped=").append(skipped)
                    .append(" failures=").append(failures.size())
                    .append('\n');
            for (String failure : failures) {
                out.append(failure).append('\n');
            }
            if (replayMetrics != null && !replayMetrics.isBlank()) {
                out.append(replayMetrics).append('\n');
            }
            if (candidates == 0) {
                out.append("no path-style ListObjectsV2 requests found in HAR\n");
            } else if (compared == 0) {
                out.append("no comparable ListObjectsV2 requests found in HAR\n");
            }
            return out.toString();
        }
    }
}
