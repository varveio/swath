/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * The token-walk benchmark driver: starts a real in-process {@link
 * ReplayServer} per requested {@link ServingMode}, walks the FULL listing
 * token-by-token over real HTTP exactly like a client would (mirroring the loopback pattern in
 * {@code ReplayConformanceComparator}), and reports the §6 latency/throughput corridor numbers —
 * index-load/startup time, page/key counts, wall time, pages/sec, keys/sec, and ms/page measured
 * two independent ways: client-side (this class's own request timing) and server-side (the {@code
 * page.read.latency}/{@code fixture.list.latency} timers already publishing p50/p99, read straight
 * off {@link ReplayMetrics#registry()}). Both are reported and labeled so a corridor check never
 * conflates "what the client observed" with "what the store itself spent".
 */
public final class TokenWalkBenchmark {

    private static final int MAX_PAGES_GUARD = 10_000_000;

    private TokenWalkBenchmark() {
    }

    public static Report run(Options options) throws Exception {
        List<ModeReport> modeReports = new ArrayList<>(options.modes().size());
        for (ServingMode mode : options.modes()) {
            modeReports.add(runMode(options, mode));
        }
        return new Report(options.fixture(), options.bucket(), options.maxKeys(), options.prefix(),
                options.delimiter(), modeReports, ratios(modeReports));
    }

    private static ModeReport runMode(Options options, ServingMode mode) throws Exception {
        long startupStartNanos = System.nanoTime();
        try (ReplayServer server = new ReplayServer(
                options.host(), 0, options.bucket(), options.fixture(), options.parquetConnections(), mode)) {
            server.start();
            double startupMs = nanosToMillis(System.nanoTime() - startupStartNanos);
            ServingMode resolved = server.resolvedServingMode();

            HttpClient client = HttpClient.newHttpClient();
            List<Double> clientMs = new ArrayList<>();
            int pages = 0;
            long keys = 0;
            String token = null;
            long wallStartNanos = System.nanoTime();
            while (true) {
                if (pages > MAX_PAGES_GUARD) {
                    throw new IllegalStateException("token walk did not terminate within " + MAX_PAGES_GUARD
                            + " pages");
                }
                URI uri = URI.create("http://" + options.host() + ":" + server.port() + "/" + options.bucket()
                        + buildQuery(options, token));
                HttpRequest request = HttpRequest.newBuilder(uri).GET().build();
                long requestStartNanos = System.nanoTime();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                long requestEndNanos = System.nanoTime();
                if (response.statusCode() != 200) {
                    throw new IllegalStateException(
                            "bench request failed status=" + response.statusCode() + " body=" + response.body());
                }
                clientMs.add(nanosToMillis(requestEndNanos - requestStartNanos));
                pages++;
                keys += countOccurrences(response.body(), "<Contents>");
                if (!"true".equals(extractTag(response.body(), "IsTruncated"))) {
                    break;
                }
                token = extractTag(response.body(), "NextContinuationToken");
                if (token == null) {
                    throw new IllegalStateException("a truncated page carried no continuation token");
                }
            }
            double wallMs = nanosToMillis(System.nanoTime() - wallStartNanos);

            MeterRegistry registry = server.metrics().registry();
            PercentileStats pageRead = percentiles(registry, "swath.replay.page.read.latency");
            PercentileStats fixtureList = percentiles(registry, "swath.replay.fixture.list.latency");

            return new ModeReport(
                    mode.name().toLowerCase(Locale.ROOT), resolved.name().toLowerCase(Locale.ROOT),
                    startupMs, pages, keys, wallMs,
                    perSecond(pages, wallMs), perSecond(keys, wallMs),
                    average(clientMs), percentileOf(clientMs, 0.50), percentileOf(clientMs, 0.99),
                    pageRead, fixtureList);
        }
    }

    private static String buildQuery(Options options, String continuationToken) {
        StringBuilder query = new StringBuilder("?list-type=2&max-keys=").append(options.maxKeys());
        if (options.prefix() != null && !options.prefix().isEmpty()) {
            query.append("&prefix=").append(percentEncode(options.prefix()));
        }
        if (options.delimiter() != null && !options.delimiter().isEmpty()) {
            query.append("&delimiter=").append(percentEncode(options.delimiter()));
        }
        if (continuationToken != null) {
            query.append("&continuation-token=").append(percentEncode(continuationToken));
        }
        return query.toString();
    }

    /** Percent-encodes every non-alphanumeric byte, so it round-trips through {@code ByteKeys.percentDecode}
     *  without relying on {@code +}-for-space (which that decoder does not special-case). */
    private static String percentEncode(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        StringBuilder out = new StringBuilder(bytes.length * 3);
        for (byte b : bytes) {
            int v = b & 0xff;
            if ((v >= 'A' && v <= 'Z') || (v >= 'a' && v <= 'z') || (v >= '0' && v <= '9')) {
                out.append((char) v);
            } else {
                out.append('%').append(Character.forDigit((v >>> 4) & 0xF, 16))
                        .append(Character.forDigit(v & 0xF, 16));
            }
        }
        return out.toString();
    }

    private static int countOccurrences(String body, String needle) {
        int count = 0;
        int index = 0;
        while ((index = body.indexOf(needle, index)) >= 0) {
            count++;
            index += needle.length();
        }
        return count;
    }

    private static String extractTag(String xml, String tag) {
        String open = "<" + tag + ">";
        int start = xml.indexOf(open);
        if (start < 0) {
            return null;
        }
        int end = xml.indexOf("</" + tag + ">", start);
        if (end < 0) {
            return null;
        }
        return xml.substring(start + open.length(), end);
    }

    private static PercentileStats percentiles(MeterRegistry registry, String meterName) {
        Timer timer = registry.find(meterName).timer();
        if (timer == null || timer.count() == 0) {
            return new PercentileStats(0, 0.0, 0.0, 0.0);
        }
        double p50 = 0.0;
        double p99 = 0.0;
        HistogramSnapshot snapshot = timer.takeSnapshot();
        for (ValueAtPercentile value : snapshot.percentileValues()) {
            if (Math.abs(value.percentile() - 0.5) < 1e-9) {
                p50 = value.value(TimeUnit.MILLISECONDS);
            } else if (Math.abs(value.percentile() - 0.99) < 1e-9) {
                p99 = value.value(TimeUnit.MILLISECONDS);
            }
        }
        return new PercentileStats(timer.count(), timer.mean(TimeUnit.MILLISECONDS), p50, p99);
    }

    private static double average(List<Double> values) {
        if (values.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        for (double v : values) {
            sum += v;
        }
        return sum / values.size();
    }

    private static double percentileOf(List<Double> values, double percentile) {
        if (values.isEmpty()) {
            return 0.0;
        }
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int index = (int) Math.ceil(percentile * sorted.size()) - 1;
        index = Math.max(0, Math.min(sorted.size() - 1, index));
        return sorted.get(index);
    }

    private static double perSecond(long count, double millis) {
        return millis <= 0.0 ? 0.0 : count / (millis / 1000.0);
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private static List<RatioEntry> ratios(List<ModeReport> modeReports) {
        if (modeReports.size() < 2) {
            return List.of();
        }
        ModeReport base = modeReports.get(0);
        List<RatioEntry> out = new ArrayList<>(modeReports.size() - 1);
        for (int i = 1; i < modeReports.size(); i++) {
            ModeReport compare = modeReports.get(i);
            out.add(new RatioEntry(base.mode(), compare.mode(),
                    ratio(compare.wallMs(), base.wallMs()),
                    ratio(compare.clientMsPerPageAvg(), base.clientMsPerPageAvg()),
                    ratio(compare.serverPageRead().avgMs(), base.serverPageRead().avgMs())));
        }
        return out;
    }

    private static double ratio(double numerator, double denominator) {
        return denominator == 0.0 ? 0.0 : numerator / denominator;
    }

    /** One serving-mode run's report: {@code mode} is what was requested, {@code resolvedMode} is what
     *  {@code auto} actually resolved to (identical to {@code mode} for {@code sorted}/{@code duckdb}). */
    public record ModeReport(String mode, String resolvedMode, double startupMs, int pages, long keys,
                             double wallMs, double pagesPerSec, double keysPerSec,
                             double clientMsPerPageAvg, double clientMsPerPageP50, double clientMsPerPageP99,
                             PercentileStats serverPageRead, PercentileStats serverFixtureList) {
    }

    /** A registry-sourced timer's count + mean + the p50/p99 the meter itself publishes. */
    public record PercentileStats(long count, double avgMs, double p50Ms, double p99Ms) {
    }

    /** {@code compareMode} vs {@code baseMode} (the first requested mode), &gt; 1 means slower. */
    public record RatioEntry(String baseMode, String compareMode, double wallMsRatio,
                             double clientMsPerPageAvgRatio, double serverPageReadAvgRatio) {
    }

    public record Options(Path fixture, String bucket, String host, List<ServingMode> modes, int maxKeys,
                          String prefix, String delimiter, int parquetConnections) {
    }

    public record Report(Path fixture, String bucket, int maxKeys, String prefix, String delimiter,
                         List<ModeReport> modes, List<RatioEntry> ratios) {

        public String humanTable() {
            StringBuilder out = new StringBuilder();
            out.append("token_walk_bench fixture=").append(fixture.toAbsolutePath())
                    .append(" bucket=").append(bucket)
                    .append(" max_keys=").append(maxKeys)
                    .append(" prefix=").append(prefix == null ? "-" : prefix)
                    .append(" delimiter=").append(delimiter == null ? "-" : delimiter)
                    .append('\n');
            for (ModeReport m : modes) {
                out.append(String.format(Locale.ROOT,
                        "mode=%s resolved=%s startup_ms=%.3f pages=%d keys=%d wall_ms=%.3f "
                                + "pages_per_sec=%.3f keys_per_sec=%.3f%n",
                        m.mode(), m.resolvedMode(), m.startupMs(), m.pages(), m.keys(), m.wallMs(),
                        m.pagesPerSec(), m.keysPerSec()));
                out.append(String.format(Locale.ROOT,
                        "  client   ms/page avg=%.3f p50=%.3f p99=%.3f%n",
                        m.clientMsPerPageAvg(), m.clientMsPerPageP50(), m.clientMsPerPageP99()));
                out.append(String.format(Locale.ROOT,
                        "  server page.read.latency    count=%d avg_ms=%.3f p50_ms=%.3f p99_ms=%.3f%n",
                        m.serverPageRead().count(), m.serverPageRead().avgMs(), m.serverPageRead().p50Ms(),
                        m.serverPageRead().p99Ms()));
                out.append(String.format(Locale.ROOT,
                        "  server fixture.list.latency count=%d avg_ms=%.3f p50_ms=%.3f p99_ms=%.3f%n",
                        m.serverFixtureList().count(), m.serverFixtureList().avgMs(), m.serverFixtureList().p50Ms(),
                        m.serverFixtureList().p99Ms()));
            }
            for (RatioEntry r : ratios) {
                out.append(String.format(Locale.ROOT,
                        "ratio %s/%s: wall=%.3fx client_avg=%.3fx server_page_read_avg=%.3fx%n",
                        r.compareMode(), r.baseMode(), r.wallMsRatio(), r.clientMsPerPageAvgRatio(),
                        r.serverPageReadAvgRatio()));
            }
            return out.toString();
        }

        public String toJson() {
            StringBuilder json = new StringBuilder();
            json.append("{");
            field(json, "fixture", fixture.toAbsolutePath().toString());
            json.append(",");
            field(json, "bucket", bucket);
            json.append(",\"max_keys\":").append(maxKeys).append(",");
            fieldOrNull(json, "prefix", prefix);
            json.append(",");
            fieldOrNull(json, "delimiter", delimiter);
            json.append(",\"modes\":[");
            for (int i = 0; i < modes.size(); i++) {
                if (i > 0) {
                    json.append(",");
                }
                appendMode(json, modes.get(i));
            }
            json.append("],\"ratios\":[");
            for (int i = 0; i < ratios.size(); i++) {
                if (i > 0) {
                    json.append(",");
                }
                appendRatio(json, ratios.get(i));
            }
            json.append("]}");
            return json.toString();
        }

        private static void appendMode(StringBuilder json, ModeReport m) {
            json.append("{");
            field(json, "mode", m.mode());
            json.append(",");
            field(json, "resolved_mode", m.resolvedMode());
            json.append(",\"startup_ms\":").append(m.startupMs());
            json.append(",\"pages\":").append(m.pages());
            json.append(",\"keys\":").append(m.keys());
            json.append(",\"wall_ms\":").append(m.wallMs());
            json.append(",\"pages_per_sec\":").append(m.pagesPerSec());
            json.append(",\"keys_per_sec\":").append(m.keysPerSec());
            json.append(",\"client\":{\"avg_ms\":").append(m.clientMsPerPageAvg())
                    .append(",\"p50_ms\":").append(m.clientMsPerPageP50())
                    .append(",\"p99_ms\":").append(m.clientMsPerPageP99()).append("}");
            json.append(",\"server_page_read\":");
            appendStats(json, m.serverPageRead());
            json.append(",\"server_fixture_list\":");
            appendStats(json, m.serverFixtureList());
            json.append("}");
        }

        private static void appendStats(StringBuilder json, PercentileStats s) {
            json.append("{\"count\":").append(s.count())
                    .append(",\"avg_ms\":").append(s.avgMs())
                    .append(",\"p50_ms\":").append(s.p50Ms())
                    .append(",\"p99_ms\":").append(s.p99Ms()).append("}");
        }

        private static void appendRatio(StringBuilder json, RatioEntry r) {
            json.append("{");
            field(json, "base_mode", r.baseMode());
            json.append(",");
            field(json, "compare_mode", r.compareMode());
            json.append(",\"wall_ms_ratio\":").append(r.wallMsRatio());
            json.append(",\"client_ms_per_page_avg_ratio\":").append(r.clientMsPerPageAvgRatio());
            json.append(",\"server_page_read_avg_ratio\":").append(r.serverPageReadAvgRatio());
            json.append("}");
        }

        private static void field(StringBuilder json, String name, String value) {
            json.append('"').append(name).append("\":");
            quote(json, value);
        }

        private static void fieldOrNull(StringBuilder json, String name, String value) {
            json.append('"').append(name).append("\":");
            if (value == null) {
                json.append("null");
            } else {
                quote(json, value);
            }
        }

        private static void quote(StringBuilder json, String value) {
            json.append('"');
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                switch (c) {
                    case '"' -> json.append("\\\"");
                    case '\\' -> json.append("\\\\");
                    case '\n' -> json.append("\\n");
                    case '\r' -> json.append("\\r");
                    case '\t' -> json.append("\\t");
                    default -> json.append(c);
                }
            }
            json.append('"');
        }
    }
}
