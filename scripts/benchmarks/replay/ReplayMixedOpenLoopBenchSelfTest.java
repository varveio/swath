/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

/** Short nonfinal pages, real opaque native tokens, exact fixture oracle and open-loop counts. */
public final class ReplayMixedOpenLoopBenchSelfTest {
    private static final AtomicBoolean MEASURED = new AtomicBoolean();
    private static final AtomicBoolean RATE_WARMUP = new AtomicBoolean();
    private ReplayMixedOpenLoopBenchSelfTest() { }

    public static void main(String[] args) throws Exception {
        verifyFixedLanePlan();
        verifyCursorUsesCompletedToken();
        verifyAbsoluteSchedule();
        verifyLongPhaseDeadline();
        Path fixture = Files.createTempFile("swath-mixed-selftest-", ".parquet");
        Path emptyFinalFixture = Files.createTempFile("swath-mixed-empty-final-", ".parquet");
        Files.delete(fixture);
        Files.delete(emptyFinalFixture);
        try {
            try (var connection = DriverManager.getConnection("jdbc:duckdb:");
                 var statement = connection.createStatement()) {
                statement.execute("CREATE TABLE fixture(key BLOB, size BIGINT, last_modified TIMESTAMPTZ)");
                statement.execute("INSERT INTO fixture SELECT encode(printf('k%04d', i)), 1, "
                        + "TIMESTAMPTZ '2020-01-01 00:00:00+00' FROM range(17) t(i) ORDER BY i");
                statement.execute("COPY fixture TO '" + fixture.toString().replace("'", "''")
                        + "' (FORMAT PARQUET)");
                statement.execute("COPY (SELECT * FROM fixture WHERE key < encode('k0016')) TO '"
                        + emptyFinalFixture.toString().replace("'", "''") + "' (FORMAT PARQUET)");
            }
            ReplayHttpBench.Inventory inventory = ReplayHttpBench.fixtureInventory(fixture.toString(), 0);
            String declared = inventory.count() + ":" + inventory.digest();
            AtomicBoolean corruptToken = new AtomicBoolean();
            AtomicBoolean dropMeasuredToken = new AtomicBoolean();
            AtomicBoolean dropMeasuredBlockEndToken = new AtomicBoolean();
            AtomicBoolean mutateMeasuredName = new AtomicBoolean();
            AtomicBoolean refuseMeasured = new AtomicBoolean();
            AtomicBoolean slowMeasured = new AtomicBoolean();
            AtomicBoolean slowTargetWarmup = new AtomicBoolean();
            AtomicBoolean emptyFinal = new AtomicBoolean();
            AtomicInteger availableKeys = new AtomicInteger(17);
            Map<String, AtomicIntegerArray> measuredVisits = new ConcurrentHashMap<>();
            AtomicLong tokenRequests = new AtomicLong();
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            var workers = Executors.newVirtualThreadPerTaskExecutor();
            server.setExecutor(workers);
            server.createContext("/", exchange -> {
                String path = exchange.getRequestURI().getPath();
                String protocol = path.startsWith("/storage/v1/") ? "gcs"
                        : path.startsWith("/replay/") ? "azure" : "s3";
                Map<String, String> query = query(exchange.getRequestURI().getRawQuery());
                int requested = Integer.parseInt(query.getOrDefault("max-keys",
                        query.getOrDefault("maxResults", query.getOrDefault("maxresults", "-1"))));
                String token = query.getOrDefault("continuation-token",
                        query.getOrDefault("pageToken", query.get("marker")));
                boolean measured = MEASURED.get();
                if (token != null) tokenRequests.incrementAndGet();
                if (slowMeasured.get() && measured || slowTargetWarmup.get() && RATE_WARMUP.get()) {
                    try { Thread.sleep(250); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                }
                int start = token == null ? 0 : token.startsWith("tok-")
                        ? tokenPosition(token.substring(4)) : -1;
                if (measured && start >= 0 && start < availableKeys.get()) {
                    measuredVisits.computeIfAbsent(protocol, ignored -> new AtomicIntegerArray(17))
                            .incrementAndGet(start);
                }
                String body;
                int status;
                if (refuseMeasured.get() && measured && "tok-1".equals(token)) {
                    status = 503;
                    exchange.getResponseHeaders().set("x-swath-replay-error", "benchmark-refusal");
                    body = "refused";
                } else if (start < 0 || requested != 1000 || protocol.equals("azure")
                        && !"2026-06-06".equals(exchange.getRequestHeaders().getFirst("x-ms-version"))) {
                    status = 400;
                    body = "invalid native token";
                } else {
                    status = 200;
                    int end = Math.min(availableKeys.get(), start + 1);
                    String next = end < availableKeys.get() || emptyFinal.get()
                            && end == availableKeys.get() && start < end ? "tok-" + end : null;
                    if (corruptToken.get() && start == 1) next = "tok-1";
                    if (dropMeasuredToken.get() && measured && start == 0) next = null;
                    if (dropMeasuredBlockEndToken.get() && measured && start == 2) next = null;
                    body = switch (protocol) {
                        case "gcs" -> gcs(start, end, next);
                        case "azure" -> azure(start, end, next);
                        default -> s3(start, end, next);
                    };
                    if (mutateMeasuredName.get() && measured && protocol.equals("s3")
                            && start == 1) {
                        body = body.replace("<Key>k0001</Key>", "<Key>k0001x</Key>");
                    }
                }
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status, bytes.length);
                try (var output = exchange.getResponseBody()) { output.write(bytes); }
            });
            server.start();
            try {
                String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
                String preflight = inspect(endpoint, fixture, declared);
                if (!preflight.contains("\"purpose\":\"native_page_plan_preflight\"")
                        || !preflight.contains("\"native_pages\":17")) {
                    throw new AssertionError("native page-plan preflight missing: " + preflight);
                }
                tokenRequests.set(0);
                measuredVisits.clear();
                String mixed = run(endpoint, fixture, declared);
                if (!mixed.contains("\"native_pages\":17")
                        || !mixed.contains("\"RATE_WARMUP_START\"")
                        || !mixed.contains("\"RATE_WARMUP_END\"")
                        || !mixed.contains("\"complete_inventory_cycles\":1")
                        || !mixed.contains("\"partial_tail_pages\":0")
                        || !mixed.contains("\"page_plan_sha256\":\"")
                        || !mixed.contains("\"worker_dispatch_p99_lag_ns\":")
                        || !mixed.contains("\"client_send_p99_lag_ns\":")
                        || !mixed.contains("\"offered_requests_per_group\":17")
                        || !mixed.contains("\"attempted_requests\":51")
                        || !mixed.contains("\"successful_requests\":51")
                        || !mixed.contains("\"unsent_requests\":0")
                        || number(mixed, "predecessor_wait_p99_ns") > 10_000_000_000L
                        || number(mixed, "predecessor_wait_max_ns") > 10_000_000_000L
                        || tokenRequests.get() < 12) {
                    throw new AssertionError("mixed native-token result missing: " + mixed);
                }
                assertGroups(mixed, List.of("s3", "gcs", "azure"),
                        1, 17, 17, inventory.digest());
                assertVisits(measuredVisits, List.of("s3", "gcs", "azure"), 1, 17);
                measuredVisits.clear();
                RunOutcome twoCycles = capture(endpoint, fixture, declared,
                        "s3,gcs,azure", "30", "34", "64");
                if (twoCycles.failure() != null) throw twoCycles.failure();
                assertGroups(twoCycles.output(), List.of("s3", "gcs", "azure"),
                        2, 34, 34, inventory.digest());
                assertVisits(measuredVisits, List.of("s3", "gcs", "azure"), 2, 17);
                measuredVisits.clear();
                RunOutcome isolatedGcs = capture(endpoint, fixture, declared,
                        "gcs", "30", "64");
                if (isolatedGcs.failure() != null) throw isolatedGcs.failure();
                assertGroups(isolatedGcs.output(), List.of("gcs", "gcs", "gcs"),
                        1, 17, 17, inventory.digest());
                assertVisits(measuredVisits, List.of("gcs"), 3, 17);
                corruptToken.set(true);
                try {
                    run(endpoint, fixture, declared);
                    throw new AssertionError("repeating native token was accepted");
                } catch (IllegalStateException expected) {
                    if (!expected.getMessage().contains("repeated native token")) throw expected;
                }
                corruptToken.set(false);
                dropMeasuredToken.set(true);
                expectFailure(endpoint, fixture, declared, "measured continuation token");
                dropMeasuredToken.set(false);
                dropMeasuredBlockEndToken.set(true);
                expectFailure(endpoint, fixture, declared, "measured continuation token");
                dropMeasuredBlockEndToken.set(false);
                mutateMeasuredName.set(true);
                expectFailure(endpoint, fixture, declared, "page inventory mismatch");
                mutateMeasuredName.set(false);
                refuseMeasured.set(true);
                expectFailure(endpoint, fixture, declared, "benchmark-refusal");
                refuseMeasured.set(false);
                slowMeasured.set(true);
                RunOutcome slow = capture(endpoint, fixture, declared, "30", "64");
                if (slow.failure() != null) throw slow.failure();
                var latency = java.util.regex.Pattern.compile("\\\"p50_ns\\\":(\\d+)")
                        .matcher(slow.output());
                if (!latency.find() || Long.parseLong(latency.group(1)) < 200_000_000L) {
                    throw new AssertionError("scheduled-send latency omitted stalled responses");
                }
                RunOutcome overloaded = capture(endpoint, fixture, declared, "1000", "4");
                var unsent = java.util.regex.Pattern.compile("\\\"unsent_requests\\\":([1-9]\\d*)")
                        .matcher(overloaded.output());
                if (overloaded.failure() == null || !unsent.find()
                        || !overloaded.output().contains("\"status\":\"failed\"")
                        || overloaded.output().contains("\"MEASURE_END\"")
                        || overloaded.output().contains("\"workload\":\"mixed_open_loop\"")) {
                    throw new AssertionError("bounded outstanding overflow lacked a failed receipt: "
                            + overloaded.output(), overloaded.failure());
                }
                slowMeasured.set(false);
                slowTargetWarmup.set(true);
                RunOutcome warmupLimited = capture(endpoint, fixture, declared, "1000", "4");
                if (warmupLimited.failure() == null
                        || !warmupLimited.output().contains("\"phase\":\"target_rate_warmup\"")
                        || !warmupLimited.output().contains(
                                "\"event\":\"RATE_WARMUP_END\",\"status\":\"failed\"")
                        || warmupLimited.output().contains("\"MEASURE_START\"")) {
                    throw new AssertionError("target-rate warmup failure phase was not preserved: "
                            + warmupLimited.output(), warmupLimited.failure());
                }
                long scheduled = number(warmupLimited.output(), "target_warmup_scheduled");
                long admitted = number(warmupLimited.output(), "target_warmup_admitted");
                long completed = number(warmupLimited.output(), "target_warmup_completed");
                long missed = number(warmupLimited.output(), "target_warmup_unsent");
                if (scheduled < 1 || missed < 1 || admitted + missed != scheduled
                        || completed > admitted
                        || number(warmupLimited.output(), "offered_requests") != scheduled
                        || number(warmupLimited.output(), "successful_requests") != completed
                        || number(warmupLimited.output(), "measured_scheduled") != 0) {
                    throw new AssertionError("target-rate warmup counters were mislabeled: "
                            + warmupLimited.output());
                }
                slowTargetWarmup.set(false);
                emptyFinal.set(true);
                availableKeys.set(16);
                ReplayHttpBench.Inventory exactlySixteen = ReplayHttpBench.fixtureInventory(
                        emptyFinalFixture.toString(), 0);
                String emptyDeclared = exactlySixteen.count() + ":" + exactlySixteen.digest();
                String emptyResult = run(endpoint, emptyFinalFixture, emptyDeclared);
                if (!emptyResult.contains("\"native_pages\":17")
                        || !emptyResult.contains("\"objects\":48")) {
                    throw new AssertionError("empty final native page was rejected: " + emptyResult);
                }
            } finally {
                server.stop(0);
                workers.shutdownNow();
            }
        } finally {
            Files.deleteIfExists(fixture);
            Files.deleteIfExists(emptyFinalFixture);
        }
        System.out.println("ReplayMixedOpenLoopBenchSelfTest passed");
    }

    private static String run(String endpoint, Path fixture, String declared) throws Exception {
        RunOutcome outcome = capture(endpoint, fixture, declared, "30", "64");
        if (outcome.failure() != null) throw outcome.failure();
        return outcome.output();
    }

    private static String inspect(String endpoint, Path fixture, String declared) throws Exception {
        PrintStream original = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        MEASURED.set(false);
        try (PrintStream capture = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            System.setOut(capture);
            ReplayMixedOpenLoopBench.main(new String[] {"--page-plan", endpoint, "bench",
                    fixture.toString(), declared});
        } finally {
            System.setOut(original);
        }
        return output.toString(StandardCharsets.UTF_8);
    }

    private static RunOutcome capture(String endpoint, Path fixture, String declared,
                                      String rate, String maxOutstanding) throws Exception {
        return capture(endpoint, fixture, declared, "s3,gcs,azure", rate, "17", maxOutstanding);
    }

    private static RunOutcome capture(String endpoint, Path fixture, String declared,
                                      String protocols, String rate, String maxOutstanding) throws Exception {
        return capture(endpoint, fixture, declared, protocols, rate, "17", maxOutstanding);
    }

    private static RunOutcome capture(String endpoint, Path fixture, String declared,
                                      String protocols, String rate, String offeredEach,
                                      String maxOutstanding) throws Exception {
        var input = System.in;
        var output = System.out;
        ByteArrayOutputStream capture = new ByteArrayOutputStream();
        Exception failure = null;
        MEASURED.set(false);
        RATE_WARMUP.set(false);
        OutputStream marked = new OutputStream() {
            @Override public void write(int value) throws IOException { capture.write(value); }
            @Override public void write(byte[] bytes, int offset, int length) throws IOException {
                capture.write(bytes, offset, length);
                String text = capture.toString(StandardCharsets.UTF_8);
                if (text.contains("\"RATE_WARMUP_START\"")) RATE_WARMUP.set(true);
                if (text.contains("\"RATE_WARMUP_END\"")) RATE_WARMUP.set(false);
                if (text.contains("\"MEASURE_START\"")) {
                    MEASURED.set(true);
                }
            }
        };
        try (PrintStream stream = new PrintStream(marked, true, StandardCharsets.UTF_8)) {
            System.setIn(new ByteArrayInputStream(new byte[] {'\n', '\n', '\n', '\n'}));
            System.setOut(stream);
            try {
                ReplayMixedOpenLoopBench.main(new String[] {endpoint, "bench", fixture.toString(),
                        declared, protocols, rate, offeredEach, "1000", "1", "1",
                        maxOutstanding, "bracket", "end_ack"});
            } catch (Exception error) {
                failure = error;
            }
        } finally {
            System.setIn(input);
            System.setOut(output);
        }
        return new RunOutcome(capture.toString(StandardCharsets.UTF_8), failure);
    }

    private static void expectFailure(String endpoint, Path fixture, String declared,
                                      String expected) throws Exception {
        RunOutcome outcome = capture(endpoint, fixture, declared, "30", "64");
        if (outcome.failure() == null) throw new AssertionError(expected + " failure was accepted");
        String detail = outcome.failure().toString()
                + (outcome.failure().getCause() == null ? "" : outcome.failure().getCause());
        if (!detail.contains(expected) || !outcome.output().contains("\"status\":\"failed\"")) {
            throw new AssertionError("failure receipt omitted " + expected + ": " + outcome.output(),
                    outcome.failure());
        }
    }

    private record RunOutcome(String output, Exception failure) { }

    private static void verifyFixedLanePlan() {
        if (!ReplayMixedOpenLoopBench.logicalGroupProtocols(List.of("s3", "gcs", "azure"))
                .equals(List.of("s3", "gcs", "azure"))
                || !ReplayMixedOpenLoopBench.logicalGroupProtocols(List.of("gcs"))
                .equals(List.of("gcs", "gcs", "gcs"))) {
            throw new AssertionError("logical group mapping differs between mixed and isolated");
        }
        for (int pageCount : new int[] {17, 13_721}) {
            var ranges = ReplayMixedOpenLoopBench.laneRanges(pageCount);
            if (ranges.size() != 16) throw new AssertionError("fixed arm lacks 16 lanes");
            int[] visits = new int[pageCount];
            int[] scheduledSlots = new int[pageCount * 2];
            for (int lane = 0; lane < ranges.size(); lane++) {
                var range = ranges.get(lane);
                if (range.endExclusive() <= range.startInclusive()) {
                    throw new AssertionError("empty fixed-capacity lane");
                }
                for (int cycle = 0; cycle < 2; cycle++) {
                    for (int localTicket = 0; localTicket < range.endExclusive()
                            - range.startInclusive(); localTicket++) {
                        int slot = cycle * pageCount + localTicket * 16 + lane;
                        if (slot >= scheduledSlots.length) {
                            throw new AssertionError("fixed-lane schedule has a sparse tail");
                        }
                        scheduledSlots[slot]++;
                    }
                }
                for (int page = range.startInclusive(); page < range.endExclusive(); page++) {
                    if (page < 0 || page >= pageCount) throw new AssertionError("lane outside page plan");
                    visits[page]++;
                }
            }
            for (int page = 0; page < pageCount; page++) {
                if (visits[page] != 1 || scheduledSlots[page] != 1
                        || scheduledSlots[page + pageCount] != 1) {
                    throw new AssertionError("page/slot " + page + " visits=" + visits[page]
                            + " scheduled first=" + scheduledSlots[page]
                            + " second=" + scheduledSlots[page + pageCount]);
                }
            }
            long lastInFirstCycle = ReplayMixedOpenLoopBench.scheduledNanos(0, 0,
                    (pageCount - 1) % 16, (pageCount - 1) / 16, 0, pageCount, 30);
            long firstInSecondCycle = ReplayMixedOpenLoopBench.scheduledNanos(0, 0,
                    0, 0, 1, pageCount, 30);
            if (Math.abs(firstInSecondCycle - lastInFirstCycle - 1_000_000_000L / 30) > 2) {
                throw new AssertionError("whole-cycle boundary introduced a schedule gap");
            }
        }
        try {
            ReplayMixedOpenLoopBench.laneRanges(15);
            throw new AssertionError("capacity arm admitted fewer than 16 native pages");
        } catch (IllegalArgumentException expected) {
            // A sparse-lane test plan would change the fixed-capacity workload.
        }
    }

    private static void verifyCursorUsesCompletedToken() {
        List<ReplayMixedOpenLoopBench.PageSpec> pages = new java.util.ArrayList<>();
        for (int page = 0; page < 17; page++) {
            byte[] key = ("k" + page).getBytes(StandardCharsets.UTF_8);
            String next = page == 16 ? null : "next-" + page;
            String token = page == 0 ? null : "next-" + (page - 1);
            pages.add(new ReplayMixedOpenLoopBench.PageSpec(token, next, key, key, null,
                    1, "digest-" + page, null));
        }
        int[] visits = new int[17];
        for (var range : ReplayMixedOpenLoopBench.laneRanges(17)) {
            var cursor = new ReplayMixedOpenLoopBench.LaneCursor(pages, range, 2);
            if (ReplayMixedOpenLoopBench.readyNanos(1000, cursor.predecessorCompleted()) != 1000) {
                throw new AssertionError("first lane ticket inherited a nonexistent predecessor");
            }
            while (cursor.hasNext()) {
                var expected = cursor.expected();
                int page = Integer.parseInt(new String(expected.first(), StandardCharsets.UTF_8)
                        .substring(1));
                if (!java.util.Objects.equals(cursor.token(), expected.token())) {
                    throw new AssertionError("lane did not use preceding response token");
                }
                var response = new ReplayMixedOpenLoopBench.FetchResult(1, expected.digest(),
                        null, expected.first(), expected.nextToken(), 10, 0);
                cursor.complete(response, 1000L + page);
                visits[page]++;
            }
            if (cursor.completed() != 2 * (range.endExclusive() - range.startInclusive())
                    || cursor.objects() != cursor.completed()) {
                throw new AssertionError("lane failed to complete exact full cycles");
            }
        }
        for (int page = 0; page < visits.length; page++) {
            if (visits[page] != 2) throw new AssertionError("cursor skipped or duplicated page " + page);
        }
        var twoPageLane = ReplayMixedOpenLoopBench.laneRanges(17).stream()
                .filter(range -> range.endExclusive() - range.startInclusive() == 2)
                .findFirst().orElseThrow();
        var cursor = new ReplayMixedOpenLoopBench.LaneCursor(pages, twoPageLane, 1);
        var first = cursor.expected();
        try {
            cursor.complete(new ReplayMixedOpenLoopBench.FetchResult(1, first.digest(), null,
                    first.first(), "mutated-token", 10, 0), 1000);
            throw new AssertionError("lane accepted a mutated continuation token");
        } catch (IllegalStateException expected) {
            if (cursor.completed() != 0 || cursor.expected() != first) {
                throw new AssertionError("failed page advanced lane cursor");
            }
        }
    }

    private static void verifyAbsoluteSchedule() {
        long start = 1_000_000_000L;
        long first = ReplayMixedOpenLoopBench.scheduledNanos(start, 0, 0, 0, 0, 17, 30);
        long secondGroup = ReplayMixedOpenLoopBench.scheduledNanos(start, 1, 0, 0, 0, 17, 30);
        long thirdGroup = ReplayMixedOpenLoopBench.scheduledNanos(start, 2, 0, 0, 0, 17, 30);
        long nextCycle = ReplayMixedOpenLoopBench.scheduledNanos(start, 0, 0, 0, 1, 17, 30);
        if (first != start || !(first < secondGroup && secondGroup < thirdGroup
                && thirdGroup < nextCycle)) {
            throw new AssertionError("logical group phase altered the absolute ticket order");
        }
        long predecessor = first + 250_000_000L;
        if (ReplayMixedOpenLoopBench.readyNanos(first, predecessor) != predecessor
                || ReplayMixedOpenLoopBench.readyNanos(predecessor, first) != predecessor) {
            throw new AssertionError("predecessor delay was hidden from the absolute schedule");
        }
    }

    private static void verifyLongPhaseDeadline() {
        long start = 1_000_000_000L;
        long thousandSecondWindow = 1_000_000_000_000L;
        long drain = 120_000_000_000L;
        long deadline = ReplayMixedOpenLoopBench.phaseDeadlineNanos(start, 100_000, 100, drain);
        if (deadline != start + thousandSecondWindow + drain
                || deadline - (start + thousandSecondWindow) != drain) {
            throw new AssertionError("planned window consumed the response drain allowance");
        }
        for (Runnable invalid : List.<Runnable>of(
                () -> ReplayMixedOpenLoopBench.phaseDeadlineNanos(start, 0, 100, drain),
                () -> ReplayMixedOpenLoopBench.phaseDeadlineNanos(start, 100, Double.NaN, drain),
                () -> ReplayMixedOpenLoopBench.phaseDeadlineNanos(start, 100, 100, -1),
                () -> ReplayMixedOpenLoopBench.phaseDeadlineNanos(
                        Long.MAX_VALUE - 10, 1, 1, drain))) {
            try {
                invalid.run();
                throw new AssertionError("invalid or overflowing phase deadline was accepted");
            } catch (IllegalArgumentException expected) {
                // An invalid plan must fail before scheduling client work.
            }
        }
    }

    private static int tokenPosition(String digits) {
        try {
            return Integer.parseInt(digits);
        } catch (NumberFormatException malformed) {
            return -1;
        }
    }

    private static void assertVisits(Map<String, AtomicIntegerArray> visits, List<String> protocols,
                                     int expectedPerPage, int pageCount) {
        if (!visits.keySet().equals(new java.util.HashSet<>(protocols))) {
            throw new AssertionError("measured protocol visits differ: " + visits.keySet());
        }
        for (String protocol : protocols) {
            AtomicIntegerArray pages = visits.get(protocol);
            for (int page = 0; page < pageCount; page++) {
                if (pages.get(page) != expectedPerPage) {
                    throw new AssertionError(protocol + " page " + page + " visited "
                            + pages.get(page) + " times, expected " + expectedPerPage);
                }
            }
        }
    }

    private static void assertGroups(String output, List<String> protocols, int cycles,
                                     int tickets, int objects, String fixtureDigest) {
        var groups = java.util.regex.Pattern.compile(
                "\\{\"group_id\":(\\d+),\"protocol\":\"([^\"]+)\","
                + "\"cycles\":(\\d+),\"tickets\":(\\d+),\"objects\":(\\d+),"
                + "\"key_digest\":\"([0-9a-f]+)\",\"page_plan_sha\":\"([0-9a-f]+)\","
                + "\"phase\":([0-9.]+)").matcher(output);
        int seen = 0;
        while (groups.find()) {
            if (seen >= protocols.size() || Integer.parseInt(groups.group(1)) != seen
                    || !groups.group(2).equals(protocols.get(seen))
                    || Integer.parseInt(groups.group(3)) != cycles
                    || Integer.parseInt(groups.group(4)) != tickets
                    || Integer.parseInt(groups.group(5)) != objects
                    || !groups.group(6).equals(fixtureDigest)
                    || groups.group(7).length() != 64
                    || Math.abs(Double.parseDouble(groups.group(8)) - seen / 3.0) > 0.000001) {
                throw new AssertionError("group identity, count, digest or phase differs: " + output);
            }
            seen++;
        }
        if (seen != 3) throw new AssertionError("missing three logical groups: " + output);
    }

    private static long number(String output, String field) {
        var match = java.util.regex.Pattern.compile("\\\"" + field + "\\\":(\\d+)")
                .matcher(output);
        if (!match.find()) throw new AssertionError("missing " + field + " in " + output);
        return Long.parseLong(match.group(1));
    }

    private static Map<String, String> query(String raw) {
        Map<String, String> result = new HashMap<>();
        if (raw == null) return result;
        for (String part : raw.split("&")) {
            String[] fields = part.split("=", 2);
            result.put(URLDecoder.decode(fields[0], StandardCharsets.UTF_8),
                    fields.length == 2 ? URLDecoder.decode(fields[1], StandardCharsets.UTF_8) : "");
        }
        return result;
    }

    private static String s3(int start, int end, String next) {
        StringBuilder out = new StringBuilder("<ListBucketResult><KeyCount>").append(end - start)
                .append("</KeyCount><IsTruncated>").append(next != null).append("</IsTruncated>");
        for (int i = start; i < end; i++) {
            out.append("<Contents><Key>k").append(String.format("%04d", i))
                    .append("</Key><Size>1</Size><LastModified>2020-01-01T00:00:00.000Z</LastModified></Contents>");
        }
        if (next != null) out.append("<NextContinuationToken>").append(next)
                .append("</NextContinuationToken>");
        return out.append("</ListBucketResult>").toString();
    }

    private static String gcs(int start, int end, String next) {
        StringBuilder out = new StringBuilder("{\"items\":[");
        for (int i = start; i < end; i++) {
            if (i > start) out.append(',');
            out.append("{\"name\":\"k").append(String.format("%04d", i))
                    .append("\",\"size\":\"1\",\"updated\":\"2020-01-01T00:00:00.000000Z\"}");
        }
        out.append(']');
        if (next != null) out.append(",\"nextPageToken\":\"").append(next).append('"');
        return out.append('}').toString();
    }

    private static String azure(int start, int end, String next) {
        StringBuilder out = new StringBuilder("<EnumerationResults><Blobs>");
        for (int i = start; i < end; i++) {
            out.append("<Blob><Name>k").append(String.format("%04d", i))
                    .append("</Name><Properties><Content-Length>1</Content-Length>"
                            + "<Last-Modified>Wed, 01 Jan 2020 00:00:00 GMT</Last-Modified>"
                            + "</Properties></Blob>");
        }
        out.append("</Blobs>");
        if (next != null) out.append("<NextMarker>").append(next).append("</NextMarker>");
        return out.append("</EnumerationResults>").toString();
    }
}
