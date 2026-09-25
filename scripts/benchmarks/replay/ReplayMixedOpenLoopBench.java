/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import java.io.FilterInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/** Fixed-rate native 1k listing requests, with latency measured from each scheduled send. */
public final class ReplayMixedOpenLoopBench {
    private static final Duration BODY_DEADLINE = Duration.ofSeconds(30);
    private static final long MAX_BODY_BYTES = 64L * 1024 * 1024;
    private static final List<String> ALL = List.of("s3", "gcs", "azure");
    private static final int GROUP_COUNT = 3;
    private static final int LANES_PER_GROUP = 16;

    private ReplayMixedOpenLoopBench() { }

    public static void main(String[] argv) throws Exception {
        if (argv.length == 5 && "--page-plan".equals(argv[0])) {
            inspectPagePlan(URI.create(argv[1]), argv[2], argv[3], argv[4]);
            return;
        }
        boolean endAck = argv.length > 0 && "end_ack".equals(argv[argv.length - 1]);
        int fields = argv.length - (endAck ? 1 : 0);
        if (fields != 12 || !"bracket".equals(argv[11])) {
            throw new IllegalArgumentException("usage: ReplayMixedOpenLoopBench ENDPOINT BUCKET "
                    + "FIXTURE_GLOB COUNT:DIGEST s3|gcs|azure|s3,gcs,azure RATE_PER_GROUP_RPS "
                    + "OFFERED_PER_GROUP_COUNT PAGE_SIZE WARMUP_FULL_CYCLES RATE_WARMUP_CYCLES "
                    + "MAX_OUTSTANDING bracket [end_ack]");
        }
        URI endpoint = URI.create(argv[0]);
        String bucket = argv[1];
        String fixture = argv[2];
        String declared = argv[3];
        List<String> selected = argv[4].equals("s3,gcs,azure") ? ALL : List.of(argv[4]);
        List<String> groupProtocols = logicalGroupProtocols(selected);
        double rate = Double.parseDouble(argv[5]);
        int offeredEach = Integer.parseInt(argv[6]);
        double nominalDuration = offeredEach / rate;
        int pageSize = Integer.parseInt(argv[7]);
        int warmups = Integer.parseInt(argv[8]);
        int rateWarmupCycles = Integer.parseInt(argv[9]);
        int maxOutstanding = Integer.parseInt(argv[10]);
        if (!Double.isFinite(rate) || rate <= 0 || offeredEach < 1 || offeredEach > 1_000_000
                || !Double.isFinite(nominalDuration) || nominalDuration <= 0
                || pageSize != 1000 || warmups != 1
                || rateWarmupCycles < 1 || rateWarmupCycles > 10
                || maxOutstanding < 1 || maxOutstanding > 512) {
            throw new IllegalArgumentException("invalid fixed open-loop plan");
        }
        ReplayHttpBench.Inventory inventory = ReplayHttpBench.fixtureInventory(fixture, 0);
        if (!declared.equals(inventory.count() + ":" + inventory.digest())) {
            throw new IllegalStateException("fixture changed since predeclared plan");
        }
        if (inventory.count() == 0) {
            throw new IllegalArgumentException("mixed capacity arm requires a nonempty fixture");
        }
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
                .version(HttpClient.Version.HTTP_1_1).build();
        ScheduledThreadPoolExecutor watchdog = new ScheduledThreadPoolExecutor(1);
        watchdog.setRemoveOnCancelPolicy(true);
        var workers = Executors.newVirtualThreadPerTaskExecutor();
        Semaphore permits = new Semaphore(maxOutstanding);
        AtomicLong outstanding = new AtomicLong();
        AtomicLong peakOutstanding = new AtomicLong();
        List<List<PageSpec>> nativePages = new ArrayList<>(GROUP_COUNT);
        PhaseCounts nativeWarmupCounts = new PhaseCounts();
        PhaseCounts targetWarmupCounts = new PhaseCounts();
        PhaseCounts measuredCounts = new PhaseCounts();
        boolean targetWarmupStarted = false;
        boolean rateWarmupEndEmitted = false;
        boolean measurementStarted = false;
        PhaseOutcome targetWarmup = null;
        PhaseOutcome measured = null;
        try {
            nativePages.addAll(warmupNativeWalks(client, watchdog, endpoint, bucket,
                    groupProtocols, fixture, inventory, pageSize, nativeWarmupCounts));
            int pageCount = nativePages.getFirst().size();
            laneRanges(pageCount); // All 48 capacity lanes must be active.
            if (offeredEach < pageCount || offeredEach % pageCount != 0) {
                throw new IllegalArgumentException("offered tickets must be exact full native cycles");
            }
            for (int group = 0; group < GROUP_COUNT; group++) {
                if (nativePages.get(group).size() != pageCount) {
                    throw new IllegalStateException("three logical groups have different native page counts");
                }
                for (int prior = 0; prior < group; prior++) {
                    if (groupProtocols.get(group).equals(groupProtocols.get(prior))
                            && !pagePlanSha256(nativePages.get(group))
                                    .equals(pagePlanSha256(nativePages.get(prior)))) {
                        throw new IllegalStateException("same-protocol group warmups changed page boundaries");
                    }
                }
            }
            int rateWarmupTickets = Math.multiplyExact(pageCount, rateWarmupCycles);
            if (rateWarmupTickets > 1_000_000) {
                throw new IllegalArgumentException("target-rate warmup exceeds request bound");
            }
            targetWarmupStarted = true;
            System.out.println("{\"event\":\"RATE_WARMUP_START\"}");
            System.out.flush();
            if (System.in.read() < 0) throw new IllegalStateException("rate warmup start ACK missing");
            targetWarmup = runLanePhase(client, watchdog, workers, endpoint, bucket,
                    groupProtocols, nativePages, rate, rateWarmupCycles, pageSize,
                    permits, outstanding, peakOutstanding, targetWarmupCounts);
            if (outstanding.get() != 0 || permits.availablePermits() != maxOutstanding
                    || targetWarmupCounts.unsent.get() != 0) {
                throw new IllegalStateException("target-rate warmup left incomplete lane work");
            }
            double warmupNominalSeconds = rateWarmupTickets / rate;
            if (targetWarmup.elapsedNs() > Math.round(warmupNominalSeconds * 1_100_000_000.0)
                    + 1_000_000_000L) {
                throw new IllegalStateException("target-rate warmup did not sustain its fixed offered schedule");
            }
            System.out.println("{\"event\":\"RATE_WARMUP_END\"}");
            System.out.flush();
            rateWarmupEndEmitted = true;
            if (System.in.read() < 0) throw new IllegalStateException("rate warmup end ACK missing");
            peakOutstanding.set(0);
            measurementStarted = true;
            System.out.println("{\"event\":\"MEASURE_START\"}");
            System.out.flush();
            if (System.in.read() < 0) throw new IllegalStateException("measurement start ACK missing");
            measured = runLanePhase(client, watchdog, workers, endpoint, bucket,
                    groupProtocols, nativePages, rate, offeredEach / pageCount, pageSize,
                    permits, outstanding, peakOutstanding, measuredCounts);
            System.out.println("{\"event\":\"MEASURE_END\"}");
            System.out.flush();
            if (endAck && System.in.read() < 0) throw new IllegalStateException("measurement end ACK missing");
        } catch (Exception error) {
            if (targetWarmupStarted && !rateWarmupEndEmitted && !measurementStarted) {
                System.out.println("{\"event\":\"RATE_WARMUP_END\",\"status\":\"failed\"}");
                System.out.flush();
                try {
                    if (System.in.read() < 0) throw new IllegalStateException("rate warmup failure end ACK missing");
                } catch (Exception ackError) {
                    error.addSuppressed(ackError);
                }
            }
            String phase = measurementStarted ? "measurement"
                    : targetWarmupStarted ? "target_rate_warmup" : "native_warmup";
            PhaseCounts current = measurementStarted ? measuredCounts
                    : targetWarmupStarted ? targetWarmupCounts : nativeWarmupCounts;
            System.out.printf(Locale.ROOT,
                    "{\"status\":\"failed\",\"phase\":\"%s\","
                    + "\"offered_requests\":%d,\"unsent_requests\":%d,"
                    + "\"successful_requests\":%d,\"max_outstanding_limit\":%d,"
                    + "\"native_warmup_scheduled\":%d,\"native_warmup_completed\":%d,"
                    + "\"target_warmup_scheduled\":%d,\"target_warmup_admitted\":%d,"
                    + "\"target_warmup_completed\":%d,\"target_warmup_unsent\":%d,"
                    + "\"measured_scheduled\":%d,\"measured_completed\":%d,"
                    + "\"duration_plan_s\":%.3f,\"failure_reason\":\"%s\"}%n",
                    phase, current.scheduled.get(), current.unsent.get(), current.successful.get(),
                    maxOutstanding, nativeWarmupCounts.scheduled.get(), nativeWarmupCounts.successful.get(),
                    targetWarmupCounts.scheduled.get(), targetWarmupCounts.admitted.get(),
                    targetWarmupCounts.successful.get(), targetWarmupCounts.unsent.get(),
                    measuredCounts.scheduled.get(), measuredCounts.successful.get(),
                    nominalDuration, jsonEscape(error.toString()));
            throw error;
        } finally {
            watchdog.shutdownNow();
            workers.shutdownNow();
            client.shutdownNow();
        }
        if (measured == null || targetWarmup == null || outstanding.get() != 0
                || permits.availablePermits() != maxOutstanding) {
            throw new IllegalStateException("fixed lane phase did not drain");
        }
        long expectedNative = (long) nativePages.getFirst().size() * GROUP_COUNT;
        long expectedRateWarmup = (long) targetWarmup.offeredEach() * GROUP_COUNT;
        long expectedMeasured = (long) offeredEach * GROUP_COUNT;
        if (nativeWarmupCounts.scheduled.get() != expectedNative
                || nativeWarmupCounts.successful.get() != expectedNative
                || targetWarmupCounts.scheduled.get() != expectedRateWarmup
                || targetWarmupCounts.admitted.get() != expectedRateWarmup
                || targetWarmupCounts.successful.get() != expectedRateWarmup
                || targetWarmupCounts.unsent.get() != 0
                || measuredCounts.scheduled.get() != expectedMeasured
                || measuredCounts.admitted.get() != expectedMeasured
                || measuredCounts.successful.get() != expectedMeasured
                || measuredCounts.unsent.get() != 0) {
            throw new IllegalStateException("fixed lane phase request accounting disagrees");
        }
        printResult(inventory, groupProtocols, nativePages, rate, offeredEach, pageSize,
                rateWarmupCycles, maxOutstanding, nativeWarmupCounts, targetWarmupCounts,
                measuredCounts, targetWarmup, measured);
    }

    private static void printResult(ReplayHttpBench.Inventory inventory,
                                    List<String> groupProtocols, List<List<PageSpec>> nativePages,
                                    double rate, int offeredEach, int pageSize, int rateWarmupCycles,
                                    int maxOutstanding, PhaseCounts nativeWarmupCounts,
                                    PhaseCounts targetWarmupCounts, PhaseCounts measuredCounts,
                                    PhaseOutcome targetWarmup, PhaseOutcome measured) throws Exception {
        long elapsed = measured.elapsedNs();
        long totalObjects = 0, totalRequests = 0, totalBytes = 0;
        Histogram latency = new Histogram();
        Histogram wake = new Histogram();
        Histogram dispatch = new Histogram();
        Histogram send = new Histogram();
        Histogram predecessor = new Histogram();
        long maxWake = 0, maxDispatch = 0, maxSend = 0, maxPredecessor = 0;
        int cycles = offeredEach / nativePages.getFirst().size();
        for (int group = 0; group < GROUP_COUNT; group++) {
            Stats stat = measured.groups().get(group);
            if (stat.requests.get() != offeredEach
                    || stat.objects.get() != Math.multiplyExact(inventory.count(), cycles)) {
                throw new IllegalStateException("logical group omitted or duplicated a native page");
            }
            totalObjects += stat.objects.get();
            totalRequests += stat.requests.get();
            totalBytes += stat.bytes.get();
            latency.addAll(stat.latencies);
            wake.addAll(stat.wakeLag);
            dispatch.addAll(stat.dispatchLag);
            send.addAll(stat.sendLag);
            predecessor.addAll(stat.predecessorWait);
            maxWake = Math.max(maxWake, stat.maxWakeLag.get());
            maxDispatch = Math.max(maxDispatch, stat.maxDispatchLag.get());
            maxSend = Math.max(maxSend, stat.maxSendLag.get());
            maxPredecessor = Math.max(maxPredecessor, stat.maxPredecessorWait.get());
        }
        StringBuilder out = new StringBuilder();
        out.append("{\"workload\":\"mixed_open_loop\",\"fixture_count\":").append(inventory.count())
                .append(",\"fixture_digest\":\"").append(inventory.digest())
                .append("\",\"page_size\":").append(pageSize)
                .append(",\"logical_groups\":3,\"lanes_per_group\":16")
                .append(",\"offered_rate_per_group_rps\":").append(rate)
                .append(",\"duration_plan_s\":").append(offeredEach / rate)
                .append(",\"max_outstanding_limit\":").append(maxOutstanding)
                .append(",\"offered_requests_per_group\":").append(offeredEach)
                .append(",\"offered_requests\":").append((long) offeredEach * GROUP_COUNT)
                .append(",\"unsent_requests\":0")
                .append(",\"peak_outstanding_requests\":").append(measured.peakOutstanding())
                .append(",\"scheduler_wakeup_p99_lag_ns\":").append(wake.percentile(.99))
                .append(",\"scheduler_wakeup_max_lag_ns\":").append(maxWake)
                .append(",\"worker_dispatch_p99_lag_ns\":").append(dispatch.percentile(.99))
                .append(",\"worker_dispatch_max_lag_ns\":").append(maxDispatch)
                .append(",\"client_send_p99_lag_ns\":").append(send.percentile(.99))
                .append(",\"client_send_max_lag_ns\":").append(maxSend)
                .append(",\"predecessor_wait_p99_ns\":").append(predecessor.percentile(.99))
                .append(",\"predecessor_wait_max_ns\":").append(maxPredecessor)
                .append(",\"elapsed_ns\":").append(elapsed)
                .append(",\"scheduled_window_ns\":").append(measured.scheduledWindowNs())
                .append(",\"completion_drain_ns\":")
                .append(Math.max(0, elapsed - measured.scheduledWindowNs()))
                .append(",\"objects\":").append(totalObjects)
                .append(",\"emitted_objects\":").append(totalObjects)
                .append(",\"requests\":").append(totalRequests)
                .append(",\"bytes\":").append(totalBytes)
                .append(",\"objects_per_s\":").append(String.format(Locale.ROOT, "%.6f", totalObjects * 1e9 / elapsed))
                .append(",\"requests_per_s\":").append(String.format(Locale.ROOT, "%.6f", totalRequests * 1e9 / elapsed))
                .append(",\"bytes_per_s\":").append(String.format(Locale.ROOT, "%.6f", totalBytes * 1e9 / elapsed))
                .append(",\"p50_ns\":").append(latency.percentile(.50))
                .append(",\"p95_ns\":").append(latency.percentile(.95))
                .append(",\"p99_ns\":").append(latency.percentile(.99))
                .append(",\"latency_histogram_relative_error_max\":0.016")
                .append(",\"metadata_profile\":\"fixture_name_size_time\"")
                .append(",\"warmup_metadata_verified_objects\":")
                .append(inventory.count() * GROUP_COUNT)
                .append(",\"key_digest_basis\":\"fixture_oracle_certified_by_measured_page_validation\"")
                .append(",\"target_rate_warmup_cycles\":").append(rateWarmupCycles)
                .append(",\"target_rate_warmup_seconds\":").append(targetWarmup.offeredEach() / rate)
                .append(",\"target_rate_warmup_offered_each\":").append(targetWarmup.offeredEach())
                .append(",\"target_rate_warmup_elapsed_ns\":").append(targetWarmup.elapsedNs())
                .append(",\"native_warmup_scheduled\":").append(nativeWarmupCounts.scheduled.get())
                .append(",\"native_warmup_completed\":").append(nativeWarmupCounts.successful.get())
                .append(",\"target_warmup_scheduled\":").append(targetWarmupCounts.scheduled.get())
                .append(",\"target_warmup_admitted\":").append(targetWarmupCounts.admitted.get())
                .append(",\"target_warmup_completed\":").append(targetWarmupCounts.successful.get())
                .append(",\"target_warmup_unsent\":").append(targetWarmupCounts.unsent.get())
                .append(",\"measured_scheduled\":").append(measuredCounts.scheduled.get())
                .append(",\"measured_admitted\":").append(measuredCounts.admitted.get())
                .append(",\"measured_completed\":").append(measuredCounts.successful.get())
                .append(",\"warmup_attempted_requests\":")
                .append(nativeWarmupCounts.scheduled.get() + targetWarmupCounts.scheduled.get())
                .append(",\"warmup_successful_requests\":")
                .append(nativeWarmupCounts.successful.get() + targetWarmupCounts.successful.get())
                .append(",\"attempted_requests\":").append(measuredCounts.scheduled.get())
                .append(",\"successful_requests\":").append(totalRequests)
                .append(",\"groups\":[");
        for (int group = 0; group < GROUP_COUNT; group++) {
            if (group > 0) out.append(',');
            Stats stat = measured.groups().get(group);
            out.append("{\"group_id\":").append(group)
                    .append(",\"protocol\":\"").append(groupProtocols.get(group)).append('"')
                    .append(",\"cycles\":").append(cycles)
                    .append(",\"tickets\":").append(stat.requests.get())
                    .append(",\"objects\":").append(stat.objects.get())
                    .append(",\"key_digest\":\"").append(inventory.digest()).append('"')
                    .append(",\"page_plan_sha\":\"").append(pagePlanSha256(nativePages.get(group))).append('"')
                    .append(",\"phase\":").append((double) group / GROUP_COUNT)
                    .append(",\"native_pages\":").append(nativePages.get(group).size())
                    .append(",\"native_warmup_requests\":").append(nativePages.get(group).size())
                    .append(",\"native_warmup_objects\":").append(inventory.count())
                    .append(",\"attempted_requests\":").append(offeredEach)
                    .append(",\"successful_requests\":").append(stat.requests.get())
                    .append(",\"bytes\":").append(stat.bytes.get())
                    .append(",\"delivered_fraction\":1.0")
                    .append(",\"drain_inclusive_rate_rps\":")
                    .append(String.format(Locale.ROOT, "%.6f", stat.requests.get() * 1e9 / elapsed))
                    .append(",\"predecessor_wait_p99_ns\":").append(stat.predecessorWait.percentile(.99))
                    .append(",\"client_send_p99_lag_ns\":").append(stat.sendLag.percentile(.99))
                    .append(",\"p99_ns\":").append(stat.latencies.percentile(.99)).append('}');
        }
        out.append("],\"protocols\":{");
        for (int p = 0; p < ALL.size(); p++) {
            String protocol = ALL.get(p);
            List<Integer> selectedGroups = new ArrayList<>();
            for (int group = 0; group < GROUP_COUNT; group++) {
                if (groupProtocols.get(group).equals(protocol)) selectedGroups.add(group);
            }
            if (selectedGroups.isEmpty()) continue;
            if (out.charAt(out.length() - 1) != '{') out.append(',');
            long requests = 0, objects = 0, bytes = 0;
            Histogram pLatency = new Histogram(), pSend = new Histogram();
            for (int group : selectedGroups) {
                Stats stat = measured.groups().get(group);
                requests += stat.requests.get();
                objects += stat.objects.get();
                bytes += stat.bytes.get();
                pLatency.addAll(stat.latencies);
                pSend.addAll(stat.sendLag);
            }
            List<PageSpec> pages = nativePages.get(selectedGroups.getFirst());
            out.append('"').append(protocol).append("\":{\"native_pages\":").append(pages.size())
                    .append(",\"page_plan_sha256\":\"").append(pagePlanSha256(pages)).append('"')
                    .append(",\"complete_inventory_cycles\":").append(cycles * selectedGroups.size())
                    .append(",\"partial_tail_pages\":0")
                    .append(",\"attempted_requests\":").append(offeredEach * selectedGroups.size())
                    .append(",\"successful_requests\":").append(requests)
                    .append(",\"delivered_fraction\":1.0")
                    .append(",\"objects\":").append(objects)
                    .append(",\"bytes\":").append(bytes)
                    .append(",\"drain_inclusive_rate_rps\":")
                    .append(String.format(Locale.ROOT, "%.6f", requests * 1e9 / elapsed))
                    .append(",\"client_send_p99_lag_ns\":").append(pSend.percentile(.99))
                    .append(",\"p50_ns\":").append(pLatency.percentile(.50))
                    .append(",\"p95_ns\":").append(pLatency.percentile(.95))
                    .append(",\"p99_ns\":").append(pLatency.percentile(.99)).append('}');
        }
        System.out.println(out.append("}}").toString());
    }

    /** Diagnostic preflight: freeze native page counts/signatures before offered rates are declared. */
    private static void inspectPagePlan(URI endpoint, String bucket, String fixture,
                                        String declared) throws Exception {
        ReplayHttpBench.Inventory inventory = ReplayHttpBench.fixtureInventory(fixture, 0);
        if (inventory.count() == 0 || !declared.equals(inventory.count() + ":" + inventory.digest())) {
            throw new IllegalStateException("preflight fixture inventory changed or is empty");
        }
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
                .version(HttpClient.Version.HTTP_1_1).build();
        ScheduledThreadPoolExecutor watchdog = new ScheduledThreadPoolExecutor(1);
        watchdog.setRemoveOnCancelPolicy(true);
        try {
            List<List<PageSpec>> plans = warmupNativeWalks(client, watchdog, endpoint, bucket,
                    ALL, fixture, inventory, 1000, new PhaseCounts());
            StringBuilder json = new StringBuilder("{\"purpose\":\"native_page_plan_preflight\","
                    + "\"fixture_count\":").append(inventory.count())
                    .append(",\"fixture_digest\":\"").append(inventory.digest())
                    .append("\",\"protocols\":{");
            for (int i = 0; i < ALL.size(); i++) {
                if (i > 0) json.append(',');
                json.append('"').append(ALL.get(i)).append("\":{\"native_pages\":")
                        .append(plans.get(i).size()).append(",\"page_plan_sha256\":\"")
                        .append(pagePlanSha256(plans.get(i))).append("\"}");
            }
            System.out.println(json.append("}}").toString());
        } finally {
            watchdog.shutdownNow();
            client.shutdownNow();
        }
    }

    private static List<List<PageSpec>> warmupNativeWalks(HttpClient client,
                                                           ScheduledThreadPoolExecutor watchdog,
                                                           URI endpoint, String bucket,
                                                           List<String> protocols, String fixture,
                                                           ReplayHttpBench.Inventory inventory,
                                                           int pageSize, PhaseCounts counts) throws Exception {
        List<List<ObservedPage>> observed = new ArrayList<>(protocols.size());
        List<java.util.Set<String>> seenTokens = new ArrayList<>(protocols.size());
        String[] nextTokens = new String[protocols.size()];
        byte[][] priorKeys = new byte[protocols.size()][];
        long[] ownedCounts = new long[protocols.size()];
        boolean[] finished = new boolean[protocols.size()];
        for (String ignored : protocols) {
            observed.add(new ArrayList<>());
            seenTokens.add(new java.util.HashSet<>());
        }
        int remaining = protocols.size();
        while (remaining != 0) {
            for (int p = 0; p < protocols.size(); p++) {
                if (finished[p]) continue;
                List<ObservedPage> pages = observed.get(p);
                if (pages.size() >= Math.min(1_000_000L, inventory.count() + 1)) {
                    throw new IllegalStateException("native pagination exceeded inventory-derived bound");
                }
                String token = nextTokens[p];
                counts.scheduled.incrementAndGet();
                counts.admitted.incrementAndGet();
                FetchResult response = fetch(client, watchdog, endpoint, bucket, protocols.get(p),
                        token, priorKeys[p], null, null, pageSize, true);
                counts.successful.incrementAndGet();
                if (response.count() == 0 && response.nextToken() != null) {
                    throw new IllegalStateException("empty nonfinal native page");
                }
                pages.add(new ObservedPage(token, response.count(), response.digest(),
                        response.metadata(), response.lastKey()));
                ownedCounts[p] += response.count();
                if (ownedCounts[p] > inventory.count()) {
                    throw new IllegalStateException("native walk exceeded fixture count");
                }
                priorKeys[p] = response.lastKey() == null ? priorKeys[p] : response.lastKey();
                if (response.nextToken() == null) {
                    finished[p] = true;
                    remaining--;
                } else {
                    if (response.nextToken().length() > 16_384) {
                        throw new IllegalStateException("native continuation token exceeds bound");
                    }
                    if (!seenTokens.get(p).add(response.nextToken())) {
                        throw new IllegalStateException("repeated native token");
                    }
                    nextTokens[p] = response.nextToken();
                }
            }
        }
        List<List<PageSpec>> verified = new ArrayList<>(protocols.size());
        for (int p = 0; p < protocols.size(); p++) {
            if (ownedCounts[p] != inventory.count()) {
                throw new IllegalStateException("native warmup omitted fixture keys for " + protocols.get(p));
            }
            verified.add(verifyNativePages(fixture, protocols.get(p), inventory, observed.get(p)));
        }
        return List.copyOf(verified);
    }

    /** Three fixed groups of sixteen response-ordered lanes for warmup and measurement alike. */
    private static PhaseOutcome runLanePhase(HttpClient client, ScheduledThreadPoolExecutor watchdog,
                                             java.util.concurrent.ExecutorService workers, URI endpoint,
                                             String bucket, List<String> groupProtocols,
                                             List<List<PageSpec>> pagePlans, double rate,
                                             int cycles, int pageSize, Semaphore permits,
                                             AtomicLong outstanding, AtomicLong peakOutstanding,
                                             PhaseCounts counts) throws Exception {
        if (groupProtocols.size() != GROUP_COUNT || pagePlans.size() != GROUP_COUNT || cycles < 1
                || !Double.isFinite(rate) || rate <= 0) {
            throw new IllegalArgumentException("fixed lane phase needs three groups and positive cycles");
        }
        int offeredEach = Math.multiplyExact(pagePlans.getFirst().size(), cycles);
        long scheduledWindow = scheduledWindowNanos(offeredEach, rate);
        List<Stats> groups = groupProtocols.stream().map(ignored -> new Stats()).toList();
        var completion = new java.util.concurrent.ExecutorCompletionService<Void>(workers);
        List<java.util.concurrent.Future<Void>> futures = new ArrayList<>(GROUP_COUNT * LANES_PER_GROUP);
        long start = System.nanoTime();
        long deadline = phaseDeadlineNanos(start, offeredEach, rate, TimeUnit.SECONDS.toNanos(120));
        try {
            for (int group = 0; group < GROUP_COUNT; group++) {
                final int groupId = group;
                final String protocol = groupProtocols.get(group);
                final List<PageSpec> pages = pagePlans.get(group);
                final Stats stat = groups.get(group);
                List<LaneRange> ranges = laneRanges(pages.size());
                for (int lane = 0; lane < LANES_PER_GROUP; lane++) {
                    final int laneId = lane;
                    final LaneRange range = ranges.get(lane);
                    futures.add(completion.submit(() -> {
                        LaneCursor cursor = new LaneCursor(pages, range, cycles);
                        while (cursor.hasNext()) {
                            long scheduled = scheduledNanos(start, groupId, laneId,
                                    cursor.roundWithinLane(), cursor.cycle(), pages.size(), rate);
                            long ready = readyNanos(scheduled, cursor.predecessorCompleted());
                            while (System.nanoTime() < ready) {
                                LockSupport.parkNanos(Math.min(1_000_000L, ready - System.nanoTime()));
                            }
                            long awakened = System.nanoTime();
                            counts.scheduled.incrementAndGet();
                            long predecessorWait = cursor.predecessorCompleted() == Long.MIN_VALUE ? 0
                                    : Math.max(0, cursor.predecessorCompleted() - scheduled);
                            stat.predecessorWait.add(predecessorWait);
                            stat.maxPredecessorWait.accumulateAndGet(predecessorWait, Math::max);
                            long wakeLag = Math.max(0, awakened - ready);
                            stat.wakeLag.add(wakeLag);
                            stat.maxWakeLag.accumulateAndGet(wakeLag, Math::max);
                            if (!permits.tryAcquire()) {
                                counts.unsent.incrementAndGet();
                                throw new IllegalStateException("fixed-lane client outstanding limit reached");
                            }
                            counts.admitted.incrementAndGet();
                            long active = outstanding.incrementAndGet();
                            peakOutstanding.accumulateAndGet(active, Math::max);
                            try {
                                PageSpec page = cursor.expected();
                                long dispatchLag = Math.max(0, System.nanoTime() - ready);
                                stat.dispatchLag.add(dispatchLag);
                                stat.maxDispatchLag.accumulateAndGet(dispatchLag, Math::max);
                                FetchResult response = fetch(client, watchdog, endpoint, bucket, protocol,
                                        cursor.token(), cursor.prior(), page.upper(), page, pageSize, false);
                                long completed = System.nanoTime();
                                cursor.complete(response, completed);
                                long sendLag = Math.max(0, response.sendStartedNanos() - ready);
                                stat.sendLag.add(sendLag);
                                stat.maxSendLag.accumulateAndGet(sendLag, Math::max);
                                stat.record(page.count(), response.bytes(), completed - scheduled);
                                counts.successful.incrementAndGet();
                            } finally {
                                outstanding.decrementAndGet();
                                permits.release();
                            }
                        }
                        if (cursor.completed() != Math.multiplyExact(cycles,
                                range.endExclusive() - range.startInclusive())) {
                            throw new IllegalStateException("lane omitted an assigned native page");
                        }
                        return null;
                    }));
                }
            }
            for (int i = 0; i < futures.size(); i++) {
                long remaining = deadline - System.nanoTime();
                java.util.concurrent.Future<Void> finished = remaining <= 0 ? null
                        : completion.poll(remaining, TimeUnit.NANOSECONDS);
                if (finished == null) throw new IllegalStateException("fixed-lane response drain timeout");
                finished.get();
            }
        } catch (Exception | Error failure) {
            for (java.util.concurrent.Future<Void> future : futures) future.cancel(true);
            throw failure;
        }
        long elapsed = System.nanoTime() - start;
        return new PhaseOutcome(offeredEach, elapsed, scheduledWindow, groups, peakOutstanding.get());
    }


    private static FetchResult fetch(HttpClient client, ScheduledThreadPoolExecutor watchdog, URI endpoint,
                                     String bucket, String protocol, String token, byte[] prior,
                                     byte[] upper, PageSpec expected, int pageSize,
                                     boolean verifyMetadata) throws Exception {
        URI uri = ReplayHttpBench.requestUri(endpoint, protocol, bucket, pageSize, token, null);
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(60)).GET();
        if (protocol.equals("azure")) builder.header("x-ms-version", "2026-06-06");
        HttpRequest request = builder.build();
        long sendStartedNanos = System.nanoTime();
        HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() != 200) {
            String reason = response.headers().firstValue("x-swath-replay-error").orElse("none");
            response.body().close();
            throw new IllegalStateException("HTTP " + response.statusCode()
                    + " x-swath-replay-error=" + reason + " from " + uri);
        }
        CountingInputStream body = new CountingInputStream(response.body());
        AtomicBoolean timedOut = new AtomicBoolean();
        ScheduledFuture<?> deadline = watchdog.schedule(() -> {
            timedOut.set(true);
            try { body.close(); } catch (Exception ignored) { }
        }, BODY_DEADLINE.toNanos(), TimeUnit.NANOSECONDS);
        try (body) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            MetadataVerifier metadata = verifyMetadata ? new MetadataVerifier() : null;
            ReplayHttpBench.Page parsed;
            try {
                parsed = protocol.equals("gcs")
                        ? ReplayHttpBench.parseGcs(body, digest, prior, upper, metadata)
                        : ReplayHttpBench.parseXml(body, protocol, digest, prior, upper, metadata);
                byte[] trailing = new byte[8192];
                int n;
                while ((n = body.read(trailing)) >= 0) {
                    for (int i = 0; i < n; i++) {
                        byte value = trailing[i];
                        if (value != ' ' && value != '\n' && value != '\r' && value != '\t') {
                            throw new IllegalStateException("non-whitespace after response");
                        }
                    }
                }
            } catch (Exception error) {
                if (timedOut.get()) throw new IllegalStateException("body_timeout", error);
                throw error;
            }
            if (timedOut.get()) throw new IllegalStateException("body_timeout");
            String pageDigest = HexFormat.of().formatHex(digest.digest());
            FixtureMetadataOracle.Digest metadataDigest = metadata == null ? null : metadata.finish();
            if (expected != null && (parsed.count() != expected.count()
                    || parsed.owned() != expected.count() || !pageDigest.equals(expected.digest()))) {
                throw new IllegalStateException("page inventory mismatch at " + uri);
            }
            if (expected != null && !Objects.equals(parsed.nextToken(), expected.nextToken())) {
                throw new IllegalStateException("measured continuation token differs from warmup at " + uri);
            }
            if (expected != null && metadata != null && !metadataDigest.equals(expected.metadata())) {
                throw new IllegalStateException("page name/size/time mismatch at " + uri);
            }
            return new FetchResult(parsed.count(), pageDigest, metadataDigest,
                    parsed.lastKey(), parsed.nextToken(), body.bytes, sendStartedNanos);
        } finally {
            deadline.cancel(false);
        }
    }

    /** Server-chosen short page boundaries are checked against an independent fixture scan. */
    private static List<PageSpec> verifyNativePages(String fixture, String protocol,
                                                     ReplayHttpBench.Inventory inventory,
                                                     List<ObservedPage> observed) throws Exception {
        String quoted = fixture.replace("'", "''");
        Properties properties = new Properties();
        properties.setProperty("jdbc_stream_results", "true");
        List<PageSpec> pages = new ArrayList<>();
        MessageDigest all = MessageDigest.getInstance("SHA-256");
        long total = 0;
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:", properties);
             Statement statement = connection.createStatement()) {
            String type;
            try (ResultSet result = statement.executeQuery(
                    "SELECT typeof(key) FROM read_parquet('" + quoted + "') LIMIT 1")) {
                type = result.next() ? result.getString(1) : "BLOB";
            }
            String keyExpression = switch (type.toUpperCase(Locale.ROOT)) {
                case "BLOB" -> "key";
                case "VARCHAR" -> "encode(key)";
                default -> throw new IllegalStateException("unsupported fixture key type " + type);
            };
            byte[] prior = null;
            try (ResultSet rows = statement.executeQuery("SELECT " + keyExpression
                    + ", size, epoch_us(last_modified) FROM read_parquet('" + quoted + "')")) {
                for (ObservedPage response : observed) {
                    if (response.count() < 0 || response.count() > 1000
                            || response.count() == 0 && response != observed.getLast()) {
                        throw new IllegalStateException("native page count outside 1k request limit");
                    }
                    MessageDigest pageDigest = MessageDigest.getInstance("SHA-256");
                    MetadataVerifier pageMetadata = new MetadataVerifier();
                    byte[] first = null;
                    byte[] pagePrior = prior;
                    for (long i = 0; i < response.count(); i++) {
                        if (!rows.next()) throw new IllegalStateException("fixture ended before native walk");
                        byte[] key = rows.getBytes(1);
                        if (key == null || prior != null && Arrays.compareUnsigned(prior, key) >= 0) {
                            throw new IllegalStateException("fixture is not strictly byte sorted");
                        }
                        if (first == null) first = key.clone();
                        long size = rows.getLong(2);
                        long micros = rows.getLong(3);
                        keyDigest(all, key);
                        keyDigest(pageDigest, key);
                        pageMetadata.accept(key, size,
                                FixtureMetadataOracle.wireEpochUnit(protocol, micros));
                        prior = key;
                        total++;
                    }
                    String digest = HexFormat.of().formatHex(pageDigest.digest());
                    FixtureMetadataOracle.Digest metadata = pageMetadata.finish();
                    if (!digest.equals(response.digest()) || !metadata.equals(response.metadata())
                            || !(response.count() == 0 ? response.lastKey() == null
                                    : Arrays.equals(prior, response.lastKey()))) {
                        throw new IllegalStateException("native page differs from independent fixture scope");
                    }
                    pages.add(new PageSpec(response.token(), null, first, pagePrior, null,
                            Math.toIntExact(response.count()), digest, metadata));
                }
                if (rows.next()) throw new IllegalStateException("native walk omitted fixture tail");
            }
        }
        if (total != inventory.count() || !HexFormat.of().formatHex(all.digest()).equals(inventory.digest())) {
            throw new IllegalStateException("fixture changed while verifying native token walk");
        }
        List<PageSpec> bounded = new ArrayList<>(pages.size());
        for (int i = 0; i < pages.size(); i++) {
            PageSpec page = pages.get(i);
            byte[] upper = i + 1 < pages.size() ? pages.get(i + 1).first() : null;
            String nextToken = i + 1 < pages.size() ? pages.get(i + 1).token() : null;
            bounded.add(new PageSpec(page.token(), nextToken, page.first(), page.predecessor(), upper,
                    page.count(), page.digest(), page.metadata()));
        }
        return List.copyOf(bounded);
    }

    private static void keyDigest(MessageDigest digest, byte[] key) {
        digest.update((byte) (key.length >>> 24));
        digest.update((byte) (key.length >>> 16));
        digest.update((byte) (key.length >>> 8));
        digest.update((byte) key.length);
        digest.update(key);
    }

    private static String pagePlanSha256(List<PageSpec> pages) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (PageSpec page : pages) {
            int count = page.count();
            digest.update((byte) (count >>> 24));
            digest.update((byte) (count >>> 16));
            digest.update((byte) (count >>> 8));
            digest.update((byte) count);
            digest.update(HexFormat.of().parseHex(page.digest()));
            digest.update(HexFormat.of().parseHex(page.metadata().sha256()));
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static String jsonEscape(String text) {
        StringBuilder out = new StringBuilder(Math.min(text.length(), 512));
        for (int i = 0; i < text.length() && out.length() < 512; i++) {
            char ch = text.charAt(i);
            if (ch == '"' || ch == '\\') out.append('\\');
            if (ch < 0x20) out.append(' ');
            else out.append(ch);
        }
        return out.toString();
    }

    static List<String> logicalGroupProtocols(List<String> selected) {
        if (selected.equals(ALL)) return ALL;
        if (selected.size() == 1 && ALL.contains(selected.getFirst())) {
            return List.of(selected.getFirst(), selected.getFirst(), selected.getFirst());
        }
        throw new IllegalArgumentException("mixed driver requires all protocols or one isolated protocol");
    }

    static List<LaneRange> laneRanges(int pageCount) {
        if (pageCount < LANES_PER_GROUP) {
            throw new IllegalArgumentException("fixed 16-lane arm needs at least 16 native pages");
        }
        List<LaneRange> ranges = new ArrayList<>(LANES_PER_GROUP);
        int perLane = pageCount / LANES_PER_GROUP;
        int longer = pageCount % LANES_PER_GROUP;
        for (int lane = 0; lane < LANES_PER_GROUP; lane++) {
            int start = lane * perLane + Math.min(lane, longer);
            int end = start + perLane + (lane < longer ? 1 : 0);
            ranges.add(new LaneRange(start, end));
        }
        return List.copyOf(ranges);
    }

    static long scheduledNanos(long start, int groupId, int laneId, int roundWithinLane,
                               int cycle, int pageCount, double rate) {
        if (groupId < 0 || groupId >= GROUP_COUNT || laneId < 0 || laneId >= LANES_PER_GROUP
                || roundWithinLane < 0 || cycle < 0 || pageCount < LANES_PER_GROUP
                || !Double.isFinite(rate) || rate <= 0) {
            throw new IllegalArgumentException("invalid fixed-lane schedule input");
        }
        long slotIndex = Math.addExact(Math.multiplyExact((long) cycle, pageCount),
                Math.addExact((long) roundWithinLane * LANES_PER_GROUP, laneId));
        double slot = slotIndex + (double) groupId / GROUP_COUNT;
        return Math.addExact(start, Math.round(slot * 1_000_000_000.0 / rate));
    }

    static long readyNanos(long scheduled, long predecessorCompleted) {
        return Math.max(scheduled, predecessorCompleted);
    }

    static long phaseDeadlineNanos(long start, int offeredEach, double rate, long drainAllowanceNanos) {
        if (drainAllowanceNanos < 0) throw new IllegalArgumentException("negative phase drain allowance");
        try {
            return Math.addExact(start, Math.addExact(scheduledWindowNanos(offeredEach, rate),
                    drainAllowanceNanos));
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("phase deadline overflow", overflow);
        }
    }

    private static long scheduledWindowNanos(int offeredEach, double rate) {
        if (offeredEach < 1 || !Double.isFinite(rate) || rate <= 0) {
            throw new IllegalArgumentException("invalid offered window");
        }
        double nanos = offeredEach * 1_000_000_000.0 / rate;
        if (!Double.isFinite(nanos) || nanos > Long.MAX_VALUE) {
            throw new IllegalArgumentException("offered window exceeds deadline range");
        }
        return Math.round(nanos);
    }

    record LaneRange(int startInclusive, int endExclusive) { }

    static final class LaneCursor {
        private final List<PageSpec> pages;
        private final LaneRange range;
        private final int cycles;
        private int cycle;
        private int index;
        private int completed;
        private long objects;
        private long predecessorCompleted = Long.MIN_VALUE;
        private String token;
        private byte[] prior;

        LaneCursor(List<PageSpec> pages, LaneRange range, int cycles) {
            if (cycles < 1 || range.startInclusive() < 0
                    || range.startInclusive() >= range.endExclusive()
                    || range.endExclusive() > pages.size()) {
                throw new IllegalArgumentException("invalid lane range/cycles");
            }
            this.pages = pages;
            this.range = range;
            this.cycles = cycles;
            restart();
        }

        boolean hasNext() { return cycle < cycles; }
        PageSpec expected() {
            if (!hasNext()) throw new IllegalStateException("lane completed all cycles");
            return pages.get(index);
        }
        String token() { return token; }
        byte[] prior() { return prior; }
        int completed() { return completed; }
        long objects() { return objects; }
        int cycle() { return cycle; }
        int roundWithinLane() { return index - range.startInclusive(); }
        long predecessorCompleted() { return predecessorCompleted; }

        void complete(FetchResult result, long completionNanos) {
            PageSpec expected = expected();
            if (result.count() != expected.count() || !result.digest().equals(expected.digest())
                    || !Objects.equals(result.nextToken(), expected.nextToken())) {
                throw new IllegalStateException("lane response differs from verified native page");
            }
            completed++;
            objects += result.count();
            predecessorCompleted = completionNanos;
            if (++index < range.endExclusive()) {
                token = result.nextToken();
                prior = result.lastKey();
            } else if (++cycle < cycles) {
                restart();
            }
        }

        private void restart() {
            index = range.startInclusive();
            PageSpec bootstrap = pages.get(index);
            token = bootstrap.token();
            prior = bootstrap.predecessor();
        }
    }

    record PageSpec(String token, String nextToken, byte[] first, byte[] predecessor, byte[] upper,
                    int count, String digest, FixtureMetadataOracle.Digest metadata) { }
    private record ObservedPage(String token, long count, String digest,
                                FixtureMetadataOracle.Digest metadata, byte[] lastKey) { }
    record FetchResult(long count, String digest, FixtureMetadataOracle.Digest metadata,
                       byte[] lastKey, String nextToken, long bytes, long sendStartedNanos) { }
    private record PhaseOutcome(int offeredEach, long elapsedNs, long scheduledWindowNs,
                                List<Stats> groups, long peakOutstanding) { }

    private static final class PhaseCounts {
        final AtomicLong scheduled = new AtomicLong();
        final AtomicLong admitted = new AtomicLong();
        final AtomicLong successful = new AtomicLong();
        final AtomicLong unsent = new AtomicLong();
    }

    private static final class CountingInputStream extends FilterInputStream {
        long bytes;
        CountingInputStream(InputStream in) { super(in); }
        @Override public int read() throws java.io.IOException {
            int value = super.read();
            if (value >= 0 && ++bytes > MAX_BODY_BYTES) {
                throw new java.io.IOException("native response exceeds byte bound");
            }
            return value;
        }
        @Override public int read(byte[] target, int offset, int length) throws java.io.IOException {
            int count = in.read(target, offset, length);
            if (count > 0 && (bytes += count) > MAX_BODY_BYTES) {
                throw new java.io.IOException("native response exceeds byte bound");
            }
            return count;
        }
    }

    private static final class Stats {
        final AtomicLong requests = new AtomicLong();
        final AtomicLong objects = new AtomicLong();
        final AtomicLong bytes = new AtomicLong();
        final Histogram latencies = new Histogram();
        final Histogram wakeLag = new Histogram();
        final Histogram dispatchLag = new Histogram();
        final Histogram sendLag = new Histogram();
        final Histogram predecessorWait = new Histogram();
        final AtomicLong maxWakeLag = new AtomicLong();
        final AtomicLong maxDispatchLag = new AtomicLong();
        final AtomicLong maxSendLag = new AtomicLong();
        final AtomicLong maxPredecessorWait = new AtomicLong();
        void record(long count, long byteCount, long latency) {
            requests.incrementAndGet();
            objects.addAndGet(count);
            bytes.addAndGet(byteCount);
            latencies.add(latency);
        }
    }

    private static final class Histogram {
        private final AtomicLongArray bins = new AtomicLongArray(4096);
        void addAll(Histogram other) {
            for (int i = 0; i < bins.length(); i++) bins.addAndGet(i, other.bins.get(i));
        }
        void add(long value) {
            int exponent = Math.max(0, 63 - Long.numberOfLeadingZeros(Math.max(1, value)));
            long floor = 1L << exponent;
            int sub = (int) Math.min(63, ((Math.max(1, value) - floor) * 64) / floor);
            bins.incrementAndGet(Math.min(4095, exponent * 64 + sub));
        }
        long percentile(double fraction) {
            long count = 0;
            for (int i = 0; i < bins.length(); i++) count += bins.get(i);
            if (count == 0) throw new IllegalStateException("no latency samples");
            long target = (long) Math.ceil(count * fraction);
            long seen = 0;
            for (int i = 0; i < bins.length(); i++) {
                seen += bins.get(i);
                if (seen >= target) {
                    long floor = 1L << (i / 64);
                    return floor + floor * (i % 64 + 1) / 64;
                }
            }
            throw new IllegalStateException("histogram is empty");
        }
    }
}
