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
                    + "FIXTURE_GLOB COUNT:DIGEST s3|gcs|azure|s3,gcs,azure RATE_EACH_RPS "
                    + "OFFERED_EACH_COUNT PAGE_SIZE WARMUP_FULL_CYCLES RATE_WARMUP_CYCLES "
                    + "MAX_OUTSTANDING bracket [end_ack]");
        }
        URI endpoint = URI.create(argv[0]);
        String bucket = argv[1];
        String fixture = argv[2];
        String declared = argv[3];
        List<String> protocols = argv[4].equals("s3,gcs,azure") ? ALL : List.of(argv[4]);
        if (protocols.stream().anyMatch(p -> !ALL.contains(p))) throw new IllegalArgumentException("protocol");
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
        List<Stats> stats = protocols.stream().map(Stats::new).toList();
        Semaphore permits = new Semaphore(maxOutstanding);
        AtomicLong outstanding = new AtomicLong();
        AtomicLong peakOutstanding = new AtomicLong();
        Histogram scheduleLag = new Histogram();
        long maxScheduleLag = 0;
        Histogram workerDispatchLag = new Histogram();
        AtomicLong maxWorkerDispatchLag = new AtomicLong();
        Histogram clientSendLag = new Histogram();
        AtomicLong maxClientSendLag = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<List<PageSpec>> nativePages = new ArrayList<>(protocols.size());
        long start = 0;
        long end = 0;
        int unsent = 0;
        WarmupOutcome rateWarmup = null;
        try {
            nativePages.addAll(warmupNativeWalks(client, watchdog, endpoint, bucket,
                    protocols, fixture, inventory, pageSize));
            for (List<PageSpec> pages : nativePages) {
                if (offeredEach < pages.size()) {
                    throw new IllegalArgumentException("fixed offered stream must cover every native page");
                }
                if (pages.size() != nativePages.getFirst().size()) {
                    throw new IllegalArgumentException("equal-rate mixed arm needs one common native page count");
                }
            }
            int rateWarmupTickets = Math.multiplyExact(nativePages.getFirst().size(), rateWarmupCycles);
            if (rateWarmupTickets > 1_000_000) {
                throw new IllegalArgumentException("target-rate warmup exceeds request bound");
            }
            rateWarmup = runRateWarmup(client, watchdog, workers, endpoint, bucket,
                    protocols, nativePages, rate, rateWarmupTickets, pageSize, permits, outstanding);
            if (outstanding.get() != 0 || permits.availablePermits() != maxOutstanding) {
                throw new IllegalStateException("target-rate warmup left outstanding requests");
            }
            peakOutstanding.set(0);
            System.out.println("{\"event\":\"MEASURE_START\"}");
            System.out.flush();
            if (System.in.read() < 0) throw new IllegalStateException("measurement start ACK missing");
            start = System.nanoTime();
            long period = Math.round(1_000_000_000.0 / rate);
            CountDownLatch done = new CountDownLatch(Math.multiplyExact(offeredEach, protocols.size()));
            for (int i = 0; i < offeredEach; i++) {
                for (int p = 0; p < protocols.size(); p++) {
                    String protocol = protocols.get(p);
                    long scheduled = start + Math.round((i + timePhase(protocol)) * period);
                    while (System.nanoTime() < scheduled) {
                        LockSupport.parkNanos(Math.min(1_000_000L, scheduled - System.nanoTime()));
                    }
                    long lag = Math.max(0, System.nanoTime() - scheduled);
                    scheduleLag.add(lag);
                    maxScheduleLag = Math.max(maxScheduleLag, lag);
                    Stats stat = stats.get(p);
                    List<PageSpec> pages = nativePages.get(p);
                    int phaseOffset = phaseOffset(stat.protocol, pages.size());
                    PageSpec page = pages.get((i + phaseOffset) % pages.size());
                    if (!permits.tryAcquire()) {
                        unsent++;
                        done.countDown();
                        continue;
                    }
                    long active = outstanding.incrementAndGet();
                    peakOutstanding.accumulateAndGet(active, Math::max);
                    try {
                        workers.submit(() -> {
                            try {
                                long dispatchLag = Math.max(0, System.nanoTime() - scheduled);
                                workerDispatchLag.add(dispatchLag);
                                maxWorkerDispatchLag.accumulateAndGet(dispatchLag, Math::max);
                                stat.dispatchLag.add(dispatchLag);
                                FetchResult response = fetch(client, watchdog, endpoint, bucket, stat.protocol,
                                        page.token(), page.predecessor(), page.upper(), page, pageSize, false);
                                long sendLag = Math.max(0, response.sendStartedNanos() - scheduled);
                                clientSendLag.add(sendLag);
                                maxClientSendLag.accumulateAndGet(sendLag, Math::max);
                                stat.sendLag.add(sendLag);
                                stat.record(page.count(), response.bytes(), System.nanoTime() - scheduled);
                            } catch (Throwable error) {
                                failure.compareAndSet(null, error);
                            } finally {
                                outstanding.decrementAndGet();
                                permits.release();
                                done.countDown();
                            }
                        });
                    } catch (RuntimeException submitError) {
                        failure.compareAndSet(null, submitError);
                        outstanding.decrementAndGet();
                        permits.release();
                        done.countDown();
                    }
                }
            }
            if (!done.await(120, TimeUnit.SECONDS)) throw new IllegalStateException("open-loop response drain timeout");
            end = System.nanoTime();
            System.out.println("{\"event\":\"MEASURE_END\"}");
            System.out.flush();
            if (endAck && System.in.read() < 0) throw new IllegalStateException("measurement end ACK missing");
        } catch (Exception error) {
            long successful = 0;
            for (Stats stat : stats) successful += stat.requests.get();
            System.out.printf(Locale.ROOT,
                    "{\"status\":\"failed\",\"phase\":\"%s\","
                    + "\"offered_requests\":%d,\"unsent_requests\":%d,"
                    + "\"successful_requests\":%d,\"max_outstanding_limit\":%d,"
                    + "\"duration_plan_s\":%.3f,\"failure_reason\":\"%s\"}%n",
                    start == 0 ? "warmup" : "measurement", offeredEach * protocols.size(),
                    unsent, successful, maxOutstanding, nominalDuration, jsonEscape(error.toString()));
            throw error;
        } finally {
            watchdog.shutdownNow();
            workers.shutdownNow();
            client.shutdownNow();
        }
        if (failure.get() != null || unsent != 0 || outstanding.get() != 0) {
            long successful = 0;
            for (Stats stat : stats) successful += stat.requests.get();
            System.out.printf(Locale.ROOT,
                    "{\"status\":\"failed\",\"offered_requests\":%d,"
                    + "\"unsent_requests\":%d,\"successful_requests\":%d,"
                    + "\"max_outstanding_limit\":%d,\"duration_plan_s\":%.3f,"
                    + "\"failure_reason\":\"%s\"}%n",
                    offeredEach * protocols.size(), unsent, successful, maxOutstanding, nominalDuration,
                    failure.get() != null ? jsonEscape(failure.get().toString()) : "client_limit");
            if (failure.get() != null) throw new IllegalStateException("open-loop request failed", failure.get());
            throw new IllegalStateException("open-loop client limit reached");
        }
        long elapsed = end - start;
        long scheduledWindow = Math.round(offeredEach * (1_000_000_000.0 / rate));
        long totalObjects = 0, totalRequests = 0, totalBytes = 0, warmupRequests = 0;
        Histogram aggregate = new Histogram();
        for (int i = 0; i < stats.size(); i++) {
            Stats stat = stats.get(i);
            totalObjects += stat.objects.get();
            totalRequests += stat.requests.get();
            totalBytes += stat.bytes.get();
            warmupRequests += nativePages.get(i).size();
            aggregate.addAll(stat.latencies);
        }
        StringBuilder out = new StringBuilder();
        out.append("{\"workload\":\"mixed_open_loop\",\"fixture_count\":")
                .append(inventory.count()).append(",\"fixture_digest\":\"")
                .append(inventory.digest()).append("\",\"page_size\":").append(pageSize)
                .append(",\"offered_rate_each_rps\":").append(rate)
                .append(",\"duration_plan_s\":").append(nominalDuration)
                .append(",\"max_outstanding_limit\":").append(maxOutstanding)
                .append(",\"offered_requests_each\":").append(offeredEach)
                .append(",\"unsent_requests\":").append(unsent)
                .append(",\"peak_outstanding_requests\":").append(peakOutstanding.get())
                .append(",\"scheduler_wakeup_p99_lag_ns\":").append(scheduleLag.percentile(.99))
                .append(",\"scheduler_wakeup_max_lag_ns\":").append(maxScheduleLag)
                .append(",\"worker_dispatch_p99_lag_ns\":").append(workerDispatchLag.percentile(.99))
                .append(",\"worker_dispatch_max_lag_ns\":").append(maxWorkerDispatchLag.get())
                .append(",\"client_send_p99_lag_ns\":").append(clientSendLag.percentile(.99))
                .append(",\"client_send_max_lag_ns\":").append(maxClientSendLag.get())
                .append(",\"elapsed_ns\":").append(elapsed)
                .append(",\"scheduled_window_ns\":").append(scheduledWindow)
                .append(",\"completion_drain_ns\":").append(Math.max(0, elapsed - scheduledWindow))
                .append(",\"objects\":").append(totalObjects)
                .append(",\"emitted_objects\":").append(totalObjects)
                .append(",\"requests\":").append(totalRequests)
                .append(",\"bytes\":").append(totalBytes)
                .append(",\"objects_per_s\":")
                .append(String.format(Locale.ROOT, "%.6f", totalObjects * 1e9 / elapsed))
                .append(",\"requests_per_s\":")
                .append(String.format(Locale.ROOT, "%.6f", totalRequests * 1e9 / elapsed))
                .append(",\"bytes_per_s\":")
                .append(String.format(Locale.ROOT, "%.6f", totalBytes * 1e9 / elapsed))
                .append(",\"p50_ns\":").append(aggregate.percentile(.50))
                .append(",\"p95_ns\":").append(aggregate.percentile(.95))
                .append(",\"p99_ns\":").append(aggregate.percentile(.99))
                .append(",\"latency_histogram_relative_error_max\":0.016")
                .append(",\"metadata_profile\":\"fixture_name_size_time\"")
                .append(",\"warmup_metadata_verified_objects\":")
                .append(inventory.count() * protocols.size())
                .append(",\"target_rate_warmup_cycles\":").append(rateWarmupCycles)
                .append(",\"target_rate_warmup_seconds\":")
                .append(rateWarmup.offeredEach() / rate)
                .append(",\"target_rate_warmup_offered_each\":").append(rateWarmup.offeredEach())
                .append(",\"target_rate_warmup_elapsed_ns\":").append(rateWarmup.elapsedNs())
                .append(",\"warmup_attempted_requests\":")
                .append(warmupRequests + (long) rateWarmup.offeredEach() * protocols.size())
                .append(",\"warmup_successful_requests\":")
                .append(warmupRequests + (long) rateWarmup.offeredEach() * protocols.size())
                .append(",\"attempted_requests\":").append(offeredEach * protocols.size())
                .append(",\"successful_requests\":").append(totalRequests)
                .append(",\"protocols\":{");
        for (int i = 0; i < stats.size(); i++) {
            Stats stat = stats.get(i);
            List<PageSpec> pages = nativePages.get(i);
            long expectedObjectsEach = 0;
            for (int request = 0; request < offeredEach; request++) {
                expectedObjectsEach += pages.get((request + phaseOffset(stat.protocol, pages.size()))
                        % pages.size()).count();
            }
            if (stat.requests.get() != offeredEach || stat.objects.get() != expectedObjectsEach) {
                throw new IllegalStateException("open-loop measured inventory mismatch for " + stat.protocol);
            }
            if (i > 0) out.append(',');
            out.append('"').append(stat.protocol).append("\":{")
                    .append("\"native_pages\":").append(pages.size())
                    .append(",\"phase_offset_pages\":").append(phaseOffset(stat.protocol, pages.size()))
                    .append(",\"time_phase_fraction\":")
                    .append(String.format(Locale.ROOT, "%.6f", timePhase(stat.protocol)))
                    .append(",\"page_plan_sha256\":\"").append(pagePlanSha256(pages)).append('"')
                    .append(",\"complete_inventory_cycles\":").append(offeredEach / pages.size())
                    .append(",\"partial_tail_pages\":").append(offeredEach % pages.size())
                    .append(",\"attempted_requests\":").append(offeredEach)
                    .append(",\"successful_requests\":").append(stat.requests.get())
                    .append(",\"delivered_fraction\":")
                    .append(String.format(Locale.ROOT, "%.6f",
                            (double) stat.requests.get() / offeredEach))
                    .append(",\"objects\":").append(stat.objects.get())
                    .append(",\"bytes\":").append(stat.bytes.get())
                    .append(",\"drain_inclusive_rate_rps\":")
                    .append(String.format(Locale.ROOT, "%.6f", stat.requests.get() * 1e9 / elapsed))
                    .append(",\"worker_dispatch_p99_lag_ns\":")
                    .append(stat.dispatchLag.percentile(.99))
                    .append(",\"client_send_p99_lag_ns\":").append(stat.sendLag.percentile(.99))
                    .append(",\"p50_ns\":").append(stat.latencies.percentile(.50))
                    .append(",\"p95_ns\":").append(stat.latencies.percentile(.95))
                    .append(",\"p99_ns\":").append(stat.latencies.percentile(.99)).append('}');
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
                    ALL, fixture, inventory, 1000);
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
                                                           int pageSize) throws Exception {
        List<List<ObservedPage>> observed = new ArrayList<>(protocols.size());
        List<java.util.Set<String>> seenTokens = new ArrayList<>(protocols.size());
        String[] nextTokens = new String[protocols.size()];
        byte[][] priorKeys = new byte[protocols.size()][];
        long[] counts = new long[protocols.size()];
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
                FetchResult response = fetch(client, watchdog, endpoint, bucket, protocols.get(p),
                        token, priorKeys[p], null, null, pageSize, true);
                if (response.count() == 0 && response.nextToken() != null) {
                    throw new IllegalStateException("empty nonfinal native page");
                }
                pages.add(new ObservedPage(token, response.count(), response.digest(),
                        response.metadata(), response.lastKey()));
                counts[p] += response.count();
                if (counts[p] > inventory.count()) {
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
            if (counts[p] != inventory.count()) {
                throw new IllegalStateException("native warmup omitted fixture keys for " + protocols.get(p));
            }
            verified.add(verifyNativePages(fixture, protocols.get(p), inventory, observed.get(p)));
        }
        return List.copyOf(verified);
    }

    /** Fixed offered-rate connection/JIT warmup; every response still validates its native page. */
    private static WarmupOutcome runRateWarmup(HttpClient client, ScheduledThreadPoolExecutor watchdog,
                                      java.util.concurrent.ExecutorService workers, URI endpoint,
                                      String bucket, List<String> protocols,
                                      List<List<PageSpec>> pagesByProtocol, double rate,
                                      int offeredEach, int pageSize, Semaphore permits,
                                      AtomicLong outstanding) throws Exception {
        if (offeredEach < 1) throw new IllegalArgumentException("target-rate warmup offered no requests");
        CountDownLatch done = new CountDownLatch(Math.multiplyExact(offeredEach, protocols.size()));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        int unsent = 0;
        long start = System.nanoTime();
        long period = Math.round(1_000_000_000.0 / rate);
        for (int i = 0; i < offeredEach; i++) {
            for (int p = 0; p < protocols.size(); p++) {
                String protocol = protocols.get(p);
                long scheduled = start + Math.round((i + timePhase(protocol)) * period);
                while (System.nanoTime() < scheduled) {
                    LockSupport.parkNanos(Math.min(1_000_000L, scheduled - System.nanoTime()));
                }
                List<PageSpec> pages = pagesByProtocol.get(p);
                PageSpec page = pages.get((i + phaseOffset(protocol, pages.size())) % pages.size());
                if (!permits.tryAcquire()) {
                    unsent++;
                    done.countDown();
                    continue;
                }
                outstanding.incrementAndGet();
                try {
                    workers.submit(() -> {
                        try {
                            fetch(client, watchdog, endpoint, bucket, protocol, page.token(),
                                    page.predecessor(), page.upper(), page, pageSize, false);
                        } catch (Throwable error) {
                            failure.compareAndSet(null, error);
                        } finally {
                            outstanding.decrementAndGet();
                            permits.release();
                            done.countDown();
                        }
                    });
                } catch (RuntimeException submitError) {
                    failure.compareAndSet(null, submitError);
                    outstanding.decrementAndGet();
                    permits.release();
                    done.countDown();
                }
            }
        }
        if (!done.await(120, TimeUnit.SECONDS)) throw new IllegalStateException("target-rate warmup drain timeout");
        if (failure.get() != null) throw new IllegalStateException("target-rate warmup failed", failure.get());
        if (unsent != 0) throw new IllegalStateException("target-rate warmup client limit reached: " + unsent);
        long elapsed = System.nanoTime() - start;
        double nominalSeconds = offeredEach / rate;
        if (elapsed > Math.round(nominalSeconds * 1_100_000_000.0) + 1_000_000_000L) {
            throw new IllegalStateException("target-rate warmup did not sustain its fixed offered schedule");
        }
        return new WarmupOutcome(offeredEach, elapsed);
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

    private static int phaseOffset(String protocol, int pageCount) {
        return ALL.indexOf(protocol) * pageCount / ALL.size();
    }

    private static double timePhase(String protocol) {
        return (double) ALL.indexOf(protocol) / ALL.size();
    }

    private record PageSpec(String token, String nextToken, byte[] first, byte[] predecessor, byte[] upper,
                            int count, String digest, FixtureMetadataOracle.Digest metadata) { }
    private record ObservedPage(String token, long count, String digest,
                                FixtureMetadataOracle.Digest metadata, byte[] lastKey) { }
    private record FetchResult(long count, String digest, FixtureMetadataOracle.Digest metadata,
                               byte[] lastKey, String nextToken, long bytes, long sendStartedNanos) { }
    private record WarmupOutcome(int offeredEach, long elapsedNs) { }

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
        final String protocol;
        final AtomicLong requests = new AtomicLong();
        final AtomicLong objects = new AtomicLong();
        final AtomicLong bytes = new AtomicLong();
        final Histogram latencies = new Histogram();
        final Histogram dispatchLag = new Histogram();
        final Histogram sendLag = new Histogram();
        Stats(String protocol) { this.protocol = protocol; }
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
