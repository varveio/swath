/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.ProtocolViolationException;
import io.varve.swath.error.ThrottleType;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.SafeInput;
import io.varve.swath.output.ControlCharEscaper;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.StoreCapabilities;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.EncodingType;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The S3 {@link PageFetcher}: AWS SDK v2 sync, {@code
 * encoding-type=url} ListObjectsV2 pagination purely by {@code start_after =
 * last key} (algorithms.md §2). The AWS SDK's own (always-on) {@code
 * DecodeUrlEncodedResponseInterceptor} already percent-decodes the response
 * key/prefix fields when {@code encoding-type=url} is requested, so {@code
 * o.key()} / {@code cp.prefix()} arrive as the fully-decoded key string; this
 * fetcher converts that decoded string straight to raw bytes via UTF-8 (no
 * second decode — see {@link #toEntry}). One {@code fetchPage} = one
 * ListObjectsV2 call.
 *
 * <p>VERSIONS (ListObjectVersions) is not implemented; only OBJECTS listing is supported here.
 */
public final class S3PageFetcher implements PageFetcher {

    private static final Logger log = LoggerFactory.getLogger(S3PageFetcher.class);

    private final S3Client s3;
    private final String bucket;
    private final String bucketForLog;
    private final boolean fetchOwner;
    private final boolean requestPayer;
    private final RunMetrics metrics;
    private final S3FaultClassifier faultClassifier;
    /**
     * Per-request {@code apiCallAttemptTimeout} override applied to the POINT-probe call
     * class ({@code pivot_probe} — see {@link #usesShortProbeBudget}), unless the request already
     * carries an explicit {@link PageRequest#apiCallAttemptTimeoutOverride()} (the escalation path,
     * which always wins). Worker pages and {@code delimiter=/} structure probes are never touched —
     * both are scan-class calls and keep the client-level {@link S3Config#DEFAULT_ATTEMPT_TIMEOUT}
     * budget with no per-request override. Defaults to {@link S3Config#DEFAULT_PROBE_ATTEMPT_TIMEOUT}
     * for every constructor that does not thread an explicit {@link S3Config}.
     */
    private final Duration probeApiCallAttemptTimeout;
    /**
     * The client-level per-attempt budget the SCAN call classes run under. Never applied as a
     * per-request override (the client already enforces it) — it is the DENOMINATOR that
     * {@link #escalatedAttemptTimeoutFor} uses to re-express the engine's escalation ladder as a
     * multiple, so each call class escalates against its own base.
     */
    private final Duration scanApiCallAttemptTimeout;
    private final AtomicLong apiCalls = new AtomicLong();

    /**
     * A probe (pivot or structure) at/above this elapsed budget on the SUCCESS path is a
     * slow-probe exemplar candidate — 1 s, well above a healthy cold-prefix probe's response time
     * (under 1s from a connection-less client), so a healthy probe never trips this.
     * Any exception path always logs regardless of this threshold (see {@link #maybeLogSlowProbeExemplar}).
     */
    private static final long SLOW_PROBE_THRESHOLD_MS = 1_000L;
    /** Log the first this-many slow-probe exemplars unconditionally before thinning kicks in. */
    private static final long SLOW_PROBE_LOG_FIRST_N = 20L;
    /**
     * Rate-limiter for {@link #maybeLogSlowProbeExemplar} — a probe-timeout storm can hit
     * this 10k-16k times/run, so unconditional logging would itself be a hot-path cost. Instance-scoped
     * (one {@link S3PageFetcher} per run in production).
     */
    private final AtomicLong slowProbeExemplarCount =
            new AtomicLong();

    /** The no-option convenience: an OBJECTS fetcher with {@link S3PageFetcherConfig#DEFAULT} wiring. */
    public S3PageFetcher(S3Client s3, String bucket) {
        this(s3, bucket, S3PageFetcherConfig.DEFAULT);
    }

    /**
     * Canonical constructor: the required {@code s3}/{@code bucket} plus the optional
     * {@link S3PageFetcherConfig} clump (listing flags, probe attempt-timeout, metrics sink). A
     * {@code null} {@link S3PageFetcherConfig#metrics()} installs a fresh no-op sink so the fetcher
     * never records against a shared registry.
     */
    public S3PageFetcher(S3Client s3, String bucket, S3PageFetcherConfig config) {
        this.s3 = s3;
        this.bucket = bucket;
        this.bucketForLog = SafeInput.logText(bucket);
        this.fetchOwner = config.fetchOwner();
        this.requestPayer = config.requestPayer();
        this.metrics = config.metrics() != null ? config.metrics() : new RunMetrics(new SimpleMeterRegistry());
        this.probeApiCallAttemptTimeout = config.probeApiCallAttemptTimeout();
        this.scanApiCallAttemptTimeout = config.scanApiCallAttemptTimeout();
        this.faultClassifier = new S3FaultClassifier(bucket, this.metrics);
    }

    @Override
    public StoreCapabilities capabilities() {
        return StoreCapabilities.s3();
    }

    /** Total ListObjectsV2 calls issued — drives the cost line and the INT-8 efficiency guard. */
    public long apiCalls() {
        return apiCalls.get();
    }

    /** Test seam: the live rate-limiter counter driving {@link #maybeLogSlowProbeExemplar}. */
    long slowProbeExemplarCountForTest() {
        return slowProbeExemplarCount.get();
    }

    @Override
    public ListPage fetchPage(PageRequest req) throws ListingException, InterruptedException {
        if (req.mode() == ListingMode.VERSIONS) {
            throw new UnsupportedOperationException("VERSIONS listing is not yet supported");
        }
        apiCalls.incrementAndGet();

        ListObjectsV2Request.Builder b = ListObjectsV2Request.builder()
                .bucket(bucket)
                .maxKeys(req.maxKeys())
                .encodingType(EncodingType.URL);

        if (fetchOwner) {
            b.fetchOwner(true);   // §4: populate owner_id from the Owner field
        }
        if (requestPayer) {
            b.requestPayer(RequestPayer.REQUESTER);   // requester-pays buckets
        }
        if (req.prefix() != null && req.prefix().length > 0) {
            b.prefix(toRequestParam(req.prefix()));
        }
        if (req.startAfter() != null) {
            b.startAfter(toRequestParam(req.startAfter()));
        }
        if (req.delimiter() != null && req.delimiter().length > 0) {
            b.delimiter(toRequestParam(req.delimiter()));
        }
        // Classify the call class up front (worker page fetch / thief 1-key pivot probe / thief
        // delimiter=/ structure probe) purely from the request shape -- the only signal S3PageFetcher
        // has, store-layer, never engine-aware (see #callClass's javadoc). Computed before the
        // attempt-timeout override below, which needs it to pick the right budget.
        String callClass = callClass(req);
        if (req.apiCallAttemptTimeoutOverride() != null) {
            // The caller's escalated per-attempt override (see PageRequest#apiCallAttemptTimeoutOverride)
            // always wins over the probe default immediately below -- but is first re-expressed against
            // THIS call class's own base budget (see #escalatedAttemptTimeoutFor).
            Duration override = escalatedAttemptTimeoutFor(req.apiCallAttemptTimeoutOverride(), callClass);
            b.overrideConfiguration(o -> o.apiCallAttemptTimeout(override));
        } else if (usesShortProbeBudget(callClass)) {
            // A POINT probe (pivot) gets its own short per-attempt budget instead of the client-level
            // scan budget -- see S3Config#DEFAULT_PROBE_ATTEMPT_TIMEOUT for why. A delimiter=/ structure
            // probe deliberately does NOT: it is a scan-class call and keeps the client-level budget
            // (see #usesShortProbeBudget).
            Duration probeOverride = probeApiCallAttemptTimeout;
            b.overrideConfiguration(o -> o.apiCallAttemptTimeout(probeOverride));
        }
        S3CallClassLatencyPublisher.PhaseCapture phaseCapture = S3CallClassLatencyPublisher.begin();
        Timer.Sample sample = metrics.startS3PageTimer();
        long startedNs = System.nanoTime();
        ListObjectsV2Response resp;
        // The exception CLASSIFIER below is an inner try/catch nested inside this outer try/finally
        // -- required so S3CallClassLatencyPublisher.end() fires on every exit path, including a
        // non-SdkException RuntimeException/Error the inner catch never sees (see its javadoc for why
        // that begin()/publish()/end() pairing is race-free).
        try {
        try {
            metrics.recordApiCall();
            resp = s3.listObjectsV2(b.build());
        } catch (SdkException e) {
            // Latency-phase samples are recorded on the FAILURE path too, same as swath.api.latency
            // below (SAME Timer.Sample) -- see metrics-and-observability.md §1 (swath.fetch.latency.phase)
            // for why a success-only record would be survivorship bias. connect_acquire/ttfb are
            // commonly unavailable here and silently skipped (never fabricated) -- see
            // S3CallClassLatencyPublisher; total (this fetcher's own wall-clock) is always available and
            // is the phase most likely to actually reflect the fault (e.g. a ~10s
            // ApiCallAttemptTimeoutException with no TTFB ever reported).
            //
            // This outcome-recording + top-of-classifier interrupt guard ("only OUR cancel counts
            // as an interrupt") is IDENTICAL to the RuntimeException arm's own outcome block below --
            // single-sourced in recordFailureOutcomeAndCheckInterrupt so it is applied ONCE per call
            // site, not copy-pasted per fault family: copy-pasting per family is precisely how the
            // AbortedException arm once shipped WITHOUT the guard, turning an SDK-side abort into a bare
            // InterruptedException that bypassed the crash-only resumable-STUCK contract. When the run IS
            // cancelled our own cancel interrupts the worker thread, so an AbortedException that arrives
            // WITH the interrupt flag set unwinds cooperatively here. If a given SDK/HTTP path ever
            // consumes the interrupt flag instead of propagating it, the abort simply classifies transient
            // (S3FaultClassifier, below) -- still safe: GaugedFetcher's retry re-observes the cancel and
            // the seed catch treats ANY InterruptedException as resumable, so neither outcome poisons
            // --resume. An SDK-side abort/fault (no cancel) does NOT set the flag and is classified by
            // S3FaultClassifier#classify, which dispatches most-specific-first.
            long elapsedNanos = System.nanoTime() - startedNs;
            recordFailureOutcomeAndCheckInterrupt(callClass, req, elapsedNanos, phaseCapture, sample);
            throw faultClassifier.classify(e, faultContext(callClass, req));
        } catch (RuntimeException e) {
            // A NON-SdkException RuntimeException escaping the SDK call (the SdkException arm
            // above already claims every modeled SDK fault family). A socket closure surfacing from a
            // transient S3 500 burst arrives here as `UncheckedIOException(SocketException("Socket
            // closed"))` — a plain RuntimeException that the SdkException classifier above does not
            // see. A socket closure is a client-local network fault (its cause chain holds an
            // IOException) — the SAME class as S3FaultClassifier's network-exhaustion path — so it must
            // be ridden out transiently, not crash the run.
            //
            // Be surgical about the codomain: ONLY reclassify when the cause chain contains an
            // IOException. Any other unexpected RuntimeException is a genuine bug (never a transient
            // network fault) and must keep propagating exactly as today — rethrown here BEFORE any
            // metric/latency side effect, so its disposition is unchanged.
            if (!S3FaultClassifier.hasIOExceptionCause(e)) {
                throw e;
            }
            // From here this is the transient NETWORK path — mirror the SdkException arm above (same
            // failure-path latency record so a wrapper-escape storm is not survivorship-biased out of
            // probe_latency[], same interrupt guard so our own cancel still unwinds cooperatively).
            long elapsedNanos = System.nanoTime() - startedNs;
            recordFailureOutcomeAndCheckInterrupt(callClass, req, elapsedNanos, phaseCapture, sample);
            throw faultClassifier.classifySocketClosure(e, faultContext(callClass, req));
        }
        } finally {
            // Fires exactly once regardless of how the inner try/catch above exits.
            S3CallClassLatencyPublisher.end();
        }
        Duration latency = Duration.ofNanos(System.nanoTime() - startedNs);
        metrics.recordS3Latency(sample);
        // Per-call-class latency-phase decomposition -- connect-acquire/TTFB best-effort from
        // the SDK (may be -1/unavailable on a given attempt, silently skipped by recordCallClassLatency),
        // total always available (this fetcher's own measured wall-clock).
        metrics.recordCallClassLatency(callClass, RunMetrics.LATENCY_PHASE_CONNECT_ACQUIRE,
                phaseCapture.connectAcquireNanos());
        metrics.recordCallClassLatency(callClass, RunMetrics.LATENCY_PHASE_TTFB, phaseCapture.timeToFirstByteNanos());
        metrics.recordCallClassLatency(callClass, RunMetrics.LATENCY_PHASE_TOTAL, latency.toNanos());
        maybeLogSlowProbeExemplar(callClass, req, latency.toNanos(), phaseCapture, false);

        int keyCount = resp.contents().size() + resp.commonPrefixes().size();
        if (keyCount > req.maxKeys()) {
            // S3 bounds a page's KeyCount (objects + rolled-up common prefixes) by the requested
            // MaxKeys, but a hostile or broken --endpoint-url can simply ignore it, and everything
            // below sizes swath's own lists straight off the wire. Refuse the page: keeping a
            // prefix of it would silently drop keys from the listing, and classifying it as
            // transient would spin against an endpoint that answers the retry the same way.
            metrics.recordStealReason("FATAL", "oversized_page");
            log.warn("s3_oversized_page bucket={} max_keys={} keys={} common_prefixes={}",
                    bucketForLog, req.maxKeys(), resp.contents().size(), resp.commonPrefixes().size());
            throw ProtocolViolationException.oversizedPage(bucketForLog, req.maxKeys(),
                    resp.contents().size(), resp.commonPrefixes().size());
        }

        List<ListEntry> entries = new ArrayList<>(resp.contents().size());
        for (S3Object o : resp.contents()) {
            entries.add(toEntry(o));
        }
        List<KeyBytes> commonPrefixes = new ArrayList<>(resp.commonPrefixes().size());
        for (CommonPrefix cp : resp.commonPrefixes()) {
            commonPrefixes.add(KeyBytes.of(cp.prefix().getBytes(StandardCharsets.UTF_8)));
        }

        int httpStatus = resp.sdkHttpResponse() != null ? resp.sdkHttpResponse().statusCode() : 200;
        if (httpStatus == 503) {
            // A 503 that surfaced as a returned page rather than an S3Exception (belt-and-suspenders).
            // Record the throttle event HERE, once, so this classification point stays the single
            // recording site for swath.throttle.events{type}; the gauge (fed this page's status by
            // GaugedFetcher) only casts the AIMD vote and does not record the event.
            metrics.recordS3Throttle();
            metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);
            log.warn("s3_throttle bucket={} status={} s3_code={}", bucketForLog, httpStatus, "SlowDown");
        }
        if (log.isDebugEnabled()) {
            log.debug("s3_page_fetched run_id={} worker_id={} node_id={} bucket={} prefix={} start_after={} keys={} common_prefixes={} truncated={} status={} latency_ms={}",
                    RunContext.runIdOrNone(), RunContext.workerIdOrNone(), RunContext.nodeIdOrNone(),
                    bucketForLog, describe(req.prefix()), describe(req.startAfter()), entries.size(),
                    commonPrefixes.size(), Boolean.TRUE.equals(resp.isTruncated()), httpStatus,
                    latency.toMillis());
        }
        return new ListPage(entries, commonPrefixes,
                Boolean.TRUE.equals(resp.isTruncated()),
                resp.nextContinuationToken(), null, null, httpStatus, latency);
    }

    /**
     * The fetch-outcome latency+interrupt block, shared VERBATIM between the {@code
     * catch (SdkException e)} and {@code catch (RuntimeException e)} arms of {@link #fetchPage}
     * (they were byte-identical before this extraction) — records the same three latency-phase
     * samples + the overall S3-call timer, logs a slow-probe exemplar candidate ({@code
     * forceLog=true}, since any exception path is always a candidate regardless of elapsed time),
     * then re-asserts "only OUR cancel counts as an interrupt": if the thread's interrupt flag is
     * set, throws {@link InterruptedException} BEFORE any fault classification runs, so a genuine
     * cancel can never be mis-classified as a fault. The SUCCESS path at the tail of {@link
     * #fetchPage} has its own, differently-ordered block (no interrupt check, {@code forceLog=false},
     * {@code recordS3Latency} before the phase records) and is deliberately NOT unified with this one.
     */
    private void recordFailureOutcomeAndCheckInterrupt(String callClass, PageRequest req, long elapsedNanos,
            S3CallClassLatencyPublisher.PhaseCapture phaseCapture, Timer.Sample sample) throws InterruptedException {
        metrics.recordCallClassLatency(callClass, RunMetrics.LATENCY_PHASE_CONNECT_ACQUIRE,
                phaseCapture.connectAcquireNanos());
        metrics.recordCallClassLatency(callClass, RunMetrics.LATENCY_PHASE_TTFB,
                phaseCapture.timeToFirstByteNanos());
        metrics.recordCallClassLatency(callClass, RunMetrics.LATENCY_PHASE_TOTAL, elapsedNanos);
        // A probe (pivot or structure) that reached this catch arm -- whatever the eventual
        // classification (throttle/timeout/network/other) -- is a candidate slow-probe exemplar;
        // rate-limited (first N + power-of-two thinning) so a 10k-16k/run probe-timeout storm stays
        // cheap. Worker-page fetches are excluded -- this concerns probe pressure specifically.
        maybeLogSlowProbeExemplar(callClass, req, elapsedNanos, phaseCapture, true);
        metrics.recordS3Latency(sample);
        if (Thread.interrupted()) {
            throw new InterruptedException("interrupted during S3 listObjectsV2");
        }
    }

    /**
     * {@code o.key()} is already the fully percent-decoded key string — the SDK's own {@code
     * DecodeUrlEncodedResponseInterceptor} decoded it while unmarshalling the {@code
     * encoding-type=url} response (see the class javadoc). Converting it to raw bytes is a plain
     * UTF-8 encode, NOT a second percent-decode (decoding twice corrupts any key whose
     * literal bytes contain a {@code %XX} pattern, e.g. {@code site_name=Coffs%20Harbour}).
     */
    private static ObjectEntry toEntry(S3Object o) {
        byte[] key = o.key().getBytes(StandardCharsets.UTF_8);
        long micros = toEpochMicros(o.lastModified());
        String etag = stripEtagQuotes(o.eTag());
        String storageClass = o.storageClassAsString();
        String ownerId = o.owner() != null ? o.owner().id() : null;
        String ownerDisplayName = o.owner() != null ? o.owner().displayName() : null;
        String checksumAlgorithm = (o.checksumAlgorithm() != null && !o.checksumAlgorithm().isEmpty())
                ? o.checksumAlgorithm().getFirst().toString()
                : null;
        String checksumType = (o.checksumTypeAsString() != null && !o.checksumTypeAsString().isBlank())
                ? o.checksumTypeAsString()
                : null;
        return new ObjectEntry(KeyBytes.of(key),
                o.size() != null ? o.size() : 0L,
                micros, etag, storageClass, null, true, ownerId, ownerDisplayName,
                checksumAlgorithm, checksumType);
    }

    /** Strip the surrounding quotes S3 wraps ETags in; keep the multipart {@code hex-N} form verbatim (§4). */
    static String stripEtagQuotes(String etag) {
        if (etag == null) {
            return null;
        }
        if (etag.length() >= 2 && etag.charAt(0) == '"' && etag.charAt(etag.length() - 1) == '"') {
            return etag.substring(1, etag.length() - 1);
        }
        return etag;
    }

    static long toEpochMicros(Instant instant) {
        if (instant == null) {
            return 0L;
        }
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }

    /**
     * Convert raw key bytes to the {@code String} the SDK sends as a request
     * parameter ({@code prefix} / {@code start-after} / {@code delimiter}),
     * <b>byte-exactly</b> (algorithms.md §1.1: "{@code start_after} is sent as
     * the raw decoded key").
     *
     * <p>The AWS SDK URL-encodes a query-parameter {@code String} via its UTF-8
     * bytes ({@code SdkHttpUtils.urlEncode}), so the wire bytes equal {@code raw}
     * <b>iff {@code raw} is valid UTF-8</b> (then {@code new String(raw,
     * UTF_8).getBytes(UTF_8) == raw}). Both kinds of bound we ever pass here are
     * valid UTF-8: every <b>real</b> S3 key is a Unicode string ≤1024 UTF-8 bytes,
     * and every split <b>pivot</b> is chosen UTF-8-safe by construction
     * ({@link io.varve.swath.model.ByteMidpoint},
     * algorithms.md §3.1 — supersedes the earlier non-UTF-8 {@code a ++ [0x80]}
     * boundary, which the SDK could not transmit byte-exact). The invariant is
     * enforced and property-tested at the source (PROP-2: every pivot is valid
     * UTF-8). XML-illegal real cursor bytes are a documented `start-after`
     * capability limitation, not something this byte-to-String conversion can fix.
     */
    static String toRequestParam(byte[] raw) {
        return new String(raw, StandardCharsets.UTF_8);
    }

    /**
     * The faulting request's identity for {@link S3FaultClassifier}'s retryable fault lines — built
     * here, not in the classifier, so {@link #describe}'s control-char escaping stays single-sourced.
     */
    private static S3FaultClassifier.FaultContext faultContext(String callClass, PageRequest req) {
        return new S3FaultClassifier.FaultContext(
                callClass, describe(req.prefix()), describe(req.startAfter()));
    }

    private static String describe(byte[] raw) {
        return raw == null ? "<none>" : ControlCharEscaper.escape(toRequestParam(raw));
    }

    /**
     * Classify this fetch's call class purely from the request shape — the only signal
     * available here (store-layer; deliberately never engine-aware of {@code slotGated}/{@code Thief}).
     * A {@code delimiter=/} request is a structure probe ({@code Thief#structurePivot});
     * a {@code delimiter}-less, {@code max_keys<=1} request is a 1-key pivot probe ({@code
     * Thief#probeNonEmpty}); everything else (the configured page size, no delimiter) is a worker's
     * range page fetch. Holds because a worker's {@code PageRequest.objects(...)} never sets a delimiter and
     * always uses the configured page size (never 1), and every probe-shaped request this fetcher ever
     * sees is exactly one of these two shapes — see {@code docs/internals/metrics-internals.md} §5.
     *
     * <p><b>Not thief-exclusive.</b> {@code structure_probe} also catches the seed
     * step's own {@code delimiter=/} structure probes ({@code SeedStep} issues {@code
     * PageRequest.objectsDelimited(...)} through this SAME fetcher at run start, before any thief
     * exists) — the classifier cannot and does not distinguish "thief's demand-driven probe" from
     * "seed-time probe", only the request SHAPE. Read {@code structure_probe} as "any delimiter=/
     * probe this run issued", not "the thief's probes specifically".
     *
     * <p><b>{@code max_keys<=1} is a shape proxy, not a guarantee.</b> A run configured
     * with {@code --max-keys=1} would make every ordinary worker page fetch ALSO satisfy this
     * classifier's {@code max_keys<=1} check and misclassify as {@code pivot_probe} — the classifier
     * assumes the configured page size is {@code > 1} (true of every documented/sane {@code
     * --max-keys} value; S3's own page-size ceiling is 1000), never validated against the run's
     * actual configured page size.
     */
    static String callClass(PageRequest req) {
        if (req.delimiter() != null && req.delimiter().length > 0) {
            return RunMetrics.CALL_CLASS_STRUCTURE_PROBE;
        }
        if (req.maxKeys() <= 1) {
            return RunMetrics.CALL_CLASS_PIVOT_PROBE;
        }
        return RunMetrics.CALL_CLASS_WORKER_PAGE;
    }

    /**
     * True for the POINT-probe call class only — the set of call classes that get the short
     * {@link #probeApiCallAttemptTimeout} per-request override instead of the client-level
     * {@link S3Config#DEFAULT_ATTEMPT_TIMEOUT} scan budget. See {@link #callClass}.
     *
     * <p><b>Why {@code structure_probe} is deliberately NOT in this set.</b> A {@code max_keys<=1}
     * pivot probe is a point lookup: S3 answers it from the first key at/after the cursor, so it is
     * cheap and near-constant (a genomeark run measured p50 103 ms / p99 300 ms over 3169 calls, with
     * ZERO attempt timeouts under the 3 s budget). A {@code delimiter=/} structure probe is the
     * opposite: S3 must SCAN forward, rolling keys up into {@code CommonPrefixes}, so its cost tracks
     * the keyspace it crosses — the same run measured p50 1.15 s standalone and 5.4 s at the run's own
     * 64-way concurrency. Sharing the point-probe budget put a scan-class call behind a 3 s fuse, and
     * roughly half of all structure-probe attempts (1308 of 2612) tripped it. That storm is what
     * starved the thief of pivots; see {@code docs/internals/probe-budgets.md} §2.
     */
    private static boolean usesShortProbeBudget(String callClass) {
        return RunMetrics.CALL_CLASS_PIVOT_PROBE.equals(callClass);
    }

    /**
     * The per-attempt budget this call class runs under absent any escalation — the
     * denominator/multiplicand of {@link #escalatedAttemptTimeoutFor}.
     */
    private Duration baseAttemptTimeoutFor(String callClass) {
        return usesShortProbeBudget(callClass) ? probeApiCallAttemptTimeout : scanApiCallAttemptTimeout;
    }

    /**
     * Re-express the engine's escalated per-attempt ask against THIS call class's own base budget.
     *
     * <p>{@code GaugedFetcher} escalates a logical fetch's per-attempt budget on consecutive
     * {@code ATTEMPT_TIMEOUT} faults via {@code TransientRetryFetcher.ATTEMPT_TIMEOUT_ESCALATION_LEVELS}
     * — 20 s then 40 s. Those are ABSOLUTE durations authored against the SCAN base of 10 s, i.e. the
     * ladder is really "2x base, then 4x base". The engine cannot know any better: escalation level is
     * all it has, and only this store layer knows what each call class's base budget actually is.
     *
     * <p>Applied unclamped, a 3 s point probe's first escalation is a 6.7x jump straight to 20 s.
     * Combined with {@code PROBE_TRANSIENT_RETRY_CAP=1} (one retry, then fail fast) a failing pivot
     * probe would burn 3 s + 20 s and still return nothing. So the ask is converted to the MULTIPLE it
     * represents over the scan base and re-applied to this class's base — preserving the ladder's
     * progression instead of flattening it to a ceiling:
     *
     * <ul>
     *   <li>scan class (10 s base): 20 s / 40 s — returned untouched, the ladder exactly as authored.
     *   <li>point class (3 s base): 6 s / 12 s.
     * </ul>
     *
     * <p>Deriving the multiple from the two base durations (rather than hardcoding 2x/4x here) keeps
     * the ladder single-sourced in the engine: re-tune it there and this rescale follows. See
     * {@code docs/internals/probe-budgets.md} §3.
     */
    Duration escalatedAttemptTimeoutFor(Duration engineEscalated, String callClass) {
        Duration classBase = baseAttemptTimeoutFor(callClass);
        long scanNanos = scanApiCallAttemptTimeout.toNanos();
        if (classBase.equals(scanApiCallAttemptTimeout) || scanNanos <= 0L) {
            return engineEscalated;   // the ladder's own base: nothing to re-express
        }
        // Ratio in floating point (never nanos*nanos, which overflows a long at these magnitudes).
        double multiple = (double) engineEscalated.toNanos() / (double) scanNanos;
        Duration rescaled = Duration.ofNanos((long) Math.ceil(classBase.toNanos() * multiple));
        // Escalation only ever BUYS room: never hand back less than the class's own base budget.
        return rescaled.compareTo(classBase) < 0 ? classBase : rescaled;
    }

    /**
     * A rate-limited (first {@link #SLOW_PROBE_LOG_FIRST_N} + power-of-two exponential
     * thinning) exemplar log for a slow or failed probe fetch — enough to reproduce one manually with
     * the AWS CLI (bucket/prefix/start-after/elapsed/phase breakdown/attempt-timeout escalation).
     * Worker-page fetches are never logged here -- this concerns probe pressure specifically.
     *
     * @param forceLog {@code true} on any exception path (always a candidate regardless of elapsed);
     *                 {@code false} on the success path (gated by {@link #SLOW_PROBE_THRESHOLD_MS})
     */
    private void maybeLogSlowProbeExemplar(String callClass, PageRequest req, long elapsedNanos,
            S3CallClassLatencyPublisher.PhaseCapture phaseCapture, boolean forceLog) {
        if (RunMetrics.CALL_CLASS_WORKER_PAGE.equals(callClass)) {
            return;
        }
        long elapsedMs = elapsedNanos / 1_000_000L;
        if (!forceLog && elapsedMs < SLOW_PROBE_THRESHOLD_MS) {
            return;
        }
        long n = slowProbeExemplarCount.incrementAndGet();
        if (n > SLOW_PROBE_LOG_FIRST_N && Long.bitCount(n) != 1) {
            return;   // rate-limited: first N unconditionally, then only powers of two
        }
        long overrideMs = req.apiCallAttemptTimeoutOverride() == null
                ? 0L : req.apiCallAttemptTimeoutOverride().toMillis();
        log.warn("slow_probe_exemplar bucket={} call_class={} prefix={} start_after={} elapsed_ms={} "
                        + "connect_acquire_ms={} ttfb_ms={} attempt_timeout_override_ms={} exemplar_n={}",
                bucketForLog, callClass, describe(req.prefix()), describe(req.startAfter()), elapsedMs,
                phaseCapture.connectAcquireNanos() < 0 ? -1 : phaseCapture.connectAcquireNanos() / 1_000_000L,
                phaseCapture.timeToFirstByteNanos() < 0 ? -1 : phaseCapture.timeToFirstByteNanos() / 1_000_000L,
                overrideMs, n);
    }
}
