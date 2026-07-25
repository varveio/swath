/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.error.ListingException;
import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.StoreCapabilities;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * The test workhorse: a deterministic, seedable,
 * fault-injectable {@link PageFetcher} backed by an in-memory sorted {@code
 * byte[]} keyspace (the source of truth).
 *
 * <p>Honors {@code start_after} (exclusive), {@code prefix}, {@code delimiter}
 * (common-prefix rollup), {@code max_keys}, and {@code IsTruncated} exactly as
 * S3 does — including unsigned byte order. Faults are injected with a {@link
 * PageInterceptor} that sees every computed page (latency, 503, stuck token,
 * truncated-without-keys, short pages, kill-at — each test composes what it
 * needs). A {@link PageInterceptor} may also <b>throw</b> to model an error
 * surfaced as an exception rather than a page — notably a
 * {@link io.varve.swath.error.ThrottleException} for the real S3 throttle shape (a
 * sustained {@code 503 SlowDown} reaches the fetcher as an exception after the
 * SDK's own retries, NOT as a successful page carrying {@code httpStatus=503}).
 * An opt-in {@link Builder#strictS3Params(boolean)} mode rejects XML-illegal or
 * over-1024-byte {@code prefix}/{@code start_after} request params with a fatal
 * {@link ListingException}, matching the real-S3 HTTP 400 shape. The default is
 * deliberately permissive so existing tests can still model arbitrary byte
 * ranges and legacy fixtures.
 *
 * <p>VERSIONS mode is not implemented: a VERSIONS request throws {@code UnsupportedOperationException}.
 */
public final class MockPageFetcher implements PageFetcher {

    /** Sees every computed page; may rewrite it or throw to inject a fault. */
    @FunctionalInterface
    public interface PageInterceptor {
        ListPage intercept(PageRequest req, int callIndex, ListPage computed)
                throws ListingException, InterruptedException;
    }

    /**
     * Bounded window of recently-served page BOUNDARIES (the last row's key of a recently
     * computed page), used to classify a call as {@link LatencyModel.Context#warm()} — see {@link
     * #isWarmContinuation}. Small on purpose: this models "did we JUST serve this position", not a
     * general LRU cache.
     */
    private static final int RECENCY_WINDOW = 32;

    private final List<MockObject> objects;        // sorted by key (unsigned)
    private final StoreCapabilities capabilities;
    private final PageInterceptor interceptor;
    private final Function<PageRequest, Duration> latency;
    private final LatencyModel latencyModel;
    private final boolean strictS3Params;

    private final AtomicLong apiCalls = new AtomicLong();
    private final AtomicInteger callIndex = new AtomicInteger();
    private final Object recencyLock = new Object();
    private final LinkedHashMap<KeyBytes, Boolean> recentBoundaries = new LinkedHashMap<>(RECENCY_WINDOW, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<KeyBytes, Boolean> eldest) {
            return size() > RECENCY_WINDOW;
        }
    };

    private MockPageFetcher(List<MockObject> objects, StoreCapabilities capabilities,
                            PageInterceptor interceptor, Function<PageRequest, Duration> latency,
                            LatencyModel latencyModel, boolean strictS3Params) {
        this.objects = objects;
        this.capabilities = capabilities;
        this.interceptor = interceptor;
        this.latency = latency;
        this.latencyModel = latencyModel;
        this.strictS3Params = strictS3Params;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Total fetchPage calls — used by INT-8 (≈ceil(N/1000) LIST calls, not ≈N). */
    public long apiCalls() {
        return apiCalls.get();
    }

    public int objectCount() {
        return objects.size();
    }

    @Override
    public StoreCapabilities capabilities() {
        return capabilities;
    }

    @Override
    public ListPage fetchPage(PageRequest req) throws ListingException, InterruptedException {
        apiCalls.incrementAndGet();
        int idx = callIndex.getAndIncrement();
        if (req.mode() == ListingMode.VERSIONS) {
            throw new UnsupportedOperationException("MockPageFetcher does not support VERSIONS listing yet");
        }
        if (strictS3Params) {
            validateS3RequestParam("prefix", req.prefix());
            validateS3RequestParam("start_after", req.startAfter());
        }
        ListPage page = (req.delimiter() == null || req.delimiter().length == 0)
                ? computeFlat(req)
                : computeDelimited(req);
        ListPage intercepted = interceptor.intercept(req, idx, page);
        // Classify BEFORE recording this call's own boundary, so a call is never "warm"
        // relative to itself; record from the true (pre-interceptor) page, since that reflects
        // the fetcher's actual keyspace boundary regardless of what a fault hook did to it.
        boolean warm = isWarmContinuation(req.startAfter());
        boolean probe = req.maxKeys() <= 1;
        boolean delimiterVisible = req.delimiter() != null && req.delimiter().length > 0;
        recordBoundary(page);
        Duration delay = latency.apply(req)
                .plus(latencyModel.delayFor(new LatencyModel.Context(req, idx, warm, probe, delimiterVisible)));
        sleep(delay);
        return intercepted.withLatency(intercepted.latency().plus(delay));
    }

    /** See {@link LatencyModel.Context#warm()} for the operational definition. */
    private boolean isWarmContinuation(byte[] startAfter) {
        if (startAfter == null) {
            return false;
        }
        KeyBytes key = KeyBytes.of(Arrays.copyOf(startAfter, startAfter.length));
        synchronized (recencyLock) {
            return recentBoundaries.containsKey(key);
        }
    }

    /**
     * Remembers {@code page}'s last served row (by unsigned sort order across entries AND
     * common prefixes — whichever bucket held it) as a future "warm continuation" boundary.
     */
    private void recordBoundary(ListPage page) {
        byte[] boundary = lastServedBoundary(page);
        if (boundary == null) {
            return;
        }
        KeyBytes key = KeyBytes.of(Arrays.copyOf(boundary, boundary.length));
        synchronized (recencyLock) {
            recentBoundaries.put(key, Boolean.TRUE);
        }
    }

    private static byte[] lastServedBoundary(ListPage page) {
        byte[] lastEntry = page.entries().isEmpty()
                ? null : page.entries().getLast().key().rawUnsafe();
        byte[] lastPrefix = page.commonPrefixes().isEmpty()
                ? null : page.commonPrefixes().getLast().rawUnsafe();
        if (lastEntry == null) {
            return lastPrefix;
        }
        if (lastPrefix == null) {
            return lastEntry;
        }
        return KeyBytes.compareUnsigned(lastEntry, lastPrefix) >= 0 ? lastEntry : lastPrefix;
    }

    // ---- flat (recursive) OBJECTS listing: the engine's main path -------------

    private ListPage computeFlat(PageRequest req) {
        int max = effectiveMaxKeys(req.maxKeys());
        byte[] prefix = nullToEmpty(req.prefix());
        byte[] startAfter = req.startAfter();

        List<ListEntry> entries = new ArrayList<>(Math.min(max, 1024));
        boolean truncated = false;
        for (MockObject o : objects) {
            if (!startsWith(o.key(), prefix)) {
                continue;
            }
            if (startAfter != null && KeyBytes.compareUnsigned(o.key(), startAfter) <= 0) {
                continue;   // start_after is exclusive
            }
            if (entries.size() == max) {
                truncated = true;   // at least one more match exists
                break;
            }
            entries.add(o.toEntry());
        }
        return page(entries, List.of(), truncated);
    }

    // ---- delimiter (folder) listing: common-prefix rollup, first-page seed -----

    private ListPage computeDelimited(PageRequest req) {
        int max = effectiveMaxKeys(req.maxKeys());
        byte[] prefix = nullToEmpty(req.prefix());
        byte[] delimiter = req.delimiter();
        byte[] startAfter = req.startAfter();

        // Merge objects and rolled-up common prefixes into one unsigned-sorted set.
        TreeMap<byte[], ListEntry> merged = new TreeMap<>(KeyBytes::compareUnsigned);
        for (MockObject o : objects) {
            if (!startsWith(o.key(), prefix)) {
                continue;
            }
            int d = indexOf(o.key(), prefix.length, delimiter);
            if (d >= 0) {
                byte[] cp = Arrays.copyOf(o.key(), d + delimiter.length);   // prefix..delimiter inclusive
                merged.putIfAbsent(cp, new CommonPrefixEntry(KeyBytes.of(cp)));
            } else {
                merged.put(o.key(), o.toEntry());
            }
        }

        List<ListEntry> entries = new ArrayList<>();
        List<KeyBytes> commonPrefixes = new ArrayList<>();
        boolean truncated = false;
        int taken = 0;
        for (var e : merged.entrySet()) {
            byte[] sortKey = e.getKey();
            if (startAfter != null && KeyBytes.compareUnsigned(sortKey, startAfter) <= 0) {
                continue;
            }
            if (taken == max) {
                truncated = true;
                break;
            }
            switch (e.getValue()) {
                case CommonPrefixEntry cp -> commonPrefixes.add(cp.key());
                default -> entries.add(e.getValue());
            }
            taken++;
        }
        return page(entries, commonPrefixes, truncated);
    }

    private ListPage page(List<ListEntry> entries, List<KeyBytes> commonPrefixes, boolean truncated) {
        return new ListPage(entries, commonPrefixes, truncated, null, null, null, 200, Duration.ZERO);
    }

    private static void sleep(Duration delay) throws InterruptedException {
        if (delay.isZero()) {
            return;
        }
        long millis = delay.toMillis();
        int nanos = delay.minusMillis(millis).getNano();
        Thread.sleep(millis, nanos);
    }

    private int effectiveMaxKeys(int requested) {
        int cap = capabilities.maxKeysCap();
        return Math.max(1, Math.min(requested <= 0 ? cap : requested, cap));
    }

    private static byte[] nullToEmpty(byte[] b) {
        return b == null ? new byte[0] : b;
    }

    private static void validateS3RequestParam(String name, byte[] raw) throws ListingException {
        if (raw == null || raw.length == 0) {
            return;
        }
        if (raw.length > ByteMidpoint.MAX_KEY_LEN) {
            throw badRequest(name, "exceeds S3's 1024-byte key limit");
        }
        if (!isValidUtf8(raw)) {
            throw badRequest(name, "is not valid UTF-8");
        }
        String s = new String(raw, StandardCharsets.UTF_8);
        int[] unsafe = s.codePoints().filter(MockPageFetcher::isXmlIllegalForS3Param).limit(1).toArray();
        if (unsafe.length > 0) {
            throw badRequest(name, "contains XML-illegal scalar U+%04X".formatted(unsafe[0]));
        }
    }

    private static ListingException badRequest(String name, String detail) {
        return new ListingException("S3 listObjectsV2 failed with HTTP 400 Bad Request: "
                + name + " " + detail);
    }

    private static boolean isValidUtf8(byte[] raw) {
        try {
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(raw));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }

    /** Strict S3 request-param guard: XML 1.0 Char excludes C0 except TAB/LF/CR, U+FFFE, U+FFFF. */
    private static boolean isXmlIllegalForS3Param(int cp) {
        return (cp < 0x20 && cp != 0x09 && cp != 0x0A && cp != 0x0D)
                || cp == 0xFFFE
                || cp == 0xFFFF;
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        return Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length);
    }

    /** First index ≥ {@code from} where {@code needle} occurs in {@code hay}, or -1. */
    private static int indexOf(byte[] hay, int from, byte[] needle) {
        if (needle.length == 0) {
            return -1;
        }
        int last = hay.length - needle.length;
        for (int i = from; i <= last; i++) {
            if (Arrays.equals(hay, i, i + needle.length, needle, 0, needle.length)) {
                return i;
            }
        }
        return -1;
    }

    // ---- builder ---------------------------------------------------------------

    public static final class Builder {
        private final List<MockObject> objects = new ArrayList<>();
        private StoreCapabilities capabilities = StoreCapabilities.s3();
        private PageInterceptor interceptor = (req, idx, page) -> page;
        private Function<PageRequest, Duration> latency = req -> Duration.ZERO;
        private LatencyModel latencyModel = LatencyModel.NONE;
        private boolean strictS3Params;

        public Builder object(MockObject o) {
            objects.add(o);
            return this;
        }

        public Builder objects(List<MockObject> os) {
            objects.addAll(os);
            return this;
        }

        /** Add keys (UTF-8 / raw) with deterministic synthetic metadata. */
        public Builder keys(List<byte[]> keys) {
            for (byte[] k : keys) {
                objects.add(syntheticObject(k));
            }
            return this;
        }

        public Builder capabilities(StoreCapabilities caps) {
            this.capabilities = caps;
            return this;
        }

        public Builder maxKeysCap(int cap) {
            this.capabilities = capabilities.withMaxKeysCap(cap);
            return this;
        }

        public Builder interceptor(PageInterceptor interceptor) {
            this.interceptor = Objects.requireNonNull(interceptor, "interceptor");
            return this;
        }

        /** Reject XML-illegal / overlength S3 request params like real S3. Default is off. */
        public Builder strictS3Params(boolean strictS3Params) {
            this.strictS3Params = strictS3Params;
            return this;
        }

        /** Inject per-call latency before the mock returns a page. Default is {@link Duration#ZERO}. */
        public Builder latency(Function<PageRequest, Duration> latency) {
            Function<PageRequest, Duration> checked = Objects.requireNonNull(latency, "latency");
            this.latency = req -> requireNonNegative(checked.apply(req));
            return this;
        }

        /** Inject the same delay before every page response. */
        public Builder pageDelay(Duration delay) {
            Duration checked = requireNonNegative(delay);
            this.latency = req -> checked;
            return this;
        }

        /**
         * Delay only the first worker listing page for the seed range. Thief probes use
         * {@code maxKeys == 1}, and later child/continuation listings have a non-null
         * {@code startAfter}, so they remain instant.
         */
        public Builder slowFirstPage(Duration delay) {
            Duration checked = requireNonNegative(delay);
            AtomicBoolean used = new AtomicBoolean();
            this.latency = req -> {
                if (req.maxKeys() > 1 && req.startAfter() == null && used.compareAndSet(false, true)) {
                    return checked;
                }
                return Duration.ZERO;
            };
            return this;
        }

        /**
         * Inject per-call latency via a {@link LatencyModel}: call-class-aware
         * (warm/cold/probe/delimiter), deterministic, fault-free timing. Additive with {@link
         * #latency}/{@link #pageDelay}/{@link #slowFirstPage}. Default {@link LatencyModel#NONE}
         * — zero latency, so a test that doesn't configure one is unaffected.
         */
        public Builder latencyModel(LatencyModel latencyModel) {
            this.latencyModel = Objects.requireNonNull(latencyModel, "latencyModel");
            return this;
        }

        public MockPageFetcher build() {
            // The keyspace is the source of truth: sort unsigned, dedup by key.
            TreeMap<byte[], MockObject> byKey = new TreeMap<>(KeyBytes::compareUnsigned);
            for (MockObject o : objects) {
                byKey.put(o.key(), o);
            }
            return new MockPageFetcher(new ArrayList<>(byKey.values()), capabilities, interceptor, latency,
                    latencyModel, strictS3Params);
        }

        private static Duration requireNonNegative(Duration delay) {
            Objects.requireNonNull(delay, "delay");
            if (delay.isNegative()) {
                throw new IllegalArgumentException("delay must be non-negative: " + delay);
            }
            return delay;
        }

        private static MockObject syntheticObject(byte[] key) {
            long size = 1L + (Integer.toUnsignedLong(Arrays.hashCode(key)) % 1_000_000L);
            long mtime = 1_700_000_000_000_000L + size;   // micros
            String etag = String.format("%08x", Arrays.hashCode(key));
            return new MockObject(key, size, mtime, etag, "STANDARD");
        }
    }
}
