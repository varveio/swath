/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Locale;
import java.util.function.Supplier;

/** Backend selection and streaming-tier metrics for simulator sweeps. */
public final class SimStoreMetrics {

    /** Bumped once per resolved store, tagged with the backend that will serve the fixture. */
    public static final String BACKEND_METRIC = "swath.sim.store.backend";

    /** Auto-resolution decline of the arena tier. */
    public static final String ARENA_DECLINE_METRIC = "swath.sim.store.arena.decline";

    /** {@link #ARENA_DECLINE_METRIC} reason: the fixture's keys exceed the configured byte budget. */
    public static final String DECLINE_OVER_BUDGET = "over-budget";

    /** Auto-resolution decline of streaming, tagged with its sorted-eligibility reason. */
    public static final String STREAMING_DECLINE_METRIC = "swath.sim.store.streaming.decline";

    /** Already-decoded segments touched; a read may also fault a different segment. */
    public static final String SEGMENT_HIT_METRIC = "swath.sim.store.streaming.segment.hit";

    /** Bumped once per segment fault, tagged {@link #FAULT_FORWARD} or {@link #FAULT_SEEK}. */
    public static final String SEGMENT_FAULT_METRIC = "swath.sim.store.streaming.segment.fault";

    /** Fault with the preceding row group resident, usually from a cursor walking forward. */
    public static final String FAULT_FORWARD = "forward";

    /** Fault with no preceding resident row group, such as a split, steal, or first touch. */
    public static final String FAULT_SEEK = "seek";

    /** Wall time of one segment decode; its count is the fault count and its sum the decode total. */
    public static final String SEGMENT_DECODE_METRIC = "swath.sim.store.streaming.segment.decode";

    /** Rows decoded across every segment fault — the numerator of the decode-once claim. */
    public static final String SEGMENT_DECODE_ROWS_METRIC = "swath.sim.store.streaming.segment.decode.rows";

    /** Bumped per decoded segment dropped to stay inside the residency budget. */
    public static final String SEGMENT_EVICT_METRIC = "swath.sim.store.streaming.segment.evict";

    /** Decode refusal, tagged with a typed reason; unlike a decline, no fallback tier served it. */
    public static final String SEGMENT_REFUSED_METRIC = "swath.sim.store.streaming.segment.refused";

    /** The decoded segments' current footprint in bytes. */
    public static final String RESIDENT_BYTES_METRIC = "swath.sim.store.streaming.resident.bytes";

    private static final String BACKEND_TAG = "backend";
    private static final String REASON_TAG = "reason";
    private static final String KIND_TAG = "kind";

    private final MeterRegistry registry;

    public SimStoreMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void recordBackend(SimStoreBackend resolved) {
        Counter.builder(BACKEND_METRIC).tag(BACKEND_TAG, tagValue(resolved)).register(registry).increment();
    }

    public void recordArenaDecline(String reason) {
        Counter.builder(ARENA_DECLINE_METRIC).tag(REASON_TAG, reason).register(registry).increment();
    }

    public void recordStreamingDecline(String reason) {
        Counter.builder(STREAMING_DECLINE_METRIC).tag(REASON_TAG, reason).register(registry).increment();
    }

    public void recordStreamingSegmentHit() {
        Counter.builder(SEGMENT_HIT_METRIC).register(registry).increment();
    }

    public void recordStreamingSegmentFault(String kind) {
        Counter.builder(SEGMENT_FAULT_METRIC).tag(KIND_TAG, kind).register(registry).increment();
    }

    public void recordStreamingSegmentEvict() {
        Counter.builder(SEGMENT_EVICT_METRIC).register(registry).increment();
    }

    public void recordStreamingSegmentRefused(String reason) {
        Counter.builder(SEGMENT_REFUSED_METRIC).tag(REASON_TAG, reason).register(registry).increment();
    }

    public Timer.Sample startStreamingDecodeTimer() {
        return Timer.start(registry);
    }

    public void recordStreamingDecode(Timer.Sample sample, long rows) {
        sample.stop(Timer.builder(SEGMENT_DECODE_METRIC).register(registry));
        Counter.builder(SEGMENT_DECODE_ROWS_METRIC).register(registry).increment(rows);
    }

    /** Publishes current residency as a gauge. */
    public void registerStreamingResidentBytes(Supplier<Number> residentBytes) {
        // The registered gauge keeps this supplier (and its store) strongly. Each open has an
        // unshared registry, so that lifetime is bounded by the store's registry.
        Gauge.builder(RESIDENT_BYTES_METRIC, residentBytes).register(registry);
    }

    /** Lowercase backend tag value. */
    public static String tagValue(SimStoreBackend backend) {
        return backend.name().toLowerCase(Locale.ROOT);
    }
}
