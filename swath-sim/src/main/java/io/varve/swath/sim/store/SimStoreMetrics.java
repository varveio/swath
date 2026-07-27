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

/**
 * The simulator store's own signals: which backend {@link SimStoreFactory} resolved and why it
 * declined the ones above it (mirroring the replay server's {@code serving.path} /
 * {@code serving.fallback} pair), plus how the {@link SimStoreBackend#STREAMING} tier behaved while
 * serving. Both must be answerable from a sweep's metrics alone — otherwise a result set carries no
 * record of what produced it, and a threshold or residency regression looks like a throughput
 * regression.
 */
public final class SimStoreMetrics {

    /** Bumped once per resolved store, tagged with the backend that will serve the fixture. */
    public static final String BACKEND_METRIC = "swath.sim.store.backend";

    /** Bumped when an {@link SimStoreBackend#AUTO} resolution declines the arena tier. */
    public static final String ARENA_DECLINE_METRIC = "swath.sim.store.arena.decline";

    /** {@link #ARENA_DECLINE_METRIC} reason: the fixture's keys exceed the configured byte budget. */
    public static final String DECLINE_OVER_BUDGET = "over-budget";

    /** Bumped when an {@link SimStoreBackend#AUTO} resolution declines the streaming tier (the fixture
     *  is not sorted-eligible), tagged with the {@code io.varve.swath.replay.fixture.SortedFixtures}
     *  eligibility reason that fired. */
    public static final String STREAMING_DECLINE_METRIC = "swath.sim.store.streaming.decline";

    /** Bumped once per range read served entirely from an already-decoded segment. */
    public static final String SEGMENT_HIT_METRIC = "swath.sim.store.streaming.segment.hit";

    /** Bumped once per segment fault, tagged {@link #FAULT_FORWARD} or {@link #FAULT_SEEK}. */
    public static final String SEGMENT_FAULT_METRIC = "swath.sim.store.streaming.segment.fault";

    /** {@link #SEGMENT_FAULT_METRIC} kind: a cursor walked off the end of the segment it was served
     *  from, so the preceding row group is still resident — the sequential-stream shape. */
    public static final String FAULT_FORWARD = "forward";

    /** {@link #SEGMENT_FAULT_METRIC} kind: nothing precedes the faulted row group in the resident set
     *  — a cursor started mid-keyspace (a steal or a split), or the first touch of a run. */
    public static final String FAULT_SEEK = "seek";

    /** Wall time of one segment decode; its count is the fault count and its sum the decode total. */
    public static final String SEGMENT_DECODE_METRIC = "swath.sim.store.streaming.segment.decode";

    /** Rows decoded across every segment fault — the numerator of the decode-once claim. */
    public static final String SEGMENT_DECODE_ROWS_METRIC = "swath.sim.store.streaming.segment.decode.rows";

    /** Bumped per decoded segment dropped to stay inside the residency budget. */
    public static final String SEGMENT_EVICT_METRIC = "swath.sim.store.streaming.segment.evict";

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

    public Timer.Sample startStreamingDecodeTimer() {
        return Timer.start(registry);
    }

    public void recordStreamingDecode(Timer.Sample sample, long rows) {
        sample.stop(Timer.builder(SEGMENT_DECODE_METRIC).register(registry));
        Counter.builder(SEGMENT_DECODE_ROWS_METRIC).register(registry).increment(rows);
    }

    /**
     * Publishes {@code residentBytes} as {@link #RESIDENT_BYTES_METRIC}. A gauge, not a counter: the
     * quantity a residency budget is checked against is the value right now, not a total.
     */
    public void registerStreamingResidentBytes(Supplier<Number> residentBytes) {
        Gauge.builder(RESIDENT_BYTES_METRIC, residentBytes).register(registry);
    }

    /** The lowercase spelling of a backend, matching how the replay server tags its serving path. */
    public static String tagValue(SimStoreBackend backend) {
        return backend.name().toLowerCase(Locale.ROOT);
    }
}
