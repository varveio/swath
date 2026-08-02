/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.sim.model.ClientCostModel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * Runs scenarios in order against one caller-owned open store.
 *
 * <p>Each leg receives a fresh client-cost model: some implementations retain queues. Store metrics
 * belong to the open handle, so each result reports its counter and timer deltas rather than cumulative
 * values.
 */
public final class SimSweep {

    /** A run result and its per-leg store counter/timer deltas. */
    public record Leg(PolicyRunResult run, SortedMap<String, Double> storeMetricsDelta) {

        public Leg {
            storeMetricsDelta = Collections.unmodifiableSortedMap(new TreeMap<>(storeMetricsDelta));
        }
    }

    private SimSweep() {
    }

    /**
     * Runs every scenario in {@code legs} against {@code store}, in order.
     *
     * @param freshClientCost supplies a new client-cost model per leg
     * @param registry        the store handle's meter registry, or {@code null} when it has none
     * @param storeLabel      what served the sweep, recorded on every leg's run record
     */
    public static List<Leg> run(List<PolicyScenario> legs, Supplier<ClientCostModel> freshClientCost,
                                ListingStore store, MeterRegistry registry, String storeLabel) {
        if (legs == null || legs.isEmpty()) {
            throw new IllegalArgumentException("a sweep needs at least one scenario");
        }
        if (freshClientCost == null) {
            throw new IllegalArgumentException("a sweep needs a supplier of client-cost models, not one "
                    + "model: a stateful model belongs to exactly one run");
        }
        List<Leg> results = new ArrayList<>(legs.size());
        for (PolicyScenario leg : legs) {
            SortedMap<String, Double> before = snapshot(registry);
            PolicyRunResult result = SimExecutor.run(leg.withClientCost(freshClientCost.get()), store,
                    storeLabel);
            results.add(new Leg(result, delta(before, snapshot(registry))));
        }
        return List.copyOf(results);
    }

    /**
     * Snapshots counters and timer counts by name, or returns an empty map for no registry.
     * Gauges are levels, not accumulations, and are deliberately not differenced.
     */
    private static SortedMap<String, Double> snapshot(MeterRegistry registry) {
        TreeMap<String, Double> values = new TreeMap<>();
        if (registry == null) {
            return values;
        }
        for (Meter meter : registry.getMeters()) {
            String name = meter.getId().getName() + tags(meter);
            if (meter instanceof Counter counter) {
                values.merge(name, counter.count(), Double::sum);
            } else if (meter instanceof Timer timer) {
                values.merge(name + ".count", (double) timer.count(), Double::sum);
                values.merge(name + ".total_nanos", timer.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS),
                        Double::sum);
            }
        }
        return values;
    }

    private static String tags(Meter meter) {
        StringBuilder out = new StringBuilder();
        meter.getId().getTags().forEach(tag -> out.append('{').append(tag.getKey()).append('=')
                .append(tag.getValue()).append('}'));
        return out.toString();
    }

    private static SortedMap<String, Double> delta(SortedMap<String, Double> before,
                                                   SortedMap<String, Double> after) {
        TreeMap<String, Double> out = new TreeMap<>();
        for (var entry : after.entrySet()) {
            double moved = entry.getValue() - before.getOrDefault(entry.getKey(), 0.0);
            if (moved != 0.0) {
                out.put(entry.getKey(), moved);
            }
        }
        return out;
    }
}
