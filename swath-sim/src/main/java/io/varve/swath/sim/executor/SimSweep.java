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
 * Runs many scenarios against <b>one open store</b>, which is the only shape in which a sweep is
 * affordable: opening a large fixture costs more than a whole run does, so a sweep that opened per leg
 * would spend most of its time doing the one thing every leg has in common.
 *
 * <p>Reusing the handle is what makes the two rules below necessary, and both are the kind of mistake
 * that produces plausible numbers rather than an error:
 *
 * <ul>
 *   <li><b>A fresh client-cost model per leg.</b> The composite and contended forms own queues. A run
 *       cut short by a ceiling leaves work in them, and carrying that into the next leg would make one
 *       leg's result depend on its predecessor's — the single thing a reproducible simulator may not
 *       do. This class takes a supplier, not a model, so there is no way to pass one twice.</li>
 *   <li><b>Store metrics are per-open, not per-run.</b> The meter registry belongs to the handle, so
 *       every leg after the first reads counters that include its predecessors'. Each leg's result
 *       therefore carries the <em>delta</em> over that leg, taken here, rather than a cumulative
 *       reading that would silently attribute the whole sweep's store work to its last leg.</li>
 * </ul>
 *
 * <p>The handle is the caller's throughout: this class uses it, never opens it, never closes it.
 */
public final class SimSweep {

    /** One leg's outcome: what the run produced, and what the store did during it. */
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
     * @param freshClientCost supplies a new client-cost model per leg (see the class note)
     * @param registry        the store handle's own meter registry, or {@code null} when the caller has
     *                        none — an in-memory test fixture, typically
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
     * Every counter and timer count in {@code registry}, by name, or an empty map when there is none.
     *
     * <p>Counters and timers only: a gauge is a level, not an accumulation, so subtracting one leg's
     * reading from the next would produce a "delta" with no meaning — a store's open-file gauge that
     * read the same at both ends would report zero change over a leg that opened and closed a hundred.
     * Levels are deliberately absent rather than silently differenced.
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
