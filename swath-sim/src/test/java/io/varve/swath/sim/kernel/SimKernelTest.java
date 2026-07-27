/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.kernel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.sim.model.EngineTimeBudgets;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The scheduler's own contract: the order it dispatches in, the atomicity of a body, and the ceilings. */
class SimKernelTest {

    private static final EngineTimeBudgets BUDGETS = EngineTimeBudgets.engineDefaults();

    @Test
    void eventsDispatchInVirtualTimeOrderRegardlessOfSchedulingOrder() {
        List<String> order = new ArrayList<>();
        SimKernel kernel = kernel(BUDGETS, 100);
        kernel.scheduleBootstrap(300, 0, "c", ctx -> order.add("c@" + ctx.nowNanos()));
        kernel.scheduleBootstrap(100, 1, "a", ctx -> order.add("a@" + ctx.nowNanos()));
        kernel.scheduleBootstrap(200, 2, "b", ctx -> order.add("b@" + ctx.nowNanos()));

        SimRunResult result = kernel.run();

        assertThat(order).containsExactly("a@100", "b@200", "c@300");
        assertThat(result.wallNanos()).isEqualTo(300);
        assertThat(result.eventsProcessed()).isEqualTo(3);
        assertThat(result.stopReason()).isEqualTo(SimStopReason.QUIESCED);
    }

    /**
     * Two actors at one instant are ordered by which scheduled first. This is not a tidiness rule:
     * it is what gives a simulated race a winner, and it has to be a property of the model's own
     * history rather than of anything outside it, or the run stops being reproducible.
     */
    @Test
    void simultaneousEventsAreOrderedByWhoScheduledFirst() {
        List<Integer> order = new ArrayList<>();
        SimKernel kernel = kernel(BUDGETS, 100);
        kernel.scheduleBootstrap(0, 7, "first", ctx -> {
            ctx.scheduleFor(9, 50, "late-scheduled", later -> order.add(later.actorId()));
            order.add(ctx.actorId());
        });
        kernel.scheduleBootstrap(50, 8, "early-scheduled", ctx -> order.add(ctx.actorId()));

        kernel.run();

        assertThat(order).as("actor 8's event was scheduled before actor 9's, so it runs first at t=50")
                .containsExactly(7, 8, 9);
    }

    /**
     * An action body sees no other actor's writes part-way through, which is how a simulated lock
     * hold is expressed: the observer scheduled at the same instant runs strictly after the whole
     * body, never inside it.
     */
    @Test
    void anActionBodyIsAtomicWithRespectToEveryOtherActor() {
        int[] shared = {0};
        List<Integer> observed = new ArrayList<>();
        SimKernel kernel = kernel(BUDGETS, 100);
        kernel.scheduleBootstrap(0, 0, "mutate", ctx -> {
            shared[0] = 1;
            shared[0] = 2;
            shared[0] = 3;
        });
        kernel.scheduleBootstrap(0, 1, "observe", ctx -> observed.add(shared[0]));

        kernel.run();

        assertThat(observed).containsExactly(3);
    }

    @Test
    void theDeclaredMaxDurationStopsTheRunAtTheBudgetNotPastIt() {
        long budget = TimeUnit.SECONDS.toNanos(1);
        List<Long> ticks = new ArrayList<>();
        SimKernel kernel = kernel(BUDGETS.withMaxDuration(budget), 1000);
        kernel.scheduleBootstrap(0, 0, "tick", ctx -> tick(ctx, ticks));

        SimRunResult result = kernel.run();

        assertThat(result.stopReason()).isEqualTo(SimStopReason.MAX_DURATION);
        assertThat(result.wallNanos()).as("the clock stops at the declared ceiling, never beyond it")
                .isEqualTo(budget);
        assertThat(ticks).allSatisfy(t -> assertThat(t).isLessThanOrEqualTo(budget));
    }

    @Test
    void theEventCapStopsARunawayScenario() {
        SimKernel kernel = kernel(BUDGETS, 25);
        kernel.scheduleBootstrap(0, 0, "tick", ctx -> tick(ctx, new ArrayList<>()));

        SimRunResult result = kernel.run();

        assertThat(result.stopReason()).isEqualTo(SimStopReason.EVENT_CAP);
        assertThat(result.eventsProcessed()).isEqualTo(25);
    }

    @Test
    void counterAndTraceEntriesCarryTheActingActorAndInstant() {
        SimEventLog log = SimEventLog.recording();
        SimKernel recording = new SimKernel(1L, BUDGETS, log, 100);
        recording.scheduleBootstrap(400, 3, "work", ctx -> {
            ctx.record("detail", "payload=1");
            ctx.count("things", 2);
            ctx.count("things", 5);
        });

        SimRunResult result = recording.run();

        assertThat(result.counter("things")).isEqualTo(7);
        assertThat(new String(log.canonicalBytes(), StandardCharsets.UTF_8))
                .isEqualTo("400\t0\t3\twork\t\n400\t1\t3\tdetail\tpayload=1\n");
    }

    @Test
    void anEmptyScheduleQuiescesImmediately() {
        SimRunResult result = kernel(BUDGETS, 100).run();

        assertThat(result.eventsProcessed()).isZero();
        assertThat(result.wallNanos()).isZero();
        assertThat(result.stopReason()).isEqualTo(SimStopReason.QUIESCED);
    }

    @Test
    void aDisabledLogRetainsNothingButTheRunIsOtherwiseIdentical() {
        SimKernel recording = new SimKernel(1L, BUDGETS, SimEventLog.recording(), 100);
        SimKernel silent = new SimKernel(1L, BUDGETS, SimEventLog.disabled(), 100);
        recording.scheduleBootstrap(10, 0, "work", ctx -> ctx.count("n", 1));
        silent.scheduleBootstrap(10, 0, "work", ctx -> ctx.count("n", 1));

        SimRunResult loud = recording.run();
        SimRunResult quiet = silent.run();

        assertThat(loud.log().entries()).isNotEmpty();
        assertThat(quiet.log().entries()).isEmpty();
        assertThat(quiet.wallNanos()).isEqualTo(loud.wallNanos());
        assertThat(quiet.counters()).isEqualTo(loud.counters());
    }

    @Test
    void aKernelRefusesToRunTwiceAndRefusesABootstrapAfterStarting() {
        SimKernel kernel = kernel(BUDGETS, 100);
        kernel.scheduleBootstrap(0, 0, "noop", ctx -> {
        });
        kernel.run();

        assertThatThrownBy(kernel::run).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("runs exactly once");
        assertThatThrownBy(() -> kernel.scheduleBootstrap(0, 0, "late", ctx -> {
        })).isInstanceOf(IllegalStateException.class).hasMessageContaining("before run()");
    }

    @Test
    void schedulingIntoThePastIsRejected() {
        SimKernel kernel = kernel(BUDGETS, 100);
        kernel.scheduleBootstrap(0, 0, "backwards", ctx ->
                assertThatThrownBy(() -> ctx.schedule(-1, "impossible", ignored -> {
                })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("into the past"));

        assertThat(kernel.run().eventsProcessed()).isEqualTo(1);
    }

    /** Reschedules itself every 100 ms forever — the shape both ceilings exist to bound. */
    private static void tick(SimContext ctx, List<Long> ticks) {
        ticks.add(ctx.nowNanos());
        ctx.schedule(TimeUnit.MILLISECONDS.toNanos(100), "tick", next -> tick(next, ticks));
    }

    private static SimKernel kernel(EngineTimeBudgets budgets, long maxEvents) {
        return new SimKernel(1L, budgets, SimEventLog.disabled(), maxEvents);
    }
}
