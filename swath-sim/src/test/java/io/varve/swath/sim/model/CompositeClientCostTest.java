/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.sim.kernel.SimEventLog;
import io.varve.swath.sim.kernel.SimKernel;
import io.varve.swath.sim.kernel.SimRunResult;
import io.varve.swath.sim.model.ClientCostTerm.Provenance;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The composite client-cost model's two load-bearing claims, each stated as an equality against a
 * hand-computed timeline.
 *
 * <p>The first is that the three page-serial stages really are serial: a page's cost is the sum of
 * them, not the largest of them, because the engine converts the page, then waits for its durability
 * commit, then hands it to the consumer stage. The second is that the consumer stage is a genuine
 * ceiling: two pages arriving at once cost two service times, not one, which is what makes a bursty
 * policy pay for its burst instead of being rewarded for it.
 */
class CompositeClientCostTest {

    private static final long WORKER_NANOS = 6_000_000L;
    private static final long CHECKPOINT_NANOS = 100_000L;
    private static final long SINK_NANOS = 1_000_000L;
    private static final long OFFLOAD_NANOS = 2_000_000L;

    @Test
    void aPagePaysItsThreeSerialStagesInSeries() {
        CompositeClientCost cost = composite(null);
        List<Long> completions = new ArrayList<>();

        SimRunResult result = charge(cost, completions, 1);

        assertThat(completions).containsExactly(WORKER_NANOS + CHECKPOINT_NANOS + SINK_NANOS);
        assertThat(result.counter(ClientCostModel.CHARGES_COUNTER)).isEqualTo(1);
        assertThat(result.counter(ClientCostModel.NANOS_COUNTER))
                .isEqualTo(WORKER_NANOS + CHECKPOINT_NANOS + SINK_NANOS);
    }

    @Test
    void twoPagesArrivingAtOnceQueueForTheConsumerStage() {
        CompositeClientCost cost = composite(null);
        List<Long> completions = new ArrayList<>();

        charge(cost, completions, 2);

        long first = WORKER_NANOS + CHECKPOINT_NANOS + SINK_NANOS;
        assertThat(completions)
                .as("the worker stage is parallel; the second page is held one consumer-stage service "
                        + "behind the first, which is the ceiling that stage imposes on the whole fleet")
                .containsExactly(first, first + SINK_NANOS);
    }

    @Test
    void theOffloadPoolCostsCapacityButNeverDelaysThePageThatCausedIt() {
        CompositeClientCost withPool = composite(SampledClientCostTerm.scalar(
                new ClientCostTerm(Provenance.FINAL, OFFLOAD_NANOS, 0L, "encode pool")));
        List<Long> completions = new ArrayList<>();

        SimRunResult result = charge(withPool, completions, 1);

        assertThat(completions)
                .as("measured to run on other threads, after dispatch: it is not on this page's path")
                .containsExactly(WORKER_NANOS + CHECKPOINT_NANOS + SINK_NANOS);
        assertThat(result.counter(CompositeClientCost.OFFLOAD_RESOURCE + ".busy_nanos"))
                .as("the pool's work is still charged somewhere, or it would be free")
                .isEqualTo(OFFLOAD_NANOS);
    }

    @Test
    void aModelStillHoldingAnEarlierRunsQueueIsRefused() {
        CompositeClientCost cost = composite(null);
        // A run cut short by a ceiling leaves the consumer stage mid-service; carrying it into the next
        // run would make that run's result depend on its predecessor's.
        SimKernel kernel = new SimKernel(1L, EngineTimeBudgets.engineDefaults()
                .withMaxDuration(WORKER_NANOS + 1L), SimEventLog.disabled(), 1_000L);
        kernel.scheduleBootstrap(0, 0, "arrive", ctx -> cost.chargePage(ctx, 1, done -> {
        }));
        kernel.run();

        assertThatThrownBy(cost::requireReadyForNewRun)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("belongs to one run");
    }

    @Test
    void everyStageIsRequired() {
        SampledClientCostTerm term = SampledClientCostTerm.scalar(
                new ClientCostTerm(Provenance.FINAL, 1L, 0L, "stage"));

        assertThatThrownBy(() -> new CompositeClientCost(null, term, term, null, 1))
                .isInstanceOf(MissingSimDependencyException.class).hasMessageContaining("worker-side");
        assertThatThrownBy(() -> new CompositeClientCost(term, null, term, null, 1))
                .isInstanceOf(MissingSimDependencyException.class).hasMessageContaining("checkpoint");
        assertThatThrownBy(() -> new CompositeClientCost(term, term, null, null, 1))
                .isInstanceOf(MissingSimDependencyException.class).hasMessageContaining("consumer");
    }

    private static CompositeClientCost composite(SampledClientCostTerm offload) {
        return new CompositeClientCost(
                SampledClientCostTerm.scalar(new ClientCostTerm(Provenance.FINAL, WORKER_NANOS, 0L, "worker")),
                SampledClientCostTerm.scalar(new ClientCostTerm(Provenance.FINAL, CHECKPOINT_NANOS, 0L, "commit")),
                SampledClientCostTerm.scalar(new ClientCostTerm(Provenance.FINAL, SINK_NANOS, 0L, "sink")),
                offload, 4);
    }

    /** Charges {@code pages} pages, all arriving at instant zero on distinct actors. */
    private static SimRunResult charge(CompositeClientCost cost, List<Long> completions, int pages) {
        SimKernel kernel = new SimKernel(1L, EngineTimeBudgets.engineDefaults(), SimEventLog.disabled(),
                10_000L);
        for (int actor = 0; actor < pages; actor++) {
            kernel.scheduleBootstrap(0, actor, "page.arrive",
                    ctx -> cost.chargePage(ctx, 100, done -> completions.add(done.nowNanos())));
        }
        return kernel.run();
    }
}
