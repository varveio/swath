/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.varve.swath.runtime.RunContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * CONC-1 (I8): the pipeline does not deadlock when the consumer
 * stops while the producer is blocked on a full queue. Capacity 1,
 * pre-cancelled token, 100 entries, 2 s budget. Exercises
 * drop-receiver-before-join + {@code shutdownNow()}-before-{@code close()}.
 */
class PipelineShutdownTest {

    @Test
    void preCancelledConsumerOnFullQueueShutsDownWithoutDeadlock() {
        RunContext ctx = RunContext.create();
        ctx.cancellation().cancel();   // pre-cancelled

        Pipeline<Integer> pipeline = new Pipeline<>(1);   // queue size 1

        Pipeline.Producer<Integer> producer = (c, out) -> {
            for (int i = 0; i < 100; i++) {
                if (!out.send(new Item<>(i))) {
                    return;   // receiver dropped → wind down
                }
            }
        };
        // The consumer sees cancellation immediately and drains nothing.
        Pipeline.Consumer<Integer> consumer = (c, in) -> {
            if (c.cancellation().isCancelled()) {
                return;
            }
            while (in.receive() instanceof Item) {
                // drain
            }
        };

        assertTimeoutPreemptively(Duration.ofSeconds(2),
                () -> pipeline.run(ctx, producer, consumer),
                "pipeline deadlocked on a full queue under cancellation");
    }

    @Test
    void happyPathDeliversEveryItemThenEnd() throws Exception {
        RunContext ctx = RunContext.create();
        Pipeline<Integer> pipeline = new Pipeline<>(4);

        int n = 250;
        Pipeline.Producer<Integer> producer = (c, out) -> {
            for (int i = 0; i < n; i++) {
                out.send(new Item<>(i));
            }
        };

        List<Integer> received = new ArrayList<>();
        AtomicInteger ends = new AtomicInteger();
        Pipeline.Consumer<Integer> consumer = (c, in) -> {
            while (true) {
                Msg<Integer> msg = in.receive();
                switch (msg) {
                    case Item<Integer> item -> received.add(item.value());
                    case End<Integer> ignored -> {
                        ends.incrementAndGet();
                        return;
                    }
                    case Failure<Integer> f -> throw new AssertionError("unexpected failure", f.cause());
                }
            }
        };

        assertTimeoutPreemptively(Duration.ofSeconds(5),
                () -> pipeline.run(ctx, producer, consumer));

        assertThat(received).hasSize(n);
        assertThat(received).containsExactlyElementsOf(IntStream.range(0, n).boxed().toList());
        assertThat(ends.get()).isEqualTo(1);   // exactly one End
    }
}
