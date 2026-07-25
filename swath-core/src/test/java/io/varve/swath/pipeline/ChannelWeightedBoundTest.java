/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Guards the {@link Channel} entry-weight budget that backs {@code
 * --object-listing-queue-size} (I11): a page is admitted <b>while in-flight
 * entry count {@code < cap}</b>, then the channel blocks — the bound is on summed
 * entry weight, not a slot count, because slot counting would let a {@code
 * 1}-entry cap admit a full {@code 1000}-entry page.
 *
 * <p>Weight here = the Integer payload itself, so the assertions are exact.
 */
class ChannelWeightedBoundTest {

    private static Channel<Integer> weighted(long cap) {
        return new Channel<>(cap, (Integer i) -> (long) i);
    }

    @Test
    void admitsWhileInFlightBelowCapThenBlocksOnWeightNotSlotCount() throws Exception {
        Channel<Integer> ch = weighted(100);

        // Two items totalling 120 weight are admitted because each was offered while
        // in-flight was still < 100 (0, then 60). A slot-count channel would have let
        // through far more / far fewer; this is the entry-weight semantics.
        assertThat(ch.send(new Item<>(60))).isTrue();   // in-flight 0  < 100 → admit → 60
        assertThat(ch.send(new Item<>(60))).isTrue();   // in-flight 60 < 100 → admit → 120

        // Now in-flight (120) >= cap (100): the next send MUST block.
        AtomicBoolean thirdReturned = new AtomicBoolean(false);
        AtomicReference<Boolean> thirdResult = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread producer = Thread.ofVirtual().start(() -> {
            started.countDown();
            try {
                boolean ok = ch.send(new Item<>(1));   // blocks until weight frees up
                thirdResult.set(ok);
                thirdReturned.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        started.await();
        // It stays blocked while over budget.
        assertThat(thirdReturned.get()).isFalse();
        TimeUnit.MILLISECONDS.sleep(150);
        assertThat(thirdReturned.get()).as("send must block while in-flight >= cap").isFalse();

        // Draining one item releases 60 of weight → in-flight 60 < 100 → the blocked send wakes.
        assertThat(((Item<Integer>) ch.receive()).value()).isEqualTo(60);
        await().atMost(Duration.ofSeconds(2)).untilTrue(thirdReturned);
        assertThat(thirdResult.get()).isTrue();
        producer.join();
    }

    @Test
    void singleItemHeavierThanCapIsAdmittedWhenEmptyNeverDeadlocks() throws Exception {
        // Cap 10, but a 1000-entry page must still be admitted when in-flight == 0,
        // or a tiny queue knob would wedge the pipeline. (Matches "admit while < cap".)
        Channel<Integer> ch = weighted(10);
        assertThat(ch.send(new Item<>(1000))).isTrue();
        assertThat(((Item<Integer>) ch.receive()).value()).isEqualTo(1000);
    }

    @Test
    void endAndFailureEnvelopesWeighZeroSoTheyNeverBlock() throws Exception {
        Channel<Integer> ch = weighted(5);
        ch.send(new Item<>(5));            // fills the budget exactly (in-flight 5 >= cap)
        // Control envelopes carry no payload → weight 0 → admitted without blocking.
        assertThat(ch.send(new End<>())).isTrue();
        assertThat(ch.send(new Failure<>(new RuntimeException("x")))).isTrue();
        assertThat(ch.receive()).isInstanceOf(Item.class);
        assertThat(ch.receive()).isInstanceOf(End.class);
        assertThat(ch.receive()).isInstanceOf(Failure.class);
    }

    @Test
    void dropReceiverWakesABlockedProducerWhichReturnsFalse() throws Exception {
        Channel<Integer> ch = weighted(10);
        ch.send(new Item<>(10));   // in-flight 10 >= cap → next send blocks

        AtomicReference<Boolean> result = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread producer = Thread.ofVirtual().start(() -> {
            started.countDown();
            try {
                result.set(ch.send(new Item<>(1)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        started.await();
        TimeUnit.MILLISECONDS.sleep(100);
        assertThat(result.get()).isNull();   // still blocked

        ch.dropReceiver();                    // I8: consumer gone
        await().atMost(Duration.ofSeconds(2)).until(() -> result.get() != null);
        assertThat(result.get()).as("send returns false once the receiver is dropped").isFalse();
        producer.join();
    }

    @Test
    void defaultIntConstructorIsSlotCounting() throws Exception {
        // Channel(int) weighs every item 1, so capacity == slot count.
        Channel<Integer> ch = new Channel<>(1);
        assertThat(ch.send(new Item<>(999))).isTrue();   // one slot used (weight 1)

        AtomicReference<Boolean> second = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread producer = Thread.ofVirtual().start(() -> {
            started.countDown();
            try {
                second.set(ch.send(new Item<>(123)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        started.await();
        TimeUnit.MILLISECONDS.sleep(100);
        assertThat(second.get()).as("1-slot channel blocks the 2nd item").isNull();

        ch.receive();   // free the slot
        await().atMost(Duration.ofSeconds(2)).until(() -> second.get() != null);
        assertThat(second.get()).isTrue();
        producer.join();
    }
}
