/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The {@link Channel} wakeup discipline is a relay, not a broadcast (varveio/swath#206): a release
 * signals one parked sender, and an admitted sender signals the next while budget remains. These
 * tests pin the two properties a relay can lose and a broadcast cannot: no sender is ever left
 * parked while budget is free (lost wakeup), and the relay chain admits every sender the freed
 * budget can take without waiting for the 50 ms {@code await} backstop.
 */
class ChannelRelaySignalTest {

    private static Channel<Integer> weighted(long cap) {
        return new Channel<>(cap, (Integer i) -> (long) i);
    }

    /**
     * Many senders, weights straddling the capacity (some single items heavier than the whole
     * budget), one receiver draining everything: every send completes and no send waits longer
     * than a few backstop periods — a lost wakeup would strand a sender until the receiver stops.
     */
    @Test
    void everySendCompletesUnderContentionWithWeightsStraddlingTheCap() throws Exception {
        long cap = 100;
        int senders = 64;
        int itemsPerSender = 50;
        int[] weights = {1, 30, 60, 99, 100, 150, 250};
        Channel<Integer> ch = weighted(cap);

        AtomicLong maxSendNs = new AtomicLong();
        AtomicInteger sent = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(senders);
        List<Thread> threads = new ArrayList<>();
        for (int s = 0; s < senders; s++) {
            int offset = s;
            threads.add(Thread.ofVirtual().start(() -> {
                try {
                    for (int i = 0; i < itemsPerSender; i++) {
                        int weight = weights[(offset + i) % weights.length];
                        long t0 = System.nanoTime();
                        assertThat(ch.send(new Item<>(weight))).isTrue();
                        maxSendNs.accumulateAndGet(System.nanoTime() - t0, Math::max);
                        sent.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }));
        }

        int expected = senders * itemsPerSender;
        long receivedWeight = 0;
        for (int i = 0; i < expected; i++) {
            Msg<Integer> msg = ch.receive();
            receivedWeight += ((Item<Integer>) msg).value();
        }
        assertThat(done.await(10, TimeUnit.SECONDS)).as("every sender finished").isTrue();
        for (Thread t : threads) {
            t.join();
        }
        assertThat(sent.get()).isEqualTo(expected);
        assertThat(receivedWeight).isPositive();
        assertThat(Duration.ofNanos(maxSendNs.get()))
                .as("no send waited for more than a handful of backstop periods")
                .isLessThan(Duration.ofSeconds(1));
    }

    /**
     * Cap 10, budget full, 200 weight-1 senders parked. One receive frees 10 of weight: the relay
     * chain must admit exactly 10 senders (1 signal + 9 relays — the 10th fills the budget again and
     * relays nothing), and it must do so well inside the 50 ms backstop. Exactly 10, never more:
     * the admission predicate is untouched, so the budget still bounds in-flight weight.
     */
    @Test
    void oneReleaseRelaysThroughExactlyTheSendersTheFreedBudgetAdmits() throws Exception {
        long cap = 10;
        Channel<Integer> ch = weighted(cap);
        assertThat(ch.send(new Item<>((int) cap))).isTrue();   // budget full: in-flight 10 >= cap

        int parked = 200;
        AtomicInteger admitted = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(parked);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < parked; i++) {
            threads.add(Thread.ofVirtual().start(() -> {
                started.countDown();
                try {
                    if (ch.send(new Item<>(1))) {
                        admitted.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        started.await();
        TimeUnit.MILLISECONDS.sleep(200);                        // let every sender park (>= 3 backstops)
        assertThat(admitted.get()).as("nothing admitted while the budget is full").isZero();

        assertThat(((Item<Integer>) ch.receive()).value()).isEqualTo((int) cap);   // frees 10
        await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(1))
                .atMost(Duration.ofMillis(40))                    // the backstop alone would take >= 50 ms
                .until(() -> admitted.get() == 10);
        TimeUnit.MILLISECONDS.sleep(120);                        // two more backstop periods pass...
        assertThat(admitted.get()).as("...and the budget still bounds admission").isEqualTo(10);

        ch.dropReceiver();                                        // release the other 190 (I8)
        for (Thread t : threads) {
            t.join();
        }
        assertThat(admitted.get()).isEqualTo(10);
    }

    /**
     * An interrupted parked sender leaves with {@link InterruptedException} and does not break the
     * chain for the senders still parked behind it: the next release still admits one of them.
     */
    @Test
    void anInterruptedWaiterDoesNotStrandTheSendersBehindIt() throws Exception {
        Channel<Integer> ch = weighted(1);
        assertThat(ch.send(new Item<>(1))).isTrue();             // full

        AtomicInteger admitted = new AtomicInteger();
        AtomicInteger interrupted = new AtomicInteger();
        List<Thread> waiters = new ArrayList<>();
        CountDownLatch started = new CountDownLatch(2);
        for (int i = 0; i < 2; i++) {
            waiters.add(Thread.ofVirtual().start(() -> {
                started.countDown();
                try {
                    if (ch.send(new Item<>(1))) {
                        admitted.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    interrupted.incrementAndGet();
                }
            }));
        }
        started.await();
        TimeUnit.MILLISECONDS.sleep(100);                        // both parked

        waiters.getFirst().interrupt();
        await().atMost(Duration.ofSeconds(2)).until(() -> interrupted.get() == 1);
        assertThat(admitted.get()).as("the budget is still full").isZero();

        ch.receive();                                             // frees one unit
        await().atMost(Duration.ofSeconds(2)).until(() -> admitted.get() == 1);
        for (Thread t : waiters) {
            t.join();
        }
    }
}
