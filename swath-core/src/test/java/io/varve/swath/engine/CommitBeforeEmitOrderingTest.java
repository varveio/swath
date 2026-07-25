/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.PageBatch;
import io.varve.swath.pipeline.Channel;
import io.varve.swath.pipeline.End;
import io.varve.swath.pipeline.Item;
import io.varve.swath.pipeline.Msg;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.HeldCommitCheckpointStore;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Engine-level pin for I1 (commit-before-emit, contracts.md §0): a page's checkpoint commits
 * durably BEFORE its entries are pushed downstream. The kill-9 crash-resume suites pin the durable
 * END-STATE this invariant produces; this test pins the ORDERING itself inside {@link
 * WorkStealingScan#runClaim}, so a decomposition of the engine that reorders commit and emit is
 * caught directly rather than only via a crash matrix.
 *
 * <p>The scan runs one worker over one node whose keyspace is exactly three sequential pages (no
 * stealing, owner-split disabled), so page order on the node is deterministic and the ONLY thing
 * that can gate the middle page's emission is its own commit future. A {@link
 * HeldCommitCheckpointStore} holds that page's durability pending; the test drives the engine on a
 * background thread and is itself the channel receiver, so the non-emission check has no drain
 * thread racing it. The barrier is deterministic: {@link HeldCommitCheckpointStore#awaitParked()}
 * returns only once the worker is awaiting the held commit, at which point everything the engine
 * has emitted is already visible and nothing more can be produced until the commit is released.
 */
final class CommitBeforeEmitOrderingTest {

    private static final int PAGE = 4;
    private static final int PAGES = 3;
    private static final int KEYS = PAGE * PAGES;   // three sequential pages of four on one worker

    // Keys are pure ASCII, so UTF-8 round-trips byte-exactly — comparing by name is content-exact.
    private static byte[] key(int i) {
        return String.format("data/%05d", i).getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey runKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "i1-order-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    @Timeout(60)
    void targetedPageEmitsOnlyAfterItsCommitCompletes(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < KEYS; i++) {
            keyspace.add(key(i));
        }
        // The middle page's last key identifies the commit we hold pending; its four keys (indices
        // PAGE..2*PAGE-1) must not reach the channel until that commit completes.
        byte[] targetAdvanceTo = keyspace.get(2 * PAGE - 1);
        List<byte[]> firstPageKeys = keyspace.subList(0, PAGE);
        List<byte[]> targetPageKeys = keyspace.subList(PAGE, 2 * PAGE);

        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"))) {
            HeldCommitCheckpointStore store = new HeldCommitCheckpointStore(
                    sqlite, c -> Arrays.equals(c.advanceTo(), targetAdvanceTo));
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            // One worker + a page size that yields exactly three sequential pages, owner-split off:
            // no split ever runs, so the node stays whole and its pages commit/emit in order.
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS)
                            .withToggles(EngineToggles.DEFAULT.withOwnerSplit(false)),
                    fetcher, store, 1, PAGE, seeds, FilterChain.EMPTY);

            // Drive the engine on its own thread and receive from the channel on THIS thread, so the
            // test itself is the deterministic receiver. A generous slot budget means send() never
            // blocks, so the worker only ever parks on the held commit.
            Channel<PageBatch> channel = new Channel<>(10_000);
            RunContext ctx = RunContext.create();
            ExecutorService producer = Executors.newSingleThreadExecutor();
            Future<?> producing = producer.submit(() -> {
                engine.produce(ctx, channel);
                channel.send(new End<>());
                return null;
            });
            try {
                // Barrier: the worker is awaiting the held page's commit. For a correct engine this
                // is strictly BEFORE the page is emitted; for a reordered one the page was already
                // sent, so it is already queued in the channel by the time this returns.
                store.awaitParked();
                assertThat(store.released()).as("held commit still pending at the barrier").isFalse();

                // Non-emission: drain everything the engine has emitted so far. The worker is parked
                // and there is no other worker, so this is its complete pre-commit output. It must be
                // page one only; none of the held page's keys may have escaped.
                List<byte[]> beforeCommit = drainAvailable(channel);
                assertThat(names(beforeCommit))
                        .as("page one emitted while the held page waits (no deadlock, others proceed)")
                        .containsExactlyElementsOf(names(firstPageKeys));
                assertThat(names(beforeCommit))
                        .as("held page must not be emitted before its commit completes (I1)")
                        .doesNotContainAnyElementsOf(names(targetPageKeys));

                // Complete the held commit → the durable page commit lands, then the worker emits.
                store.release();

                // The rest of the stream: the held page emits first, then page three, then End.
                List<byte[]> afterCommit = receiveUntilEnd(channel);
                assertThat(names(afterCommit).subList(0, PAGE))
                        .as("held page emits immediately after its commit completes")
                        .containsExactlyElementsOf(names(targetPageKeys));

                // Whole scan: exactly the keyspace, in order, each key once.
                List<byte[]> all = new ArrayList<>(beforeCommit);
                all.addAll(afterCommit);
                assertThat(names(all)).containsExactlyElementsOf(names(keyspace));

                producing.get(30, TimeUnit.SECONDS);   // surface any producer-side failure
            } finally {
                channel.dropReceiver();
                if (!store.released()) {
                    store.release();   // unblock a parked worker so the run can wind down
                }
                producer.shutdownNow();
            }
        }
    }

    /** Receive every envelope already queued (non-blocking), stopping at the first empty poll. */
    private static List<byte[]> drainAvailable(Channel<PageBatch> channel) throws InterruptedException {
        List<byte[]> keys = new ArrayList<>();
        Msg<PageBatch> msg;
        while ((msg = channel.poll(0, TimeUnit.NANOSECONDS)) != null) {
            addKeys(keys, msg);
        }
        return keys;
    }

    /** Receive until {@link End}, collecting every item's keys (bounded; fails on timeout). */
    private static List<byte[]> receiveUntilEnd(Channel<PageBatch> channel) throws InterruptedException {
        List<byte[]> keys = new ArrayList<>();
        while (true) {
            Msg<PageBatch> msg = channel.poll(30, TimeUnit.SECONDS);
            if (msg == null) {
                throw new AssertionError("timed out waiting for the rest of the stream");
            }
            if (msg instanceof End<PageBatch>) {
                return keys;
            }
            addKeys(keys, msg);
        }
    }

    private static void addKeys(List<byte[]> into, Msg<PageBatch> msg) {
        if (msg instanceof Item<PageBatch> item) {
            for (ListEntry e : item.value().entries()) {
                into.add(e.key().raw());
            }
        }
    }

    private static List<String> names(List<byte[]> keys) {
        List<String> out = new ArrayList<>(keys.size());
        for (byte[] k : keys) {
            out.add(new String(k, StandardCharsets.UTF_8));
        }
        return out;
    }
}
