/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.CancelledException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.PageBatch;
import io.varve.swath.observability.StopReason;
import io.varve.swath.pipeline.End;
import io.varve.swath.pipeline.Failure;
import io.varve.swath.pipeline.Item;
import io.varve.swath.pipeline.Msg;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.RecordingSplitStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Liveness: a run over a PERSISTENTLY-throttled bucket must honor cancellation
 * ({@code --max-duration} / SIGTERM) PROMPTLY, instead of spinning forever in the
 * {@code GaugedFetcher} throttle-retry loop until an external SIGKILL.
 *
 * <p>The engine's throttle wiring retries a thrown {@link ThrottleException} after an AIMD
 * backoff so no page is lost (THR-1). But a {@code --max-duration} deadline / SIGTERM only
 * FLIPS the cancellation flag (it does not interrupt), so a retry loop that never polls the
 * flag never unwinds — the terminal summary + checkpoint are never written and the process
 * is killable only by SIGKILL. The retry loop consults cancellation
 * between attempts and aborts, so the run unwinds with the contractual
 * {@link CancelledException} and the attributed {@link StopReason} the terminal summary
 * would name.
 *
 * <p><b>Adversarial:</b> a single worker (no sibling to fail-fast and {@code shutdownNow()}
 * the stuck fetch) over an ALWAYS-throttling fetcher — the exact shape a retry loop that never
 * polls cancellation hangs on. Without the in-loop cancellation check the bounded {@code await}
 * below times out (the run never finishes); with it the run aborts within a backoff window.
 */
final class ThrottledRunCancelsPromptlyTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "p11-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    @Timeout(30)
    void cancelDuringSustainedThrottle_abortsRetryLoopPromptly_surfacesAttributedStop(@TempDir Path dir)
            throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            keyspace.add(b(String.format("data/%05d", i)));
        }
        // EVERY fetch throttles: the store never serves a page, so a single worker is stuck in the
        // GaugedFetcher retry loop with nothing else to unblock it (no sibling failure ⇒ no
        // shutdownNow interrupt) — only an in-loop cancellation check can stop it.
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keyspace)
                .interceptor(new AlwaysThrottleInterceptor())
                .build();

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(dir.resolve("throttle.sqlite"))) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            int workers = 1;   // no sibling to fail-fast/shutdownNow the stuck fetch ⇒ a pure hang
                               // without the in-loop cancellation check
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS),
                    fetcher, store, workers, 24, seeds, FilterChain.EMPTY);

            RunContext ctx = RunContext.create();
            CancellationToken token = ctx.cancellation();

            // Fire the --max-duration-style cancel once the worker is provably spinning in the
            // retry loop (several throttled attempts made). This only flips the flag — exactly as
            // DeadlineCanceller does — it does NOT interrupt the worker.
            Thread canceller = new Thread(() -> {
                await().atMost(Duration.ofSeconds(10)).until(() -> fetcher.apiCalls() >= 3L);
                token.cancel(StopReason.MAX_DURATION);
            }, "p11-canceller");
            canceller.setDaemon(true);
            canceller.start();

            AtomicReference<Throwable> thrown = new AtomicReference<>();
            ExecutorService exec = Executors.newSingleThreadExecutor();
            Future<?> f = exec.submit(() -> {
                try {
                    // Local drain, NOT PipelineDrain: a Failure must be swallowed here so the typed
                    // cancellation cause surfaces from joinAllOrThrow, not rethrown by the consumer.
                    new Pipeline<PageBatch>(1000).run(ctx, engine, (c, in) -> {
                        while (true) {
                            Msg<PageBatch> msg = in.receive();
                            if (msg instanceof End<PageBatch>) {
                                return;
                            }
                            if (msg instanceof Failure<PageBatch>) {
                                return;   // let joinAllOrThrow surface the producer's typed cause
                            }
                            if (msg instanceof Item<PageBatch>) {
                                // no keys will ever be emitted under sustained throttle
                            }
                        }
                    });
                } catch (Throwable t) {
                    thrown.set(t);
                }
            });
            try {
                // BOUNDED liveness wait: a retry loop that ignores cancellation never finishes ⇒
                // this times out and fails the test, fast — instead of hanging the suite.
                await().atMost(Duration.ofSeconds(15)).until(f::isDone);
            } finally {
                exec.shutdownNow();
                canceller.join(2000);
            }

            // The run unwound via the contractual CancelledException (not a hang, not a fatal
            // ListingException from a mishandled throttle) ...
            assertThat(thrown.get())
                    .as("sustained-throttle run aborts on cancel with CancelledException")
                    .isInstanceOf(CancelledException.class);
            // ... and the FIRST attributed reason stuck, so the terminal summary names max_duration
            // (the exit-code path maps that to a clean, resumable exit 0).
            assertThat(token.stopReason())
                    .as("stop_reason attributed to the --max-duration cancel")
                    .isEqualTo(StopReason.MAX_DURATION);
        }
    }

    /** Throttles EVERY fetch — the persistently-backing-off bucket a retry loop that never polls
     * cancellation hangs on. */
    private record AlwaysThrottleInterceptor() implements MockPageFetcher.PageInterceptor {
        @Override
        public ListPage intercept(PageRequest req, int callIndex, ListPage computed) throws ThrottleException {
            throw ThrottleException.slowDown("injected sustained SlowDown (call " + callIndex + ")");
        }
    }
}
