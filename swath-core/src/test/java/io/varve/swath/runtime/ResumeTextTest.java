/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.PartFinalize;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SplitSpec;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.CheckpointException;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.SwathException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.PageBatch;
import io.varve.swath.pipeline.Channel;
import io.varve.swath.pipeline.End;
import io.varve.swath.pipeline.Failure;
import io.varve.swath.pipeline.Item;
import io.varve.swath.pipeline.Msg;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * RES-1 (commit-before-emit, I1) and RES-2 (cursor-preserving revert, I5) for the
 * text sink, driven through {@link CheckpointedScanProducer} + the resume spine
 * with {@code MockPageFetcher} fault injection.
 */
final class ResumeTextTest {

    private static List<byte[]> keys(int n) {
        List<byte[]> ks = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ks.add(String.format("k%03d", i).getBytes(StandardCharsets.UTF_8));
        }
        return ks;
    }

    private static String s(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    private static RunKey runKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "h1",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    /**
     * A recording text consumer writing into an external {@code sink} (so partial
     * output survives a thrown crash); {@code onPage} runs <i>before</i> the page's
     * keys are appended and may assert or throw to model a crash.
     */
    private static final class Recorder implements Pipeline.Consumer<PageBatch> {
        private final List<String> sink;
        private final Consumer<PageBatch> onPage;

        Recorder(List<String> sink, Consumer<PageBatch> onPage) {
            this.sink = sink;
            this.onPage = onPage;
        }

        @Override
        public void consume(RunContext ctx, Channel<PageBatch> in) throws SwathException, InterruptedException {
            while (true) {
                Msg<PageBatch> msg = in.receive();
                switch (msg) {
                    case Item<PageBatch> it -> {
                        if (onPage != null) {
                            onPage.accept(it.value());
                        }
                        for (ListEntry e : it.value().entries()) {
                            sink.add(s(e.key().raw()));
                        }
                    }
                    case End<PageBatch> ignored -> {
                        return;
                    }
                    case Failure<PageBatch> f -> {
                        switch (f.cause()) {
                            case SwathException se -> throw se;
                            case InterruptedException ie -> throw ie;
                            case null -> throw new ListingException("upstream failure");
                            default -> throw new ListingException("upstream failure", f.cause());
                        }
                    }
                }
            }
        }
    }

    /** Run one checkpointed scan into {@code sink}; {@code capacity} bounds how far the producer leads. */
    private static void runInto(List<String> sink, long capacity, RunContext ctx, MockPageFetcher fetcher,
                                CheckpointStore store, Node node, Consumer<PageBatch> onPage)
            throws SwathException, InterruptedException {
        Recorder rec = new Recorder(sink, onPage);
        CheckpointedScanProducer producer = new CheckpointedScanProducer(
                fetcher, store, node, new byte[0], 5, FilterChain.EMPTY);
        new Pipeline<PageBatch>(capacity, PageBatch::entryCount).run(ctx, producer, rec);
    }

    @Test
    void res2_killMidRange_resumesFromCursor_noReListBeforeIt(@TempDir Path dir) throws Exception {
        List<byte[]> all = keys(20);
        // pageMax 5 ⇒ pages [k000..k004], [k005..k009], [k010..]. Fail the 3rd fetch (idx 2).
        MockPageFetcher faulty = MockPageFetcher.builder().keys(all).maxKeysCap(5)
                .interceptor((req, idx, page) -> {
                    if (idx == 2) {
                        throw new ListingException("injected mid-range kill");
                    }
                    return page;
                })
                .build();
        MockPageFetcher clean = MockPageFetcher.builder().keys(all).maxKeysCap(5).build();

        RunContext ctx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), false).getFirst();

            // Run 1 dies on the 3rd page — pages 1–2 committed + emitted.
            List<String> run1 = new ArrayList<>();
            assertThatThrownBy(() -> runInto(run1, 1000, ctx, faulty, store, node, null))
                    .isInstanceOf(ListingException.class);
            assertThat(run1).containsExactlyElementsOf(stringify(keys(10)));   // k000..k009

            // Resume: cursor preserved at k009 (I5); re-lists nothing ≤ k009.
            Node resumed = store.loadResumable(run.id(), false).getFirst();
            assertThat(resumed.cursor()).isEqualTo("k009".getBytes(StandardCharsets.UTF_8));
            List<String> run2 = new ArrayList<>();
            runInto(run2, 1000, ctx, clean, store, resumed, null);

            assertThat(run2.getFirst()).isEqualTo("k010");                    // first re-listed key > cursor
            List<String> union = new ArrayList<>(run1);
            union.addAll(run2);
            assertThat(union).containsExactlyElementsOf(stringify(all));      // == clean run, in order
            assertThat(union).doesNotHaveDuplicates();
        }
    }

    @Test
    void res1_commitBeforeEmit_everyEmittedPageWasCommittedFirst(@TempDir Path dir) throws Exception {
        List<byte[]> all = keys(23);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(all).maxKeysCap(5).build();
        RunContext ctx = RunContext.create();

        AtomicReference<byte[]> lastCommitted = new AtomicReference<>();
        try (SqliteCheckpointStore raw = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            CheckpointStore spy = commitSpy(raw, lastCommitted);
            RunMeta run = spy.openRun(runKey(), false, false);
            spy.insertNode(NodeSpec.rootRange(run.id()));
            Node node = spy.loadResumable(run.id(), false).getFirst();

            List<Boolean> violations = new ArrayList<>();
            List<String> out = new ArrayList<>();
            runInto(out, 1000, ctx, fetcher, spy, node, page -> {
                // The page reached the consumer ⇒ its commit must already be durable (I1):
                // the committed cursor leads (or equals) this page's last key.
                byte[] committed = lastCommitted.get();
                byte[] pageLast = page.entries().getLast().key().raw();
                if (committed == null || KeyBytes.compareUnsigned(committed, pageLast) < 0) {
                    violations.add(Boolean.TRUE);
                }
            });

            assertThat(violations).as("commit-before-emit violations").isEmpty();
            assertThat(out).containsExactlyElementsOf(stringify(all));
        }
    }

    @Test
    void res1_crashAfterCommit_resumeHasNoDuplicates_atMostOnce(@TempDir Path dir) throws Exception {
        List<byte[]> all = keys(50);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(all).maxKeysCap(5).build();
        MockPageFetcher fetcher2 = MockPageFetcher.builder().keys(all).maxKeysCap(5).build();
        RunContext ctx = RunContext.create();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), false).getFirst();

            // Crash output mid-stream (after committing/emitting some pages). A small queue
            // capacity keeps the producer from racing to completion before the crash, so the
            // committed-but-unemitted page is genuinely dropped (at-most-once), not re-listed.
            List<String> run1 = new ArrayList<>();
            boolean[] crashed = {false};
            assertThatThrownBy(() -> runInto(run1, 5, ctx, fetcher, store, node, page -> {
                if (run1.size() + page.entries().size() >= 12 && !crashed[0]) {
                    crashed[0] = true;
                    throw new RuntimeException("injected output crash");
                }
            })).isInstanceOf(RuntimeException.class);

            Node resumed = store.loadResumable(run.id(), false).getFirst();
            List<String> run2 = new ArrayList<>();
            runInto(run2, 1000, ctx, fetcher2, store, resumed, null);

            List<String> union = new ArrayList<>(run1);
            union.addAll(run2);
            assertThat(union).doesNotHaveDuplicates();                         // no re-listed page (no dup)
            assertThat(stringify(all)).containsAll(union);                     // emitted ⊆ clean run
            assertThat(run2).isNotEmpty();                                     // resume made progress
        }
    }

    private static List<String> stringify(List<byte[]> ks) {
        List<String> out = new ArrayList<>(ks.size());
        for (byte[] k : ks) {
            out.add(s(k));
        }
        return out;
    }

    /** A delegating store that records the latest committed cursor after each durable commit. */
    private static CheckpointStore commitSpy(CheckpointStore d, AtomicReference<byte[]> lastCommitted) {
        return new CheckpointStore() {
            @Override
            public RunMeta openRun(RunKey key, boolean resume, boolean restart)
                    throws CheckpointException, InvalidArgsException {
                return d.openRun(key, resume, restart);
            }

            @Override
            public long insertNode(NodeSpec spec) throws CheckpointException {
                return d.insertNode(spec);
            }

            @Override
            public List<Node> loadResumable(long runId, boolean fileSink)
                    throws CheckpointException {
                return d.loadResumable(runId, fileSink);
            }

            @Override
            public void commitPage(PageCommit c) throws CheckpointException {
                d.commitPage(c);                       // durable first …
                if (c.advanceTo() != null) {
                    lastCommitted.set(c.advanceTo());  // … then visible to a would-be emitter
                }
            }

            @Override
            public CompletableFuture<Void> commitPageAsync(PageCommit c)
                    throws CheckpointException {
                return d.commitPageAsync(c);
            }

            @Override
            public long splitNode(SplitSpec sspec) throws CheckpointException {
                return d.splitNode(sspec);
            }

            @Override
            public void partFinalized(PartFinalize f) throws CheckpointException {
                d.partFinalized(f);
            }

            @Override
            public List<PartRef> finalizedParts(long runId) throws CheckpointException {
                return d.finalizedParts(runId);
            }

            @Override
            public void markOutputComplete(long runId) throws CheckpointException {
                d.markOutputComplete(runId);
            }

            @Override
            public void markRunFinished(long runId, RunStatus status)
                    throws CheckpointException {
                d.markRunFinished(runId, status);
            }

            @Override
            public void close() throws CheckpointException {
                d.close();
            }
        };
    }
}
