/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.ListingException;
import io.varve.swath.filter.ExcludeRegexFilter;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.ParquetResume;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.output.parquet.PartInfo;
import io.varve.swath.output.parquet.PartWriter;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * RES-4: kill after part-file pages but before the footer ⇒ the
 * non-finalized part is discarded and its node re-listed; the Parquet union equals
 * the clean run with no duplicate rows (exactly-once for file sinks, I6, via the
 * {@code durable_cursor} two-cursor model).
 */
final class ResumeParquetTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static List<byte[]> keys(int n) {
        List<byte[]> ks = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ks.add(String.format("k%04d", i).getBytes(StandardCharsets.UTF_8));
        }
        return ks;
    }

    private static RunKey runKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "h1",
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet");
    }

    private static ListRunner.ParquetSpec spec(long partBytes) {
        // numWriters 1 ⇒ a single ordered part stream (deterministic for the union check).
        return new ListRunner.ParquetSpec(new byte[0], 1000, 5, FilterChain.EMPTY,
                1, partBytes, 64, "h1", null, null, 0L, 0L, "");
    }

    private static List<String> allPartKeys(Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (Path part : DatasetLayout.of(dir).dataParts()) {
            keys.addAll(ParquetReads.keys(part));
        }
        return keys;
    }

    private static List<String> manifestPartKeys(Path dir) throws IOException {
        JsonNode files = MAPPER.readTree(DatasetLayout.of(dir).manifest().toFile()).path("files");
        List<String> keys = new ArrayList<>();
        for (JsonNode file : files) {
            keys.add(file.path("key").textValue());
        }
        return keys;
    }

    private static List<String> retainedPartKeys(Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (Path part : DatasetLayout.of(dir).dataParts()) {
            keys.add(DatasetLayout.key(part.getFileName().toString()));
        }
        return keys;
    }

    /** Mirror the CLI's resume reconciliation: discard non-finalized parts; carry finalized to publication. */
    private static List<PartInfo> reconcileResume(SqliteCheckpointStore store, long runId, Path dir)
            throws Exception {
        List<PartRef> finalized = store.finalizedParts(runId);
        Set<String> names = finalized.stream().map(PartRef::path).collect(Collectors.toSet());
        ParquetResume.discardNonFinalized(dir, names);
        return finalized.stream()
                .map(p -> new PartInfo(p.path(), p.writerId(), p.rows(), p.bytes(), ""))
                .toList();
    }

    /**
     * Drop a real {@code part-*.parquet} on disk the way a kill-9 leaves one: a
     * {@link PartWriter} that wrote rows and was {@link PartWriter#discard() discard}ed,
     * so the handle is released <b>without</b> the finalizing fsync and the part is never
     * recorded in the checkpoint. Models the open part the graceful {@code pool.abort()}
     * never got to delete.
     */
    private static void dropNonFinalizedPart(Path path, List<byte[]> keys) throws Exception {
        // Model the kill-9 leftover deterministically: ensure the data/ directory exists before writing
        // the open part into it. The crashed run above creates data/ LAZILY, on its async writer lane's
        // first row — under cross-fork CPU contention the crash-abort can win that race, leaving data/
        // absent (a NoSuchFileException here on the part's parent). Creating it makes the setup
        // schedule-independent; it changes nothing the resume invariant below asserts (the leftover is
        // still discarded on resume, the union still equals the clean run).
        Files.createDirectories(path.getParent());
        PartWriter w = new PartWriter(path, ParquetSchema.canonical());
        for (byte[] k : keys) {
            w.write(ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.of(k), 1L, 1_700_000_000_000_000L,
                    "etag", "STANDARD", null, true, null, null));
        }
        w.discard();
    }

    @Test
    void res4_killMidRun_nonFinalizedPartDiscarded_unionEqualsCleanRun(@TempDir Path tmp) throws Exception {
        List<byte[]> all = keys(60);
        List<String> clean = all.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList();
        Path dir = tmp.resolve("out");
        Files.createDirectories(dir);

        // Crash on the 5th fetch (idx 4); large part target ⇒ nothing rotated, so all
        // written rows live in the single OPEN (non-finalized) part — discarded on abort.
        MockPageFetcher faulty = MockPageFetcher.builder().keys(all).maxKeysCap(5)
                .interceptor((req, idx, page) -> {
                    if (idx == 4) {
                        throw new ListingException("injected mid-run kill");
                    }
                    return page;
                }).build();
        MockPageFetcher clean2 = MockPageFetcher.builder().keys(all).maxKeysCap(5).build();
        RunContext ctx = RunContext.create();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(tmp.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), true).getFirst();

            assertThatThrownBy(() -> new ListRunner().runToParquetCheckpointed(
                    ctx, faulty, dir, spec(Long.MAX_VALUE), store, run.id(), node, List.of()))
                    .isInstanceOf(ListingException.class);

            // Nothing finalized ⇒ durable_cursor stayed ⊥ ⇒ resume re-lists everything.
            assertThat(store.finalizedParts(run.id())).isEmpty();
            List<PartInfo> existing = reconcileResume(store, run.id(), dir);
            Node resumed = store.loadResumable(run.id(), true).getFirst();
            new ListRunner().runToParquetCheckpointed(
                    ctx, clean2, dir, spec(Long.MAX_VALUE), store, run.id(), resumed, existing);
        }

        List<String> union = allPartKeys(dir);
        assertThat(union).doesNotHaveDuplicates();
        assertThat(union).containsExactlyInAnyOrderElementsOf(clean);   // exactly-once: nothing lost or duplicated
    }

    @Test
    void res4_killAfterRotation_finalizedPartsSurvive_tailReListed(@TempDir Path tmp) throws Exception {
        List<byte[]> all = keys(60);
        List<String> clean = all.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList();
        Path dir = tmp.resolve("out");
        Files.createDirectories(dir);

        MockPageFetcher clean2 = MockPageFetcher.builder().keys(all).maxKeysCap(5).build();
        RunContext ctx = RunContext.create();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(tmp.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), true).getFirst();

            // Small part target ⇒ parts rotate (finalize) mid-run, advancing durable_cursor;
            // crash on the 9th fetch leaves earlier finalized parts durable + an open part to discard.
            // Finalization runs on the ParquetWriterPool's own writer-lane thread
            // (ParquetWriterPool#runLane, forked in the constructor), decoupled from this fetch
            // thread — so on a slow/contended runner the abort at fetch idx 9 can win the race
            // against the first part's finalize+record (ParquetWriterPool#finalizeCurrent), leaving
            // zero finalized parts (#36). Force the ordering the precondition below depends on
            // instead of hoping for it: block THIS fetch thread on the designated kill fetch until
            // the store itself reports a finalized part, then throw. This cannot deadlock — the
            // writer lane drains its already-submitted batches on its own thread regardless of
            // whether the producer ever reaches fetch idx 9.
            MockPageFetcher faulty = MockPageFetcher.builder().keys(all).maxKeysCap(5)
                    .interceptor((req, idx, page) -> {
                        if (idx == 9) {
                            await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20))
                                    .until(() -> !store.finalizedParts(run.id()).isEmpty());
                            throw new ListingException("injected late kill");
                        }
                        return page;
                    }).build();

            assertThatThrownBy(() -> new ListRunner().runToParquetCheckpointed(
                    ctx, faulty, dir, spec(256), store, run.id(), node, List.of()))
                    .isInstanceOf(ListingException.class);

            // At least one part finalized before the crash ⇒ durable_cursor advanced.
            List<PartRef> finalized = store.finalizedParts(run.id());
            assertThat(finalized).as("a part rotated + finalized before the kill").isNotEmpty();
            assertThat(DatasetLayout.of(dir).manifest())
                    .as("a crash can retain checkpoint-finalized parts without publishing a consumer snapshot")
                    .doesNotExist();
            Node before = store.loadResumable(run.id(), true).getFirst();
            assertThat(before.durableCursor()).as("durable_cursor advanced").isNotNull();

            List<PartInfo> existing = reconcileResume(store, run.id(), dir);
            Node resumed = store.loadResumable(run.id(), true).getFirst();
            new ListRunner().runToParquetCheckpointed(
                    ctx, clean2, dir, spec(256), store, run.id(), resumed, existing);
            assertThat(manifestPartKeys(dir))
                    .as("resume publishes every retained part in the consumer snapshot")
                    .containsExactlyInAnyOrderElementsOf(retainedPartKeys(dir));
        }

        List<String> union = allPartKeys(dir);
        assertThat(union).doesNotHaveDuplicates();
        assertThat(union).containsExactlyInAnyOrderElementsOf(clean);
    }

    /**
     * RES-4 / I6 hard-crash leftover: a kill-9 leaves a real, footered-but-un-fsynced
     * {@code part-*.parquet} on disk that the graceful {@code pool.abort()} never deleted.
     * Resume's reconciliation ({@link ParquetResume#discardNonFinalized}) must throw it
     * away — it isn't recorded finalized, so its rows aren't durable — and then re-list
     * the tail exactly once. The leftover holds the same tail rows resume re-lists, so if
     * it survives the union duplicates them: {@code doesNotHaveDuplicates} is the forcing
     * function that exactly-once (I6) was honored.
     */
    @Test
    void res4_hardCrashLeftover_discardedOnResume_unionEqualsCleanRun(@TempDir Path tmp) throws Exception {
        List<byte[]> all = keys(60);
        List<String> clean = all.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList();
        Path dir = tmp.resolve("out");
        Files.createDirectories(dir);

        MockPageFetcher faulty = MockPageFetcher.builder().keys(all).maxKeysCap(5)
                .interceptor((req, idx, page) -> {
                    if (idx == 4) {
                        throw new ListingException("injected mid-run kill");
                    }
                    return page;
                }).build();
        MockPageFetcher clean2 = MockPageFetcher.builder().keys(all).maxKeysCap(5).build();
        RunContext ctx = RunContext.create();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(tmp.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), true).getFirst();

            assertThatThrownBy(() -> new ListRunner().runToParquetCheckpointed(
                    ctx, faulty, dir, spec(Long.MAX_VALUE), store, run.id(), node, List.of()))
                    .isInstanceOf(ListingException.class);
            assertThat(store.finalizedParts(run.id())).isEmpty();   // nothing durable ⇒ full re-list

            // Model the kill-9: the open part graceful abort never deleted, dropped at the lane's
            // next part name (resume, with no rotation, only ever writes part-w0-00000.parquet, so
            // it never reuses/overwrites this one). Its rows overlap the tail resume re-lists.
            dropNonFinalizedPart(DatasetLayout.of(dir).dataFile("part-w0-00001.parquet"), all.subList(55, 60));

            List<PartInfo> existing = reconcileResume(store, run.id(), dir);   // → discardNonFinalized
            Node resumed = store.loadResumable(run.id(), true).getFirst();
            new ListRunner().runToParquetCheckpointed(
                    ctx, clean2, dir, spec(Long.MAX_VALUE), store, run.id(), resumed, existing);
        }

        List<String> union = allPartKeys(dir);
        assertThat(union).doesNotHaveDuplicates();   // leftover discarded ⇒ tail re-listed exactly once (I6)
        assertThat(union).containsExactlyInAnyOrderElementsOf(clean);
    }

    /**
     * Output-complete latch (I6, §4.5): a CLEAN checkpointed Parquet run whose final
     * listed page is ENTIRELY filtered out advances the raw {@code cursor} past the last
     * KEPT (durable) row. On a clean close {@code markOutputComplete} latches
     * {@code durable_cursor := cursor}, so a file-sink resume treats the node as done.
     * Without the latch {@code durable_cursor} would lag {@code cursor} and the finished
     * node would be reopened. The guard: {@code loadResumable(runId, true)} is EMPTY.
     */
    @Test
    void cleanRun_trailingFilteredPage_latchedOutputComplete_resumeIsNoOp(@TempDir Path tmp) throws Exception {
        List<byte[]> all = keys(60);
        Path dir = tmp.resolve("out");
        Files.createDirectories(dir);

        // Drop the top page (k0055..k0059): the final listed page is entirely filtered,
        // so the cursor advances past the last kept row (k0054).
        FilterChain trailingDrop = FilterChain.of(List.of(ExcludeRegexFilter.of("k005[5-9]")));
        ListRunner.ParquetSpec filteredSpec = new ListRunner.ParquetSpec(
                new byte[0], 1000, 5, trailingDrop, 1, Long.MAX_VALUE, 64, "h1", null, null, 0L, 0L, "");

        MockPageFetcher fetcher = MockPageFetcher.builder().keys(all).maxKeysCap(5).build();
        RunContext ctx = RunContext.create();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(tmp.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), true).getFirst();

            new ListRunner().runToParquetCheckpointed(
                    ctx, fetcher, dir, filteredSpec, store, run.id(), node, List.of());

            // The latch: even though cursor ran past the last kept row, a file-sink resume
            // finds nothing to do — the finished node is not reopened.
            assertThat(store.loadResumable(run.id(), true)).isEmpty();
        }

        // The kept subset (everything below the filtered tail) is exactly what landed in the parts.
        List<String> kept = all.subList(0, 55).stream()
                .map(b -> new String(b, StandardCharsets.UTF_8)).toList();
        assertThat(allPartKeys(dir)).containsExactlyInAnyOrderElementsOf(kept);
    }
}
