/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.ListingException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.ParquetResume;
import io.varve.swath.output.parquet.PartInfo;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import io.varve.swath.testkit.RecordingSplitStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The process-internal approximation of a hard kill partway through a scan, then resume (the
 * in-process cousin of the true {@code kill -9} {@code HardCrashSigkillResumeProcessIT}).
 *
 * <p>A scan is pre-empted mid-page with no graceful flush (an operator
 * {@code kill -9}, an OOM, a spot-instance reclaim). The open, footerless output part is abandoned on
 * disk exactly as a SIGKILL would leave it; only durably-finalized parts and durable cursors survive.
 *
 * <p><b>Invariant this guards.</b> Resume must {@link ParquetResume#discardNonFinalized discard} the
 * footerless part, carry the finalized ones, re-list only the bounded durable tail, and CONVERGE
 * byte-exact to a clean non-resumed run: {@code count(*) == count(DISTINCT key) == baseline}, no
 * dropped key, no duplicate (exactly-once across the pre-empt boundary).
 *
 * <p>Deterministic (the crash fires at a fixed bulk-page offset, in-JVM, so it is bit-reproducible and
 * runs in CI). This reuses the exact durable-cursor recovery spine
 * ({@code ListRunner.runToParquetWorkStealing} + {@code loadResumable} + Parquet finalize/discard) that
 * {@code HardCrashResumeExactlyOnceTest} proves, narrowed to one offset for this composition-level test.
 */
@Tag("deep")
final class InProcessSigkillResumeTest {

    private static final int OBJECTS = 4_000;
    private static final int WORKERS = 4;
    private static final int MAX_KEYS = 32;
    private static final int KILL_AFTER_BULK_PAGES = 25;   // several parts finalized; only the tail re-lists
    private static final String ARGS_HASH = "regime-preempt-216-hash";

    private static RunKey runKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet");
    }

    private static ListRunner.ParquetSpec spec() {
        // Small target bytes so parts rotate/finalize across the run: the pre-empt leaves finalized parts
        // durable and one open footerless part to discard.
        return new ListRunner.ParquetSpec(
                new byte[0], 256, MAX_KEYS, FilterChain.EMPTY, 2, 1024, 16, ARGS_HASH, null, null, 0L, 0L, "");
    }

    @Test
    @Timeout(120)
    void sigkillMidRun_resumeConvergesByteExact(@TempDir Path tmp) throws Exception {
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(OBJECTS);
        Path resumedDir = tmp.resolve("resumed");
        Path cleanDir = tmp.resolve("clean");
        Path db = tmp.resolve("c.sqlite");
        Files.createDirectories(resumedDir);
        Files.createDirectories(cleanDir);

        AtomicInteger bulkSeen = new AtomicInteger();
        AtomicBoolean killed = new AtomicBoolean();
        MockPageFetcher preempting = MockPageFetcher.builder()
                .keys(keyspace)
                .maxKeysCap(MAX_KEYS)
                .interceptor((req, idx, page) -> {
                    // Count only bulk data pages (probes use maxKeys==1); once enough have flowed, abort
                    // mid-run with no clean flush — the in-process SIGKILL analog.
                    if (req.maxKeys() > 1 && bulkSeen.incrementAndGet() >= KILL_AFTER_BULK_PAGES
                            && killed.compareAndSet(false, true)) {
                        throw new ListingException(
                                "injected in-process SIGKILL (kill -9 analog) after "
                                        + KILL_AFTER_BULK_PAGES + " bulk pages");
                    }
                    return page;
                })
                .build();

        List<String> resumedUnion;
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(db)) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> initialSeeds = store.loadResumable(run.id(), true);
            assertThat(initialSeeds).hasSize(1);

            assertThatThrownBy(() -> new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), preempting, resumedDir, spec(), store, run.id(),
                    WORKERS, initialSeeds, List.of()))
                    .as("the run aborts mid-page with no clean shutdown (SIGKILL analog)")
                    .isInstanceOf(ListingException.class);
            assertThat(killed).as("the deterministic pre-empt seam fired").isTrue();

            // Resume: discard the footerless open part, carry the finalized ones, re-list the tail.
            RunMeta resumed = store.openRun(runKey(), true, false);
            assertThat(resumed.resumed()).isTrue();
            List<PartInfo> existing = reconcileResume(store, resumed.id(), resumedDir);
            List<Node> resumedSeeds = store.loadResumable(resumed.id(), true);
            assertThat(resumedSeeds).as("resume reopens the not-yet-durable tail").isNotEmpty();

            MockPageFetcher cleanFetcher =
                    MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();
            new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), cleanFetcher, resumedDir, spec(), store, resumed.id(),
                    WORKERS, resumedSeeds, existing);
            assertThat(store.loadResumable(resumed.id(), true))
                    .as("the resumed run is now output-complete").isEmpty();
            resumedUnion = allPartKeys(resumedDir);
        }

        List<String> cleanUnion = runClean(tmp.resolve("clean.sqlite"), cleanDir, keyspace);

        assertExactlyOnce(resumedUnion, cleanUnion);
        assertExactlyOnce(cleanUnion, expectedKeys(keyspace));
    }

    private static List<String> runClean(Path db, Path dir, List<byte[]> keyspace) throws Exception {
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(db)) {
            RunMeta run = sqlite.openRun(runKey(), false, false);
            sqlite.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = sqlite.loadResumable(run.id(), true);
            MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();
            new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), fetcher, dir, spec(), sqlite, run.id(), WORKERS, seeds, List.of());
            assertThat(sqlite.loadResumable(run.id(), true)).as("clean run is output-complete").isEmpty();
        }
        return allPartKeys(dir);
    }

    private static List<PartInfo> reconcileResume(RecordingSplitStore store, long runId, Path dir)
            throws Exception {
        List<PartRef> finalized = store.finalizedParts(runId);
        Set<String> names = finalized.stream().map(PartRef::path).collect(Collectors.toSet());
        ParquetResume.discardNonFinalized(dir, names);
        return finalized.stream()
                .map(p -> new PartInfo(p.path(), p.writerId(), p.rows(), p.bytes(), ""))
                .toList();
    }

    private static List<String> allPartKeys(Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (Path part : DatasetLayout.of(dir).dataParts()) {
            keys.addAll(ParquetReads.keys(part));
        }
        return keys;
    }

    private static List<String> expectedKeys(List<byte[]> keyspace) {
        return keyspace.stream().map(k -> new String(k, StandardCharsets.UTF_8)).toList();
    }

    /** Exactly-once: no duplicate rows AND the same key set — count(*) == count(DISTINCT) == baseline. */
    private static void assertExactlyOnce(List<String> actual, List<String> expected) {
        assertThat(actual).as("no duplicate rows (count == count DISTINCT)").doesNotHaveDuplicates();
        assertThat(expected).as("expected side has no duplicate rows").doesNotHaveDuplicates();
        assertThat(actual.stream().sorted().toList())
                .as("same key set as the clean baseline (no dropped key, no duplicate)")
                .containsExactlyElementsOf(expected.stream().sorted().toList());
    }
}
