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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * <b>Hard-crash (kill&nbsp;-9) mid-run → resume exactly-once — deterministic stand-in.</b>
 *
 * <p>A true SIGKILL of a real {@code swath} subprocess against LocalStack is captured separately by
 * {@code io.varve.swath.cli.HardCrashSigkillResumeProcessIT}; that IT is currently blocked in this environment
 * by a pre-existing decode bug (the LocalStack listing path throws
 * {@code URLDecoder: Incomplete trailing escape} before any crash injection — the same failure that keeps
 * {@code Int12BrokenPipeProcessIT} red), and it is engine/product-code territory outside this test-only
 * change. This test is the <b>strongest deterministic equivalent</b>: it exercises the identical
 * durable-cursor recovery spine (Parquet finalize/discard + {@code loadResumable}) that a real SIGKILL
 * would, but drives the crash in-JVM so it runs in CI and is bit-reproducible.
 *
 * <p>What a hard crash means here (vs a clean {@code SIGINT}→130 shutdown, already covered elsewhere): the
 * injected {@link ListingException} aborts the run <b>mid-page with no graceful flush</b> — the open,
 * footerless Parquet part is abandoned exactly as {@code kill -9} would leave it on disk. Resume must
 * {@link ParquetResume#discardNonFinalized discard} that footerless part and re-list only the bounded tail,
 * and the final union must equal a clean non-resumed run <b>exactly once</b>:
 * {@code count(*) == count(DISTINCT key) == baseline}, no dropped key, no duplicate (I1/I6).
 *
 * <p>The crash point is <b>parameterized across a spread of page offsets</b> — early (before any part
 * finalizes, so nothing durable survives and the whole run re-lists) through late (several finalized parts
 * survive and only the tail re-lists) — the deterministic analog of "{@code kill -9} at an arbitrary byte
 * offset". Every offset must recover exactly-once.
 */
final class HardCrashResumeExactlyOnceTest {

    private static final int OBJECTS = 6_000;
    private static final int WORKERS = 4;
    private static final int MAX_KEYS = 32;
    private static final String ARGS_HASH = "hard-crash-33-hash";

    private static RunKey runKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet");
    }

    private static ListRunner.ParquetSpec spec() {
        // Small target bytes so parts rotate/finalize across the run: a late kill leaves finalized parts
        // durable, an early kill leaves none — both must resume exactly-once.
        return new ListRunner.ParquetSpec(
                new byte[0], 256, MAX_KEYS, FilterChain.EMPTY, 2, 1024, 16, ARGS_HASH, null, null, 0L, 0L, "");
    }

    @ParameterizedTest(name = "hard crash after {0} bulk pages")
    @ValueSource(ints = {3, 10, 25, 60})
    @Timeout(120)
    void hardCrashAtArbitraryOffset_resumeIsExactlyOnce(int killAfterBulkPages, @TempDir Path tmp)
            throws Exception {
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(OBJECTS);
        Path resumedDir = tmp.resolve("resumed");
        Path cleanDir = tmp.resolve("clean");
        Path db = tmp.resolve("c.sqlite");
        Files.createDirectories(resumedDir);
        Files.createDirectories(cleanDir);

        AtomicInteger bulkSeen = new AtomicInteger();
        AtomicBoolean killed = new AtomicBoolean();
        MockPageFetcher crashing = MockPageFetcher.builder()
                .keys(keyspace)
                .maxKeysCap(MAX_KEYS)
                .interceptor((req, idx, page) -> {
                    // Count only bulk data pages (probes use maxKeys==1); once enough have flowed, abort
                    // mid-run with no clean flush — the SIGKILL analog.
                    if (req.maxKeys() > 1 && bulkSeen.incrementAndGet() >= killAfterBulkPages
                            && killed.compareAndSet(false, true)) {
                        throw new ListingException("injected hard crash (kill -9 analog) after "
                                + killAfterBulkPages + " bulk pages");
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
                    RunContext.create(), crashing, resumedDir, spec(), store, run.id(),
                    WORKERS, initialSeeds, List.of()))
                    .as("the run aborts mid-page with no clean shutdown (hard crash)")
                    .isInstanceOf(ListingException.class);
            assertThat(killed).as("the deterministic hard-crash seam fired").isTrue();

            // The abandoned open part is footerless on disk; only finalized parts are durable. Resume
            // discards the footerless part and carries the finalized ones — exactly the CLI resume spine.
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
                    .as("resumed run is now output-complete").isEmpty();
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

    /** Exactly-once acceptance: no duplicate rows AND the same key set — count(*) == count(DISTINCT) == baseline. */
    private static void assertExactlyOnce(List<String> actual, List<String> expected) {
        assertThat(actual).as("no duplicate rows (count == count DISTINCT)").doesNotHaveDuplicates();
        assertThat(expected).as("expected side has no duplicate rows").doesNotHaveDuplicates();
        assertThat(actual.stream().sorted().toList())
                .as("same key set as the clean baseline (no dropped key, no duplicate)")
                .containsExactlyElementsOf(expected.stream().sorted().toList());
    }
}
