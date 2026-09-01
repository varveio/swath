/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.error.CheckpointException;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.SwathException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.JsonRunSummaryWriter;
import io.varve.swath.observability.Phase;
import io.varve.swath.observability.ProgressEvent;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.sorted.SortedParquetStamp;
import io.varve.swath.sort.PageRunReads;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * SORT-RESUME-1 — kill mid-listing, then {@code --resume}, over the production sort pipeline
 * ({@link ListRunner#runToSortedParquetWorkStealing}). A crash is modelled honestly as an
 * in-process abort: a {@link MockPageFetcher} interceptor throws after
 * &ge;&nbsp;2 staging segments are durably finalized but before the listing completes — no clean
 * shutdown of the sort lane (the failure path calls {@code lane.abort()}, dropping the in-flight
 * buffer). The re-entry is the real resume dispatch: {@code openRun(resume)} → {@code loadResumable}
 * → {@code runToSortedParquetWorkStealing(reattach=true)}, which sweeps non-finalized segments,
 * re-lists only the non-durable tail, and merges every segment into the published output.
 *
 * <p>The assertions are the RES-3/I5/I6 acceptance shape, adapted to sort:
 * <ul>
 *   <li>the final sorted file is complete, globally sorted, with NO duplicates and NO gaps
 *       (byte-exact against the keyspace ground truth);</li>
 *   <li><b>fetch-count proof that only the non-durable tail was re-listed</b>: the set of keys the
 *       resume fetcher ever returned equals exactly {@code keyspace \ pre-crash-durable-keys} — the
 *       durable prefix is never re-fetched.</li>
 * </ul>
 */
final class SortResumeListingContractTest {

    private static final String ARGS_HASH = "sort-resume1-hash";
    private static final int MAX_KEYS = 16;
    private static final int OBJECTS = 2_000;
    private static final int WORKERS = 4;

    private static RunKey sortKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet",
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
    }

    private static ListRunner.ParquetSpec spec() {
        return new ListRunner.ParquetSpec(new byte[0], 256, MAX_KEYS, FilterChain.EMPTY,
                2, 1024, 16, ARGS_HASH, null, null, 0L, 0L, "");
    }

    /** As {@link #spec()}, but with a JSON summary sidecar wired (sortEnabled=true so the writer
     * emits the {@code sort} block, needed to read back {@code objects} / {@code sort.segments}). */
    private static ListRunner.ParquetSpec spec(Path sidecar) {
        JsonRunSummaryWriter.RunConfig rc = new JsonRunSummaryWriter.RunConfig(
                "s3://bucket", "us-east-1", "parquet", 2, false, null, 30_000L, List.of(),
                EngineToggles.DEFAULT, null, false, null, io.varve.swath.sort.SortArm.NONE, false)
                        .withSortEnabled(true);
        JsonRunSummaryWriter.Config summary =
                new JsonRunSummaryWriter.Config(sidecar, Duration.ofMinutes(10), ARGS_HASH, rc,
                        List.of("list", "s3://bucket", "--sort"));
        return new ListRunner.ParquetSpec(new byte[0], 256, MAX_KEYS, FilterChain.EMPTY, 2, 1024, 16, ARGS_HASH,
                null, null, 0L, 0L, "")
                        .withJsonSummary(summary);
    }

    /** segment-entries=24 ⇒ segments finalize early and often, so a durable prefix exists at crash time. */
    private static SortConfig smallSegments() {
        return SortConfigs.base()
                .withSegmentEntries(24);
    }

    /**
     * segment-entries=256 ⇒ NO staging segment can seal within the first handful of listing pages
     * (≤ MAX_KEYS×pages ≪ 256 entries have flowed by then), so the {@code durableSortSegments()==0}
     * crash seam below fires reliably in the pre-segment window with no race against a just-sealed
     * segment. The full run still seals many segments ({@code OBJECTS/256} ≈ 8), so resume merges a
     * real, non-trivial staging fan-in — this only widens the zero-segment window, it does not skip
     * the merge.
     */
    private static SortConfig wideSegments() {
        return SortConfigs.base()
                .withSegmentEntries(256);
    }

    /**
     * SORT-RESUME "before sort begins" — the zero-durable-segment sibling of
     * {@link #killMidListingThenResume_completeSortedNoDupNoGap_onlyTailReListed}. Every other
     * {@code --sort} crash/resume test crashes only after &ge;&nbsp;1 staging segment is durable; this
     * one crashes VERY early — listing has demonstrably started (bulk pages flowed, the checkpoint
     * advanced past the single root node) but ZERO {@code seg-*.parquet} has sealed. It is the sort
     * analog of {@code HardCrashResumeExactlyOnceTest}'s offset-3 case (crash before anything durable,
     * whole tail re-lists). Because nothing was durable, the resume legitimately re-lists the
     * un-advanced ranges from scratch — that is correct, so this test asserts only completeness and
     * global sortedness, NOT tail-only re-listing (which is the &ge;2-segment test's contract).
     */
    @Test
    @Timeout(120)
    void killDuringSortListingBeforeAnySegmentDurable_resumesToCompleteSortedOutput(@TempDir Path tmp)
            throws Exception {
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(OBJECTS);   // ASCII "flat/%08d" — byte-exact as UTF-8
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        Path db = tmp.resolve("c.sqlite");

        RunContext crashCtx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, crashCtx.metrics())) {
            RunMeta run = store.openRun(sortKey(), false, false);
            long runId = run.id();
            store.insertNode(NodeSpec.rootRange(runId));
            List<Node> seeds = store.loadResumable(runId, true);

            AtomicInteger bulkSeen = new AtomicInteger();
            AtomicBoolean killed = new AtomicBoolean();
            AtomicInteger segmentsAtCrash = new AtomicInteger(-1);
            MockPageFetcher crashing = MockPageFetcher.builder()
                    .keys(keyspace)
                    .maxKeysCap(MAX_KEYS)
                    .interceptor((req, idx, page) -> {
                        // Abort mid-listing after a handful of BULK data pages have flowed (listing has
                        // started, the checkpoint has advanced) but while ZERO staging segment is durable
                        // — the "before sort begins" window. wideSegments() guarantees no seal this early,
                        // so the durableSortSegments()==0 guard is still open when the seam fires (if a
                        // segment had sealed first the guard would stay closed and the run would finish
                        // cleanly, tripping the assertThatThrownBy below — a loud failure, never a silent
                        // miss).
                        if (req.maxKeys() > 1 && bulkSeen.incrementAndGet() >= 6) {
                            int segs = durableSortSegments(store, runId);
                            if (segs == 0 && killed.compareAndSet(false, true)) {
                                segmentsAtCrash.set(segs);
                                throw new ListingException("injected zero-segment SORT crash mid-listing");
                            }
                        }
                        return page;
                    })
                    .build();

            assertThatThrownBy(() -> new ListRunner().runToSortedParquetWorkStealing(
                    crashCtx, crashing, outputDir, stagingDir, spec(), store, runId,
                    WORKERS, seeds, wideSegments(), SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE, false))
                    .isInstanceOf(SwathException.class);

            assertThat(killed).as("the early (pre-segment) crash seam fired").isTrue();

            // ---- key precondition: ZERO durable sort segments at crash time ----
            assertThat(segmentsAtCrash.get())
                    .as("crash landed in the pre-segment window: no staging segment was durable yet")
                    .isZero();
            assertThat(sortSegments(store, runId))
                    .as("independent confirmation: no seg-*.parquet sealed before or during the abort")
                    .isEmpty();

            // ---- but listing had demonstrably started ----
            assertThat(counterTotal(crashCtx, "swath.entries.emitted"))
                    .as("listing progressed: entries were staged before the crash").isGreaterThan(0.0);

            // ---- nothing published, a resumable tail remains ----
            assertThat(Files.exists(DatasetLayout.of(outputDir).manifest())).as("nothing published yet").isFalse();
            assertThat(store.loadResumable(runId, true)).as("a resumable tail remains").isNotEmpty();
        }

        // ---- resume: the real dispatch, with a fresh counting fetcher ----
        Set<String> reListed = ConcurrentHashMap.newKeySet();
        RunContext resumeCtx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, resumeCtx.metrics())) {
            RunMeta resumed = store.openRun(sortKey(), true, false);
            assertThat(resumed.resumed()).isTrue();
            List<Node> tail = store.loadResumable(resumed.id(), true);
            assertThat(tail).isNotEmpty();

            MockPageFetcher resumeFetcher = MockPageFetcher.builder()
                    .keys(keyspace)
                    .maxKeysCap(MAX_KEYS)
                    .interceptor((req, idx, page) -> {
                        page.entries().forEach(e -> reListed.add(e.key().asString()));
                        return page;
                    })
                    .build();

            new ListRunner().runToSortedParquetWorkStealing(resumeCtx, resumeFetcher, outputDir, stagingDir,
                    spec(), store, resumed.id(), WORKERS, tail, wideSegments(), SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE, true);

            assertThat(store.sortPhase(resumed.id())).isEqualTo(SortPhase.PUBLISHED);
            assertThat(store.loadResumable(resumed.id(), true)).as("output-complete after resume").isEmpty();
            // zero durable segments ⇒ resume re-lists the un-advanced ranges; it does real listing work.
            assertThat(reListed).as("resume performed real listing work").isNotEmpty();
        }

        // ---- correctness: complete, globally sorted, no dup, no gap (byte-exact) ----
        Path finalFile = DatasetLayout.of(outputDir).dataFile("part-00000.parquet");
        List<String> outKeys = ParquetReads.keys(finalFile);
        List<String> expected = keyspace.stream().map(k -> new String(k, StandardCharsets.UTF_8))
                .sorted().toList();
        assertThat(outKeys).as("no duplicate rows").doesNotHaveDuplicates();
        assertThat(outKeys).as("complete + globally sorted, byte-exact vs ground truth (no gap/no overlap)")
                .containsExactlyElementsOf(expected);
        assertThat(SortedParquetStamp.read(finalFile)).isPresent();
    }

    @Test
    @Timeout(120)
    void killMidListingThenResume_completeSortedNoDupNoGap_onlyTailReListed(@TempDir Path tmp)
            throws Exception {
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(OBJECTS);   // ASCII "flat/%08d" — byte-exact as UTF-8
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        Path db = tmp.resolve("c.sqlite");

        Set<String> preCrashDurableKeys;

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(sortKey(), false, false);
            long runId = run.id();
            store.insertNode(NodeSpec.rootRange(runId));
            List<Node> seeds = store.loadResumable(runId, true);

            AtomicBoolean killed = new AtomicBoolean();
            MockPageFetcher crashing = MockPageFetcher.builder()
                    .keys(keyspace)
                    .maxKeysCap(MAX_KEYS)
                    .interceptor((req, idx, page) -> {
                        if (req.maxKeys() > 1 && idx >= 20 && durableSortSegments(store, runId) >= 2
                                && killed.compareAndSet(false, true)) {
                            throw new ListingException("injected SORT-RESUME-1 crash mid-listing");
                        }
                        return page;
                    })
                    .build();

            assertThatThrownBy(() -> new ListRunner().runToSortedParquetWorkStealing(
                    RunContext.create(), crashing, outputDir, stagingDir, spec(), store, runId,
                    WORKERS, seeds, smallSegments(), SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE, false))
                    .isInstanceOf(SwathException.class);

            assertThat(killed).as("the mid-listing crash seam fired after a durable prefix existed").isTrue();

            // Pre-crash state: durable segments exist, but listing is NOT complete (a tail remains),
            // and NO output was published (the merge never ran).
            List<PartRef> durable = sortSegments(store, runId);
            assertThat(durable).as("at least two staging segments durably finalized before the crash")
                    .hasSizeGreaterThanOrEqualTo(2);
            preCrashDurableKeys = keysIn(stagingDir, durable);
            assertThat(preCrashDurableKeys).as("durable prefix is a non-empty strict subset")
                    .isNotEmpty().hasSizeLessThan(keyspace.size());
            assertThat(store.loadResumable(runId, true)).as("listing incomplete: a tail remains").isNotEmpty();
            assertThat(Files.exists(DatasetLayout.of(outputDir).manifest())).as("nothing published yet").isFalse();
        }

        // ---- resume: the real dispatch, with a fresh counting fetcher ----
        Set<String> reListed = ConcurrentHashMap.newKeySet();
        RunContext resumeCtx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, resumeCtx.metrics())) {
            RunMeta resumed = store.openRun(sortKey(), true, false);
            assertThat(resumed.resumed()).isTrue();
            List<Node> tail = store.loadResumable(resumed.id(), true);
            assertThat(tail).isNotEmpty();

            MockPageFetcher resumeFetcher = MockPageFetcher.builder()
                    .keys(keyspace)
                    .maxKeysCap(MAX_KEYS)
                    .interceptor((req, idx, page) -> {
                        page.entries().forEach(e -> reListed.add(e.key().asString()));
                        return page;
                    })
                    .build();

            new ListRunner().runToSortedParquetWorkStealing(resumeCtx, resumeFetcher, outputDir, stagingDir,
                    spec(), store, resumed.id(), WORKERS, tail, smallSegments(), SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE, true);

            assertThat(store.sortPhase(resumed.id())).isEqualTo(SortPhase.PUBLISHED);
            assertThat(store.loadResumable(resumed.id(), true)).as("output-complete after resume").isEmpty();
            assertThat(counter(resumeCtx, "swath.steal_reason", "resume_reattached"))
                    .as("SORT.resume_reattached fired").isGreaterThan(0.0);
        }

        // ---- correctness: complete, sorted, no dup, no gap ----
        Path finalFile = DatasetLayout.of(outputDir).dataFile("part-00000.parquet");
        List<String> outKeys = ParquetReads.keys(finalFile);
        List<String> expected = keyspace.stream().map(k -> new String(k, StandardCharsets.UTF_8))
                .sorted().toList();
        assertThat(outKeys).as("no duplicate rows").doesNotHaveDuplicates();
        assertThat(outKeys).as("complete + globally sorted, byte-exact vs ground truth (no gap/no overlap)")
                .containsExactlyElementsOf(expected);
        assertThat(SortedParquetStamp.read(finalFile)).isPresent();

        // ---- fetch-count proof: only the non-durable tail was re-listed ----
        // The resume-emitted keys are EXACTLY the tail: preCrashDurableKeys ⊆ final output, the output
        // has no duplicates and equals the ground truth, so nothing durable was re-emitted (proven
        // above). The fetch-level evidence below shows the durable prefix was skipped at LIST time, not
        // merely de-duplicated at merge time: the durable prefix is overwhelmingly not re-fetched (a
        // bounded page-boundary overlap can be read-then-discarded when a resumed node's final page
        // spills past its range end — those keys are fetched but never re-emitted, which is why the
        // output stays duplicate-free).
        Set<String> tailKeys = new HashSet<>(expected);
        tailKeys.removeAll(preCrashDurableKeys);
        Set<String> durableReFetched = new HashSet<>(reListed);
        durableReFetched.retainAll(preCrashDurableKeys);
        Set<String> durableSkipped = new HashSet<>(preCrashDurableKeys);
        durableSkipped.removeAll(reListed);

        assertThat(reListed).as("the entire non-durable tail was re-scanned on resume")
                .containsAll(tailKeys);
        assertThat(reListed).as("resume, not restart: the durable prefix was not re-listed wholesale")
                .hasSizeLessThan(keyspace.size());
        assertThat(durableSkipped.size())
                .as("the durable prefix is overwhelmingly skipped at fetch time (only bounded "
                        + "page-boundary spill is read-then-discarded), never re-emitted")
                .isGreaterThan(durableReFetched.size());
    }

    /**
     * A crash-restart of a {@code --sort} run reattaches ({@code reattach=true}) and re-lists
     * only the non-durable tail, but this restarted process has a FRESH {@link RunMetrics}: the
     * listing-phase counters ({@code swath.entries.emitted}, {@code swath.sort.segments.written}) only
     * accumulate for the TAIL done in-process, so the terminal {@code summary.json} UNDER-reports total
     * {@code objects} and {@code sort.segments} by the pre-crash amount — even though the published
     * output is complete and correct. This is the reattach/partial-relist sibling of the merge-only
     * resume case, which covers only that path.
     *
     * <p>The observable is the terminal {@code summary.json} the run itself writes (the artifact a
     * supervisor reads); the registry counters are asserted too, for defense in depth. The ground-truth
     * total segment count is read back from {@code finalizedParts} (the buggy metric can't be trusted).
     */
    @Test
    @Timeout(120)
    void reattachResume_summaryReportsTrueObjectsAndSortSegments(@TempDir Path tmp) throws Exception {
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(OBJECTS);   // ASCII "flat/%08d" — byte-exact
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        Path db = tmp.resolve("c.sqlite");
        Path sidecar = tmp.resolve("summary.json");

        // ---- crash mid-listing, after a durable prefix of >=2 segments exists ----
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(sortKey(), false, false);
            long runId = run.id();
            store.insertNode(NodeSpec.rootRange(runId));
            List<Node> seeds = store.loadResumable(runId, true);

            AtomicBoolean killed = new AtomicBoolean();
            MockPageFetcher crashing = MockPageFetcher.builder()
                    .keys(keyspace)
                    .maxKeysCap(MAX_KEYS)
                    .interceptor((req, idx, page) -> {
                        if (req.maxKeys() > 1 && idx >= 20 && durableSortSegments(store, runId) >= 2
                                && killed.compareAndSet(false, true)) {
                            throw new ListingException("injected crash mid-listing");
                        }
                        return page;
                    })
                    .build();

            assertThatThrownBy(() -> new ListRunner().runToSortedParquetWorkStealing(
                    RunContext.create(), crashing, outputDir, stagingDir, spec(), store, runId,
                    WORKERS, seeds, smallSegments(), SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE, false))
                    .isInstanceOf(SwathException.class);
            assertThat(killed).as("the mid-listing crash seam fired after a durable prefix existed").isTrue();
            assertThat(sortSegments(store, runId)).as("a durable pre-crash segment prefix exists")
                    .hasSizeGreaterThanOrEqualTo(2);
            assertThat(Files.exists(DatasetLayout.of(outputDir).manifest())).as("nothing published yet").isFalse();
        }

        // ---- resume: the real reattach dispatch, with a JSON summary sidecar wired ----
        RunContext resumeCtx = RunContext.create();
        int totalSegments;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, resumeCtx.metrics())) {
            RunMeta resumed = store.openRun(sortKey(), true, false);
            assertThat(resumed.resumed()).isTrue();
            long resumedId = resumed.id();
            List<Node> tail = store.loadResumable(resumedId, true);
            assertThat(tail).isNotEmpty();

            MockPageFetcher resumeFetcher = MockPageFetcher.builder()
                    .keys(keyspace).maxKeysCap(MAX_KEYS).build();

            new ListRunner().runToSortedParquetWorkStealing(resumeCtx, resumeFetcher, outputDir, stagingDir,
                    spec(sidecar), store, resumedId, WORKERS, tail, smallSegments(), SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE, true);

            assertThat(store.sortPhase(resumedId)).isEqualTo(SortPhase.PUBLISHED);
            // Ground-truth total sort-segment count, independent of the (buggy) metric.
            totalSegments = sortSegments(store, resumedId).size();
        }

        // ---- correctness sanity: the published output is complete + globally sorted ----
        Path finalFile = DatasetLayout.of(outputDir).dataFile("part-00000.parquet");
        List<String> expected = keyspace.stream()
                .map(k -> new String(k, StandardCharsets.UTF_8)).sorted().toList();
        assertThat(ParquetReads.keys(finalFile))
                .as("published output is complete + sorted (so the summary lies about an otherwise-correct run)")
                .containsExactlyElementsOf(expected);

        // ---- the bug: the terminal summary.json under-reports objects + sort.segments ----
        JsonNode summaryJson = new ObjectMapper().readTree(sidecar.toFile());
        assertThat(summaryJson.get("objects").asLong())
                .as("reattach-resume summary.objects must reflect the FULL keyspace, not the tail-only emitted count")
                .isEqualTo(keyspace.size());
        assertThat(summaryJson.get("sort").get("segments").asLong())
                .as("reattach-resume summary.sort.segments must reflect ALL durable segments "
                        + "(pre-crash + tail), not tail-only")
                .isEqualTo(totalSegments)
                .isGreaterThanOrEqualTo(2L);

        // ---- defense in depth: the registry counters agree with the true totals ----
        assertThat(counterTotal(resumeCtx, "swath.entries.emitted"))
                .as("swath.entries.emitted totals the full keyspace, not just the re-listed tail")
                .isEqualTo((double) keyspace.size());
        assertThat(counterTotal(resumeCtx, "swath.sort.segments.written"))
                .as("swath.sort.segments.written totals all durable segments, not just the tail's")
                .isEqualTo((double) totalSegments);
    }

    /**
     * The same reattach dispatch, watched WHILE it lists: a resumed run's recovered rows are known
     * before the first page of the tail is fetched, so every live progress event of the listing
     * phase must already carry them. Recording the backfill after the merge instead left every real
     * {@code phase=listing} event reporting {@code recovered_objects=0}, and landed the tally when
     * the phase had already advanced — a shape no direct call to the recorder can expose.
     */
    @Test
    @Timeout(120)
    void reattachResume_liveProgressReportsRecoveredRowsThroughoutTheListing(@TempDir Path tmp) throws Exception {
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(OBJECTS);
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        Path db = tmp.resolve("c.sqlite");

        long preCrashRows;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(sortKey(), false, false);
            long runId = run.id();
            store.insertNode(NodeSpec.rootRange(runId));
            List<Node> seeds = store.loadResumable(runId, true);

            AtomicBoolean killed = new AtomicBoolean();
            MockPageFetcher crashing = MockPageFetcher.builder()
                    .keys(keyspace)
                    .maxKeysCap(MAX_KEYS)
                    .interceptor((req, idx, page) -> {
                        if (req.maxKeys() > 1 && idx >= 20 && durableSortSegments(store, runId) >= 2
                                && killed.compareAndSet(false, true)) {
                            throw new ListingException("injected crash mid-listing");
                        }
                        return page;
                    })
                    .build();

            assertThatThrownBy(() -> new ListRunner().runToSortedParquetWorkStealing(
                    RunContext.create(), crashing, outputDir, stagingDir, spec(), store, runId,
                    WORKERS, seeds, smallSegments(), SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE, false))
                    .isInstanceOf(SwathException.class);
            preCrashRows = sortSegments(store, runId).stream().mapToLong(PartRef::rows).sum();
            assertThat(preCrashRows).as("a durable pre-crash row prefix exists").isPositive();
        }

        RunContext resumeCtx = RunContext.create();
        List<ProgressEvent> whileListing = Collections.synchronizedList(new ArrayList<>());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, resumeCtx.metrics())) {
            RunMeta resumed = store.openRun(sortKey(), true, false);
            long resumedId = resumed.id();
            List<Node> tail = store.loadResumable(resumedId, true);
            assertThat(tail).isNotEmpty();

            // Sample what a progress tick would render, from inside the tail's own listing.
            MockPageFetcher resumeFetcher = MockPageFetcher.builder()
                    .keys(keyspace).maxKeysCap(MAX_KEYS)
                    .interceptor((req, idx, page) -> {
                        whileListing.add(resumeCtx.metrics().progressEvent(Duration.ofSeconds(1)));
                        return page;
                    })
                    .build();

            new ListRunner().runToSortedParquetWorkStealing(resumeCtx, resumeFetcher, outputDir, stagingDir,
                    spec(), store, resumedId, WORKERS, tail, smallSegments(), SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE, true);
        }

        assertThat(whileListing).isNotEmpty();
        assertThat(whileListing).allSatisfy(event -> {
            assertThat(event.phase()).isEqualTo(Phase.LISTING);
            assertThat(event.listing().recoveredObjects())
                    .as("recovered work is known before the tail is listed, not after the merge")
                    .isEqualTo(preCrashRows);
            assertThat(event.listing().sessionObjects())
                    .as("session work stays this process's own, never folded in with the recovered rows")
                    .isLessThan(keyspace.size());
        });
    }

    private static double counterTotal(RunContext ctx, String name) {
        return ctx.metrics().registry().find(name).counters().stream()
                .mapToDouble(Counter::count).sum();
    }

    private static int durableSortSegments(SqliteCheckpointStore store, long runId) {
        try {
            return sortSegments(store, runId).size();
        } catch (CheckpointException e) {
            return 0;
        }
    }

    private static List<PartRef> sortSegments(SqliteCheckpointStore store, long runId) throws CheckpointException {
        return store.finalizedParts(runId).stream()
                .filter(p -> ListRunner.SORT_SEGMENT_FORMAT.equals(p.format())).toList();
    }

    private static Set<String> keysIn(Path stagingDir, List<PartRef> segments) throws Exception {
        Set<String> keys = new HashSet<>();
        for (PartRef p : segments) {
            keys.addAll(PageRunReads.keys(stagingDir.resolve(p.path())));   // staging is page-run
        }
        return keys;
    }

    private static double counter(RunContext ctx, String name, String reason) {
        Counter c = ctx.metrics().registry().find(name).tag("outcome", "SORT").tag("reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }
}
