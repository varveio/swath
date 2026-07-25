/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins the observable ordering of a FILE-kind (single-file text) destination's lifecycle — stage to
 * a temp sibling, stream the data into it, publish by atomic rename, and only THEN record the run
 * complete — plus the {@code --max-duration} timebox exit semantics.
 *
 * <p>Publication (`ListCommand` → {@code output.commitFileSink()}) happens INSIDE the engine call
 * ({@code ListRunner#runWorkStealing}): after the output writer closes cleanly and BEFORE
 * {@code CheckpointStore#markRunFinished(COMPLETED)} and the terminal summary. A publish that fails
 * therefore aborts completion — the run is recorded FAILED and no {@code completed=true} summary is
 * ever written.
 *
 * <p><b>Observation model.</b> A FILE-kind destination is only reachable with {@code
 * --checkpoint none} (see {@code SingleFileAtomicityAndResumeTest}), whose store is an in-memory
 * ephemeral SQLite DB — so the {@code run_meta} row {@code markRunFinished} writes cannot be read
 * back after the process/run. The observable stand-in used here is the run-summary sidecar
 * (an explicit {@code --report} path): the terminal {@code completed=true} record is written only
 * after publication succeeds, so "no sidecar claims completed=true while nothing is published" is
 * the observable shape of "publication precedes completion".
 */
final class FilePublicationOrderingCharacterizationTest {

    private static final String BUCKET = "bucket";
    private static final String PREFIX = "data/";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** One filesystem observation taken from inside a page fetch, i.e. mid-run. */
    private record MidRunView(int page, boolean destinationExists, int tempSiblings, long tempBytes) { }

    /**
     * Key count for the streaming observation. Sized well past the point where staging must be
     * visible mid-run rather than only at close: rows reach the sink per COMMITTED RANGE (I1
     * commit-before-emit), and the text writer buffers ~8 KiB, so a small run can legitimately
     * finish every fetch before the first byte lands on disk. At this size many ranges commit and
     * flush while pages are still being served, on any machine speed. (An observation must never
     * WAIT for those bytes: every mid-run observation runs on a fetch thread, so blocking them all
     * stops the ranges from ever completing — the writer starves and the wait deadlocks itself.)
     */
    private static final int STREAMING_KEYS = 20_000;

    // ---- ordering: stage → write → publish ---------------------------------------------------

    /**
     * Pins ordering points 1-3 at the observable layer: while the run is in flight the data is
     * accumulating in a temp sibling and the real destination path does not exist at all; the
     * destination appears only after the run, fully populated. No assertion here touches a private
     * method — every claim is "what is on disk, when".
     */
    @Test
    void dataStreamsIntoATempSiblingAndTheDestinationAppearsOnlyAfterTheRun(@TempDir Path dir)
            throws Exception {
        Path out = dir.resolve("listing.jsonl");
        List<MidRunView> views = Collections.synchronizedList(new ArrayList<>());

        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keys(STREAMING_KEYS))
                .maxKeysCap(100)   // many pages, so mid-run observations exist and the buffer flushes
                .interceptor((req, idx, page) -> {
                    views.add(observe(dir, out, idx));
                    return page;
                })
                .build();

        ListCommand cmd = freshCommand(out);
        cmd.fetcherOverride = fetcher;

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);

        assertThat(views).as("mid-run observations were taken").isNotEmpty();
        assertThat(views).as("the real destination path is NEVER observable mid-run — no partial, "
                        + "no zero-length placeholder: publication is a single rename at the end")
                .noneMatch(MidRunView::destinationExists);
        assertThat(views)
                .as("at most one hidden temp sibling stages the run at any moment")
                .allMatch(view -> view.tempSiblings() <= 1);
        assertThat(views)
                .as("listing data is written into the STAGED file, not the destination, before publish")
                .anyMatch(view -> view.tempSiblings() == 1 && view.tempBytes() > 0L);

        assertThat(Files.readAllLines(out)).hasSize(STREAMING_KEYS);
        assertThat(stagingSiblings(dir, out)).as("the rename consumes the temp sibling").isEmpty();
    }

    // ---- ordering: publication precedes completion --------------------------------------------

    /**
     * The load-bearing ordering pin. Publication is made to fail (the destination path is an
     * existing non-empty directory, so the rename cannot succeed) after a listing that finished
     * streaming normally. Because publication now runs INSIDE the completion commit — after the
     * writer closes but before the run is recorded complete — a failed publish aborts completion:
     * it surfaces as an {@code IOException}, the run is never recorded COMPLETE, and the run-summary
     * sidecar the process leaves behind is the {@code close()}-time partial ({@code completed=false}),
     * never a {@code completed=true} record. That is the observable signature of publication living
     * INSIDE the completion commit.
     */
    @Test
    void publicationFailureAbortsCompletion(@TempDir Path dir) throws Exception {
        // The destination path exists as a NON-EMPTY DIRECTORY: destination-kind inference is
        // extension-based, so this is still a FILE-kind run, but the final rename cannot land.
        Path out = dir.resolve("listing.jsonl");
        Files.createDirectories(out);
        Files.writeString(out.resolve("occupant.txt"), "blocks the rename\n");
        Path sidecar = dir.resolve("summary.json");

        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys(20)).build();

        ListCommand cmd = freshCommand(out);
        cmd.output.summaryJson = sidecar.toString();
        cmd.fetcherOverride = fetcher;

        Throwable thrown = catchThrowable(cmd::call);

        assertThat(thrown).as("a failed publish surfaces as an I/O failure").isInstanceOf(IOException.class);
        // The terminal completed=true record is written only after a successful publish, so the abort
        // must never leave one behind. The sidecar that survives is the close()-time partial, which
        // carries completed=false — an honest INCOMPLETE record, not a lie about completion.
        JsonNode summary = MAPPER.readTree(sidecar.toFile());
        JsonNode completed = summary.get("completed");
        // Strict accessor on purpose (repo guardrail 8): asBoolean() would render a TextNode
        // "false" as false, so a boolean->string schema drift would slip through silently. Pin the
        // NODE TYPE as well as the value, so this stays a real guard on the summary contract.
        assertThat(completed).as("the summary sidecar carries a `completed` field").isNotNull();
        assertThat(completed.isBoolean())
                .as("`completed` is a JSON boolean, not a stringified one")
                .isTrue();
        assertThat(completed.booleanValue())
                .as("a failed publish aborts completion — the run is NOT recorded complete, so no "
                        + "sidecar claims completed=true; publication is inside the completion commit")
                .isFalse();
        try (var entries = Files.list(out)) {
            assertThat(entries.map(p -> p.getFileName().toString()).toList())
                    .as("nothing was published into the destination path")
                    .containsExactly("occupant.txt");
        }
        assertThat(stagingSiblings(dir, out))
                .as("the failed publish still cleans its temp sibling (zero-litter contract)")
                .isEmpty();
    }

    // ---- timebox exit semantics ---------------------------------------------------------------

    /**
     * A {@code --max-duration} timebox stop exits <b>124</b> (a resumable partial, distinct from a
     * clean, complete exit 0 so a resume-until-done supervisor can tell the two apart) and publishes
     * nothing: the cancel unwinds out of the engine before the rename, so the destination is never
     * created and the staged temp is removed.
     *
     * <p>Note on coverage: whether the deadline lands before or after {@code openSink} is timing
     * dependent, so the "publishes nothing" assertions hold on both orderings (nothing staged yet,
     * or staged-then-cleaned). The exit code is the load-bearing pin here.
     */
    @Test
    void maxDurationTimeboxExits124AndPublishesNothing(@TempDir Path dir) throws Exception {
        Path out = dir.resolve("listing.jsonl");
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keys(400))
                .maxKeysCap(20)
                .interceptor((req, idx, page) -> {
                    if (idx >= 1) {
                        Thread.sleep(400);   // outlast the deadline below, on any machine speed
                    }
                    return page;
                })
                .build();

        ListCommand cmd = freshCommand(out);
        cmd.liveness.maxDuration = "250ms";
        cmd.fetcherOverride = fetcher;

        assertThat(cmd.call())
                .as("a MAX_DURATION stop is a resumable partial → exit 124, not a clean 0")
                .isEqualTo(ExitCodes.TIMEBOX);
        assertThat(out).as("a timeboxed partial is never published").doesNotExist();
        assertThat(stagingSiblings(dir, out)).as("the timeboxed run cleans its temp sibling").isEmpty();
    }

    // ---- helpers ------------------------------------------------------------------------------

    /** Takes one non-blocking mid-run observation from the fetch thread serving {@code page}. */
    private static MidRunView observe(Path dir, Path out, int page) {
        try {
            List<Path> siblings = stagingSiblings(dir, out);
            long bytes = siblings.isEmpty() ? 0L : Files.size(siblings.get(0));
            return new MidRunView(page, Files.exists(out), siblings.size(), bytes);
        } catch (Exception e) {
            throw new IllegalStateException("mid-run observation failed", e);
        }
    }

    private static List<Path> stagingSiblings(Path dir, Path out) throws IOException {
        String prefix = "." + out.getFileName() + ".swath.";
        try (var paths = Files.list(dir)) {
            return paths.filter(path -> path.getFileName().toString().startsWith(prefix)
                            && path.getFileName().toString().endsWith(".tmp"))
                    .toList();
        }
    }

    private static List<byte[]> keys(int count) {
        List<byte[]> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(String.format("data/key-%05d", i).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    private static ListCommand freshCommand(Path destination) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = "http://localhost:4566";
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        cmd.checkpoint.location = "none";   // FILE-kind destinations are creatable only here
        cmd.output.format = OutputFormat.JSONL;
        cmd.output.destination = destination.toString();
        return cmd;
    }
}
