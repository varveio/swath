/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.runtime.ArgsHashFields;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the directory-lifecycle safety behaviour: a fresh {@code swath list -o <dir>} refuses
 * rather than destroys when the directory holds an existing run or unowned files.
 *
 * <p>The matrix: absent/empty -> create+run; unfinished prior run -> refuse ("swath resume or --restart");
 * completed prior run -> refuse unless {@code --overwrite}/{@code --force}; foreign/non-empty ->
 * refuse, never write; damaged/foreign manifest -> refuse with a diagnostic. The unfinished/completed
 * refusals are the checkpoint-status gate in {@link
 * io.varve.swath.checkpoint.SqliteCheckpointStore#openRun}; the foreign/damaged refusals are
 * {@code ListCommand}'s pre-run dataset-dir guard; and {@link DatasetDirGuard#prepareDatasetForFreshRun} is
 * now ownership-bounded — {@code --restart} deletes ONLY swath-owned part files, never unowned ones.
 *
 * <p>No assertion here reaches past {@code cmd.call()}'s return value / thrown exception and the
 * filesystem/checkpoint-DB state left behind — never an internal method.
 */
final class DirectoryLifecycleCharacterizationTest {

    private static final String BUCKET = "bucket";
    private static final String PREFIX = "data/";
    private static final String NO_FILTER_SPEC =
            FilterSpecCodec.encode(null, null, null, null, null, null, null);

    private static String argsHash() {
        return ArgsHashFields.forListing("s3", "", BUCKET, PREFIX).hash();
    }

    private static RunKey key(String argsHash) {
        return new RunKey("s3", null, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8), argsHash,
                "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, OutputFormat.PARQUET.name());
    }

    private static ListCommand freshCommand(Path outputDir, Path checkpointDb, MockPageFetcher fetcher) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        cmd.checkpoint.location = checkpointDb.toString();
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = outputDir.toString();
        cmd.fetcherOverride = fetcher;
        return cmd;
    }

    private static MockPageFetcher fetcher(int count) {
        List<byte[]> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(String.format("data/key-%05d", i).getBytes(StandardCharsets.UTF_8));
        }
        return MockPageFetcher.builder().keys(keys).build();
    }

    // ---- matrix row 1: absent / empty -> create, run -------------------------------------------

    /**
     * An absent {@code -o} dir is created and the
     * run proceeds to completion, populating the standard marker set + {@code data/}.
     *
     * <p>Mutation: temporarily made {@link DatasetDirGuard#prepareDatasetForFreshRun} throw
     * unconditionally -> {@code cmd.call()} threw instead of returning {@code SUCCESS}; reverted.
     */
    @Test
    void absentDirIsCreatedAndTheRunProceeds(@TempDir Path root) throws Exception {
        Path outputDir = root.resolve("out");
        Path db = root.resolve("cp.sqlite");
        assertThat(outputDir).doesNotExist();

        ListCommand cmd = freshCommand(outputDir, db, fetcher(50));
        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);

        DatasetLayout layout = DatasetLayout.of(outputDir);
        assertThat(outputDir).isDirectory();
        assertThat(layout.success()).exists();
        assertThat(layout.manifest()).exists();
        assertThat(layout.state()).exists();
        assertThat(layout.dataParts()).isNotEmpty();
    }

    // ---- matrix row 2: valid swath run, UNFINISHED -> refuse ("swath resume or --restart") -------

    /**
     * A plain {@code swath list} (no {@code swath resume}, no {@code --restart}) against a
     * dir backed by a genuinely UNFINISHED run (an incomplete node, status still RUNNING in the
     * checkpoint) REFUSES — the checkpoint-status gate in {@code SqliteCheckpointStore#openRun} now
     * inspects {@code existing.status()} and, for a RUNNING run with none of resume/restart/overwrite,
     * throws steering the user to {@code swath resume} or {@code --restart}. The unfinished run's own
     * on-disk part is preserved (the refusal fires BEFORE any clear), so a resume can still continue
     * it, and the original {@code run_meta} row is untouched.
     *
     * <p>Mutation-verified: reverting the status gate (fresh-start branch skips the refusal) makes the
     * {@code assertThatThrownBy} go red — {@code cmd.call()} returns {@code SUCCESS} and wipes the part.
     */
    @Test
    void unfinishedPriorRunIsRefused(@TempDir Path root) throws Exception {
        Path outputDir = root.resolve("out");
        Path db = root.resolve("cp.sqlite");
        String hash = argsHash();

        // Plant a genuinely UNFINISHED run: a root node inserted, never committed, status RUNNING.
        long priorRunId;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key(hash), false, false);
            priorRunId = run.id();
            store.insertNode(NodeSpec.rootRange(run.id()));
            // No commitPage, no markRunFinished -- this run is mid-flight.
        }
        // The unfinished run's own on-disk footprint: identity marker + one part it had staged.
        DatasetLayout layout = DatasetLayout.of(outputDir);
        Files.createDirectories(layout.dataDir());
        Files.writeString(layout.state(), "{\"args_hash\":\"" + hash + "\",\"run_id\":" + priorRunId + "}");
        Path priorPart = layout.dataDir().resolve("part-w0-00000.parquet");
        Files.writeString(priorPart, "unfinished run's own in-flight part");

        ListCommand cmd = freshCommand(outputDir, db, fetcher(20));
        // An unfinished prior run refuses a plain fresh run and steers to swath resume / --restart,
        // rather than silently discarding it.
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("unfinished")
                .hasMessageContaining("--restart");

        // The unfinished run's own part is preserved for resume, not
        // wiped (the refusal fires before any dataset clear).
        assertThat(priorPart).exists();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta reopened = store.openRun(key(hash), true, false);
            // The original run row survives unchanged (still RUNNING,
            // same id) -- nothing replaced or discarded it.
            assertThat(reopened.status()).isEqualTo(RunStatus.RUNNING);
            assertThat(reopened.id()).isEqualTo(priorRunId);
        }
    }

    // ---- crash before first finalize: swath's OWN dir, a partial part but no markers -> resume ----

    /**
     * A hard crash (kill-9/OOM) after a run starts but before its first part finalizes leaves the
     * {@code -o} dir holding a swath-owned partial {@code part-*.parquet} under {@code data/} and NONE
     * of the markers a graceful finalize/completion writes (no {@code manifest.json}, no {@code
     * .swath-state.json}); the checkpoint DB still carries that run, RUNNING. A plain {@code swath list}
     * must recognize the directory as swath's OWN — not foreign — and fall through to the
     * checkpoint-status gate, which refuses the unfinished run and steers to {@code swath resume} /
     * {@code --restart}. And {@code --restart} must actually work: it discards the partial and runs
     * fresh, the documented recovery.
     *
     * <p>Mutation-verified against the old manifest/state-only ownership guard: it classified this dir
     * as foreign, so the plain-list case got the "not empty" foreign refusal (not the "unfinished"
     * steer) and {@code --restart} itself was refused with "not empty" before the checkpoint gate ran.
     * The assertions below distinguish the two messages, so both go red on the old guard.
     */
    @Test
    void crashedRunOwnPartialDirSteersToResumeAndRestartWorks(@TempDir Path root) throws Exception {
        Path outputDir = root.resolve("out");
        Path db = root.resolve("cp.sqlite");
        String hash = argsHash();

        // The crash left a RUNNING run in the checkpoint DB...
        long priorRunId;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key(hash), false, false);
            priorRunId = run.id();
            store.insertNode(NodeSpec.rootRange(run.id()));
            // No commitPage, no markRunFinished -- mid-flight when the process died.
        }
        // ...and on disk the early ownership identity plus a partial part under data/: none of the
        // finalize/completion markers.
        DatasetLayout layout = DatasetLayout.of(outputDir);
        Files.createDirectories(layout.dataDir());
        Manifest.writeState(outputDir, hash, priorRunId);
        Path partial = layout.dataDir().resolve("part-w0-00000.parquet");
        Files.writeString(partial, "a partial part the process was mid-write on when it was killed");
        assertThat(layout.manifest()).doesNotExist();
        assertThat(layout.state()).exists();

        // A plain fresh run recognizes its OWN crashed directory and steers to resume/--restart,
        // rather than the foreign "not empty" refusal.
        ListCommand refused = freshCommand(outputDir, db, fetcher(20));
        assertThatThrownBy(refused::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("unfinished")
                .hasMessageContaining("--restart")
                .hasMessageNotContaining("not empty");
        assertThat(partial).as("the plain-list refusal fires before any clear").exists();

        // --restart is the documented recovery: it discards the partial and runs fresh (exit 0).
        ListCommand restart = freshCommand(outputDir, db, fetcher(20));
        restart.checkpoint.restart = true;
        assertThat(restart.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(partial).as("--restart discarded the crashed run's partial part").doesNotExist();

        DatasetLayout after = DatasetLayout.of(outputDir);
        assertThat(after.success()).exists();
        assertThat(after.dataParts()).isNotEmpty();
    }

    /**
     * The {@code --sort} counterpart of the crash-before-finalize recovery: a hard crash before the
     * merge leaves the {@code -o} dir holding the early {@code .swath-state.json} ownership marker
     * and a {@code _staging/} directory with segment files, plus a RUNNING checkpoint row. The dir is
     * swath's OWN — its state marker predates the first segment — so a plain
     * {@code swath list --sort} must recognize it and fall through to the checkpoint-status gate
     * (refuse the unfinished run, steer to {@code swath resume} / {@code --restart}), NOT the foreign
     * "not empty" refusal. And {@code --restart} must run fresh, leaving no stale segment from the
     * discarded run: a fresh sorted run wipes the abandoned staging before listing begins.
     *
     * <p>Mutation-verified by omitting the early state marker: the guard classifies the directory as
     * foreign, so the plain-list case gets the "not empty" refusal instead of the "unfinished" steer.
     */
    @Test
    void crashedSortRunStagingDirSteersToResumeAndRestartRunsClean(@TempDir Path root) throws Exception {
        Path outputDir = root.resolve("out");
        Path db = root.resolve("cp.sqlite");
        String hash = argsHash();

        // The crash left a RUNNING run in the checkpoint DB, mid-listing.
        long priorRunId;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key(hash), false, false);
            priorRunId = run.id();
            store.insertNode(NodeSpec.rootRange(run.id()));
        }
        // On disk, the early ownership identity and sort staging dir with a segment: no manifest or
        // data/ parts.
        Files.createDirectories(outputDir);
        DatasetLayout layout = DatasetLayout.of(outputDir);
        Manifest.writeState(outputDir, hash, priorRunId);
        Path stagingDir = Files.createDirectories(outputDir.resolve(ListCommand.SORT_STAGING_DIR));
        Path staleSegment = stagingDir.resolve("seg-" + priorRunId + "-0.pageseg");
        Files.writeString(staleSegment, "a sort segment the crashed run had staged before the merge");
        assertThat(layout.manifest()).doesNotExist();
        assertThat(layout.state()).exists();

        // A plain fresh --sort run recognizes its OWN crashed staging dir and steers to
        // resume/--restart, rather than the foreign "not empty" refusal.
        ListCommand refused = freshCommand(outputDir, db, fetcher(30));
        refused.sorting.sort = true;
        refused.sorting.forceSort = true;
        assertThatThrownBy(refused::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("unfinished")
                .hasMessageContaining("--restart")
                .hasMessageNotContaining("not empty");
        assertThat(staleSegment).as("the plain-list refusal fires before any clear").exists();

        // --restart runs the sorted listing fresh (exit 0) and the abandoned run's staged segment is
        // swept, never merged into this run's output.
        ListCommand restart = freshCommand(outputDir, db, fetcher(30));
        restart.sorting.sort = true;
        restart.sorting.forceSort = true;
        restart.checkpoint.restart = true;
        assertThat(restart.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(staleSegment).as("--restart swept the discarded run's stale staging segment").doesNotExist();

        DatasetLayout after = DatasetLayout.of(outputDir);
        assertThat(after.success()).exists();
        assertThat(after.dataParts()).isNotEmpty();
    }

    // ---- --overwrite is scoped to COMPLETED: an unfinished run + --overwrite still refuses ---------

    /**
     * {@code --overwrite} discards a COMPLETED run only. An UNFINISHED (RUNNING) run plus {@code
     * --overwrite} still REFUSES and steers to {@code --restart}, so {@code --overwrite} never silently
     * throws away in-progress work. (The COMPLETED + {@code --overwrite} success path is covered by
     * {@link #completedPriorRunIsRefusedUnlessOverwrite}, which must stay green.)
     *
     * <p>Mutation-verified: widening the {@code openRun} overwrite scope back to any status makes this
     * refusal go red — {@code --overwrite} discards the RUNNING run and returns {@code SUCCESS}.
     */
    @Test
    void unfinishedRunWithOverwriteStillRefuses(@TempDir Path root) throws Exception {
        Path outputDir = root.resolve("out");
        Path db = root.resolve("cp.sqlite");
        String hash = argsHash();

        long priorRunId;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key(hash), false, false);
            priorRunId = run.id();
            store.insertNode(NodeSpec.rootRange(run.id()));
            // RUNNING, never finished.
        }
        DatasetLayout layout = DatasetLayout.of(outputDir);
        Files.createDirectories(layout.dataDir());
        Files.writeString(layout.state(), "{\"args_hash\":\"" + hash + "\",\"run_id\":" + priorRunId + "}");
        Path priorPart = layout.dataDir().resolve("part-w0-00000.parquet");
        Files.writeString(priorPart, "the unfinished run's in-flight part");

        ListCommand cmd = freshCommand(outputDir, db, fetcher(20));
        cmd.checkpoint.overwrite = true;
        // --overwrite may not discard an unfinished run: it still refuses and steers to --restart.
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("unfinished")
                .hasMessageContaining("--restart");

        // The in-flight part is untouched and the RUNNING run row survives.
        assertThat(priorPart).exists();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta reopened = store.openRun(key(hash), true, false);
            assertThat(reopened.status()).isEqualTo(RunStatus.RUNNING);
            assertThat(reopened.id()).isEqualTo(priorRunId);
        }
    }

    // ---- matrix row 3: valid swath run, COMPLETED -> refuse unless --overwrite/--force -----------

    /**
     * A plain {@code swath list} against a dir holding a genuinely COMPLETED prior run
     * (its own {@code _SUCCESS}, a committed node, {@code markRunFinished(COMPLETED)}) REFUSES,
     * steering to the new {@code --overwrite} flag; passing {@code --overwrite} is the sanctioned
     * escape hatch that discards the completed run and re-lists. The completed dataset is protected by
     * the refusal, and {@code --overwrite}'s discard is manifest-bounded (it removes the run's OWN
     * {@code part-*.parquet}, not unowned files).
     *
     * <p>Mutation-verified: reverting the status gate makes the {@code assertThatThrownBy} go red
     * (a plain run returns {@code SUCCESS}); dropping the {@code overwrite} pass-through into
     * {@code openRun} makes the {@code --overwrite} call itself refuse.
     */
    @Test
    void completedPriorRunIsRefusedUnlessOverwrite(@TempDir Path root) throws Exception {
        Path outputDir = root.resolve("out");
        Path db = root.resolve("cp.sqlite");
        String hash = argsHash();

        long priorRunId;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key(hash), false, false);
            priorRunId = run.id();
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "z".getBytes(StandardCharsets.UTF_8), true));
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
        }
        DatasetLayout layout = DatasetLayout.of(outputDir);
        Files.createDirectories(layout.dataDir());
        Files.writeString(layout.state(), "{\"args_hash\":\"" + hash + "\",\"run_id\":" + priorRunId + "}");
        Files.writeString(layout.success(), "");
        Manifest.write(outputDir, BUCKET, "message swath { required binary key; }",
                List.of(), false, null);
        Path priorPart = layout.dataDir().resolve("part-w0-00000.parquet");
        Files.writeString(priorPart, "the completed prior run's real output");

        // A completed prior run refuses a plain fresh run; the --overwrite/--force flag on
        // ListCommand is the only way through.
        ListCommand refused = freshCommand(outputDir, db, fetcher(20));
        assertThatThrownBy(refused::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("completed")
                .hasMessageContaining("--overwrite");
        // The completed run's real output is protected by the refusal.
        assertThat(priorPart).exists();

        // --overwrite is the escape hatch — it discards the
        // completed run and re-lists (exit 0), removing the run's OWN owned part.
        ListCommand overwrite = freshCommand(outputDir, db, fetcher(20));
        overwrite.checkpoint.overwrite = true;
        assertThat(overwrite.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(priorPart).as("--overwrite discarded the owned prior part").doesNotExist();
        assertThat(layout.dataParts()).isNotEmpty();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta reopened = store.openRun(key(hash), true, false);
            assertThat(reopened.status()).isEqualTo(RunStatus.COMPLETED);
        }
    }

    // ---- matrix row 4: foreign / non-empty dir (unowned files) -> refuse, never write -----------

    /**
     * A totally foreign {@code -o} dir (no matching checkpoint row, no valid swath
     * manifest/state — just unowned files) is REFUSED by {@code ListCommand}'s pre-run dataset-dir
     * guard before anything is written. No unowned file is touched, at the ROOT or inside {@code
     * data/}, and no swath marker is created.
     *
     * <p>Mutation-verified: reverting the foreign-dir guard makes the {@code assertThatThrownBy} go
     * red — the run proceeds to {@code SUCCESS} and the data/ sweep deletes {@code dataJunk}.
     */
    @Test
    void foreignNonEmptyDirIsRefusedAndNeverWritten(@TempDir Path root) throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("out"));
        Path db = root.resolve("cp.sqlite");   // no matching row: a genuinely foreign dir

        Path rootJunk = outputDir.resolve("README-not-ours.txt");
        Files.writeString(rootJunk, "unowned file living at the dataset root");
        Path dataDir = Files.createDirectories(outputDir.resolve("data"));
        Path dataJunk = dataDir.resolve("not-a-swath-part.bin");
        Files.writeString(dataJunk, "unowned file sitting inside data/, never in any manifest");

        ListCommand cmd = freshCommand(outputDir, db, fetcher(20));
        // A foreign non-empty dir is refused, never written into.
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("not empty");

        // Both unowned files survive -- refused before any clear;
        // no swath dataset marker was written.
        assertThat(rootJunk).exists();
        assertThat(dataJunk).exists();
        assertThat(DatasetLayout.of(outputDir).success()).doesNotExist();
    }

    // ---- matrix row 5: damaged / foreign manifest.json -> spec says refuse with a diagnostic -----

    /**
     * A corrupt, non-JSON {@code manifest.json} is parsed by the pre-run dataset-dir guard
     * ({@link io.varve.swath.output.parquet.Manifest#probe}) and REFUSED with a clear diagnostic
     * naming {@code manifest.json} — never silently deleted and replaced. The damaged content is left
     * on disk for the user to inspect.
     *
     * <p>Mutation-verified: reverting the DAMAGED-manifest branch of the guard makes the {@code
     * assertThatThrownBy} go red — the run proceeds and republishes over the corrupt manifest.
     */
    @Test
    void damagedManifestIsRefusedWithADiagnostic(@TempDir Path root) throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("out"));
        Path db = root.resolve("cp.sqlite");   // no matching row

        DatasetLayout layout = DatasetLayout.of(outputDir);
        Path manifest = layout.manifest();
        Files.writeString(manifest, "{ this is not valid json at all !!");

        ListCommand cmd = freshCommand(outputDir, db, fetcher(20));
        // A damaged/foreign manifest refuses with a diagnostic.
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("manifest.json")
                .hasMessageContaining("damaged");

        // The damaged content is preserved for inspection, not
        // silently overwritten by this run's own manifest.
        assertThat(Files.readString(manifest)).contains("this is not valid json");
    }

    // ---- --restart's blast radius: bounded to swath-OWNED files; unowned survive ----------------

    /**
     * The precise {@code --restart} blast-radius, now BOUNDED
     * ({@code DatasetDirGuard#prepareDatasetForFreshRun} is ownership-bounded): plant a dir holding BOTH a
     * real swath-owned footprint (matching checkpoint identity, markers, a real {@code
     * part-*.parquet}) AND an unowned file at two locations (root and inside {@code data/}), run
     * {@code --restart}, and assert exactly what survives. The swath-owned part is discarded; the
     * unowned files — inside {@code data/} AND at the root — SURVIVE. The data/ survival is the
     * single most important assertion: it is the data-loss hole this guard closes.
     *
     * <p>Mutation-verified: reverting {@code prepareDatasetForFreshRun} to an unconditional {@code
     * data/} sweep makes the "{@code dataJunk} survives" assertion go red (the unowned file is
     * destroyed again).
     */
    @Test
    void restartDeletesOnlySwathOwnedFilesUnownedSurvive(@TempDir Path root) throws Exception {
        Path outputDir = root.resolve("out");
        Path db = root.resolve("cp.sqlite");
        String hash = argsHash();

        long priorRunId;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key(hash), false, false);
            priorRunId = run.id();
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "z".getBytes(StandardCharsets.UTF_8), true));
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
        }
        DatasetLayout layout = DatasetLayout.of(outputDir);
        Files.createDirectories(layout.dataDir());
        Files.writeString(layout.state(), "{\"args_hash\":\"" + hash + "\",\"run_id\":" + priorRunId + "}");
        Files.writeString(layout.success(), "");
        Manifest.write(outputDir, BUCKET, "message swath { required binary key; }",
                List.of(), false, null);
        Path ownedPart = layout.dataDir().resolve("part-w0-00000.parquet");
        Files.writeString(ownedPart, "swath-owned part from the run --restart is discarding");
        Path rootJunk = outputDir.resolve("notes.txt");
        Files.writeString(rootJunk, "an unowned file dropped at the dataset root by something else");
        Path dataJunk = layout.dataDir().resolve("unrelated.bin");
        Files.writeString(dataJunk, "an unowned file dropped inside data/ by something else");

        ListCommand cmd = freshCommand(outputDir, db, fetcher(20));
        cmd.checkpoint.restart = true;

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);

        assertThat(ownedPart).as("the swath-owned part --restart discards: gone").doesNotExist();
        // The data-loss fix: an unowned file inside data/ survives
        // --restart; the blast radius is bounded to swath-owned part files, never arbitrary data/.
        assertThat(dataJunk).exists();
        assertThat(rootJunk).as("the unowned file at the dataset root: survives").exists();
    }
}
