/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.Manifest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Directory-dataset ownership and lifecycle authority for {@code swath list}'s Parquet, TSV, and
 * JSONL directory destinations — the one place that decides whether swath may write into, clear, or
 * finalize an {@code -o} directory, and the data-loss bound on what it is ever allowed to delete.
 *
 * <h2>The ownership model</h2>
 * A directory is a swath dataset when it carries durable swath ownership evidence under the
 * {@link io.varve.swath.output.parquet.DatasetLayout} shape: the four root markers ({@code
 * _SUCCESS}, {@code .swath-state.json}, {@code manifest.json}, {@code symlink.txt}), a {@code
 * data/} directory whose entries are swath-authored part files, an internal {@code .swath/}
 * sidecar (the co-located checkpoint), and the {@code --sort} staging directory
 * ({@link ListCommand#SORT_STAGING_DIR}). Before the first part or staging segment is written,
 * swath atomically writes {@code .swath-state.json}; a valid consumer manifest is also durable
 * ownership evidence. Part-looking filenames are never evidence by themselves. Only after the
 * directory is known to be swath-owned does the reserved part naming for Parquet, TSV, or JSONL
 * (optionally gzip/Zstandard-compressed) bound the files a fresh-run sweep may remove.
 *
 * <p>The guard refuses a FOREIGN or DAMAGED directory rather than write into it: a plain file
 * where a dataset is expected, a {@code manifest.json} that is present but unparseable, or a
 * non-empty directory with no valid ownership marker. Once ownership is established, unrelated
 * files are preserved rather than making the dataset unmanageable. A COMPLETE dataset (a valid
 * manifest + {@code _SUCCESS}) is likewise refused without {@code
 * --overwrite}/{@code --restart}, on the on-disk markers alone: the co-located checkpoint is
 * deleted on completion, so it can never be the completed-run gate.
 *
 * <h2>The manifest-bounded sweep</h2>
 * When a fresh (non-resumed) run reuses an {@code -o} directory that may already hold an older
 * dataset, {@link #prepareDatasetForFreshRun} clears the prior dataset so the new run never inherits
 * it — but deletes ONLY swath-owned files (the four root markers and, under {@code data/}, only
 * part files matching the reserved naming). A foreign file dropped into {@code data/} SURVIVES:
 * {@code --restart}/{@code --overwrite} cleanup requires durable ownership evidence and is limited
 * to managed markers plus the reserved part namespace, never arbitrary contents or an arbitrary
 * directory sweep.
 */
final class DatasetDirGuard {

    private static final Logger log = LoggerFactory.getLogger(DatasetDirGuard.class);
    private static final String ATOMIC_WRITE_TMP_SUFFIX = ".tmp";

    private DatasetDirGuard() {
    }

    enum FreshDirectoryState { EMPTY, OWNED }

    /**
     * Directory-lifecycle safety gate for a FRESH (non-resume) directory-dataset run: inspect the
     * {@code -o} dir BEFORE any checkpoint row, seed probe, or file write, and REFUSE (exit 2 via
     * {@link InvalidArgsException}) rather than write into a directory swath does not own. Refusable
     * states:
     * <ul>
     *   <li>a plain file where a dataset directory is expected;</li>
     *   <li>a DAMAGED/foreign {@code manifest.json} (present but unparseable) — refuse with a diagnostic;</li>
     *   <li>a COMPLETE swath dataset ({@link #isCompletedDataset}) without {@code --overwrite} — see
     *       the class javadoc for why this reads the on-disk markers, never the checkpoint;</li>
     *   <li>a non-empty dir holding no valid manifest or identity marker — part-looking filenames
     *       are never ownership evidence, so never write into or delete them.</li>
     * </ul>
     * An absent or empty dir is created and run (the unchanged happy path). A dir that holds a valid
     * but UNFINISHED swath dataset (no {@code _SUCCESS}) falls through: its unfinished disposition is
     * the checkpoint-status gate's job
     * ({@link io.varve.swath.checkpoint.SqliteCheckpointStore#openRun}), which steers to
     * swath resume / {@code --restart}. Deliberately careful with {@code --restart}/{@code --overwrite}:
     * those discard swath-OWNED state only, never a foreign dir.
     */
    static FreshDirectoryState guardFreshRunDatasetDir(
            Path outputDir, boolean overwrite, boolean restart)
            throws InvalidArgsException, IOException {
        requireNoManagedSymlinks(outputDir);
        if (!Files.exists(outputDir, LinkOption.NOFOLLOW_LINKS)) {
            return FreshDirectoryState.EMPTY;   // absent -> created + run
        }
        if (!Files.isDirectory(outputDir, LinkOption.NOFOLLOW_LINKS)) {
            throw new InvalidArgsException("-o " + outputDir + " is a file, not a dataset directory; "
                    + "point -o at a new or empty directory (or a .parquet single-file destination)");
        }
        Manifest.ManifestState manifestState =
                Manifest.probe(outputDir);
        if (manifestState == Manifest.ManifestState.DAMAGED) {
            throw new InvalidArgsException("the manifest.json in " + outputDir + " is damaged or "
                    + "foreign and could not be parsed; refusing to overwrite it — inspect the "
                    + "directory, then remove it or choose a different -o");
        }
        boolean ownedBySwath =
                manifestState == Manifest.ManifestState.VALID
                        || Manifest.readIdentity(outputDir).isPresent();
        if (ownedBySwath) {
            // Load-bearing: on-disk markers gate completion, never the checkpoint (class javadoc) —
            // the checkpoint is gone by completion, so without this check a completed dataset would
            // look unfinished, fall through, and get clobbered as fresh. --overwrite/--restart are the
            // sanctioned discard-and-relist escapes; an INCOMPLETE dataset (checkpoint still present)
            // falls through to openRun's unfinished-run refusal, which steers to swath resume / --restart.
            if (!overwrite && !restart && isCompletedDataset(outputDir)) {
                throw new InvalidArgsException("-o " + outputDir + " holds a completed swath dataset; "
                        + "refusing to overwrite it — pass --overwrite (--force) to discard it and "
                        + "re-list, or choose a different -o");
            }
            return FreshDirectoryState.OWNED;
        }
        if (isEmptyDir(outputDir)) {
            return FreshDirectoryState.EMPTY;
        }
        throw new InvalidArgsException(outputDir + " is not empty and holds no swath dataset (no "
                + "valid manifest.json or .swath-state.json); refusing to write into it — point -o at "
                + "a new or empty directory, or remove its contents first");
    }

    /**
     * Refuse a pre-existing symbolic link at the managed dataset root, at a managed directory, or
     * at a fixed file path that swath may open or truncate. The attributes are deliberately read
     * with {@link LinkOption#NOFOLLOW_LINKS}: following first and checking the target would turn a
     * link to a directory into an apparently-valid dataset and let validation, checkpoint creation, or a
     * lifecycle sweep escape the requested root.
     *
     * <p>This is the common precondition for every managed-path entry point in this class and for
     * {@link ListCommand}'s checkpoint opening. It protects the supported case of links planted
     * before swath starts. It is not a claim that a concurrently-hostile process cannot swap a
     * checked directory entry afterward; directory datasets have a single-process ownership model.
     */
    static void requireNoManagedSymlinks(Path outputDir)
            throws InvalidArgsException, IOException {
        BasicFileAttributes rootAttributes = readAttributesNoFollowIfExists(outputDir);
        if (rootAttributes == null) {
            return;
        }
        if (rootAttributes.isSymbolicLink()) {
            throw symlinkRefusal(outputDir, "dataset root");
        }
        if (!rootAttributes.isDirectory()) {
            return;   // guardFreshRunDatasetDir supplies the more specific file-vs-directory steer
        }
        DatasetLayout layout = DatasetLayout.of(outputDir);
        List<Path> managedDirectories = List.of(
                layout.dataDir(),
                outputDir.resolve(ListCommand.SORT_STAGING_DIR),
                outputDir.resolve(CheckpointOptions.COLOCATED_DIR));
        for (Path managedPath : managedDirectories) {
            BasicFileAttributes attributes = readAttributesNoFollowIfExists(managedPath);
            if (attributes != null && attributes.isSymbolicLink()) {
                throw symlinkRefusal(managedPath, "managed dataset directory");
            }
            if (attributes != null && !attributes.isDirectory()) {
                throw new InvalidArgsException("managed dataset path " + managedPath
                        + " is not a directory; refusing to use it as swath-managed storage — "
                        + "remove the file and restore a real directory, or choose a different -o directory");
            }
        }

        Path checkpoint = CheckpointOptions.CheckpointMode.colocatedCheckpoint(outputDir);
        for (Path sqliteFile : List.of(
                checkpoint,
                withSuffix(checkpoint, "-wal"),
                withSuffix(checkpoint, "-shm"),
                withSuffix(checkpoint, "-journal"))) {
            requireNotSymlink(sqliteFile, "managed checkpoint file");
        }

        // Atomic writers truncate their fixed .tmp path before renaming it over the final marker.
        // The final manifest/state markers are also read during validation and resume. Check both
        // forms without following them so neither read nor truncate can escape the dataset root.
        List<Path> rootArtifacts = List.of(
                layout.manifest(), layout.state(), layout.success(), layout.symlink());
        for (Path artifact : rootArtifacts) {
            requireNotSymlink(artifact, "managed dataset artifact");
            requireNotSymlink(withSuffix(artifact, ATOMIC_WRITE_TMP_SUFFIX),
                    "managed dataset atomic-write temporary file");
        }
        requireNotSymlink(
                outputDir.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME + ATOMIC_WRITE_TMP_SUFFIX),
                "managed dataset summary temporary file");
    }

    private static void requireNotSymlink(Path path, String role)
            throws InvalidArgsException, IOException {
        BasicFileAttributes attributes = readAttributesNoFollowIfExists(path);
        if (attributes != null && attributes.isSymbolicLink()) {
            throw symlinkRefusal(path, role);
        }
    }

    private static Path withSuffix(Path path, String suffix) {
        return path.resolveSibling(path.getFileName().toString() + suffix);
    }

    private static BasicFileAttributes readAttributesNoFollowIfExists(Path path) throws IOException {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    private static InvalidArgsException symlinkRefusal(Path path, String role) {
        return new InvalidArgsException(role + " " + path + " is a symbolic link; refusing to "
                + "access it as swath-managed storage — remove the link and restore a real directory, "
                + "or choose a different -o directory");
    }

    private static boolean isEmptyDir(Path dir) throws IOException {
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.findAny().isEmpty();
        }
    }

    /**
     * Establish durable ownership, then clear a prior dataset before a FRESH (non-resumed) run can
     * create its first part. The pre-run guard must have classified the directory as either empty or
     * already owned. The empty disposition is carried across checkpoint creation because an auto
     * checkpoint legitimately creates {@code .swath/} between the guard and this method.
     *
     * <p>Called ONLY when {@code run.resumed() == false} — a resume MUST keep its finalized parts and
     * prior markers for checkpoint-driven reentry. ({@code --restart}/{@code --overwrite} yield a
     * fresh, non-resumed run, so they correctly clear.) Never touches the {@code _staging} dir (the
     * sort path owns it). {@link Manifest#writeState} is atomic and fsynced; once it returns, a crash
     * at any later point leaves durable evidence that the part namespace belongs to swath.
     */
    static void prepareDatasetForFreshRun(Path outputDir, String argsHash, long runId,
                                          FreshDirectoryState guardedState)
            throws IOException, InvalidArgsException {
        requireNoManagedSymlinks(outputDir);
        boolean hasOwnershipEvidence = Manifest.probe(outputDir) == Manifest.ManifestState.VALID
                || Manifest.readIdentity(outputDir).isPresent();
        if (guardedState != FreshDirectoryState.EMPTY && !hasOwnershipEvidence) {
            throw new InvalidArgsException(outputDir + " has no durable swath ownership evidence; "
                    + "refusing to clear part-looking files");
        }

        // Load-bearing ordering: durable ownership exists before any filename-based deletion and
        // before any writer can create a part or staging segment.
        Manifest.writeState(outputDir, argsHash, runId);
        DatasetLayout layout =
                DatasetLayout.of(outputDir);
        Files.deleteIfExists(layout.success());
        Files.deleteIfExists(layout.manifest());
        Files.deleteIfExists(layout.symlink());
        Path dataDir = layout.dataDir();
        if (Files.isDirectory(dataDir)) {
            try (Stream<Path> entries = Files.list(dataDir)) {
                for (Path p : entries.toList()) {
                    if (isSwathOwnedPart(p.getFileName().toString())) {
                        Files.deleteIfExists(p);
                    }
                }
            }
        } else {
            Files.createDirectories(dataDir);
        }
    }

    /**
     * Ownership test for a file under {@code data/}: the reserved dataset-part naming defined in
     * the class javadoc. Anything else is unowned and MUST NOT
     * be deleted on a fresh/restart clear.
     */
    private static boolean isSwathOwnedPart(String fileName) {
        if (!fileName.startsWith("part-")) {
            return false;
        }
        String finalName = fileName.endsWith(".tmp")
                ? fileName.substring(0, fileName.length() - ".tmp".length()) : fileName;
        return finalName.endsWith(".parquet")
                || finalName.endsWith(".tsv") || finalName.endsWith(".tsv.gz") || finalName.endsWith(".tsv.zst")
                || finalName.endsWith(".jsonl") || finalName.endsWith(".jsonl.gz") || finalName.endsWith(".jsonl.zst");
    }

    /**
     * A directory dataset is COMPLETE when its {@code _SUCCESS} marker is present alongside a VALID
     * {@code manifest.json} — the checkpoint-independent completion authority described in the class
     * javadoc. Both the fresh-run completed-dataset refusal ({@link #guardFreshRunDatasetDir}) and
     * {@code swath resume}'s "already complete" path read these markers, never the checkpoint.
     */
    static boolean isCompletedDataset(Path dir) {
        return Files.exists(DatasetLayout.of(dir).success())
                && Manifest.probe(dir)
                        == Manifest.ManifestState.VALID;
    }

    /**
     * On clean completion of a co-located run, remove the run-handle checkpoint: the dataset
     * (manifest, summary, {@code _SUCCESS}, {@code data/}) is the durable artifact and the checkpoint
     * was only the resume ledger. Fires only for the co-located {@code <dir>/.swath/checkpoint.sqlite}
     * of a genuinely COMPLETE dataset ({@link #isCompletedDataset}) — never an explicit
     * {@code --checkpoint <path>} DB (kept as the user pinned it), never an ephemeral run, and never a
     * partial run (the caller gates this on a clean exit). The emptied {@code .swath/} dir is removed
     * too, so a completed dataset carries no checkpoint bookkeeping.
     */
    static void deleteColocatedRunHandleCheckpointIfComplete(Path dbPath,
            OutputOptions.DestinationKind resolvedKind, String destination) {
        if (dbPath == null || resolvedKind != OutputOptions.DestinationKind.DIRECTORY
                || destination == null) {
            return;
        }
        Path outputDir = Path.of(destination);
        try {
            requireNoManagedSymlinks(outputDir);
        } catch (InvalidArgsException | IOException e) {
            log.warn("colocated_checkpoint_delete_refused path={} message={}", dbPath, e.getMessage());
            return;
        }
        Path colocated = CheckpointOptions.CheckpointMode.colocatedCheckpoint(outputDir)
                .toAbsolutePath().normalize();
        if (!colocated.equals(dbPath.toAbsolutePath().normalize()) || !isCompletedDataset(outputDir)) {
            return;
        }
        try {
            Files.deleteIfExists(dbPath);
            // WAL mode leaves -wal/-shm siblings; SQLite removes them on a clean last-connection
            // close, but sweep them defensively so the .swath/ dir can be emptied and removed.
            Files.deleteIfExists(Path.of(dbPath + "-wal"));
            Files.deleteIfExists(Path.of(dbPath + "-shm"));
            Path swathDir = dbPath.getParent();
            if (swathDir != null && isEmptyDir(swathDir)) {
                Files.deleteIfExists(swathDir);
            }
        } catch (IOException e) {
            log.warn("colocated_checkpoint_delete_failed path={} message={} (dataset is complete; a "
                    + "leftover run-handle checkpoint is harmless)", dbPath, e.getMessage());
        }
    }
}
