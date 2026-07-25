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
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Directory-dataset ownership and lifecycle authority for {@code swath list}'s Parquet
 * directory destination — the one place that decides whether swath may write into, clear, or
 * finalize an {@code -o} directory, and the data-loss bound on what it is ever allowed to delete.
 *
 * <h2>The ownership model</h2>
 * A directory is a swath dataset when it carries swath's own markers under the
 * {@link io.varve.swath.output.parquet.DatasetLayout} shape: the four root markers ({@code
 * _SUCCESS}, {@code .swath-state.json}, {@code manifest.json}, {@code symlink.txt}), a {@code
 * data/} directory whose entries are swath-authored part files, an internal {@code .swath/}
 * sidecar (the co-located checkpoint), and the {@code --sort} staging directory
 * ({@link ListCommand#SORT_STAGING_DIR}). A file under {@code data/} is swath-OWNED only under the
 * reserved part naming — {@code part-*.parquet} (finalized/committed parts, exactly what a valid
 * manifest records) and {@code part-*.parquet.tmp} (in-flight finals). Anything else is foreign.
 *
 * <p>The guard refuses a FOREIGN or DAMAGED directory rather than write into it: a plain file
 * where a dataset is expected, a {@code manifest.json} that is present but unparseable, or a
 * non-empty directory holding any entry that is neither a known marker nor a swath-owned part. A
 * COMPLETE dataset (a valid manifest + {@code _SUCCESS}) is likewise refused without {@code
 * --overwrite}/{@code --restart}, on the on-disk markers alone: the co-located checkpoint is
 * deleted on completion, so it can never be the completed-run gate.
 *
 * <h2>The manifest-bounded sweep</h2>
 * When a fresh (non-resumed) run reuses an {@code -o} directory that may already hold an older
 * dataset, {@link #clearDatasetForFreshRun} clears the prior dataset so the new run never inherits
 * it — but deletes ONLY swath-owned files (the four root markers and, under {@code data/}, only
 * part files matching the reserved naming). A foreign file dropped into {@code data/} SURVIVES:
 * {@code --restart}/{@code --overwrite}'s blast radius is what a valid swath manifest records,
 * never arbitrary contents. What the guard accepts as owned is exactly what the sweep would
 * remove, so the two share one ownership definition.
 */
final class DatasetDirGuard {

    private static final Logger log = LoggerFactory.getLogger(DatasetDirGuard.class);

    private DatasetDirGuard() {
    }

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
     *   <li>a non-empty dir holding foreign files — anything that is neither a valid swath dataset nor
     *       an entirely swath-shaped directory ({@link #isEntirelySwathShaped}) — never write into
     *       unowned files.</li>
     * </ul>
     * An absent or empty dir is created and run (the unchanged happy path). A dir that holds a valid
     * but UNFINISHED swath dataset (no {@code _SUCCESS}) — or that is entirely swath-shaped but carries
     * no readable marker, the shape a hard crash leaves before the first part finalizes — falls
     * through: its unfinished disposition is the checkpoint-status gate's job
     * ({@link io.varve.swath.checkpoint.SqliteCheckpointStore#openRun}), which steers to
     * swath resume / {@code --restart}. Deliberately careful with {@code --restart}/{@code --overwrite}:
     * those discard swath-OWNED state only, never a foreign dir.
     */
    static void guardFreshRunDatasetDir(Path outputDir, boolean overwrite, boolean restart)
            throws InvalidArgsException, IOException {
        if (!Files.exists(outputDir)) {
            return;   // absent -> created + run
        }
        if (!Files.isDirectory(outputDir)) {
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
            return;   // a valid but unfinished swath dataset -> the checkpoint-status gate governs it
        }
        if (isEmptyDir(outputDir)) {
            return;   // empty -> created + run
        }
        if (isEntirelySwathShaped(outputDir)) {
            // A directory swath itself created but whose readable markers were never written: a hard
            // crash (kill-9/OOM) after the run started but before the first part finalized leaves a
            // partial part file under data/ and none of the markers that only a graceful
            // finalize/completion writes. It is still swath's own, so fall through to the
            // checkpoint-status gate, which refuses the unfinished run with the resume/--restart steer
            // and lets --restart/--overwrite discard the partial.
            return;
        }
        throw new InvalidArgsException(outputDir + " is not empty and holds no swath dataset (no "
                + "valid manifest.json or .swath-state.json); refusing to write into it — point -o at "
                + "a new or empty directory, or remove its contents first");
    }

    private static boolean isEmptyDir(Path dir) throws IOException {
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.findAny().isEmpty();
        }
    }

    /**
     * Fresh-run ownership recognition for a directory that carries no readable {@code manifest.json}
     * or {@code .swath-state.json}: is every entry swath-shaped, so writing here would clobber only
     * swath's own output? Applies the marker/part-ownership definition from the class javadoc; a
     * single foreign entry — a non-marker file at the root, or a non-part entry under {@code data/}
     * ({@link #isSwathOwnedPart}) — returns false, and the caller refuses the directory as foreign.
     * Recognizing {@code _staging/} ({@link ListCommand#SORT_STAGING_DIR}) matters for crash recovery
     * of a sorted run: a hard crash before the merge leaves its segments there with no manifest/state,
     * and without this the dir would be misjudged foreign, blocking the {@code --restart} recovery
     * that a fresh sorted run performs by wiping the abandoned staging itself. The staging dir is
     * swath-owned by definition (the sorter is its only writer), so its contents need no per-entry
     * check here.
     */
    private static boolean isEntirelySwathShaped(Path outputDir) throws IOException {
        DatasetLayout layout =
                DatasetLayout.of(outputDir);
        Set<String> rootMarkers = Set.of(
                layout.success().getFileName().toString(),
                layout.state().getFileName().toString(),
                layout.manifest().getFileName().toString(),
                layout.symlink().getFileName().toString());
        String dataDirName = layout.dataDir().getFileName().toString();
        try (Stream<Path> entries = Files.list(outputDir)) {
            for (Path entry : entries.toList()) {
                String name = entry.getFileName().toString();
                if (rootMarkers.contains(name) || name.equals(".swath") || name.equals(ListCommand.SORT_STAGING_DIR)) {
                    continue;   // known marker, or a swath-owned sidecar dir (.swath/, the sort _staging/)
                }
                if (name.equals(dataDirName)) {
                    if (!dataDirHoldsOnlyOwnedParts(entry)) {
                        return false;
                    }
                    continue;
                }
                return false;   // a foreign entry at the root
            }
        }
        return true;
    }

    private static boolean dataDirHoldsOnlyOwnedParts(Path dataDir) throws IOException {
        if (!Files.isDirectory(dataDir)) {
            return false;   // data/ is a plain file -> foreign
        }
        try (Stream<Path> parts = Files.list(dataDir)) {
            return parts.allMatch(p -> isSwathOwnedPart(p.getFileName().toString()));
        }
    }

    /**
     * Manifest-bounding: before a FRESH (non-resumed) run writes into an {@code -o} dir that may
     * already hold an older dataset, clear the prior dataset so this run never inherits it — deletes
     * only the swath-owned markers and parts defined in the class javadoc; a foreign file under
     * {@code data/} survives (the data-loss bound this guard mandates).
     *
     * <p>Called ONLY when {@code run.resumed() == false} — a resume MUST keep its finalized parts and
     * prior markers for checkpoint-driven reentry. ({@code --restart}/{@code --overwrite} yield a
     * fresh, non-resumed run, so they correctly clear.) Never touches the {@code _staging} dir (the
     * sort path owns it). The foreign/damaged-dir refusal ({@link #guardFreshRunDatasetDir}) has
     * already run for a genuine CLI invocation, so by here the dataset is swath-owned or empty.
     */
    static void clearDatasetForFreshRun(Path outputDir) throws IOException {
        DatasetLayout layout =
                DatasetLayout.of(outputDir);
        Files.deleteIfExists(layout.success());
        Files.deleteIfExists(layout.state());
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
     * Ownership test for a file under {@code data/}: the reserved {@code part-*.parquet}/{@code
     * part-*.parquet.tmp} naming defined in the class javadoc. Anything else is unowned and MUST NOT
     * be deleted on a fresh/restart clear.
     */
    private static boolean isSwathOwnedPart(String fileName) {
        return fileName.startsWith("part-")
                && (fileName.endsWith(".parquet") || fileName.endsWith(".parquet.tmp"));
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
