/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;

/**
 * Physical publication owner for one sorted dataset replacement.
 *
 * <p>It owns only sorter-local temporary files, completeness stamping/close, stale sweeps, atomic
 * part renames, output-directory durability, listener invocation, and final staging policy. The
 * listener remains the owner of consumer manifest/state/symlink/{@code _SUCCESS}; this class does
 * not know their format. {@link SortTransform} stays the public orchestration façade.
 */
final class DatasetPublisher {

    private final SortRun run;
    private final SortConfig config;
    private final SortMetrics metrics;
    private final SortedFileWriterFactory finalWriterFactory;
    private final MergeInputProfile inputProfile;
    private final PublicationStepHook publicationStepHook;
    // Deliberately supplied by SortTransform: extraction must not change the logger name carried by
    // existing sweep/retention diagnostics.
    private final Logger log;

    DatasetPublisher(SortRun run, PublicationStepHook publicationStepHook, Logger log) {
        this.run = run;
        this.config = run.config();
        this.metrics = run.metrics();
        this.finalWriterFactory = run.finalWriterFactory();
        this.inputProfile = run.inputProfile();
        this.publicationStepHook = publicationStepHook;
        this.log = log;
    }

    /** Resolve and validate diagnostic retention before any merge or publication mutation. */
    StagingReconciliation retainedOriginals(List<Path> stagingSegments, Path stagingDir)
            throws IOException {
        if (!config.stagingRetention().retainsOriginals()
                || inputProfile != MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES) {
            return null;
        }
        return StagingReconciliation.fromPaths(stagingDir, stagingSegments);
    }

    /** Sweep every disposable working namespace, then expose the first WP8 boundary. */
    void sweepWorking(Path outputDir, Path stagingDir) throws IOException {
        StagingReconciliation.sweepFinalTemporaries(stagingDir);
        StagingReconciliation.sweepFinalTemporaries(outputDir);
        Sweeps.sweep(stagingDir, stale -> { }, StagingNames.CASCADE_PAGE_RUN_GLOB,
                StagingNames.LEGACY_CASCADE_PARQUET_GLOB);
        Sweeps.sweep(stagingDir, stale -> { }, StagingNames.RANGE_TMP_GLOB,
                StagingNames.RANGE_PROOF_TMP_GLOB);
        publicationStep(PublicationStep.AFTER_WORKING_SWEEP);
    }

    PendingParts serialParts(Path outputDir, Path stagingDir) {
        return new PendingParts(outputDir, stagingDir, finalWriterFactory.forOutputSequence());
    }

    /**
     * Stamp and close every range-produced part in global order. An all-empty range fleet is
     * normalized to the same one-file empty dataset as the serial path.
     */
    PendingParts parallelParts(Path outputDir, Path stagingDir, List<Path> tmpFiles,
            List<SortedFileWriter> writers) throws IOException {
        List<SortedFileWriter> open = new ArrayList<>(writers);
        try {
            for (int i = 0; i < open.size(); i++) {
                open.get(i).setFileIndex(i + 1);
            }
            if (!open.isEmpty()) {
                open.getLast().markFinal();
            }
            RolledPartWriter.closeInOrder(open);
            open.clear();
        } catch (IOException | RuntimeException e) {
            try {
                RolledPartWriter.closeQuietly(open);
            } catch (IOException | RuntimeException releaseFailure) {
                e.addSuppressed(releaseFailure);
            }
            throw e;
        }

        PendingParts pending = new PendingParts(outputDir, stagingDir,
                finalWriterFactory.forOutputSequence());
        pending.tmpFiles.addAll(tmpFiles);
        pending.writers.addAll(writers);
        if (pending.tmpFiles.isEmpty()) {
            SortedFileWriter writer = pending.openNext();
            writer.markFinal();
            writer.close();
        } else {
            for (int i = 0; i < pending.tmpFiles.size(); i++) {
                pending.finalFiles.add(outputDir.resolve(StagingNames.finalPart(i)));
            }
        }
        allTmpPartsDurable();
        return pending;
    }

    /** Serial drain closes its writers itself; this records the same post-close boundary. */
    void allTmpPartsDurable() throws IOException {
        publicationStep(PublicationStep.AFTER_ALL_TMP_PARTS_DURABLE);
    }

    /** Complete the ordered physical publish and configured staging ownership policy. */
    void publish(PendingParts pending, long totalRows, PublishListener publishListener,
            List<Path> stagingSegments, StagingReconciliation retainedOriginals,
            List<Path> disposableIntermediates) throws IOException {
        cleanStaleFinals(pending.outputDir);
        publicationStep(PublicationStep.AFTER_STALE_FINAL_SWEEP);
        for (int i = 0; i < pending.tmpFiles.size(); i++) {
            atomicRename(pending.tmpFiles.get(i), pending.finalFiles.get(i));
            publicationStep(PublicationStep.AFTER_PART_RENAME, i);
        }
        Durability.directory(pending.outputDir);
        publicationStep(PublicationStep.AFTER_OUTPUT_DIRECTORY_SYNC);
        publishListener.onPublished(finalParts(pending.finalFiles, pending.writers), totalRows);
        CommittedPublicationCleanupException.Stage cleanupStage =
                CommittedPublicationCleanupException.Stage.AFTER_PUBLISH_LISTENER_HOOK;
        try {
            publicationStep(PublicationStep.AFTER_PUBLISH_LISTENER);

            cleanupStage = CommittedPublicationCleanupException.Stage.DISPOSABLE_INTERMEDIATE_CLEANUP;
            for (Path intermediate : disposableIntermediates) {
                Files.deleteIfExists(intermediate);
            }
            cleanupStage = CommittedPublicationCleanupException.Stage.ORIGINAL_STAGING_COMPLETION;
            completeOriginalStaging(stagingSegments, pending.stagingDir, retainedOriginals);
            cleanupStage = CommittedPublicationCleanupException.Stage.AFTER_STAGING_COMPLETION_HOOK;
            publicationStep(PublicationStep.AFTER_STAGING_COMPLETION);
        } catch (IOException | RuntimeException failure) {
            metrics.recordStealReason("SORT", "post_publish_cleanup_pending");
            log.warn("sort_post_publish_cleanup_pending publication_committed=true "
                            + "cleanup_pending=true stage={} output_dir={} staging_dir={} message={}",
                    cleanupStage.logValue(), pending.outputDir, pending.stagingDir,
                    failure.getMessage());
            throw new CommittedPublicationCleanupException(cleanupStage, failure);
        }
    }

    private void completeOriginalStaging(List<Path> stagingSegments, Path stagingDir,
            StagingReconciliation retainedOriginals) throws IOException {
        if (retainedOriginals != null) {
            StagingReconciliation.Result result = retainedOriginals.reconcile(stagingDir);
            metrics.recordStealReason("SORT", "staging_retained");
            log.info("sort_staging_retained source=merge path={} retained_segments={} removed_entries={}",
                    stagingDir, result.retainedEntries(), result.removedEntries());
            return;
        }
        for (Path path : stagingSegments) {
            Files.deleteIfExists(path);
        }
        tryDeleteEmptyStagingDir(stagingDir);
    }

    private void tryDeleteEmptyStagingDir(Path stagingDir) throws IOException {
        if (!Files.isDirectory(stagingDir)) {
            return;
        }
        try (DirectoryStream<Path> remaining = Files.newDirectoryStream(stagingDir)) {
            if (remaining.iterator().hasNext()) {
                log.info("sort staging dir left in place: unexpected content remains in {}", stagingDir);
                return;
            }
        }
        Files.delete(stagingDir);
    }

    /**
     * Remove stale finals only after the complete replacement is durable under temporary names.
     *
     * <p>{@link StaleFinalSweep#ALL_PARQUET} is restricted to identity-verified managed re-entry;
     * {@link StaleFinalSweep#OWN_PARTS_ONLY} removes only this transform's {@code part-*} namespace
     * for library callers whose output directory may contain unrelated Parquet. The latter still
     * covers every abandoned final this publisher can create.
     */
    void cleanStaleFinals(Path outputDir) throws IOException {
        String glob = switch (run.staleFinalSweep()) {
            case ALL_PARQUET -> StagingNames.ALL_PARQUET_GLOB;
            case OWN_PARTS_ONLY -> StagingNames.OWN_FINAL_GLOB;
        };
        Sweeps.sweep(outputDir,
                stale -> log.info("sweeping stale sorted output before replacement publish: {}", stale),
                glob);
    }

    private void publicationStep(PublicationStep step) throws IOException {
        publicationStep(step, -1);
    }

    private void publicationStep(PublicationStep step, int ordinal) throws IOException {
        publicationStepHook.reached(step, ordinal);
    }

    private static void atomicRename(Path from, Path to) throws IOException {
        try {
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static List<FinalPart> finalParts(List<Path> paths, List<SortedFileWriter> writers) {
        if (paths.size() != writers.size()) {
            throw new IllegalStateException("final part path/writer count mismatch: paths="
                    + paths.size() + " writers=" + writers.size());
        }
        List<FinalPart> parts = new ArrayList<>(paths.size());
        for (int i = 0; i < paths.size(); i++) {
            parts.add(new FinalPart(paths.get(i), writers.get(i).finalMetadata()));
        }
        return List.copyOf(parts);
    }

    /** Mutable only while a single transform constructs its ordered replacement set. */
    final class PendingParts {
        private final Path outputDir;
        private final Path stagingDir;
        private final SortedFileWriterFactory outputSequence;
        private final List<Path> finalFiles = new ArrayList<>();
        private final List<Path> tmpFiles = new ArrayList<>();
        private final List<SortedFileWriter> writers = new ArrayList<>();

        private PendingParts(Path outputDir, Path stagingDir,
                SortedFileWriterFactory outputSequence) {
            this.outputDir = outputDir;
            this.stagingDir = stagingDir;
            this.outputSequence = outputSequence;
        }

        SortedFileWriter openNext() throws IOException {
            int ordinal = finalFiles.size();
            Path finalPath = outputDir.resolve(StagingNames.finalPart(ordinal));
            Path tmpPath = stagingDir.resolve(StagingNames.finalTmp(ordinal));
            SortedFileWriter writer = outputSequence.create(tmpPath, ordinal + 1);
            finalFiles.add(finalPath);
            tmpFiles.add(tmpPath);
            writers.add(writer);
            return writer;
        }

        List<Path> finalFiles() {
            return List.copyOf(finalFiles);
        }
    }
}
