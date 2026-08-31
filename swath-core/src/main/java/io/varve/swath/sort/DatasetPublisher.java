/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.output.parquet.sorted.SortedFileIndex;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
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
    private final PublicationStepHook publicationStepHook;
    // Deliberately supplied by SortTransform: extraction must not change the logger name carried by
    // existing sweep/retention diagnostics.
    private final Logger log;

    DatasetPublisher(SortRun run, PublicationStepHook publicationStepHook, Logger log) {
        this.run = run;
        this.config = run.config();
        this.metrics = run.metrics();
        this.finalWriterFactory = run.finalWriterFactory();
        this.publicationStepHook = publicationStepHook;
        this.log = log;
    }

    /** Derive diagnostic retention from the unconditionally validated owned input set. */
    StagingReconciliation retainedOriginals(StagingReconciliation ownedInputs) {
        if (!config.stagingRetention().retainsOriginals()) {
            return null;
        }
        return ownedInputs;
    }

    /** Sweep every disposable working namespace, then expose the post-sweep publication boundary. */
    void sweepWorking(Path outputDir, Path stagingDir, StagingReconciliation ownedInputs,
            StagingReconciliation.DirectoryAuthority outputAuthority) throws IOException {
        ownedInputs.requireOwnedStagingAuthority(stagingDir);
        outputAuthority.requireSame(outputDir);
        StagingReconciliation.sweepFinalTemporaries(stagingDir);
        StagingReconciliation.sweepFinalTemporaries(outputDir);
        ownedInputs.sweepDisposables(StagingNames.CASCADE_PAGE_RUN_GLOB);
        ownedInputs.sweepDisposables(StagingNames.LEGACY_CASCADE_PARQUET_GLOB);
        ownedInputs.sweepDisposables(StagingNames.LEGACY_RANGE_TMP_GLOB);
        ownedInputs.sweepDisposables(StagingNames.LEGACY_RANGE_PROOF_TMP_GLOB);
        ownedInputs.sweepDisposables(StagingNames.PIPELINE_TMP_GLOB);
        publicationStep(PublicationStep.AFTER_WORKING_SWEEP);
        // The hook is the deterministic stand-in for a directory replacement between phases.
        ownedInputs.requireOwnedStagingAuthority(stagingDir);
        outputAuthority.requireSame(outputDir);
    }

    /**
     * Verify and assemble parts whose producer already assigned global stamps and durably closed them.
     * The publisher deliberately does not restamp or close: pipeline encoders are the sole footer owner.
     */
    PendingParts preclosedParts(Path outputDir, Path stagingDir, List<Path> tmpFiles,
            List<SortedFileWriter> writers, StagingReconciliation ownedInputs,
            StagingReconciliation.DirectoryAuthority outputAuthority) throws IOException {
        if (tmpFiles.isEmpty() || tmpFiles.size() != writers.size()) {
            throw new IllegalArgumentException(
                    "preclosed part paths and writers must be non-empty and equally sized");
        }
        requireDisjointParts(tmpFiles, writers);
        return assembleParallelParts(outputDir, stagingDir, tmpFiles, writers, ownedInputs,
                outputAuthority);
    }

    private PendingParts assembleParallelParts(Path outputDir, Path stagingDir,
            List<Path> tmpFiles, List<SortedFileWriter> writers,
            StagingReconciliation ownedInputs,
            StagingReconciliation.DirectoryAuthority outputAuthority)
            throws IOException {
        PendingParts pending = new PendingParts(outputDir, stagingDir,
                finalWriterFactory.forOutputSequence(), ownedInputs, outputAuthority);
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

    /** Enforce strict raw-byte adjacency after durable close and before any publication mutation. */
    private void requireDisjointParts(List<Path> tmpFiles, List<SortedFileWriter> writers)
            throws IOException {
        List<FinalPartMetadata> metadata = new ArrayList<>(writers.size());
        for (int i = 0; i < writers.size(); i++) {
            var captured = writers.get(i).finalMetadata();
            if (captured.isPresent()) {
                metadata.add(captured.orElseThrow());
                continue;
            }
            metrics.recordStealReason("SORT", "cross_part_bounds_fallback_scan");
            SortedFileIndex.Bounds bounds = SortedFileIndex.bounds(
                    tmpFiles.get(i), MergeCancellation::check);
            metadata.add(new FinalPartMetadata(bounds.rowCount(), Files.size(tmpFiles.get(i)), "",
                    bounds.firstKey() == null ? null : "raw-bound",
                    bounds.lastKey() == null ? null : "raw-bound",
                    0, 0, 0, bounds.firstKey(), bounds.lastKey()));
        }
        requireDisjointParts(metadata, metrics);
    }

    static void requireDisjointParts(List<FinalPartMetadata> parts, SortMetrics metrics) {
        byte[] previousMax = null;
        int previousPart = -1;
        for (int i = 0; i < parts.size(); i++) {
            FinalPartMetadata part = parts.get(i);
            byte[] currentMin = part.rawMinKey();
            if (currentMin == null) {
                continue;
            }
            if (previousMax != null && Arrays.compareUnsigned(previousMax, currentMin) >= 0) {
                metrics.recordStealReason("SORT", "cross_part_overlap_rejected");
                throw new SortOrderException("sorted output parts overlap at adjacency "
                        + previousPart + " -> " + i + " under raw unsigned key order");
            }
            previousMax = part.rawMaxKey();
            previousPart = i;
        }
    }

    /** Serial drain closes its writers itself; this records the same post-close boundary. */
    void allTmpPartsDurable() throws IOException {
        publicationStep(PublicationStep.AFTER_ALL_TMP_PARTS_DURABLE);
    }

    /**
     * Refuse publication unless source trailers, the merge drain, and the closed final writers all
     * account for the same exact row total. Writer rows are read only after every part has closed,
     * so an open or failed footer can never satisfy this gate.
     */
    void verifyCardinality(PendingParts pending, long sourceRows, long drainedRows)
            throws IOException {
        long finalPartRows = 0;
        for (SortedFileWriter writer : pending.writers) {
            finalPartRows = Math.addExact(finalPartRows, writer.rows());
        }
        requireExactCardinality(sourceRows, drainedRows, finalPartRows, metrics);
    }

    static void requireExactCardinality(long sourceRows, long drainedRows, long finalPartRows)
            throws IOException {
        requireExactCardinality(sourceRows, drainedRows, finalPartRows, SortMetrics.NO_OP);
    }

    static void requireExactCardinality(long sourceRows, long drainedRows, long finalPartRows,
            SortMetrics metrics) throws IOException {
        if (sourceRows != drainedRows || sourceRows != finalPartRows) {
            metrics.recordStealReason("SORT", "sort_output_cardinality_mismatch");
            throw new SortCardinalityException(
                    "sort output cardinality mismatch before publication: source_rows="
                    + sourceRows + " drained_rows=" + drainedRows
                    + " final_part_rows=" + finalPartRows);
        }
    }

    /** Complete the ordered physical publish and configured staging ownership policy. */
    void publish(PendingParts pending, long totalRows, PublishListener publishListener,
            StagingReconciliation ownedInputs, StagingReconciliation retainedOriginals,
            List<Path> disposableIntermediates) throws IOException {
        // Validate both source and destination authorities before the first publication mutation.
        // In particular, stale-final cleanup must never destroy the prior generation and only then
        // discover that staging was redirected after its writers closed.
        ownedInputs.requireOwnedStagingAuthority(pending.stagingDir);
        pending.outputAuthority.requireSame(pending.outputDir);
        cleanStaleFinals(pending.outputDir, pending.outputAuthority);
        publicationStep(PublicationStep.AFTER_STALE_FINAL_SWEEP);
        for (int i = 0; i < pending.tmpFiles.size(); i++) {
            pending.outputAuthority.requireSame(pending.outputDir);
            ownedInputs.requireOwnedStagingAuthority(pending.stagingDir);
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
            for (int i = 0; i < disposableIntermediates.size(); i++) {
                Path intermediate = disposableIntermediates.get(i);
                publicationStep(PublicationStep.BEFORE_DISPOSABLE_INTERMEDIATE_CLEANUP, i);
                ownedInputs.requireOwnedStagingAuthority(pending.stagingDir);
                Files.deleteIfExists(intermediate);
            }
            cleanupStage = CommittedPublicationCleanupException.Stage.ORIGINAL_STAGING_COMPLETION;
            completeOriginalStaging(ownedInputs, pending.stagingDir, retainedOriginals);
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

    private void completeOriginalStaging(StagingReconciliation ownedInputs, Path stagingDir,
            StagingReconciliation retainedOriginals) throws IOException {
        if (retainedOriginals != null) {
            StagingReconciliation.Result result = retainedOriginals.reconcile(stagingDir);
            metrics.recordStealReason("SORT", "staging_retained");
            log.info("sort_staging_retained source=merge path={} retained_segments={} removed_entries={}",
                    stagingDir, result.retainedEntries(), result.removedEntries());
            return;
        }
        ownedInputs.deleteOwnedOriginals();
        tryDeleteEmptyStagingDir(stagingDir, ownedInputs);
    }

    private void tryDeleteEmptyStagingDir(Path stagingDir, StagingReconciliation ownedInputs)
            throws IOException {
        ownedInputs.requireOwnedStagingAuthority(stagingDir);
        try (DirectoryStream<Path> remaining = Files.newDirectoryStream(stagingDir)) {
            if (remaining.iterator().hasNext()) {
                log.info("sort staging dir left in place: unexpected content remains in {}", stagingDir);
                return;
            }
        }
        ownedInputs.requireOwnedStagingAuthority(stagingDir);
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
    void cleanStaleFinals(Path outputDir,
            StagingReconciliation.DirectoryAuthority outputAuthority) throws IOException {
        outputAuthority.requireSame(outputDir);
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
        private final StagingReconciliation ownedInputs;
        private final StagingReconciliation.DirectoryAuthority outputAuthority;
        private final List<Path> finalFiles = new ArrayList<>();
        private final List<Path> tmpFiles = new ArrayList<>();
        private final List<SortedFileWriter> writers = new ArrayList<>();

        private PendingParts(Path outputDir, Path stagingDir,
                SortedFileWriterFactory outputSequence, StagingReconciliation ownedInputs,
                StagingReconciliation.DirectoryAuthority outputAuthority) {
            this.outputDir = outputDir;
            this.stagingDir = stagingDir;
            this.outputSequence = outputSequence;
            this.ownedInputs = ownedInputs;
            this.outputAuthority = outputAuthority;
        }

        SortedFileWriter openNext() throws IOException {
            ownedInputs.requireOwnedStagingAuthority(stagingDir);
            outputAuthority.requireSame(outputDir);
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

        long outputBytes() throws IOException {
            long bytes = 0L;
            for (Path tmpFile : tmpFiles) {
                bytes = Math.addExact(bytes, Files.size(tmpFile));
            }
            return bytes;
        }
    }
}
