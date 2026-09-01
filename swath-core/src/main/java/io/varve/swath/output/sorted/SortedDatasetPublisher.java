/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.sorted;

import io.varve.swath.output.dataset.DurableFiles;
import io.varve.swath.sort.FinalPart;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.finalize.PreparedSortedParts;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;

/**
 * Physical publication owner for one sorted dataset replacement. It captures filesystem authority,
 * sweeps sorter-owned working namespaces, assigns consumer-visible part names, commits the complete
 * prepared set, invokes the consumer committer, and applies the final staging policy.
 */
final class SortedDatasetPublisher {

    private final SortRun run;
    private final SortConfig config;
    private final SortMetrics metrics;
    private final PublicationStepHook publicationStepHook;
    private final Logger log;

    SortedDatasetPublisher(SortRun run, PublicationStepHook publicationStepHook, Logger log) {
        this.run = run;
        this.config = run.config();
        this.metrics = run.metrics();
        this.publicationStepHook = publicationStepHook;
        this.log = log;
    }

    /** Capture all source/destination authority without mutating either directory. */
    SortedPublicationContext publicationContext(
            List<Path> stagingSegments,
            Path outputDir,
            Path stagingDir,
            SortedDatasetCommitter publishListener) throws IOException {
        StagingReconciliation ownedInputs =
                StagingReconciliation.fromPaths(stagingDir, stagingSegments);
        StagingReconciliation.DirectoryAuthority outputAuthority =
                StagingReconciliation.DirectoryAuthority.capture(
                        outputDir, "sort output directory");
        return new SortedPublicationContext(outputDir, stagingDir, publishListener, ownedInputs,
                retainedOriginals(ownedInputs), outputAuthority);
    }

    /** Derive diagnostic retention from the unconditionally validated owned input set. */
    StagingReconciliation retainedOriginals(StagingReconciliation ownedInputs) {
        return config.stagingRetention().retainsOriginals() ? ownedInputs : null;
    }

    /** Sweep every disposable working namespace after source preflight has succeeded. */
    void sweepWorking(SortedPublicationContext context) throws IOException {
        sweepWorking(context.outputDir(), context.stagingDir(), context.ownedInputs(),
                context.outputAuthority());
    }

    /** Package-level test seam retaining the exact authority checks around the working sweep. */
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
        ownedInputs.requireOwnedStagingAuthority(stagingDir);
        outputAuthority.requireSame(outputDir);
    }

    /**
     * Publish one already-complete prepared set. The first consumer-visible mutation is the stale
     * final sweep, after the dense set, exact cardinality, and raw adjacency proof have all completed
     * in the finalizer.
     */
    SortedDatasetCommit publish(
            PreparedSortedParts prepared, SortedPublicationContext context) throws IOException {
        validatePreparedAuthority(prepared, context);
        publicationStep(PublicationStep.AFTER_ALL_TMP_PARTS_DURABLE);
        validatePreparedAuthority(prepared, context);

        List<Path> finalFiles = new ArrayList<>(prepared.parts().size());
        for (int i = 0; i < prepared.parts().size(); i++) {
            finalFiles.add(context.outputDir().resolve(StagingNames.finalPart(i)));
        }
        SortedDatasetCommit commit = new SortedDatasetCommit(finalFiles, prepared.outputBytes(),
                prepared.outputRows(), prepared.mergeStatistics());

        cleanStaleFinals(context.outputDir(), context.outputAuthority());
        publicationStep(PublicationStep.AFTER_STALE_FINAL_SWEEP);
        for (int i = 0; i < prepared.parts().size(); i++) {
            context.outputAuthority().requireSame(context.outputDir());
            context.ownedInputs().requireOwnedStagingAuthority(context.stagingDir());
            atomicRename(prepared.parts().get(i).temporaryPath(), finalFiles.get(i));
            publicationStep(PublicationStep.AFTER_PART_RENAME, i);
        }
        DurableFiles.directory(context.outputDir());
        publicationStep(PublicationStep.AFTER_OUTPUT_DIRECTORY_SYNC);
        context.publishListener().onPublished(finalParts(finalFiles, prepared), prepared.outputRows());

        CommittedPublicationCleanupException.Stage cleanupStage =
                CommittedPublicationCleanupException.Stage.AFTER_PUBLISH_LISTENER_HOOK;
        try {
            publicationStep(PublicationStep.AFTER_PUBLISH_LISTENER);
            cleanupStage = CommittedPublicationCleanupException.Stage.DISPOSABLE_INTERMEDIATE_CLEANUP;
            List<Path> intermediates = prepared.cleanupToken().disposableIntermediates();
            for (int i = 0; i < intermediates.size(); i++) {
                publicationStep(PublicationStep.BEFORE_DISPOSABLE_INTERMEDIATE_CLEANUP, i);
                context.ownedInputs().requireOwnedStagingAuthority(context.stagingDir());
                context.ownedInputs().deleteDisposable(intermediates.get(i));
            }
            cleanupStage = CommittedPublicationCleanupException.Stage.ORIGINAL_STAGING_COMPLETION;
            completeOriginalStaging(context);
            cleanupStage = CommittedPublicationCleanupException.Stage.AFTER_STAGING_COMPLETION_HOOK;
            publicationStep(PublicationStep.AFTER_STAGING_COMPLETION);
            return commit;
        } catch (IOException | RuntimeException failure) {
            metrics.recordStealReason("SORT", "post_publish_cleanup_pending");
            log.warn("sort_post_publish_cleanup_pending publication_committed=true "
                            + "cleanup_pending=true stage={} output_dir={} staging_dir={} message={}",
                    cleanupStage.logValue(), context.outputDir(), context.stagingDir(),
                    failure.getMessage());
            throw new CommittedPublicationCleanupException(cleanupStage, failure)
                    .withPublishedCommit(commit);
        }
    }

    private static void validatePreparedAuthority(
            PreparedSortedParts prepared, SortedPublicationContext context) throws IOException {
        context.ownedInputs().requireOwnedStagingAuthority(context.stagingDir());
        context.outputAuthority().requireSame(context.outputDir());
        Path staging = context.stagingDir().toAbsolutePath().normalize();
        Set<Path> unique = new HashSet<>();
        for (PreparedSortedParts.Part part : prepared.parts()) {
            Path temporary = part.temporaryPath().toAbsolutePath().normalize();
            if (!staging.equals(temporary.getParent()) || !unique.add(temporary)
                    || !Files.isRegularFile(temporary, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("prepared sorted part is not a unique ordinary staging child: "
                        + temporary);
            }
        }
    }

    private void completeOriginalStaging(SortedPublicationContext context) throws IOException {
        if (context.retainedOriginals() != null) {
            StagingReconciliation.Result result =
                    context.retainedOriginals().reconcile(context.stagingDir());
            metrics.recordStealReason("SORT", "staging_retained");
            log.info("sort_staging_retained source=merge path={} retained_segments={} removed_entries={}",
                    context.stagingDir(), result.retainedEntries(), result.removedEntries());
            return;
        }
        context.ownedInputs().deleteOwnedOriginals();
        tryDeleteEmptyStagingDir(context.stagingDir(), context.ownedInputs());
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

    private static List<FinalPart> finalParts(
            List<Path> paths, PreparedSortedParts prepared) {
        List<FinalPart> parts = new ArrayList<>(paths.size());
        for (int i = 0; i < paths.size(); i++) {
            parts.add(new FinalPart(
                    paths.get(i), prepared.parts().get(i).publicationMetadata()));
        }
        return List.copyOf(parts);
    }
}
