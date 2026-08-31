/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.sorted;

import io.varve.swath.sort.SortTransform;
import io.varve.swath.sort.SortTransformResult;
import java.io.IOException;

/**
 * Sorter-local cleanup failed after the publication listener committed the authoritative dataset.
 *
 * <p>The final files and listener-owned manifest/state/symlink/{@code _SUCCESS} must not be rolled
 * back or republished in response to this exception. The managed runtime maps it to its resumable
 * publication-pending error and re-enters only the PUBLISHED cleanup path. When it escapes a
 * {@link SortTransform}, {@link #publishedResult()} carries the exact committed output and merge
 * facts even though cleanup prevented the ordinary return.
 */
public final class CommittedPublicationCleanupException extends IOException {

    /** Cheap classification of the post-commit operation that failed. */
    public enum Stage {
        AFTER_PUBLISH_LISTENER_HOOK("after_publish_listener_hook"),
        DISPOSABLE_INTERMEDIATE_CLEANUP("disposable_intermediate_cleanup"),
        ORIGINAL_STAGING_COMPLETION("original_staging_completion"),
        AFTER_STAGING_COMPLETION_HOOK("after_staging_completion_hook"),
        PUBLISHED_REENTRY_CLEANUP("published_reentry_cleanup");

        private final String logValue;

        Stage(String logValue) {
            this.logValue = logValue;
        }

        /** Stable value used by the post-commit cleanup diagnostic. */
        public String logValue() {
            return logValue;
        }
    }

    private final Stage stage;
    private SortedDatasetCommit publishedCommit;
    private SortTransformResult publishedResult;

    CommittedPublicationCleanupException(Stage stage, Throwable cause) {
        super("sorted dataset publication committed; cleanup pending at " + stage.logValue(), cause);
        this.stage = stage;
    }

    /** Build the typed committed-cleanup cause for a PUBLISHED cleanup-only re-entry. */
    public static CommittedPublicationCleanupException publishedReentry(Throwable cause) {
        return new CommittedPublicationCleanupException(Stage.PUBLISHED_REENTRY_CLEANUP, cause);
    }

    public CommittedPublicationCleanupException withPublishedResult(SortTransformResult result) {
        if (publishedResult != null) {
            throw new IllegalStateException("published sort result already attached");
        }
        publishedResult = result;
        return this;
    }

    CommittedPublicationCleanupException withPublishedCommit(SortedDatasetCommit commit) {
        if (publishedCommit != null) {
            throw new IllegalStateException("published dataset commit already attached");
        }
        publishedCommit = commit;
        return this;
    }

    public SortedDatasetCommit publishedCommit() {
        if (publishedCommit == null) {
            throw new IllegalStateException("committed cleanup failure has no dataset commit");
        }
        return publishedCommit;
    }

    /** The post-commit operation that failed. */
    public Stage stage() {
        return stage;
    }

    /** Exact committed result, attached by {@link SortTransform} before this exception escapes it. */
    public SortTransformResult publishedResult() {
        if (publishedResult == null) {
            throw new IllegalStateException("committed cleanup failure has no published sort result");
        }
        return publishedResult;
    }
}
