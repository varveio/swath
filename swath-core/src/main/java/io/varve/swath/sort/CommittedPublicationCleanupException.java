/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;

/**
 * Sorter-local cleanup failed after the publication listener committed the authoritative dataset.
 *
 * <p>The final files and listener-owned manifest/state/symlink/{@code _SUCCESS} must not be rolled
 * back or republished in response to this exception. The managed runtime maps it to its resumable
 * publication-pending error and re-enters only the PUBLISHED cleanup path.
 */
public final class CommittedPublicationCleanupException extends IOException {

    /** Cheap classification of the post-commit operation that failed. */
    public enum Stage {
        AFTER_PUBLISH_LISTENER_HOOK("after_publish_listener_hook"),
        DISPOSABLE_INTERMEDIATE_CLEANUP("disposable_intermediate_cleanup"),
        ORIGINAL_STAGING_COMPLETION("original_staging_completion"),
        AFTER_STAGING_COMPLETION_HOOK("after_staging_completion_hook");

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

    CommittedPublicationCleanupException(Stage stage, Throwable cause) {
        super("sorted dataset publication committed; cleanup pending at " + stage.logValue(), cause);
        this.stage = stage;
    }

    /** The post-commit operation that failed. */
    public Stage stage() {
        return stage;
    }
}
