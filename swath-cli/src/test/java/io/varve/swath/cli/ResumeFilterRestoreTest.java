/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.cli.FilterSpecCodec.Decoded;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * {@code swath resume} restores the stored filter flags onto a fresh
 * {@link ListCommand} so the rebuilt command recomputes the <i>same</i> {@code filter_spec}
 * and passes the changed-filter gate (which {@code ListCommandResumeArgsTest
 * .changedFilter_resumeRefused} proves rejects a mismatch).
 *
 * <p>This guards the restore mechanism end-to-end through the real eligibility path:
 * encode → decode → re-encode must be a fixed point.
 */
final class ResumeFilterRestoreTest {

    @Test
    void restoredFilters_recomputeSameSpec_soResumeStaysEligible() throws Exception {
        // A run started with a full filter set.
        ListCommand original = new ListCommand();
        original.filters.include = "\\.parquet$";
        original.filters.exclude = "_tmp/";
        original.filters.minSize = "1k";
        original.filters.maxSize = "256mb";
        original.filters.modifiedAfter = "2020-01-01";
        original.filters.modifiedBefore = "2024-12-31";
        original.filters.storageClasses = List.of("STANDARD", "GLACIER");
        String spec = original.filters.spec();

        // ResumeCommand decodes the stored spec and reapplies the fields to a fresh command.
        Decoded decoded = FilterSpecCodec.decode(spec);
        ListCommand restored = restoreFilters(decoded);

        // The rebuilt command must recompute the identical spec ⇒ the resume gate passes.
        assertThat(restored.filters.spec()).isEqualTo(spec);
    }

    @Test
    void restoredEmptyFilters_recomputeSameSpec() throws Exception {
        // An unfiltered run round-trips through the codec to the same canonical empty spec.
        String spec = new ListCommand().filters.spec();
        Decoded decoded = FilterSpecCodec.decode(spec);
        ListCommand restored = restoreFilters(decoded);
        assertThat(restored.filters.spec()).isEqualTo(spec);
    }

    /** Reapplies a decoded filter spec's fields onto a fresh {@link ListCommand}, as {@code swath resume} does. */
    private static ListCommand restoreFilters(Decoded decoded) {
        ListCommand restored = new ListCommand();
        restored.filters.include = decoded.include();
        restored.filters.exclude = decoded.exclude();
        restored.filters.minSize = decoded.minSize();
        restored.filters.maxSize = decoded.maxSize();
        restored.filters.modifiedAfter = decoded.modifiedAfter();
        restored.filters.modifiedBefore = decoded.modifiedBefore();
        restored.filters.storageClasses = decoded.storageClasses();
        return restored;
    }
}
