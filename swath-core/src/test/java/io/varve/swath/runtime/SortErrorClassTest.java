/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.sort.DuplicateKeyException;
import io.varve.swath.sort.EqualKeyPolicy;
import io.varve.swath.sort.SortCardinalityException;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortOrderException;
import org.junit.jupiter.api.Test;

class SortErrorClassTest {

    @Test
    void outputOrderClassificationSurvivesTheFatalMergeWrapper() {
        Throwable wrapped = new RuntimeException(
                new SortOrderException("merged output order regressed"));

        assertThat(ListRunner.sortErrorClass(wrapped))
                .isEqualTo(SortOrderException.ERROR_CLASS);
    }

    @Test
    void cardinalityAndDuplicateClassificationsSurviveFatalMergeWrappers() {
        assertThat(ListRunner.sortErrorClass(new RuntimeException(
                new SortCardinalityException("row totals differ"))))
                .isEqualTo(SortCardinalityException.ERROR_CLASS);
        assertThat(ListRunner.sortErrorClass(new RuntimeException(
                new DuplicateKeyException("duplicate raw key"))))
                .isEqualTo(DuplicateKeyException.ERROR_CLASS);
    }

    @Test
    void liveObjectsRejectAdjacentRawKeysWhileVersionsRetainEqualKeyGroups() {
        assertThat(ListRunner.equalKeyPolicy(SortMode.OBJECTS)).isEqualTo(EqualKeyPolicy.REJECT);
        assertThat(ListRunner.equalKeyPolicy(SortMode.VERSIONS)).isEqualTo(EqualKeyPolicy.ALLOW);
    }
}
