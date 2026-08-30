/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.PageBatch;
import java.util.List;
import org.junit.jupiter.api.Test;

class SortOutputStageInvariantTest {

    @Test
    void perNodeSequenceAndKeyMustStrictlyAdvance() {
        SortOutputStage stage = new SortOutputStage(null);
        assertThat(stage.verifyPageOrder(new PageBatch(
                7, 10, List.of(object("a"), object("b")))))
                .isTrue();
        assertThat(stage.verifyPageOrder(new PageBatch(
                7, 12, List.of(object("c")))))
                .isTrue();

        assertThatThrownBy(() -> stage.verifyPageOrder(new PageBatch(
                7, 12, List.of(object("d")))))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("sequence did not increase");
    }

    @Test
    void perNodeFirstKeyMustExceedTheLastAcceptedKey() {
        SortOutputStage stage = new SortOutputStage(null);
        stage.verifyPageOrder(new PageBatch(7, 1, List.of(object("m"))));

        assertThatThrownBy(() -> stage.verifyPageOrder(new PageBatch(
                7, 2, List.of(object("m"), object("z")))))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("key did not advance");
    }

    @Test
    void nodeCompletionReleasesTripwireStateIncludingForAnEmptyTerminalPage() {
        SortOutputStage stage = new SortOutputStage(null);
        stage.verifyPageOrder(new PageBatch(7, 1, List.of(object("m"))));
        assertThat(stage.trackedPageCountForTesting()).isEqualTo(1);

        stage.verifyPageOrder(PageBatch.completion(7, 2));

        assertThat(stage.trackedPageCountForTesting()).isZero();
    }

    private static ObjectEntry object(String key) {
        return ObjectEntry.withoutOwnerDisplayNameAndChecksumType(
                KeyBytes.ofUtf8(key), 1, 0, null, null, null, true, null, null);
    }
}
