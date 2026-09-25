/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.protocol.ByteKeys;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.junit.jupiter.api.Test;

class BudgetedOutputTest {
    @Test
    void growthChargesOldAndNewArraysBeforeCopyAndReleasesExactlyOnce() {
        ResponseByteBudget budget = new ResponseByteBudget(16);
        BudgetedOutput first = new BudgetedOutput(budget, 4, 12);
        first.write(new byte[8], 0, 8);
        assertThat(first.capacity()).isEqualTo(8);
        assertThat(budget.charged()).isEqualTo(8);
        assertThat(budget.peak()).isEqualTo(12);

        BudgetedOutput second = new BudgetedOutput(budget, 8, 12);
        assertThatThrownBy(() -> second.write(new byte[9], 0, 9))
                .isInstanceOf(ReplayOutputException.class)
                .satisfies(error -> assertThat(((ReplayOutputException) error).responseTooLarge()).isFalse());
        assertThat(budget.charged()).isEqualTo(16);
        first.close();
        first.close();
        second.close();
        assertThat(budget.charged()).isZero();
    }

    @Test
    void perResponseCapIsDistinctFromAggregateExhaustion() {
        ResponseByteBudget budget = new ResponseByteBudget(32);
        try (BudgetedOutput output = new BudgetedOutput(budget, 4, 8)) {
            assertThatThrownBy(() -> output.write(new byte[9], 0, 9))
                    .isInstanceOf(ReplayOutputException.class)
                    .satisfies(error -> assertThat(((ReplayOutputException) error).responseTooLarge()).isTrue());
        }
        assertThat(budget.charged()).isZero();
    }

    @Test
    void xmlEntityExpansionAndFollowingAsciiGrowSafelyNearCapacity() {
        ResponseByteBudget budget = new ResponseByteBudget(64);
        try (BudgetedOutput output = new BudgetedOutput(budget, 6, 16)) {
            output.appendEscaped("&abcd");
            assertThat(StandardCharsets.UTF_8.decode(output.buffer()).toString()).isEqualTo("&amp;abcd");
            assertThat(output.capacity()).isGreaterThanOrEqualTo(9);
        }
        assertThat(budget.charged()).isZero();
    }

    @Test
    void percentEncodingUsesActualEscapedLengthAtTheResponseCap() {
        byte[] safe = "abc/XYZ-_.09".getBytes(StandardCharsets.US_ASCII);
        try (BudgetedOutput output = new BudgetedOutput(new ResponseByteBudget(128), 1, safe.length)) {
            output.appendPercentEncoded(safe);
            assertThat(output.size()).isEqualTo(safe.length);
        }
        Random random = new Random(220);
        for (int n = 1; n <= 128; n++) {
            byte[] input = new byte[n];
            int expectedLength = 0;
            for (int i = 0; i < n; i++) {
                input[i] = (byte) (random.nextBoolean() ? 'a' : ' ');
                expectedLength += input[i] == 'a' ? 1 : 3;
            }
            try (BudgetedOutput output = new BudgetedOutput(
                    new ResponseByteBudget(4L * expectedLength), 1, expectedLength)) {
                output.appendPercentEncoded(input);
                assertThat(output.size()).isEqualTo(expectedLength);
                byte[] rendered = new byte[output.size()];
                output.buffer().get(rendered);
                assertThat(ByteKeys.percentDecode(new String(rendered, StandardCharsets.US_ASCII)))
                        .isEqualTo(input);
            }
        }
    }
}
