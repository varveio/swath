/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.sim.model.ClientCostTerm.Provenance;
import org.junit.jupiter.api.Test;

/**
 * The client-cost term's handling protocol, which is as much of the design as its arithmetic is: a
 * term is always supplied, always labelled, and can only be zero on purpose.
 */
class ClientCostTermTest {

    @Test
    void anAbsentTermIsAMissingDependencyAndNotAZero() {
        assertThatThrownBy(() -> new IidClientCost(null))
                .isInstanceOf(MissingSimDependencyException.class)
                .hasMessageContaining("client cost term");
        assertThatThrownBy(() -> new ContendedClientCost(null, 1))
                .isInstanceOf(MissingSimDependencyException.class)
                .hasMessageContaining("client cost term");
    }

    /**
     * "Not measured yet" and "measured as zero" must not look the same to the model. A term that is
     * zero without saying so is rejected, so the only way to get a zero-cost run is to ask for one.
     */
    @Test
    void aSilentlyZeroTermIsRejectedButADeliberateOneIsNot() {
        assertThatThrownBy(() -> new ClientCostTerm(Provenance.PROVISIONAL, 0, 0, "forgot to fill this in"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("zeroedForExactMode");

        ClientCostTerm deliberate = ClientCostTerm.zeroedForExactMode("closed-form invariants");

        assertThat(deliberate.provenance()).isEqualTo(Provenance.ZEROED_FOR_EXACT_MODE);
        assertThat(deliberate.costNanos(1000)).isZero();
        assertThat(deliberate.isPredictive())
                .as("a zeroed run is an arithmetic check, never a statement about the real system")
                .isFalse();
    }

    @Test
    void aZeroedTermThatIsNotActuallyZeroIsRejected() {
        assertThatThrownBy(() -> new ClientCostTerm(Provenance.ZEROED_FOR_EXACT_MODE, 1, 0, "inconsistent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must actually be zero");
    }

    @Test
    void everyTermSaysWhereItCameFrom() {
        assertThatThrownBy(() -> new ClientCostTerm(Provenance.FINAL, 1, 1, "  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("where it came from");
    }

    /**
     * A measured term travels with the verdict of its own cross-checks, so a result computed from a
     * term that failed one can be read as such rather than quoted as a finding.
     */
    @Test
    void aMeasuredTermCarriesTheVerdictOfItsOwnCrossChecks() {
        ClientCostTerm defective = new ClientCostTerm(Provenance.PROVISIONAL_WITH_DEFECT, 100, 5,
                "illustrative term, this test only");

        assertThat(defective.isPredictive()).isTrue();
        assertThat(defective.costNanos(10)).isEqualTo(100 + 5 * 10);
        assertThat(defective.provenance()).isNotEqualTo(Provenance.FINAL);
    }

    @Test
    void negativeCostsAndNegativeKeyCountsAreRejected() {
        assertThatThrownBy(() -> new ClientCostTerm(Provenance.FINAL, -1, 0, "negative"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ClientCostTerm.zeroedForExactMode("x").costNanos(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
