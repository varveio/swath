/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The INT-8 flat-scan ceiling (the INT-8 adversarial tests guard it): total LIST/API
 * calls — and, under the same shape, structure-probe counts — must stay at
 * {@code 4*pages + 4*fanout + 64}, i.e. O(pages)+O(fanout), never O(directories)/O(N). Five
 * distinct adversarial scenario classes (structure probes, owner self-split, far-ahead pivot, dense-root seed,
 * dense hex scan) independently re-derived this identical arithmetic; this is the single place
 * it lives now, so a formula change updates once. {@code fanout} is whatever secondary term the
 * scenario budgets against — usually worker count, sometimes seed count.
 *
 * <p>This only unifies the ceiling number/assertion. Each caller keeps its own scenario,
 * fixture, and any additional chained assertions (e.g. a co-bound against directory count) —
 * those are NOT duplicated logic and are not touched here.
 */
public final class ApiCallBudget {

    private ApiCallBudget() {
    }

    /** {@code 4*pages + 4*fanout + 64} — the INT-8 flat-scan ceiling. */
    public static long int8Budget(long pages, long fanout) {
        return 4L * pages + 4L * fanout + 64L;
    }

    /**
     * Asserts {@code actual <= int8Budget(pages, fanout)}. {@code context} labels the failure
     * message (e.g. the scenario name) so a failing budget still points at its origin.
     */
    public static void assertWithinInt8Budget(long actual, long pages, long fanout, String context) {
        long budget = int8Budget(pages, fanout);
        assertThat(actual)
                .as("%s: total within the INT-8 budget (4*pages + 4*fanout + 64 = %d)", context, budget)
                .isLessThanOrEqualTo(budget);
    }
}
