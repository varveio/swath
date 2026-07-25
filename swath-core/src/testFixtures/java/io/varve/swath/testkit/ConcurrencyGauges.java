/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.engine.ConcurrencyGauge;

/**
 * Terse {@link ConcurrencyGauge} factory for engine tests: a fresh throwaway metrics sink — the
 * effective default the (now removed) 1-arg {@link ConcurrencyGauge} constructor overload supplied.
 */
public final class ConcurrencyGauges {

    private ConcurrencyGauges() {
    }

    /** A {@link ConcurrencyGauge} over a fresh throwaway metrics sink. */
    public static ConcurrencyGauge of(int tMax) {
        return new ConcurrencyGauge(tMax, EngineContexts.freshMetrics());
    }
}
