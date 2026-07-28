/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import java.util.List;
import java.util.Locale;

/**
 * Which arithmetic the owner-split observed-mass child-tail floor
 * ({@link StealMath#childTailBelowObservedMassFloor}) reads, selected once per run by
 * {@code --engine-toggle tail_floor=MODE} ({@link EngineToggles#tailFloor()}). {@link #CURRENT} is
 * the shipped formula and the default; the other two are A/B arms for the wide-flat blindness
 * measured on the `nara` tail (algorithms.md §3.3), where the shipped window term multiplies an
 * honest 10^5–10^6-key estimate away.
 *
 * <p>An arm here changes only <em>whether</em> an owner carves, never how the split transaction
 * tiles (I2/I3 hold under every mode — the pivot math and the CAS are untouched), so this is a
 * measurement surface exactly like {@code rate_anchored_sensing}, not a supported configuration.
 */
public enum TailFloorMode {

    /**
     * The shipped floor: block iff {@code est × max(0, min(1, densityRatio) − f) <= 2·maxKeys}.
     * Bit-identical to every pre-toggle run.
     */
    CURRENT("current"),

    /**
     * Block iff {@code est <= 2·maxKeys} — the estimator's own reading, with the byte-geometry
     * window product dropped entirely. Under a rate-anchored sensor {@code est} already IS
     * realized-mass-anchored, so multiplying it by a density-geometry window double-counts geometry
     * (the same double-count issue #68 removed one gate upstream, at the remaining-est floor).
     */
    EST_DIRECT("est_direct"),

    /**
     * The shipped window product with its reach term floored:
     * {@code est × max(REACH_MIN, min(1, densityRatio) − f) <= 2·maxKeys}
     * ({@link StealMath#TAIL_REACH_MIN}). Keeps the geometry signal — a genuinely thinning tail
     * still shrinks the child's share — but never lets trailing density erase the estimate outright.
     */
    REACH_FLOORED("reach_floored");

    private final String code;

    TailFloorMode(String code) {
        this.code = code;
    }

    /** The {@code --engine-toggle tail_floor=} value that selects this mode. */
    public String code() {
        return code;
    }

    /** The accepted values, in declaration order — the parse error message's own list. */
    public static List<String> codes() {
        return List.of(CURRENT.code, EST_DIRECT.code, REACH_FLOORED.code);
    }

    /**
     * The mode named by {@code value} (case-insensitive), or {@code null} when it names none — the
     * caller owns the error, since {@link EngineToggles#parse} reports it as a startup validation
     * failure rather than an exception type.
     */
    public static TailFloorMode fromCode(String value) {
        String normalized = value.toLowerCase(Locale.ROOT);
        for (TailFloorMode mode : values()) {
            if (mode.code.equals(normalized)) {
                return mode;
            }
        }
        return null;
    }
}
