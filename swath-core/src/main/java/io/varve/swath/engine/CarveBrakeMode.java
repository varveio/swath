/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import java.util.List;
import java.util.Locale;

/**
 * Which realized-child-mass carve brake arithmetic {@code OwnerSplitGovernor.decide} reads,
 * selected once per run by {@code --engine-toggle carve_brake=MODE}
 * ({@link EngineToggles#carveBrake()}). Distinct from {@code confetti_feedback}'s RATE signal (a
 * binary degenerate/substantial classification of realized children): this reads the recent MASS
 * TREND of the last tagged owner-split children (algorithms.md §3.3's serial-tail over-carving
 * campaign — carving stops paying before children turn confetti-sized, which the confetti gate's
 * own suppress/carve ratio cannot see).
 *
 * <p>{@link #OFF} is the default in this commit — the race decides the shipped default (a later
 * commit flips it); every other arm suppresses a carve once the recent window-average realized
 * mass drops below {@link #k()} × {@code maxKeys}. Bigger {@code k} brakes earlier (a larger
 * mass floor to clear).
 */
public enum CarveBrakeMode {

    /** No brake: the confetti feedback gate and the observed-mass floor are the only carve limits. */
    OFF("off", 0),

    /** Brake once the window-average realized child mass drops below {@code 2 * maxKeys}. */
    MASS_K2("mass_k2", 2),

    /** Brake once the window-average realized child mass drops below {@code 4 * maxKeys}. */
    MASS_K4("mass_k4", 4),

    /** Brake once the window-average realized child mass drops below {@code 8 * maxKeys}. */
    MASS_K8("mass_k8", 8);

    private final String code;
    private final int k;

    CarveBrakeMode(String code, int k) {
        this.code = code;
        this.k = k;
    }

    /** The {@code --engine-toggle carve_brake=} value that selects this mode. */
    public String code() {
        return code;
    }

    /** The threshold multiplier ({@code K} in {@code K * maxKeys}); {@code 0} for {@link #OFF} (unused). */
    public int k() {
        return k;
    }

    /** The accepted values, in declaration order — the parse error message's own list. */
    public static List<String> codes() {
        return List.of(OFF.code, MASS_K2.code, MASS_K4.code, MASS_K8.code);
    }

    /**
     * The mode named by {@code value} (case-insensitive), or {@code null} when it names none — the
     * caller owns the error, since {@link EngineToggles#parse} reports it as a startup validation
     * failure rather than an exception type.
     */
    public static CarveBrakeMode fromCode(String value) {
        String normalized = value.toLowerCase(Locale.ROOT);
        for (CarveBrakeMode mode : values()) {
            if (mode.code.equals(normalized)) {
                return mode;
            }
        }
        return null;
    }
}
