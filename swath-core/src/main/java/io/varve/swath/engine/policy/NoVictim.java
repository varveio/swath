/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/** No candidate in the pool qualified as a steal victim this attempt (algorithms.md §3). */
public record NoVictim(NoVictimReason reason, List<Engagement> engagements, List<VictimMutation> mutations,
                       VictimScan scan)
        implements Selection {

    /** As the canonical constructor, for a policy that records no {@link VictimScan}. */
    public NoVictim(NoVictimReason reason, List<Engagement> engagements, List<VictimMutation> mutations) {
        this(reason, engagements, mutations, null);
    }
}
