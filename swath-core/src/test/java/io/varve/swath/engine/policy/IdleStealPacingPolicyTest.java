/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Direct, table-driven unit tests for {@link IdleStealPacingPolicy}'s fleet-wide idle-steal pacing
 * arithmetic: one test per named boundary (threshold/decide, bounded-exponential growth-and-cap,
 * decay via elapsed time, reset) — mirrors {@code ThiefPolicyCascadeTest}/{@code
 * OwnerSplitGovernorTest}'s shape. Drives {@link IdleStealPacingPolicy#decide}/{@code
 * onNonProductive}/{@code onReset}/{@code parkNanos} directly against hand-built {@link
 * IdleStealPacingState}s and explicit {@code nowNanos} values — no {@code IdleStealBackoff}, no
 * lock, no ambient clock. {@code IdleStealBackoff}'s own slot-ownership/monitor behavior (still
 * this class's job to leave untouched) is separately pinned by {@code IdleStealSlotOwnershipTest}/
 * {@code IdleStealProbeConcurrencyTest}, which this suite does not touch.
 */
class IdleStealPacingPolicyTest {

    private static final long BASE_NANOS = TimeUnit.MILLISECONDS.toNanos(5);
    private static final long CAP_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

    private final IdleStealPacingPolicy policy = new IdleStealPacingPolicy(BASE_NANOS, CAP_NANOS);

    /**
     * The exponential ladder saturates instead of wrapping. With a {@code baseNanos} large enough
     * that {@code baseNanos << shift} overflows a signed long, the pre-guard arithmetic produced a
     * NEGATIVE delay, which {@code Math.min(capNanos, ...)} then preferred over the cap — arming
     * the next attempt in the PAST, so a state the ladder meant to pace came back {@code ELIGIBLE}
     * immediately and the fleet-wide backoff silently stopped backing off. The engine's own 5ms/50ms
     * constants are far from this boundary; the policy is public and a simulator supplies its own,
     * so the guard is pinned here rather than left to the caller.
     */
    @Test
    void aBaseThatWouldOverflowTheShiftSaturatesToTheCapRatherThanArmingInThePast() {
        long hugeBase = Long.MAX_VALUE / 4;   // overflows at any shift >= 2
        IdleStealPacingPolicy overflowing = new IdleStealPacingPolicy(hugeBase, CAP_NANOS);
        IdleStealPacingState state = new IdleStealPacingState(8, 0L);

        IdleStealPacingState next = overflowing.onNonProductive(state, 1_000L);

        assertThat(next.nextAttemptNanos())
                .as("delay must saturate to the cap, never wrap negative")
                .isEqualTo(1_000L + CAP_NANOS);
        assertThat(overflowing.decide(next, 1_000L))
                .as("a just-armed state must still be PACED, not immediately eligible")
                .isEqualTo(IdleStealPacingDecision.PACED);
    }

    /** The saturation guard must not perturb the ordinary ladder: every in-range shift is unchanged. */
    @Test
    void saturationGuardLeavesEveryNonOverflowingRungBitIdentical() {
        for (int consecutive = 0; consecutive <= 20; consecutive++) {
            long expected = Math.min(CAP_NANOS, BASE_NANOS << Math.min(consecutive, 16));
            assertThat(policy.onNonProductive(new IdleStealPacingState(consecutive, 0L), 7L).nextAttemptNanos())
                    .as("rung %d", consecutive)
                    .isEqualTo(7L + expected);
        }
    }

    /** A fresh (never-paced) state is always eligible, whatever the clock reads. */
    @Test
    void freshStateIsAlwaysEligible() {
        assertThat(policy.decide(IdleStealPacingState.INITIAL, 0L)).isEqualTo(IdleStealPacingDecision.ELIGIBLE);
        assertThat(policy.decide(IdleStealPacingState.INITIAL, Long.MAX_VALUE)).isEqualTo(IdleStealPacingDecision.ELIGIBLE);
    }

    /** {@code decide}'s threshold: strictly before the armed instant is paced, at or after is eligible. */
    @Test
    void decideIsPacedStrictlyBeforeTheArmedInstantAndEligibleAtOrAfterIt() {
        IdleStealPacingState armed = new IdleStealPacingState(1, 1_000L);
        assertThat(policy.decide(armed, 999L)).isEqualTo(IdleStealPacingDecision.PACED);
        assertThat(policy.decide(armed, 1_000L)).isEqualTo(IdleStealPacingDecision.ELIGIBLE);
        assertThat(policy.decide(armed, 1_001L)).isEqualTo(IdleStealPacingDecision.ELIGIBLE);
    }

    /** The exponential ladder doubles per non-productive outcome, from the base through several rungs. */
    @Test
    void onNonProductiveGrowsExponentiallyUntilItSaturates() {
        long now = 0L;
        IdleStealPacingState state = IdleStealPacingState.INITIAL;

        state = policy.onNonProductive(state, now);
        assertThat(state.consecutiveNonProductive()).isEqualTo(1);
        assertThat(state.nextAttemptNanos()).isEqualTo(now + BASE_NANOS);   // base << 0

        state = policy.onNonProductive(state, now);
        assertThat(state.consecutiveNonProductive()).isEqualTo(2);
        assertThat(state.nextAttemptNanos()).isEqualTo(now + BASE_NANOS * 2);   // base << 1

        state = policy.onNonProductive(state, now);
        assertThat(state.consecutiveNonProductive()).isEqualTo(3);
        assertThat(state.nextAttemptNanos()).isEqualTo(now + BASE_NANOS * 4);   // base << 2
    }

    /** Once the raw exponential shift would exceed the cap, the armed window saturates at the cap. */
    @Test
    void onNonProductiveNeverArmsPastTheCap() {
        // base=5ms, cap=50ms: base<<3 = 40ms (below cap), base<<4 = 80ms (above cap) -- so the 5th
        // consecutive non-productive outcome (shift=4) is where the cap first binds.
        IdleStealPacingState state = new IdleStealPacingState(4, 0L);
        state = policy.onNonProductive(state, 0L);
        assertThat(state.consecutiveNonProductive()).isEqualTo(5);
        assertThat(state.nextAttemptNanos()).isEqualTo(CAP_NANOS);

        // A persistently non-productive fleet: still capped, however many trips accumulate.
        IdleStealPacingState manyTrips = new IdleStealPacingState(1_000, 0L);
        manyTrips = policy.onNonProductive(manyTrips, 0L);
        assertThat(manyTrips.nextAttemptNanos()).isEqualTo(CAP_NANOS);
    }

    /** Decay: elapsed real time (not an explicit consume) is what drains an armed window back to eligible. */
    @Test
    void anArmedWindowDecaysToEligibleOnceEnoughTimeHasElapsed() {
        IdleStealPacingState state = policy.onNonProductive(IdleStealPacingState.INITIAL, 0L);   // arms base=5ms
        assertThat(policy.decide(state, BASE_NANOS - 1)).isEqualTo(IdleStealPacingDecision.PACED);
        assertThat(policy.decide(state, BASE_NANOS)).isEqualTo(IdleStealPacingDecision.ELIGIBLE);
    }

    /** {@code parkNanos}: the un-paced base once the window has elapsed, the exact remaining window otherwise. */
    @Test
    void parkNanosIsTheBaseWhenEligibleAndTheRemainingWindowWhenPaced() {
        assertThat(policy.parkNanos(IdleStealPacingState.INITIAL, 0L)).isEqualTo(BASE_NANOS);

        IdleStealPacingState armed = new IdleStealPacingState(1, 1_000L);
        assertThat(policy.parkNanos(armed, 400L)).isEqualTo(600L);
        assertThat(policy.parkNanos(armed, 1_000L)).isEqualTo(BASE_NANOS);   // exactly elapsed -> base
        assertThat(policy.parkNanos(armed, 1_500L)).isEqualTo(BASE_NANOS);   // already elapsed -> base
    }

    /** Reset-on-carve: a created child, claimed work, or a non-empty page commit clears pacing entirely. */
    @Test
    void resetClearsToInitial() {
        IdleStealPacingState state = policy.onNonProductive(IdleStealPacingState.INITIAL, 0L);
        assertThat(policy.onReset()).isEqualTo(IdleStealPacingState.INITIAL);
        assertThat(policy.decide(policy.onReset(), 0L)).isEqualTo(IdleStealPacingDecision.ELIGIBLE);
        // onReset() is independent of the state passed to onNonProductive above -- confirms it is not
        // itself a decay off the current state but an unconditional clear.
        assertThat(state.consecutiveNonProductive()).isEqualTo(1);
    }
}
