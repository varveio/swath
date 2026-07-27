/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.kernel;

/**
 * One SplitMix64 draw stream. Every random number a simulated run consumes comes from one of these,
 * and every one of them is derived from the run's single base seed — there is no ambient randomness
 * anywhere in the simulator (enforced by the module's source guard test).
 *
 * <p><b>Per-actor, per-purpose streams, not one shared generator.</b> A single stream drawn from by
 * every actor would make each actor's sequence depend on the order the kernel happened to interleave
 * them, which is the opposite of reproducible under a policy change: adding one extra decision to
 * actor 3 would shift every subsequent draw of actors 0..N. Each (actor, purpose) pair instead owns
 * its own stream, so a change confined to one actor's latency draws cannot move another actor's
 * decision draws.
 *
 * <p><b>How far that isolation goes, precisely.</b> It is <em>between</em> (actor, purpose) pairs, not
 * within one. Inside a single pair the tape is consumed in event order, so an actor that reaches a
 * given draw one event later than it used to gets a different value — the {@code n}-th draw of a tape
 * is fixed, but which decision is the {@code n}-th is not. This is the same property the engine's own
 * per-worker generator has, and it is why the determinism claim is stated as "one scenario at one seed
 * reproduces itself", never as "a draw is pinned to a decision". Reading it more strongly than that
 * would make an ordinary policy change look like a defect.
 *
 * <p><b>Parity contract with the live engine.</b> This derivation deliberately mirrors
 * {@code io.varve.swath.engine.SeededDecisionRng#deriveWorkerSeed} in swath-core: the same SplitMix64
 * mixing constants, the same golden-ratio offset per identity, and the same refusal to read the
 * worker/actor <em>count</em>. The two are separate implementations on purpose — the sim must not
 * depend on an engine-internal class, and the engine's own seeding must stay opt-in and unchanged —
 * so the parity is a convention, and each side's javadoc names the other so that a change to one is
 * visibly a change that has to be made to both.
 *
 * <p><b>Why a mixed derivation rather than {@code baseSeed + actorId}.</b> {@link #forStream} runs
 * SplitMix64's mixing/finalizer step over the base seed offset by the actor id and again over the
 * result offset by the stream index, so adjacent actors get well-separated, decorrelated streams
 * rather than adjacent {@code java.util.Random}-style seeds. The derivation reads only the actor's
 * own id, never the actor <em>count</em>, so growing or shrinking a scenario's worker count adds or
 * removes streams without reshuffling the ones that stay — which is what lets a concurrency-scaling
 * sweep attribute a difference to concurrency rather than to a different random tape.
 *
 * <p>Not thread-safe, and deliberately not: the kernel is single-threaded.
 */
public final class SimRng {

    /** SplitMix64's golden-ratio increment, the step its state advances by between draws. */
    private static final long GOLDEN_GAMMA = 0x9E3779B97F4A7C15L;

    private long state;

    private SimRng(long seed) {
        this.state = seed;
    }

    /** A stream seeded directly from {@code seed} — for a kernel-owned draw with no owning actor. */
    public static SimRng of(long seed) {
        return new SimRng(seed);
    }

    /** Actor {@code actorId}'s {@code stream} draw tape, a pure function of the three inputs. */
    public static SimRng forStream(long baseSeed, int actorId, SimRngStream stream) {
        return new SimRng(deriveStreamSeed(baseSeed, actorId, stream));
    }

    /**
     * The seed of actor {@code actorId}'s {@code stream} tape. Exposed so a scenario can assert the
     * derivation's decorrelation properties directly, without pumping draws out of a generator.
     */
    public static long deriveStreamSeed(long baseSeed, int actorId, SimRngStream stream) {
        return mix64(mix64(baseSeed + actorId * GOLDEN_GAMMA) + stream.ordinal() * GOLDEN_GAMMA);
    }

    /** The next 64 bits of this stream. */
    public long nextLong() {
        state += GOLDEN_GAMMA;
        return mix64(state);
    }

    /**
     * A uniformly distributed {@code int} in {@code [0, bound)}. Rejection-sampled rather than
     * modulo-folded, so the distribution is exactly uniform for every bound, not only powers of two.
     *
     * @param bound exclusive upper bound, {@code > 0}
     */
    public int nextInt(int bound) {
        if (bound <= 0) {
            throw new IllegalArgumentException("bound must be positive, got " + bound);
        }
        int mask = bound - 1;
        if ((bound & mask) == 0) {
            return (int) ((bound * (nextLong() >>> 33)) >>> 31);
        }
        int candidate;
        int value;
        do {
            candidate = (int) (nextLong() >>> 33);
            value = candidate % bound;
        } while (candidate - value + mask < 0);
        return value;
    }

    /** A uniformly distributed {@code double} in {@code [0, 1)}. */
    public double nextDouble() {
        return (nextLong() >>> 11) * 0x1.0p-53;
    }

    /** SplitMix64's finalizer — the mixing function the derivation and every draw share. */
    private static long mix64(long z) {
        long x = z;
        x = (x ^ (x >>> 30)) * 0xBF58476D1CE4E5B9L;
        x = (x ^ (x >>> 27)) * 0x94D049BB133111EBL;
        return x ^ (x >>> 31);
    }
}
