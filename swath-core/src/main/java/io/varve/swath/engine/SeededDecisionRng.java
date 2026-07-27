/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.engine.policy.DecisionRng;
import io.varve.swath.runtime.RunContext;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The opt-in seeded live {@link DecisionRng}: a per-worker deterministic
 * stream instead of {@link Thief}'s ambient {@code ThreadLocalRandom} default, threaded when {@link
 * EngineContext#decisionRngSeed()} is non-null. Unset (the documented default), {@link
 * WorkStealingScan} never constructs this class at all — the live engine's default draw stays exactly
 * {@code bound -> ThreadLocalRandom.current().nextInt(bound)}, byte-identical to every run before this
 * class existed.
 *
 * <p><b>Why per-worker, not one shared stream.</b> {@link Thief} is one instance shared by every
 * worker (many workers, one {@code ThiefPolicy}, hence one draw source — see {@link DecisionRng}'s own
 * javadoc), so a single {@code java.util.Random} drawn from concurrently would make each worker's
 * sequence depend on scheduling — the opposite of reproducible. Each call instead resolves the
 * CALLING worker's own stream via {@link RunContext#workerIdOrNone()} (the same stable per-worker
 * identity every {@code worker_id=} log line and {@code RunContext.runWorkerWhereBound} scope already
 * use), lazily seeding it on first use.
 *
 * <p><b>Why a derived seed, not {@code baseSeed + workerId}.</b> {@link #deriveWorkerSeed} runs the
 * SplitMix64 mixing/finalizer step over {@code (baseSeed, workerId)} so worker {@code k}'s stream is a
 * well-distributed, collision-resistant function of both inputs rather than a trivially-correlated
 * linear offset (adjacent worker ids would otherwise draw from adjacent, weakly-decorrelated {@code
 * java.util.Random} seeds). It is also independent of how many OTHER workers a run has: growing or
 * shrinking {@code workerCount} only adds or removes streams, it never reshuffles the ones that stay,
 * because the derivation never reads worker COUNT, only the one worker's own stable identity.
 *
 * <p>Never held as a field of any {@code io.varve.swath.engine.policy} type — only the {@link
 * DecisionRng} interface is (in {@link io.varve.swath.engine.policy.ThiefPolicy}) — so this concrete
 * class, like {@link Thief}'s own ambient default, sits in {@code DecisionPathPurityTest}'s documented
 * Gap 1 (an injected implementation's body is unreachable from the policy's field-type closure): its
 * purity is this class's own construction to get right, not something that test enforces.
 */
final class SeededDecisionRng implements DecisionRng {

    private final long baseSeed;
    private final ConcurrentHashMap<Long, Random> perWorker = new ConcurrentHashMap<>();

    SeededDecisionRng(long baseSeed) {
        this.baseSeed = baseSeed;
    }

    @Override
    public int nextInt(int bound) {
        long workerId = RunContext.workerIdOrNone();
        return perWorker.computeIfAbsent(workerId, id -> new Random(deriveWorkerSeed(baseSeed, id)))
                .nextInt(bound);
    }

    /**
     * Worker {@code workerId}'s seed, as a pure function of {@code (baseSeed, workerId)} — SplitMix64's
     * own mixing/finalizer constants, applied to {@code baseSeed} offset by {@code workerId} scaled by
     * the golden-ratio increment SplitMix64 itself advances its state by between draws. Package-private
     * so it is directly unit-testable without constructing a whole {@link SeededDecisionRng}.
     */
    static long deriveWorkerSeed(long baseSeed, long workerId) {
        long z = baseSeed + workerId * 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
