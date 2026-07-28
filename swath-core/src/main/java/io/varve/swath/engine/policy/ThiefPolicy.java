/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.RemainingWorkEstimator;
import io.varve.swath.engine.StealMath;
import io.varve.swath.engine.policy.VictimMutation.Kind;
import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.model.KeyBytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The thief brain: victim selection plus the pivot cascade (algorithms.md §3, §3.1, §3.2, §3.3) as
 * a {@link StealPolicy}. Byte ordering is S3-lexicographic throughout — {@link StealMath} and
 * {@link ByteMidpoint} are called directly as the concrete things they are; this extraction
 * deliberately does not introduce an ordering/comparator/key-codec abstraction for a hypothetical
 * second source (rule of three — see contracts.md §2.1).
 */
public final class ThiefPolicy implements StealPolicy {

    /**
     * The {@code max_keys} of the demand-driven {@code delimiter=/} structure probe: one bounded
     * page (never paginated), from which the median-or-furthest qualifying boundary is taken. Public
     * so the executor can size the real {@code PageRequest} it issues for every
     * {@link RequestStructureProbe} — see that type's javadoc for why the cap isn't threaded through
     * the action itself. See the former {@code Thief.STRUCTURE_PROBE_MAX_KEYS} javadoc (moved here)
     * for the full sizing rationale against the probe attempt timeout.
     */
    public static final int STRUCTURE_PROBE_MAX_KEYS = 32;

    /**
     * The cap on extra {@code delimiter=/} probes the parent-empty sliver's adaptive back-out issues
     * past the first (coarsest) level — one steal attempt fires at most this many {@code + 1}
     * structure probes on that path.
     *
     * <p>Package-private (not {@code private}), like {@link OwnerSplitGovernor}'s
     * {@code SUPPRESS_THRESHOLD}/{@code PROBE_K} — so {@code ThiefPolicyConstantsTest} can pin its
     * literal value directly, the same reason every tuned constant in this file below is
     * package-private rather than {@code private}.
     */
    static final int MAX_STRUCTURE_BACKOUT_LEVELS = 3;

    /** Safety margin, in halvings, added to the log-scaled empty-upper bisection budget. */
    static final int EMPTY_UPPER_BISECTION_MARGIN = 6;

    /** Bytes (from the {@code (cursor, hi)} divergence position) used to estimate the bisection gap width. */
    static final int BAND_WIDTH_SUFFIX_BYTES = 2;

    /** Fallback gap width for the open frontier's bisection budget (no upper bound to measure against). */
    static final long OPEN_FRONTIER_BAND_WIDTH_FALLBACK = 40L;

    /** Consecutive zero-fan-out structure probes against one victim before probing suppresses there. */
    static final int STRUCTURE_ZERO_FANOUT_SUPPRESS_THRESHOLD = 8;

    /** Consecutive TIMED-OUT structure probes against one victim before probing suppresses there. */
    static final int STRUCTURE_TIMEOUT_SUPPRESS_THRESHOLD = 2;

    /** While suppressed, still issue 1-in-N structure probes so late-appearing structure recovers. */
    static final int STRUCTURE_SUPPRESS_RETRY_DIVISOR = 64;

    /** The ⊥ sentinel (cursor at range start) as bytes, for byte comparisons/{@link ByteMidpoint}. */
    private static final byte[] BOTTOM = new byte[0];

    private final EngineToggles toggles;
    private final byte[] scanPrefix;
    private final byte[] prefixCeiling;
    private final DecisionRng rng;
    private final RemainingWorkEstimator estimator;

    /**
     * @param toggles    the {@code --engine-toggle} ablation namespace this run was constructed with
     * @param scanPrefix the scan's own prefix {@code P} — the floor every structure-probe/leaf
     *                   directory prefix is clamped to, and the basis of {@code prefixCeil(P)} for
     *                   the open-frontier extrapolation
     * @param rng        the one random draw the cascade needs (the structure-probe suppression
     *                   recovery's escape hatch, issue #20) — injected so this decision is
     *                   reproducible from its inputs like every other; see {@link DecisionRng}
     * @param estimator  the position sensor victim selection scores on; {@code null}-defaults to
     *                   {@link RemainingWorkEstimator#WINDOW}, the shipped reading, so a caller that
     *                   does not steer this seam gets exactly the arithmetic it always had
     */
    public ThiefPolicy(EngineToggles toggles, byte[] scanPrefix, DecisionRng rng,
                       RemainingWorkEstimator estimator) {
        this.toggles = toggles == null ? EngineToggles.DEFAULT : toggles;
        this.scanPrefix = scanPrefix;
        this.prefixCeiling = StealMath.prefixCeil(scanPrefix);
        this.rng = rng;
        this.estimator = estimator == null ? RemainingWorkEstimator.WINDOW : estimator;
    }

    @Override
    public Selection selectVictim(List<VictimView> pool) {
        List<Engagement> engagements = new ArrayList<>();
        List<VictimMutation> mutations = new ArrayList<>();
        VictimView chosen = null;
        double best = Double.NEGATIVE_INFINITY;
        int seen = 0;
        int skippedUnsplittable = 0;
        int skippedPaced = 0;
        int skippedNoSpan = 0;
        for (VictimView w : pool) {
            seen++;
            if (w.unsplittable()) {
                skippedUnsplittable++;
                continue;
            }
            if (w.pacingSkipAvailable()) {
                skippedPaced++;
                mutations.add(new VictimMutation(w.nodeId(), Kind.CONSUME_PACING_SKIP));
                engagements.add(new Engagement("STEAL", "futility_paced"));
                continue;
            }
            double est = estimator.estRemaining(w.cursor(), w.lo(), w.hi(), w.keysEmitted());
            if (est <= 0.0) {
                skippedNoSpan++;
                continue;
            }
            if (est > best) {
                best = est;
                chosen = w;
            }
        }
        if (chosen == null) {
            NoVictimReason reason;
            if (seen == 0) {
                reason = NoVictimReason.POOL_EMPTY;
            } else if (skippedNoSpan == seen) {
                reason = NoVictimReason.ALL_NO_REMAINING_SPAN;
            } else if (skippedPaced == seen) {
                reason = NoVictimReason.ALL_FUTILITY_PACED;
            } else if (skippedUnsplittable == seen) {
                reason = NoVictimReason.ALL_UNSPLITTABLE;
            } else {
                reason = NoVictimReason.MIXED_SKIPS;
            }
            return new NoVictim(reason, engagements, mutations);
        }
        // What the sensor's reading of the WINNING candidate did (§5 discipline) — once per attempt
        // rather than once per scored candidate, so the signal costs one classification per steal
        // instead of one per pool member. That count is a different denominator from the owner gate's
        // per-page-commit one, so it carries its own category. Inert for the shipped reading.
        estimator.classify("SENSING_STEAL", chosen.cursor(), chosen.lo(), chosen.hi(),
                chosen.keysEmitted(), engagements);
        return new Selected(chosen.nodeId(), engagements, mutations);
    }

    @Override
    public StealAttempt beginAttempt(StealAttemptView view) {
        return new Attempt(view);
    }

    private int scanFloor() {
        return scanPrefix == null ? 0 : scanPrefix.length;
    }

    // -------------------------------------------------------------------------
    // Pure helpers moved verbatim from Thief (cascade-internal; algorithms.md §3/§3.1/§3.2).
    // -------------------------------------------------------------------------

    private static int commonPrefixLen(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        int i = 0;
        while (i < n && a[i] == b[i]) {
            i++;
        }
        return i;
    }

    /**
     * Whether {@code m} is the cursor-adjacent MIN_SAFE sliver: {@code cForPivot} with exactly one
     * {@code 0x20} byte appended — the byte-exact, deterministic signature of the parent-empty
     * degenerate pivot {@link ByteMidpoint#between} is forced to return when the bounds diverge at
     * adjacent code points.
     */
    private static boolean isCursorAdjacentSliver(byte[] m, byte[] cForPivot) {
        return m.length == cForPivot.length + 1
                && m[m.length - 1] == (byte) 0x20
                && Arrays.equals(m, 0, cForPivot.length, cForPivot, 0, cForPivot.length);
    }

    /** The end index (just past the next {@code /}) of the cursor's directory segment from {@code from}. */
    private static int nextSegmentEnd(byte[] cursor, int from) {
        for (int i = from; i < cursor.length; i++) {
            if (cursor[i] == '/') {
                return i + 1;
            }
        }
        return from;
    }

    /** The {@code /}-terminated directory prefix where {@code cursor} and {@code hi} first diverge. */
    private static int divergenceFlooredPrefixLen(byte[] cursor, byte[] hi, int floor) {
        int common = commonPrefixLen(cursor, hi);
        int len = 0;
        for (int i = common - 1; i >= 0; i--) {
            if (cursor[i] == '/') {
                len = i + 1;
                break;
            }
        }
        return Math.max(len, floor);
    }

    /** The cursor's deepest {@code /}-terminated directory prefix (the leaf directory) length. */
    private static int leafDirPrefixLen(byte[] cForPivot, int floor) {
        int len = 0;
        for (int i = cForPivot.length - 1; i >= 0; i--) {
            if (cForPivot[i] == '/') {
                len = i + 1;
                break;
            }
        }
        return Math.max(len, floor);
    }

    private static byte[] minUnsigned(byte[] a, byte[] b) {
        return KeyBytes.compareUnsigned(a, b) <= 0 ? a : b;
    }

    private static int emptyUpperBisectionBudget(byte[] cForPivot, byte[] hi) {
        long width = (hi == null) ? OPEN_FRONTIER_BAND_WIDTH_FALLBACK : bandWidthBytes(cForPivot, hi);
        int log2Width = 64 - Long.numberOfLeadingZeros(Math.max(1L, width));
        return log2Width + EMPTY_UPPER_BISECTION_MARGIN;
    }

    private static long bandWidthBytes(byte[] c, byte[] hi) {
        int i = commonPrefixLen(c, hi);
        long a = suffixValue(c, i);
        long b = suffixValue(hi, i);
        return Math.max(1L, Math.abs(b - a));
    }

    private static long suffixValue(byte[] arr, int from) {
        long v = 0L;
        for (int k = 0; k < BAND_WIDTH_SUFFIX_BYTES; k++) {
            int idx = from + k;
            v = (v << 8) | ((idx < arr.length) ? (arr[idx] & 0xFFL) : 0L);
        }
        return v;
    }

    // -------------------------------------------------------------------------
    // One steal attempt's cascade state machine.
    // -------------------------------------------------------------------------

    /** Which probe {@link #onProbeResult} is currently awaiting the answer to. */
    private enum Phase {
        AWAITING_INITIAL_KEY_PROBE,
        AWAITING_STEP_BACK_KEY_PROBE,
        AWAITING_STRUCTURE_PROBE_EMPTY_UPPER,
        AWAITING_STRUCTURE_PROBE_ADAPTIVE,
        AWAITING_REFLECT_KEY_PROBE,
        AWAITING_BISECT_KEY_PROBE,
        AWAITING_FLOOR_PROBE
    }

    private final class Attempt implements StealAttempt {

        private final StealAttemptView view;
        private final byte[] cForPivot;
        private final byte[] hi;
        private final double farAheadFraction;

        /** Marks/mutations this step has accumulated, drained into whichever {@link StealAction} it ends on. */
        private final List<Engagement> pendingEngagements = new ArrayList<>();
        private final List<VictimMutation> pendingMutations = new ArrayList<>();

        private Phase phase;
        private byte[] m;
        private PivotMechanism mechanism;
        private boolean upperEmpty;
        private boolean parentEmptySliver;

        // Parent-empty sliver's coarse→fine structure back-out.
        private int adaptiveLen;
        private int adaptiveLevel;

        // Empty-upper retry-nearer-cursor bisection.
        private int bisectBudget;
        private int bisectAttempt;

        // Density reflection: the pivot under probe, held between the request and its result.
        private byte[] reflectPivot;

        // Flat-leaf density reflection: the leaf directory and its clamp ceiling.
        private byte[] leafDir;
        private byte[] leafCeiling;

        Attempt(StealAttemptView view) {
            this.view = view;
            this.cForPivot = view.cursor() == null ? BOTTOM : view.cursor();
            this.hi = view.hi();
            this.farAheadFraction = (hi == null) ? 0.5 : toggles.farAheadFraction(view.densityFraction());
        }

        @Override
        public StealAction start() {
            if (view.unchangedSinceNonProductiveSteal()) {
                return retry(RetryReason.UNCHANGED_NONPRODUCTIVE_SNAPSHOT);
            }
            if (hi != null && KeyBytes.compareUnsigned(cForPivot, hi) >= 0) {
                addMutation(Kind.MARK_NON_PRODUCTIVE);
                return retry(RetryReason.CURSOR_AT_OR_PAST_HI);
            }
            m = (hi == null)
                    ? StealMath.extrapolate(view.lo(), cForPivot, prefixCeiling)
                    : toggles.interpolate(cForPivot, hi, farAheadFraction, view.alphabetDigest(), pendingEngagements);
            mechanism = (hi == null) ? PivotMechanism.EXTRAPOLATE
                    : (farAheadFraction > 0.5 ? PivotMechanism.FAR_AHEAD : PivotMechanism.MIDPOINT);
            if (m == null) {
                if (hi == null && StealMath.isUnstartedFrontier(view.lo(), view.cursor())) {
                    addMutation(Kind.MARK_NON_PRODUCTIVE);
                    return retry(RetryReason.UNSTARTED_FRONTIER);
                }
                addMutation(Kind.SET_UNSPLITTABLE);
                addMutation(Kind.RECORD_NO_PIVOT_TALLY);
                return new MarkUnsplittable(UnsplittableReason.NO_PIVOT, takeEngagements(), takeMutations());
            }
            addAlphabetEngagement();
            return requestKeyProbe(m, KeyProbePhase.INITIAL, Phase.AWAITING_INITIAL_KEY_PROBE);
        }

        @Override
        public StealAction onProbeResult(ProbeOutcome outcome) {
            return switch (phase) {
                case AWAITING_INITIAL_KEY_PROBE -> afterInitialKeyProbe((KeyProbeOutcome) outcome);
                case AWAITING_STEP_BACK_KEY_PROBE -> afterStepBackKeyProbe((KeyProbeOutcome) outcome);
                case AWAITING_STRUCTURE_PROBE_EMPTY_UPPER -> afterEmptyUpperStructureProbe((StructureProbeOutcome) outcome);
                case AWAITING_STRUCTURE_PROBE_ADAPTIVE -> afterAdaptiveStructureProbe((StructureProbeOutcome) outcome);
                case AWAITING_REFLECT_KEY_PROBE -> afterReflectKeyProbe((KeyProbeOutcome) outcome);
                case AWAITING_BISECT_KEY_PROBE -> afterBisectKeyProbe((KeyProbeOutcome) outcome);
                case AWAITING_FLOOR_PROBE -> afterFloorProbe((FloorProbeOutcome) outcome);
            };
        }

        // ---- Step 1/2: place, probe, far-ahead step-back --------------------------------

        private void addAlphabetEngagement() {
            if (hi == null) {
                return;
            }
            byte[] plain = StealMath.interpolate(cForPivot, hi, farAheadFraction);
            addEngagement("ALPHABET", Arrays.equals(m, plain) ? "alphabet_fallback" : "alphabet_chosen");
        }

        private StealAction afterInitialKeyProbe(KeyProbeOutcome outcome) {
            upperEmpty = !outcome.nonEmpty();
            if (upperEmpty && hi != null && farAheadFraction > 0.5) {
                byte[] midpoint = StealMath.interpolate(cForPivot, hi, 0.5);
                if (midpoint != null && !Arrays.equals(midpoint, m)) {
                    m = midpoint;
                    mechanism = PivotMechanism.MIDPOINT;
                    addEngagement("PIVOT", PivotMechanism.STEP_BACK.code());
                    return requestKeyProbe(m, KeyProbePhase.STEP_BACK, Phase.AWAITING_STEP_BACK_KEY_PROBE);
                }
            }
            return continueAfterUpperEmptyKnown();
        }

        private StealAction afterStepBackKeyProbe(KeyProbeOutcome outcome) {
            upperEmpty = !outcome.nonEmpty();
            return continueAfterUpperEmptyKnown();
        }

        // ---- Step 3: structure discovery, or fall through to reflect/bisect/flat-leaf ----

        private StealAction continueAfterUpperEmptyKnown() {
            parentEmptySliver = hi != null && !upperEmpty && view.cursor() != null
                    && isCursorAdjacentSliver(m, cForPivot);
            if (!upperEmpty && !parentEmptySliver) {
                return commit();
            }
            if (hi == null) {
                return structureExhaustedFallback();
            }
            if (!structureProbingEnabled()) {
                addStructureSuppressionMarks();
                return structureExhaustedFallback();
            }
            return parentEmptySliver ? beginAdaptiveStructureSearch() : beginEmptyUpperStructureProbe();
        }

        private boolean structureProbingEnabled() {
            if (!toggles.structureProbes()) {
                return false;
            }
            if (view.consecutiveZeroFanoutStructureProbes() < STRUCTURE_ZERO_FANOUT_SUPPRESS_THRESHOLD
                    && view.consecutiveTimedOutStructureProbes() < STRUCTURE_TIMEOUT_SUPPRESS_THRESHOLD) {
                return true;
            }
            // The 1-in-STRUCTURE_SUPPRESS_RETRY_DIVISOR escape hatch (issue #20): injected via
            // DecisionRng rather than ambient ThreadLocalRandom, so this decision is reproducible
            // from ThiefPolicy's inputs (toggles, scanPrefix, rng) like every other in the cascade.
            return rng.nextInt(STRUCTURE_SUPPRESS_RETRY_DIVISOR) == 0;
        }

        private void addStructureSuppressionMarks() {
            if (!toggles.structureProbes()) {
                addEngagement("STRUCTURE", "toggle_disabled");
                return;
            }
            boolean timedOut = view.consecutiveTimedOutStructureProbes() >= STRUCTURE_TIMEOUT_SUPPRESS_THRESHOLD;
            addEngagement("STRUCTURE", timedOut ? "suppressed_probe_timeout" : "suppressed_zero_fanout");
            // WorkerState#recordStructureSuppressed() is a slow-range-dump-only tally, never consulted
            // by any split/steal decision — so this mutation cannot affect the goldens. The original
            // Thief code only applied it when a metrics sink was attached (it lived inside the same
            // `if (metrics != null)` block as the recordStealReason calls above); the executor
            // preserves that exact coupling for this one mutation kind rather than papering over it.
            addMutation(Kind.RECORD_STRUCTURE_SUPPRESSED_TALLY);
        }

        private byte[] structureProbePrefix() {
            byte[] lo = view.lo() == null ? BOTTOM : view.lo();
            int common = commonPrefixLen(lo, cForPivot);
            int len = 0;
            for (int i = common - 1; i >= 0; i--) {
                if (cForPivot[i] == '/') {
                    len = i + 1;
                    break;
                }
            }
            return Arrays.copyOf(cForPivot, Math.max(len, scanFloor()));
        }

        private StealAction beginEmptyUpperStructureProbe() {
            return requestStructureProbe(structureProbePrefix(), Phase.AWAITING_STRUCTURE_PROBE_EMPTY_UPPER);
        }

        private StealAction beginAdaptiveStructureSearch() {
            adaptiveLen = divergenceFlooredPrefixLen(cForPivot, hi, scanFloor());
            adaptiveLevel = 0;
            return requestStructureProbe(Arrays.copyOf(cForPivot, adaptiveLen), Phase.AWAITING_STRUCTURE_PROBE_ADAPTIVE);
        }

        /**
         * Filters {@code outcome}'s raw common prefixes to those strictly in {@code (cForPivot, hi)},
         * sorts, and returns the median (complete sample) or the furthest (capped sample) — or
         * {@code null} when nothing qualifies. Always records the fan-out streak mutation and the
         * fanout-capped mark, whether or not a boundary is found (mirrors the original
         * {@code structurePivot}, which does both unconditionally before the filtering below).
         */
        private byte[] pickStructureBoundary(StructureProbeOutcome outcome) {
            addMutation(outcome.commonPrefixes().isEmpty()
                    ? Kind.RECORD_ZERO_FANOUT_STRUCTURE_PROBE
                    : Kind.RESET_ZERO_FANOUT_STRUCTURE_PROBES);
            if (outcome.fanoutSampleCapped()) {
                addEngagement("STRUCTURE", "fanout_capped");
            }
            List<byte[]> boundaries = new ArrayList<>();
            for (byte[] b : outcome.commonPrefixes()) {
                if (KeyBytes.compareUnsigned(b, cForPivot) <= 0 || KeyBytes.compareUnsigned(b, hi) >= 0) {
                    continue;
                }
                boundaries.add(b);
            }
            if (boundaries.isEmpty()) {
                return null;
            }
            boundaries.sort(KeyBytes::compareUnsigned);
            return outcome.fanoutSampleCapped()
                    ? boundaries.get(boundaries.size() - 1)
                    : boundaries.get(boundaries.size() / 2);
        }

        private StealAction afterEmptyUpperStructureProbe(StructureProbeOutcome outcome) {
            byte[] boundary = pickStructureBoundary(outcome);
            if (boundary == null) {
                return structureExhaustedFallback();
            }
            m = boundary;
            mechanism = outcome.fanoutSampleCapped() ? PivotMechanism.STRUCTURE_CAPPED : PivotMechanism.STRUCTURE_PROBE;
            return commit();
        }

        private StealAction afterAdaptiveStructureProbe(StructureProbeOutcome outcome) {
            byte[] boundary = pickStructureBoundary(outcome);
            if (boundary != null) {
                m = boundary;
                mechanism = outcome.fanoutSampleCapped()
                        ? PivotMechanism.ADAPTIVE_STRUCTURE_CAPPED : PivotMechanism.ADAPTIVE_STRUCTURE;
                return commit();
            }
            int next = nextSegmentEnd(cForPivot, adaptiveLen);
            if (next <= adaptiveLen || adaptiveLevel >= MAX_STRUCTURE_BACKOUT_LEVELS) {
                return structureExhaustedFallback();
            }
            adaptiveLen = next;
            adaptiveLevel++;
            return requestStructureProbe(Arrays.copyOf(cForPivot, adaptiveLen), Phase.AWAITING_STRUCTURE_PROBE_ADAPTIVE);
        }

        // ---- Step 4/6: density reflection, bisection, or the flat-leaf sliver ----

        private StealAction structureExhaustedFallback() {
            return upperEmpty ? beginReflectOrBisect() : beginFlatLeafOrCommitSliver();
        }

        private StealAction beginReflectOrBisect() {
            if (toggles.reflect() && hi != null) {
                byte[] mReflect = StealMath.extrapolate(view.lo(), cForPivot, hi);
                if (mReflect != null
                        && KeyBytes.compareUnsigned(cForPivot, mReflect) < 0
                        && KeyBytes.compareUnsigned(mReflect, m) < 0) {
                    reflectPivot = mReflect;
                    return requestKeyProbe(mReflect, KeyProbePhase.REFLECT, Phase.AWAITING_REFLECT_KEY_PROBE);
                }
            }
            return beginBisection();
        }

        private StealAction afterReflectKeyProbe(KeyProbeOutcome outcome) {
            m = reflectPivot;
            if (outcome.nonEmpty()) {
                addEngagement("PIVOT", PivotMechanism.REFLECT_HIT.code());
                mechanism = PivotMechanism.REFLECT;
                return commit();
            }
            addEngagement("PIVOT", PivotMechanism.REFLECT_EMPTY.code());
            return beginBisection();
        }

        private StealAction beginBisection() {
            bisectBudget = emptyUpperBisectionBudget(cForPivot, hi);
            bisectAttempt = 0;
            return nextBisectionStep();
        }

        private StealAction afterBisectKeyProbe(KeyProbeOutcome outcome) {
            if (outcome.nonEmpty()) {
                mechanism = PivotMechanism.BISECT;
                return commit();
            }
            return nextBisectionStep();
        }

        private StealAction nextBisectionStep() {
            if (bisectAttempt >= bisectBudget) {
                addMutation(Kind.MARK_NON_PRODUCTIVE);
                addMutation(Kind.RECORD_FUTILE_STEAL);
                return retry(RetryReason.BISECT_BUDGET_EXHAUSTED);
            }
            bisectAttempt++;
            m = ByteMidpoint.between(cForPivot, m);
            if (m == null) {
                addMutation(Kind.MARK_NON_PRODUCTIVE);
                return retry(RetryReason.RETRY_PIVOT_ADJACENT);
            }
            return requestKeyProbe(m, KeyProbePhase.BISECT, Phase.AWAITING_BISECT_KEY_PROBE);
        }

        private StealAction beginFlatLeafOrCommitSliver() {
            leafDir = Arrays.copyOf(cForPivot, leafDirPrefixLen(cForPivot, scanFloor()));
            byte[] leafDirCeiling = StealMath.prefixCeil(leafDir);
            leafCeiling = (leafDirCeiling == null) ? hi : minUnsigned(hi, leafDirCeiling);

            byte[] lo = view.lo();
            if (lo != null && lo.length > leafDir.length && commonPrefixLen(lo, leafDir) == leafDir.length) {
                // A real deep key inside the leaf — reflect the child's own consumed span, zero probes.
                return commitFlatLeafOrUnchangedSliver(flatLeafPivot(lo));
            }
            return requestFloorProbe(leafDir);
        }

        private StealAction afterFloorProbe(FloorProbeOutcome outcome) {
            byte[] firstKey = outcome.firstKey();
            if (firstKey == null || KeyBytes.compareUnsigned(firstKey, cForPivot) >= 0) {
                return commit();   // empty leaf, or no consumed span below the cursor — byte-exact sliver
            }
            return commitFlatLeafOrUnchangedSliver(flatLeafPivot(firstKey));
        }

        private byte[] flatLeafPivot(byte[] ref) {
            byte[] m2 = StealMath.extrapolate(ref, cForPivot, leafCeiling);
            if (m2 != null
                    && !isCursorAdjacentSliver(m2, cForPivot)
                    && KeyBytes.compareUnsigned(cForPivot, m2) < 0
                    && KeyBytes.compareUnsigned(m2, hi) < 0) {
                return m2;
            }
            return null;
        }

        private StealAction commitFlatLeafOrUnchangedSliver(byte[] flatLeaf) {
            if (flatLeaf != null) {
                m = flatLeaf;
                mechanism = PivotMechanism.FLAT_LEAF;
            }
            return commit();
        }

        // ---- Action builders: drain whatever this step accumulated -----------------------

        private StealAction commit() {
            return new Commit(m, mechanism, takeEngagements(), takeMutations());
        }

        private StealAction retry(RetryReason reason) {
            return new Retry(reason, takeEngagements(), takeMutations());
        }

        private StealAction requestKeyProbe(byte[] pivot, KeyProbePhase probePhase, Phase next) {
            phase = next;
            return new RequestKeyProbe(pivot, hi, probePhase, takeEngagements(), takeMutations());
        }

        private StealAction requestStructureProbe(byte[] probePrefix, Phase next) {
            phase = next;
            return new RequestStructureProbe(probePrefix, view.cursor(), takeEngagements(), takeMutations());
        }

        private StealAction requestFloorProbe(byte[] leafPrefix) {
            phase = Phase.AWAITING_FLOOR_PROBE;
            return new RequestFloorProbe(leafPrefix, takeEngagements(), takeMutations());
        }

        private void addEngagement(String category, String reason) {
            pendingEngagements.add(new Engagement(category, reason));
        }

        private void addMutation(Kind kind) {
            pendingMutations.add(new VictimMutation(view.victimNodeId(), kind));
        }

        private List<Engagement> takeEngagements() {
            List<Engagement> out = List.copyOf(pendingEngagements);
            pendingEngagements.clear();
            return out;
        }

        private List<VictimMutation> takeMutations() {
            List<VictimMutation> out = List.copyOf(pendingMutations);
            pendingMutations.clear();
            return out;
        }
    }
}
