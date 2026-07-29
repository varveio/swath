/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.engine.policy.Engagement;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.observability.RunMetrics;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The {@code --engine-toggle} ablation namespace: one immutable record threaded through the
 * engine constructors, so a per-mechanism A/B measurement of the {@code WorkStealingScan} engine
 * runs from one binary instead of a bespoke flag per experiment.
 *
 * <p><b>EXPERIMENTAL / DIAGNOSTIC — not a supported configuration.</b> {@link #DEFAULT} is the
 * only supported configuration, with one documented exception: {@code rate_anchored_sensing=off}
 * together with {@code tail_floor=current} is the supported rollback to pre-0.2.0 engine behaviour
 * ({@code docs/usage.md}). The ten ablation toggles below default {@code true}, {@code
 * readahead} is opt-in/default-off, and {@code mass_aware_seed} is opt-out/default-on. Turning a
 * mechanism off silences its own counters and fires an explicit {@code TOGGLE.<name>_off} mark
 * (§5 discipline, {@code docs/internals/metrics-internals.md}), so post-hoc analysis never has to
 * infer an ablation from absence alone. Per-toggle effects, defaults, and measured cost profiles
 * are the ablation and performance-toggle tables in {@code docs/usage.md}.
 *
 * <ul>
 *   <li>{@code owner_split} — {@link WorkStealingScan}'s owner-side proactive self-split.</li>
 *   <li>{@code density_ewma} — the EWMA density signal consumed by {@link #farAheadFraction} and
 *       {@link #observedDensityRatio(WorkerState)}.</li>
 *   <li>{@code radix_bands} — {@code SeedStep}'s dense-flat-region radix banding.</li>
 *   <li>{@code structure_probes} — {@link Thief}'s demand-driven {@code delimiter=/} structure
 *       probing.</li>
 *   <li>{@code far_ahead} — the bounded-range steal pivot fraction; see {@link
 *       #farAheadFraction}.</li>
 *   <li>{@code alphabet_pivots} — the {@link AlphabetDigest} consult in {@link #interpolate}.</li>
 *   <li>{@code reflect} — the density-reflected pivot placement; also gates {@code reflect_lift}
 *       below, which never fires while this is off.</li>
 *   <li>{@code confetti_feedback} — the realized-child-mass feedback gate in {@link
 *       OwnerSelfSplit#maybeOwnerSelfSplit}.</li>
 *   <li>{@code fanout_tiling} — {@code SeedStep}'s zero-probe {@code key=value/} partition-fanout
 *       tiling; its interaction with {@code mass_aware_seed} is the precedence rule in {@code
 *       docs/usage.md}.</li>
 *   <li>{@code reflect_lift} — the reflect-lift alone, independent of plain {@code reflect}
 *       staying active; gated on {@code reflect() && reflectLift()}.</li>
 *   <li>{@code readahead} — opt-in, default OFF: lets {@link RangeScanner} engage {@link
 *       SpeculativeReadahead} intra-range speculative readahead.</li>
 *   <li>{@code mass_aware_seed} — opt-out, default ON: lets {@link SeedStep} sample an ambiguous
 *       truncated cut's children to disambiguate a heavy subtree (banded to parallelize) from a
 *       1:1 tiny-leaf explosion (the INT-8 shape, left whole for work-stealing).</li>
 *   <li>{@code tail_floor} — value-taking ({@link TailFloorMode}), default {@code
 *       reach_floored}: which arithmetic the owner-split child-tail floor reads ({@link
 *       StealMath#childTailBelowObservedMassFloor}). {@code reach_floored} is the shipped cure for
 *       the wide-flat blindness that erases an honest estimate; {@code current} is the pre-0.2.0
 *       formula, kept as the rollback arm, and {@code est_direct} is the other raced candidate.</li>
 * </ul>
 */
public record EngineToggles(
        boolean ownerSplit,
        boolean densityEwma,
        boolean radixBands,
        boolean structureProbes,
        boolean farAhead,
        boolean alphabetPivots,
        boolean reflect,
        boolean confettiFeedback,
        boolean reflectLift,
        boolean fanoutTiling,
        boolean readahead,
        boolean massAwareSeed,
        boolean rateAnchoredSensing,
        TailFloorMode tailFloor) {

    /**
     * The one non-boolean component must exist: every consumer (the governor's gate consults, the
     * effective-toggle log, the run-summary writer) dereferences it, so a null would surface as an
     * NPE far from the construction that caused it. {@code parse} can never produce one (absent
     * defaults to {@link TailFloorMode#REACH_FLOORED}, the 0.2.0 default); this guards the public
     * constructor.
     */
    public EngineToggles {
        java.util.Objects.requireNonNull(tailFloor, "tailFloor");
    }

    public EngineToggles withOwnerSplit(boolean ownerSplit) {
        return new EngineToggles(ownerSplit, densityEwma, radixBands, structureProbes, farAhead, alphabetPivots,
                reflect, confettiFeedback, reflectLift, fanoutTiling, readahead, massAwareSeed,
                rateAnchoredSensing, tailFloor);
    }

    public EngineToggles withDensityEwma(boolean densityEwma) {
        return new EngineToggles(ownerSplit, densityEwma, radixBands, structureProbes, farAhead, alphabetPivots,
                reflect, confettiFeedback, reflectLift, fanoutTiling, readahead, massAwareSeed,
                rateAnchoredSensing, tailFloor);
    }

    public EngineToggles withRadixBands(boolean radixBands) {
        return new EngineToggles(ownerSplit, densityEwma, radixBands, structureProbes, farAhead, alphabetPivots,
                reflect, confettiFeedback, reflectLift, fanoutTiling, readahead, massAwareSeed,
                rateAnchoredSensing, tailFloor);
    }

    public EngineToggles withStructureProbes(boolean structureProbes) {
        return new EngineToggles(ownerSplit, densityEwma, radixBands, structureProbes, farAhead, alphabetPivots,
                reflect, confettiFeedback, reflectLift, fanoutTiling, readahead, massAwareSeed,
                rateAnchoredSensing, tailFloor);
    }

    public EngineToggles withFarAhead(boolean farAhead) {
        return new EngineToggles(ownerSplit, densityEwma, radixBands, structureProbes, farAhead, alphabetPivots,
                reflect, confettiFeedback, reflectLift, fanoutTiling, readahead, massAwareSeed,
                rateAnchoredSensing, tailFloor);
    }

    public EngineToggles withAlphabetPivots(boolean alphabetPivots) {
        return new EngineToggles(ownerSplit, densityEwma, radixBands, structureProbes, farAhead, alphabetPivots,
                reflect, confettiFeedback, reflectLift, fanoutTiling, readahead, massAwareSeed,
                rateAnchoredSensing, tailFloor);
    }

    public EngineToggles withReflect(boolean reflect) {
        return new EngineToggles(ownerSplit, densityEwma, radixBands, structureProbes, farAhead, alphabetPivots,
                reflect, confettiFeedback, reflectLift, fanoutTiling, readahead, massAwareSeed,
                rateAnchoredSensing, tailFloor);
    }

    public EngineToggles withConfettiFeedback(boolean confettiFeedback) {
        return new EngineToggles(ownerSplit, densityEwma, radixBands, structureProbes, farAhead, alphabetPivots,
                reflect, confettiFeedback, reflectLift, fanoutTiling, readahead, massAwareSeed,
                rateAnchoredSensing, tailFloor);
    }

    public EngineToggles withReflectLift(boolean reflectLift) {
        return new EngineToggles(ownerSplit, densityEwma, radixBands, structureProbes, farAhead, alphabetPivots,
                reflect, confettiFeedback, reflectLift, fanoutTiling, readahead, massAwareSeed,
                rateAnchoredSensing, tailFloor);
    }

    public EngineToggles withFanoutTiling(boolean fanoutTiling) {
        return new EngineToggles(ownerSplit, densityEwma, radixBands, structureProbes, farAhead, alphabetPivots,
                reflect, confettiFeedback, reflectLift, fanoutTiling, readahead, massAwareSeed,
                rateAnchoredSensing, tailFloor);
    }

    public EngineToggles withReadahead(boolean readahead) {
        return new EngineToggles(ownerSplit, densityEwma, radixBands, structureProbes, farAhead, alphabetPivots,
                reflect, confettiFeedback, reflectLift, fanoutTiling, readahead, massAwareSeed,
                rateAnchoredSensing, tailFloor);
    }

    public EngineToggles withMassAwareSeed(boolean massAwareSeed) {
        return new EngineToggles(ownerSplit, densityEwma, radixBands, structureProbes, farAhead, alphabetPivots,
                reflect, confettiFeedback, reflectLift, fanoutTiling, readahead, massAwareSeed,
                rateAnchoredSensing, tailFloor);
    }

    public EngineToggles withRateAnchoredSensing(boolean rateAnchoredSensing) {
        return new EngineToggles(ownerSplit, densityEwma, radixBands, structureProbes, farAhead, alphabetPivots,
                reflect, confettiFeedback, reflectLift, fanoutTiling, readahead, massAwareSeed,
                rateAnchoredSensing, tailFloor);
    }

    public EngineToggles withTailFloor(TailFloorMode tailFloor) {
        return new EngineToggles(ownerSplit, densityEwma, radixBands, structureProbes, farAhead, alphabetPivots,
                reflect, confettiFeedback, reflectLift, fanoutTiling, readahead, massAwareSeed,
                rateAnchoredSensing, tailFloor);
    }

    /**
     * The only supported configuration — bar the documented {@code rate_anchored_sensing=off} +
     * {@code tail_floor=current} rollback — every ablation toggle on, {@code readahead} off,
     * {@code mass_aware_seed} and {@code rate_anchored_sensing} on, {@code tail_floor} at {@link
     * TailFloorMode#REACH_FLOORED}.
     *
     * <p>The sensing/tail-floor pair became the default in 0.2.0. Both had shipped opt-in so a
     * one-binary A/B could measure them: the pair cures the wide-flat serial tail (~11× end-to-end
     * on a live 13.5M-key bucket, both reps, at slightly fewer API calls) and left key-set output
     * byte-identical across all 114 measurable fixtures of the cached corpus panel. The pre-0.2.0
     * behaviour remains reachable as {@code rate_anchored_sensing=off} plus {@code
     * tail_floor=current}, which is the supported rollback.
     */
    public static final EngineToggles DEFAULT =
            new EngineToggles(true, true, true, true, true, true, true, true, true, true, false, true, true,
                    TailFloorMode.REACH_FLOORED);

    /**
     * Valid ablation {@code --engine-toggle} names (each {@code on} by default, {@code off} to
     * ablate), in the order they are documented/echoed; drives {@link #disabledNames()}. The
     * opt-in {@code readahead} toggle is intentionally excluded — its being off is the normal
     * state, not an ablation.
     */
    public static final List<String> NAMES = List.of(
            "owner_split", "density_ewma", "radix_bands", "structure_probes", "far_ahead", "alphabet_pivots",
            "reflect", "confetti_feedback", "reflect_lift", "fanout_tiling");

    /** The opt-in {@code --engine-toggle readahead=on} name, default OFF; not in {@link #NAMES}. */
    public static final String READAHEAD_NAME = "readahead";

    /**
     * The {@code --engine-toggle mass_aware_seed} name; opt-out,
     * default ON — {@code mass_aware_seed=off} is
     * the documented opt-out. Not in {@link #NAMES}.
     */
    public static final String MASS_AWARE_SEED_NAME = "mass_aware_seed";

    /**
     * The {@code --engine-toggle rate_anchored_sensing} name; opt-out, default ON since 0.2.0, so
     * {@code rate_anchored_sensing=off} is the documented opt-out. Not in {@link #NAMES}. Selects
     * {@link RateAnchoredEstimator} — the simulator's promoted position sensor — as the run's
     * {@link RemainingWorkEstimator} in place of the pre-0.2.0 window reading. It shipped opt-in so
     * a real-bucket A/B could run both arms from one binary; that A/B is what promoted it. Not an
     * ablation in the {@link #NAMES} sense, so it keeps firing an engagement mark on the ON side —
     * post-hoc analysis reads which arm ran rather than assuming the default.
     */
    public static final String RATE_ANCHORED_SENSING_NAME = "rate_anchored_sensing";

    /**
     * The {@code --engine-toggle tail_floor=current|est_direct|reach_floored} name; the ONLY
     * value-taking toggle (its values are {@link TailFloorMode#codes()}, not {@code on}/{@code off}),
     * default {@link TailFloorMode#REACH_FLOORED} since 0.2.0 — the promoted cure. {@code current}
     * is the pre-0.2.0 floor arithmetic, bit-identical, and is the supported rollback. Not in
     * {@link #NAMES}: nothing is turned off, so the selected mode fires an engagement mark ({@code
     * TOGGLE.tail_floor_<mode>_on}) exactly as {@code readahead} and {@code rate_anchored_sensing}
     * do on their ON side.
     */
    public static final String TAIL_FLOOR_NAME = "tail_floor";

    /**
     * The far-ahead fraction substituted for {@link WorkerState#densityFraction()} when {@code
     * density_ewma} is off — {@link WorkerState#MAX_FAR_FRACTION}, the same ceiling the EWMA path
     * saturates at for a uniformly-dense drainer, so {@code density_ewma=off} behaves like "always
     * assume the densest case" rather than an arbitrary unrelated constant.
     */
    static final double DENSITY_EWMA_OFF_FRACTION = WorkerState.MAX_FAR_FRACTION;

    /** The plain code-point byte-midpoint fraction {@code far_ahead=off} pins the pivot at. */
    static final double PLAIN_MIDPOINT_FRACTION = 0.5;

    /**
     * Parse the repeatable {@code --engine-toggle NAME=VALUE} occurrences plus the {@code
     * --no-owner-split} alias into one {@link EngineToggles}. An unknown name, a malformed value
     * (not {@code on}/{@code off} — or, for the value-taking {@link #TAIL_FLOOR_NAME}, not one of
     * {@link TailFloorMode#codes()}), or contradictory values for the same toggle (including a
     * conflict between {@code --no-owner-split} and an explicit {@code --engine-toggle
     * owner_split=on}) is a startup validation error (exit 2) listing the valid names.
     *
     * @param raw            the raw {@code NAME=VALUE} strings, in {@code --engine-toggle}
     *                       occurrence order; {@code null}/empty means no explicit toggles
     * @param noOwnerSplit   {@code --no-owner-split} (the pre-existing kill-switch), folded in as
     *                       {@code owner_split=off} — single source of truth internally
     */
    public static EngineToggles parse(List<String> raw, boolean noOwnerSplit) throws InvalidArgsException {
        Map<String, Boolean> values = new LinkedHashMap<>();
        TailFloorMode tailFloor = null;
        if (raw != null) {
            for (String entry : raw) {
                int eq = entry.indexOf('=');
                if (eq < 0) {
                    throw new InvalidArgsException("--engine-toggle must be NAME=VALUE (got '" + entry + "')");
                }
                String name = entry.substring(0, eq).trim();
                String rawValue = entry.substring(eq + 1).trim();
                if (TAIL_FLOOR_NAME.equals(name)) {
                    TailFloorMode mode = TailFloorMode.fromCode(rawValue);
                    if (mode == null) {
                        throw new InvalidArgsException("--engine-toggle " + TAIL_FLOOR_NAME
                                + ": value must be " + String.join("|", TailFloorMode.codes())
                                + " (got '" + rawValue + "')");
                    }
                    if (tailFloor != null && tailFloor != mode) {
                        throw new InvalidArgsException("--engine-toggle " + TAIL_FLOOR_NAME
                                + " given contradictory values");
                    }
                    tailFloor = mode;
                    continue;
                }
                if (!NAMES.contains(name) && !READAHEAD_NAME.equals(name) && !MASS_AWARE_SEED_NAME.equals(name)
                        && !RATE_ANCHORED_SENSING_NAME.equals(name)) {
                    throw new InvalidArgsException("--engine-toggle: unknown name '" + name
                            + "' (valid names: " + String.join(", ", NAMES) + ", " + READAHEAD_NAME + ", "
                            + MASS_AWARE_SEED_NAME + ", " + RATE_ANCHORED_SENSING_NAME + ", "
                            + TAIL_FLOOR_NAME + ")");
                }
                boolean on = parseOnOff(name, rawValue);
                putConsistent(values, name, on, "--engine-toggle " + name + " given contradictory values");
            }
        }
        if (noOwnerSplit) {
            putConsistent(values, "owner_split", false,
                    "--no-owner-split conflicts with --engine-toggle owner_split=on");
        }
        return new EngineToggles(
                values.getOrDefault("owner_split", true),
                values.getOrDefault("density_ewma", true),
                values.getOrDefault("radix_bands", true),
                values.getOrDefault("structure_probes", true),
                values.getOrDefault("far_ahead", true),
                values.getOrDefault("alphabet_pivots", true),
                values.getOrDefault("reflect", true),
                values.getOrDefault("confetti_feedback", true),
                values.getOrDefault("reflect_lift", true),
                values.getOrDefault("fanout_tiling", true),
                values.getOrDefault(READAHEAD_NAME, false),
                values.getOrDefault(MASS_AWARE_SEED_NAME, true),
                values.getOrDefault(RATE_ANCHORED_SENSING_NAME, true),
                tailFloor == null ? TailFloorMode.REACH_FLOORED : tailFloor);
    }

    private static void putConsistent(Map<String, Boolean> values, String name, boolean on, String conflictMessage)
            throws InvalidArgsException {
        Boolean prev = values.put(name, on);
        if (prev != null && prev != on) {
            throw new InvalidArgsException(conflictMessage);
        }
    }

    private static boolean parseOnOff(String name, String value) throws InvalidArgsException {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "on" -> true;
            case "off" -> false;
            default -> throw new InvalidArgsException(
                    "--engine-toggle " + name + ": value must be on|off (got '" + value + "')");
        };
    }

    /** {@code true} iff every toggle is at its supported default. */
    public boolean isDefault() {
        return DEFAULT.equals(this);
    }

    /** The names of the toggles currently {@code off}, in {@link #NAMES} order. */
    public List<String> disabledNames() {
        List<String> out = new ArrayList<>();
        if (!ownerSplit) {
            out.add("owner_split");
        }
        if (!densityEwma) {
            out.add("density_ewma");
        }
        if (!radixBands) {
            out.add("radix_bands");
        }
        if (!structureProbes) {
            out.add("structure_probes");
        }
        if (!farAhead) {
            out.add("far_ahead");
        }
        if (!alphabetPivots) {
            out.add("alphabet_pivots");
        }
        if (!reflect) {
            out.add("reflect");
        }
        if (!confettiFeedback) {
            out.add("confetti_feedback");
        }
        if (!reflectLift) {
            out.add("reflect_lift");
        }
        if (!fanoutTiling) {
            out.add("fanout_tiling");
        }
        return out;
    }

    /**
     * Fires {@code TOGGLE.<name>_off} for each of {@code owned} that is currently off, per {@link
     * #disabledNames()} — the single source of the name → mark-string derivation ({@code name +
     * "_off"}), so {@link WorkStealingScan}, {@link Thief}, and {@link SeedStep} — each owning and
     * marking a different subset of the toggles — derive their marks from here instead of
     * separately hand-spelling the string per toggle.
     */
    public void recordOffMarks(RunMetrics metrics, String... owned) {
        for (String o : owned) {
            if (!NAMES.contains(o)) {
                throw new IllegalArgumentException("unknown toggle name: " + o);
            }
        }
        List<String> off = disabledNames();
        for (String name : off) {
            for (String o : owned) {
                if (o.equals(name)) {
                    metrics.recordStealReason("TOGGLE", name + "_off");
                }
            }
        }
    }

    /**
     * The far-ahead pivot fraction for a bounded range ({@code hi != null}) at the two sites that
     * otherwise call {@link WorkerState#densityFraction()} directly ({@link Thief}'s policy and
     * {@link WorkStealingScan}'s owner-split site). {@code far_ahead=off} wins over {@code
     * density_ewma} (checked first) — fixing the plain byte-midpoint takes precedence over any EWMA
     * substitute. Delegates to {@link #farAheadFraction(double)}, the primitive form the policy
     * package (source-agnostic — no {@link WorkerState}) calls directly with an already-read
     * {@link WorkerState#densityFraction()} value.
     */
    public double farAheadFraction(WorkerState victim) {
        return farAheadFraction(victim.densityFraction());
    }

    /**
     * The primitive form of {@link #farAheadFraction(WorkerState)}: {@code densityFraction} is
     * {@link WorkerState#densityFraction()}'s already-computed value (pure, zero-I/O), so this
     * overload needs no {@link WorkerState} — the one {@code io.varve.swath.engine.policy} calls.
     */
    public double farAheadFraction(double densityFraction) {
        if (!farAhead) {
            return PLAIN_MIDPOINT_FRACTION;
        }
        if (!densityEwma) {
            return DENSITY_EWMA_OFF_FRACTION;
        }
        return densityFraction;
    }

    /**
     * The observed-density ratio consumed by {@link WorkStealingScan}'s owner-split child-tail
     * floor ({@code StealMath.childTailBelowObservedMassFloor}) and its reflection clamp. {@code
     * density_ewma=off} must disable EVERY EWMA consumer, not just {@link #farAheadFraction} — so
     * this returns {@link Double#POSITIVE_INFINITY} (the floor's own "no signal" fallback, which
     * collapses {@code min(1, densityRatio)} to {@code 1} and makes the floor byte-for-byte the
     * plain {@code (1-f) * est} span estimate) when the toggle is off, instead of the EWMA-derived
     * {@link WorkerState#observedDensityRatio()}.
     */
    public double observedDensityRatio(WorkerState victim) {
        return densityEwma ? victim.observedDensityRatio() : Double.POSITIVE_INFINITY;
    }

    /**
     * The primitive form of {@link #observedDensityRatio(WorkerState)}: {@code rawObservedDensityRatio}
     * is {@link WorkerState#observedDensityRatio()}'s already-computed value (pure, zero-I/O), so this
     * overload needs no {@link WorkerState} — the one {@code io.varve.swath.engine.policy}'s owner-split
     * governor calls, mirroring {@link #farAheadFraction(double)}.
     */
    public double observedDensityRatio(double rawObservedDensityRatio) {
        return densityEwma ? rawObservedDensityRatio : Double.POSITIVE_INFINITY;
    }

    /**
     * The run's position sensor: {@link RateAnchoredEstimator} at its promoted floor when {@code
     * rate_anchored_sensing} is on (the default since 0.2.0), else the pre-0.2.0 {@link
     * RemainingWorkEstimator#WINDOW} reading —
     * the same shape of substitution as {@link #interpolate} and {@link #farAheadFraction}, and the
     * ONLY place the choice is made. Called once per run (the estimator is stateless and pure, so one
     * instance serves the whole fleet).
     *
     * @param maxKeys the run's page size, which is the ported sensor's no-evidence floor
     */
    public RemainingWorkEstimator remainingWorkEstimator(int maxKeys) {
        return rateAnchoredSensing
                ? new RateAnchoredEstimator(maxKeys, RateAnchoredEstimator.QUARTER_MIN_GEOMETRY)
                : RemainingWorkEstimator.WINDOW;
    }

    /**
     * {@link StealMath#interpolate(byte[], byte[], double, AlphabetDigest.Snapshot, List)} when {@code
     * alphabet_pivots} is on, else the plain code-point {@link StealMath#interpolate(byte[],
     * byte[], double)} overload (no digest consult, so {@code collector} never receives a fallback
     * mark either) — the same substitution at both call sites (Thief and the owner-split site).
     * {@code collector} is the caller's own pending-{@link Engagement} list (issue #19's fix): the
     * digest reports its fallback there, never to {@code RunMetrics} directly.
     */
    public byte[] interpolate(byte[] lo, byte[] hi, double f, AlphabetDigest.Snapshot digest,
                              List<Engagement> collector) {
        return alphabetPivots
                ? StealMath.interpolate(lo, hi, f, digest, collector)
                : StealMath.interpolate(lo, hi, f);
    }
}
