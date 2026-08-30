/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.sort.MergeBoundaryPolicy;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortFinalization;
import io.varve.swath.sort.StagingRetention;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/** Typed expert settings exposed through the repeatable {@code --tune KEY=VALUE} option. */
final class TuneOptions {

    private static final List<KeySpec> REGISTRY = List.of(
            new KeySpec("engine.readahead", "boolean", "on|off", "off",
                    "experimental", ResumeClass.FREE, "fresh list"),
            new KeySpec("seed.mode", "enum", "shallow|none|hints", "shallow",
                    "stable (hints reserved)", ResumeClass.IDENTITY, "fresh list"),
            new KeySpec("parquet.writers", "integer", "2..64 (heap-admitted above 4)", "3",
                    "stable", ResumeClass.FREE, "fresh list"),
            new KeySpec("summary.interval", "duration",
                    "positive duration (for example 2s, 500ms, or PT2S)", "--progress-interval",
                    "stable", ResumeClass.FREE, "fresh list"),
            new KeySpec("sort.merge-parallelism", "integer",
                    SortOptions.MIN_MERGE_PARALLELISM + ".."
                            + SortOptions.MAX_MERGE_PARALLELISM,
                    Integer.toString(SortConfig.DEFAULT.mergeParallelism()), "stable", ResumeClass.FREE,
                    "fresh list and resume"),
            new KeySpec(SortConfig.MERGE_BOUNDARY_POLICY_TUNE_KEY, "enum", "distinct|rows",
                    SortConfig.DEFAULT.mergeBoundaryPolicy().configValue(), "experimental",
                    ResumeClass.FREE, "fresh list and resume"),
            new KeySpec(SortConfig.FINALIZATION_TUNE_KEY, "enum", "ranges|pipeline",
                    SortConfig.DEFAULT.finalization().configValue(), "experimental",
                    ResumeClass.FREE, "fresh list and resume"),
            new KeySpec(SortConfig.KEEP_STAGING_TUNE_KEY, "boolean", "on|off",
                    SortConfig.DEFAULT.stagingRetention().tuneValue(), "diagnostic", ResumeClass.FREE,
                    "fresh list and resume"),
            new KeySpec("sort.ignore-disk-check", "boolean", "on|off", "off",
                    "diagnostic", ResumeClass.FREE, "fresh list and resume"));

    private static final Map<String, KeySpec> SPECS = REGISTRY.stream()
            .collect(Collectors.toUnmodifiableMap(KeySpec::key, spec -> spec));

    static final List<String> KEYS = REGISTRY.stream().map(KeySpec::key).toList();

    private final LinkedHashMap<String, String> effectiveValues = new LinkedHashMap<>();

    List<String> entries;

    /**
     * Validate and apply every entry to the same option fields the former dedicated flags fed.
     * Returns true after printing self-documentation, allowing the command to exit before any I/O.
     */
    boolean apply(OutputOptions output, EngineOptions engine, SortOptions sorting, PrintWriter out)
            throws InvalidArgsException {
        if (entries == null || entries.isEmpty()) {
            return false;
        }

        LinkedHashMap<String, String> values = parseEntries(out);
        if (values == null) {
            return true;
        }

        for (Map.Entry<String, String> setting : values.entrySet()) {
            applyFreshSetting(setting.getKey(), setting.getValue(), output, engine, sorting);
        }
        return false;
    }

    /** Apply only settings whose registry applicability permits changing them on resume. */
    boolean applyForResume(SortOptions sorting, PrintWriter out) throws InvalidArgsException {
        if (entries == null || entries.isEmpty()) {
            return false;
        }
        LinkedHashMap<String, String> values = parseEntries(out);
        if (values == null) {
            return true;
        }
        for (Map.Entry<String, String> setting : values.entrySet()) {
            String key = setting.getKey();
            if (!SPECS.get(key).appliesToResume()) {
                KeySpec spec = SPECS.get(key);
                if (spec.resumeClass() == ResumeClass.IDENTITY) {
                    throw new InvalidArgsException("--tune " + key
                            + " is a run-shape setting and cannot be changed by swath resume");
                }
                throw new InvalidArgsException("--tune " + key
                        + " is not applicable during swath resume");
            }
            if ("sort.ignore-disk-check".equals(key)) {
                sorting.forceSort = parseOnOff(key, setting.getValue());
                effectiveValues.put(key, setting.getValue().toLowerCase(Locale.ROOT));
            } else if ("sort.merge-parallelism".equals(key)) {
                sorting.mergeParallelism = parseMergeParallelism(key, setting.getValue());
                effectiveValues.put(key, Integer.toString(sorting.mergeParallelism));
            } else if (SortConfig.MERGE_BOUNDARY_POLICY_TUNE_KEY.equals(key)) {
                sorting.mergeBoundaryPolicy = parseMergeBoundaryPolicy(key, setting.getValue());
                effectiveValues.put(key, sorting.mergeBoundaryPolicy.configValue());
            } else if (SortConfig.FINALIZATION_TUNE_KEY.equals(key)) {
                sorting.finalization = parseFinalization(key, setting.getValue());
                effectiveValues.put(key, sorting.finalization.configValue());
            } else if (SortConfig.KEEP_STAGING_TUNE_KEY.equals(key)) {
                sorting.stagingRetention = StagingRetention.fromEnabled(parseOnOff(key, setting.getValue()));
                effectiveValues.put(key, sorting.stagingRetention.tuneValue());
            } else {
                throw new AssertionError("resume-applicable tune key has no implementation: " + key);
            }
        }
        return false;
    }

    /** Registry-ordered effective configuration, including defaults for settings not supplied. */
    String effectiveConfiguration(String resolvedSummaryInterval, SortConfig resolvedSortConfig) {
        return REGISTRY.stream()
                .map(spec -> spec.key() + "=" + effectiveValues.getOrDefault(spec.key(),
                        "summary.interval".equals(spec.key())
                                ? resolvedSummaryInterval
                                : "sort.merge-parallelism".equals(spec.key())
                                        ? Integer.toString(resolvedSortConfig.mergeParallelism())
                                        : SortConfig.MERGE_BOUNDARY_POLICY_TUNE_KEY.equals(spec.key())
                                                ? resolvedSortConfig.mergeBoundaryPolicy().configValue()
                                        : SortConfig.FINALIZATION_TUNE_KEY.equals(spec.key())
                                                ? resolvedSortConfig.finalization().configValue()
                                        : SortConfig.KEEP_STAGING_TUNE_KEY.equals(spec.key())
                                                ? resolvedSortConfig.stagingRetention().tuneValue()
                                        : spec.defaultValue()))
                .collect(Collectors.joining(", "));
    }

    void copyEffectiveValuesFrom(TuneOptions source) {
        effectiveValues.clear();
        effectiveValues.putAll(source.effectiveValues);
    }

    static List<KeySpec> registry() {
        return REGISTRY;
    }

    private LinkedHashMap<String, String> parseEntries(PrintWriter out)
            throws InvalidArgsException {
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        for (String entry : entries) {
            if ("help".equals(entry)) {
                printAll(out);
                return null;
            }
            int equals = entry.indexOf('=');
            if (equals <= 0) {
                throw new InvalidArgsException("--tune must be KEY=VALUE (got '" + entry
                        + "'); valid keys: " + String.join(", ", KEYS));
            }
            String key = entry.substring(0, equals).trim();
            String value = entry.substring(equals + 1).trim();
            if (!KEYS.contains(key)) {
                String suggestion = suggestionFor(key);
                throw new InvalidArgsException("--tune: unknown key '" + key
                        + "'" + (suggestion == null ? "" : "; did you mean '" + suggestion + "'?")
                        + "; valid keys: " + String.join(", ", KEYS));
            }
            if ("?".equals(value)) {
                printKey(out, key);
                return null;
            }
            String previous = values.putIfAbsent(key, value);
            if (previous != null && !previous.equalsIgnoreCase(value)) {
                throw new InvalidArgsException("--tune " + key
                        + " was repeated with contradictory values '" + previous
                        + "' and '" + value + "'");
            }
        }
        return values;
    }

    private void applyFreshSetting(String key, String value, OutputOptions output,
            EngineOptions engine, SortOptions sorting) throws InvalidArgsException {
        switch (key) {
                case "engine.readahead" -> {
                    parseOnOff(key, value);
                    if (engine.engineToggle == null) {
                        engine.engineToggle = new ArrayList<>();
                    } else if (!(engine.engineToggle instanceof ArrayList<?>)) {
                        engine.engineToggle = new ArrayList<>(engine.engineToggle);
                    }
                    engine.engineToggle.add("readahead=" + value.toLowerCase(Locale.ROOT));
                    effectiveValues.put(key, value.toLowerCase(Locale.ROOT));
                }
                case "seed.mode" -> {
                    String normalized = value.toLowerCase(Locale.ROOT);
                    if (!List.of("shallow", "none", "hints").contains(normalized)) {
                        throw valueError(key, value);
                    }
                    engine.seed = normalized;
                    effectiveValues.put(key, normalized);
                }
                case "parquet.writers" -> {
                    int writers;
                    try {
                        writers = Integer.parseInt(value);
                    } catch (NumberFormatException e) {
                        throw valueError(key, value);
                    }
                    if (writers < OutputOptions.MIN_PARQUET_WRITERS
                            || writers > OutputOptions.MAX_PARQUET_WRITERS) {
                        throw valueError(key, value);
                    }
                    output.parquetWriters = writers;
                    effectiveValues.put(key, Integer.toString(writers));
                }
                case "summary.interval" -> {
                    Duration interval;
                    try {
                        interval = DurationParser.progressInterval(value);
                    } catch (InvalidConfigException e) {
                        throw valueError(key, value);
                    }
                    output.summaryJsonInterval = value;
                    effectiveValues.put(key, interval.toString());
                }
                case "sort.ignore-disk-check" -> {
                    sorting.forceSort = parseOnOff(key, value);
                    effectiveValues.put(key, value.toLowerCase(Locale.ROOT));
                }
                case "sort.merge-parallelism" -> {
                    sorting.mergeParallelism = parseMergeParallelism(key, value);
                    effectiveValues.put(key, Integer.toString(sorting.mergeParallelism));
                }
                case SortConfig.MERGE_BOUNDARY_POLICY_TUNE_KEY -> {
                    sorting.mergeBoundaryPolicy = parseMergeBoundaryPolicy(key, value);
                    effectiveValues.put(key, sorting.mergeBoundaryPolicy.configValue());
                }
                case SortConfig.FINALIZATION_TUNE_KEY -> {
                    sorting.finalization = parseFinalization(key, value);
                    effectiveValues.put(key, sorting.finalization.configValue());
                }
                case SortConfig.KEEP_STAGING_TUNE_KEY -> {
                    sorting.stagingRetention = StagingRetention.fromEnabled(parseOnOff(key, value));
                    effectiveValues.put(key, sorting.stagingRetention.tuneValue());
                }
                default -> throw new AssertionError("unregistered tune key " + key);
        }
    }

    private static int parseMergeParallelism(String key, String value) throws InvalidArgsException {
        int parallelism;
        try {
            parallelism = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw valueError(key, value);
        }
        if (parallelism < SortOptions.MIN_MERGE_PARALLELISM
                || parallelism > SortOptions.MAX_MERGE_PARALLELISM) {
            throw valueError(key, value);
        }
        return parallelism;
    }

    private static MergeBoundaryPolicy parseMergeBoundaryPolicy(String key, String value)
            throws InvalidArgsException {
        try {
            return MergeBoundaryPolicy.fromConfigValue(key, value);
        } catch (IllegalArgumentException e) {
            throw valueError(key, value);
        }
    }

    private static SortFinalization parseFinalization(String key, String value)
            throws InvalidArgsException {
        try {
            return SortFinalization.fromConfigValue(key, value);
        } catch (IllegalArgumentException e) {
            throw valueError(key, value);
        }
    }

    private static boolean parseOnOff(String key, String value) throws InvalidArgsException {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "on" -> true;
            case "off" -> false;
            default -> throw valueError(key, value);
        };
    }

    private static InvalidArgsException valueError(String key, String value) {
        return new InvalidArgsException("--tune " + key + ": invalid value '" + value
                + "'; expected " + SPECS.get(key).expected());
    }

    private static void printAll(PrintWriter out) {
        out.println("Tune keys:");
        for (KeySpec spec : REGISTRY) {
            printSpec(out, spec, "  ");
        }
        out.flush();
    }

    private static void printKey(PrintWriter out, String key) {
        printSpec(out, SPECS.get(key), "");
        out.flush();
    }

    private static void printSpec(PrintWriter out, KeySpec spec, String indent) {
        out.printf("%s%s: type=%s; values=%s; default=%s; stability=%s; resume=%s; applies=%s%n",
                indent, spec.key(), spec.type(), spec.acceptedValues(), spec.defaultValue(),
                spec.stability(), spec.resumeClass().name().toLowerCase(Locale.ROOT),
                spec.applicability());
    }

    private static String suggestionFor(String unknown) {
        String nearest = null;
        int best = Integer.MAX_VALUE;
        for (String key : KEYS) {
            int distance = editDistance(unknown, key);
            if (distance < best) {
                best = distance;
                nearest = key;
            }
        }
        return best <= 3 ? nearest : null;
    }

    private static int editDistance(String left, String right) {
        int[] previous = new int[right.length() + 1];
        int[] current = new int[right.length() + 1];
        for (int j = 0; j <= right.length(); j++) {
            previous[j] = j;
        }
        for (int i = 1; i <= left.length(); i++) {
            current[0] = i;
            for (int j = 1; j <= right.length(); j++) {
                int substitution = previous[j - 1]
                        + (left.charAt(i - 1) == right.charAt(j - 1) ? 0 : 1);
                current[j] = Math.min(Math.min(previous[j] + 1, current[j - 1] + 1), substitution);
            }
            int[] swap = previous;
            previous = current;
            current = swap;
        }
        return previous[right.length()];
    }

    record KeySpec(String key, String type, String acceptedValues, String defaultValue,
                           String stability, ResumeClass resumeClass, String applicability) {

        String expected() {
            return switch (type) {
                case "integer" -> "integer " + acceptedValues;
                case "duration" -> acceptedValues;
                default -> acceptedValues;
            };
        }

        boolean appliesToResume() {
            return applicability.contains("resume");
        }

        String documentationRange() {
            return switch (type) {
                case "boolean" -> "on or off";
                case "enum" -> acceptedValues.replace("|", ", ").replaceFirst(", ([^,]+)$", ", or $1");
                case "integer" -> "integer " + acceptedValues;
                case "duration" -> "positive duration";
                default -> acceptedValues;
            };
        }
    }
}
