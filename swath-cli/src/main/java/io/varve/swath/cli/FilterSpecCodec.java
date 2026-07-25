/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.error.CheckpointException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Encodes and decodes the filter-spec string stored in {@code run_meta.filter_spec}.
 *
 * <p>The wire format is the shared, collision-free {@link LengthPrefixedFields} grammar
 * ({@code field=<len>:<value>} entries delimited by {@code ;}); this codec just maps the
 * seven filter CLI options onto ordered fields and back. Because the length prefix makes
 * value content irrelevant to field boundaries, two semantically distinct filter sets
 * always produce different specs, which the resume-safety gate relies on to detect a
 * changed filter. Representations that differ without changing meaning — an absent versus
 * empty storage-class list, or one reordered — canonicalize to the same spec by design.
 * The codec is also reversible: {@link #decode} reconstructs each CLI option exactly,
 * including the distinction between an ABSENT field ({@code null}, length {@code -1})
 * and an EMPTY-STRING field ({@code ""}, length {@code 0}), which are behaviorally
 * different (e.g. {@code --exclude ""} builds a regex that drops everything, whereas
 * no {@code --exclude} filters nothing).
 *
 * <p>Field order is fixed and must not change across releases, because the spec
 * is persisted in SQLite and compared on resume.
 */
final class FilterSpecCodec {

    private FilterSpecCodec() {}

    private static final Set<String> FIELD_NAMES = Set.of(
            "include", "exclude", "minSize", "maxSize",
            "modifiedAfter", "modifiedBefore", "storageClasses");

    /** The decoded filter fields, mirroring the CLI options on {@link ListCommand}. */
    record Decoded(
            String include,
            String exclude,
            String minSize,
            String maxSize,
            String modifiedAfter,
            String modifiedBefore,
            List<String> storageClasses) {}

    /**
     * Encode filter fields into a stable canonical string.
     *
     * <p>For the six raw string fields (include, exclude, minSize, maxSize,
     * modifiedAfter, modifiedBefore), {@code null} (absent) is encoded with length
     * {@code -1} and {@code ""} (empty-string) with length {@code 0}. These produce
     * different byte sequences so the resume gate can detect the change.
     *
     * <p>For storageClasses, {@code null} and an empty list are semantically
     * equivalent (no storage-class filter) and both collapse to an empty comma-joined
     * string ({@code ""}), encoded as {@code storageClasses=0:}. This collapsing is
     * intentional: {@link io.varve.swath.filter.StorageClassFilter} is set-membership, so
     * order and duplicates are irrelevant; the TreeSet sort+dedup canonicalizes the
     * set, and picocli's {@code split=","} pre-splits values so elements are
     * comma-free and the comma-join/split round-trip cannot collide.
     */
    static String encode(
            String include, String exclude,
            String minSize, String maxSize,
            String modifiedAfter, String modifiedBefore,
            List<String> storageClasses) {
        // null and empty-list are semantically equivalent for storageClasses (no filter).
        String sc = storageClasses == null ? "" : String.join(",", new TreeSet<>(storageClasses));
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("include", include);
        fields.put("exclude", exclude);
        fields.put("minSize", minSize);
        fields.put("maxSize", maxSize);
        fields.put("modifiedAfter", modifiedAfter);
        fields.put("modifiedBefore", modifiedBefore);
        fields.put("storageClasses", sc);
        return LengthPrefixedFields.encode(fields);
    }

    /**
     * Decode a spec string produced by {@link #encode} back into its fields.
     *
     * <p>A null or empty spec (e.g. from an old checkpoint row) decodes to all-null fields.
     *
     * <p>For the six raw string fields, the absent marker (length {@code -1}) restores
     * {@code null} and a zero-length field (length {@code 0}) restores {@code ""} —
     * exactly preserving the original value so the resume gate can compare correctly.
     *
     * <p>For storageClasses, an empty comma-joined string decodes to {@code null}
     * (consistent with the encode invariant that null and empty-list are equivalent).
     */
    static Decoded decode(String spec) throws CheckpointException {
        Map<String, String> map;
        try {
            map = LengthPrefixedFields.decode(spec);
        } catch (LengthPrefixedFields.Malformed e) {
            throw new CheckpointException("malformed checkpoint filter_spec: " + e.getMessage(), e);
        }
        for (String name : map.keySet()) {
            if (!FIELD_NAMES.contains(name)) {
                throw new CheckpointException("malformed checkpoint filter_spec: unknown field '" + name + "'");
            }
        }
        // storageClasses: empty string → null (null and empty-list are equivalent).
        String sc = map.get("storageClasses");
        List<String> storageClasses = null;
        if (sc != null && !sc.isEmpty()) {
            storageClasses = Arrays.asList(sc.split(",", -1));
        }
        // The six raw string fields are returned as-is: null means absent, "" means empty-string.
        return new Decoded(
                map.get("include"),
                map.get("exclude"),
                map.get("minSize"),
                map.get("maxSize"),
                map.get("modifiedAfter"),
                map.get("modifiedBefore"),
                storageClasses);
    }
}
