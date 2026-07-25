/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.filter.ExcludeRegexFilter;
import io.varve.swath.filter.Filter;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.filter.IncludeRegexFilter;
import io.varve.swath.filter.MtimeFilter;
import io.varve.swath.filter.SizeFilter;
import io.varve.swath.filter.SizeParser;
import io.varve.swath.filter.StorageClassFilter;
import io.varve.swath.filter.TimeParser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.PatternSyntaxException;
import picocli.CommandLine.Option;

/** The {@code swath list} key/size/mtime/storage-class filter flags and their compiled forms. */
final class FilterOptions {

    @Resume(ResumeClass.IDENTITY)
    @Option(names = "--include", paramLabel = "REGEX", description = "Keep keys matching this regex.")
    String include;

    @Resume(ResumeClass.IDENTITY)
    @Option(names = "--exclude", paramLabel = "REGEX", description = "Drop keys matching this regex.")
    String exclude;

    @Resume(ResumeClass.IDENTITY)
    @Option(names = "--min-size", paramLabel = "SIZE", description = "Keep objects >= this size (e.g. 1k, 256mb).")
    String minSize;

    @Resume(ResumeClass.IDENTITY)
    @Option(names = "--max-size", paramLabel = "SIZE", description = "Keep objects <= this size.")
    String maxSize;

    @Resume(ResumeClass.IDENTITY)
    @Option(names = "--modified-since", paramLabel = "DATE", description = "Keep rows modified at or after DATE (UTC if no offset).")
    String modifiedAfter;

    @Resume(ResumeClass.IDENTITY)
    @Option(names = "--modified-until", paramLabel = "DATE", description = "Keep rows modified at or before DATE (UTC if no offset).")
    String modifiedBefore;

    @Resume(ResumeClass.IDENTITY)
    @Option(names = "--storage-class", paramLabel = "CLASS[,CLASS]", split = ",",
            description = "Keep objects in these storage classes (e.g. STANDARD,GLACIER).")
    List<String> storageClasses;

    /** Compile the flags into the engine's {@link FilterChain} (empty chain when none are set). */
    FilterChain chain() throws InvalidConfigException, InvalidArgsException {
        List<Filter> filters = new ArrayList<>();
        if (include != null) {
            try {
                filters.add(IncludeRegexFilter.of(include));
            } catch (PatternSyntaxException e) {
                throw new InvalidConfigException("invalid --include regex: " + e.getMessage());
            }
        }
        if (exclude != null) {
            try {
                filters.add(ExcludeRegexFilter.of(exclude));
            } catch (PatternSyntaxException e) {
                throw new InvalidConfigException("invalid --exclude regex: " + e.getMessage());
            }
        }
        if (minSize != null || maxSize != null) {
            long min = minSize != null ? SizeParser.parse(minSize) : 0L;
            long max = maxSize != null ? SizeParser.parse(maxSize) : Long.MAX_VALUE;
            if (min > max) {
                throw new InvalidArgsException("--min-size must be <= --max-size (got "
                        + minSize + " > " + maxSize + ")");
            }
            filters.add(new SizeFilter(min, max));
        }
        if (modifiedAfter != null || modifiedBefore != null) {
            long after = modifiedAfter != null ? TimeParser.parseToMicros(modifiedAfter) : Long.MIN_VALUE;
            long before = modifiedBefore != null ? TimeParser.parseToMicros(modifiedBefore) : Long.MAX_VALUE;
            if (after > before) {
                throw new InvalidArgsException("--modified-since must be <= --modified-until (got "
                        + modifiedAfter + " > " + modifiedBefore + ")");
            }
            filters.add(new MtimeFilter(after, before));
        }
        if (storageClasses != null && !storageClasses.isEmpty()) {
            filters.add(new StorageClassFilter(new HashSet<>(storageClasses)));
        }
        return FilterChain.of(filters);
    }

    /**
     * A stable canonical string of the filter flags, stored in {@code run_meta} for the
     * resume-safety check (a changed filter ⇒ refuse). Not part of {@code args_hash} (filters don't
     * change <i>what is listed</i>). Delegates to {@link FilterSpecCodec#encode}, which uses a
     * length-prefix encoding so the spec is collision-free regardless of value content.
     */
    String spec() {
        return FilterSpecCodec.encode(include, exclude, minSize, maxSize,
                modifiedAfter, modifiedBefore, storageClasses);
    }

    /** Human-readable {@code "key=value"} entries for the JSON sidecar's {@code config.filters} array. */
    List<String> descriptions() {
        List<String> descriptions = new ArrayList<>();
        if (include != null) {
            descriptions.add("include=" + include);
        }
        if (exclude != null) {
            descriptions.add("exclude=" + exclude);
        }
        if (minSize != null) {
            descriptions.add("min_size=" + minSize);
        }
        if (maxSize != null) {
            descriptions.add("max_size=" + maxSize);
        }
        if (modifiedAfter != null) {
            descriptions.add("modified_after=" + modifiedAfter);
        }
        if (modifiedBefore != null) {
            descriptions.add("modified_before=" + modifiedBefore);
        }
        if (storageClasses != null && !storageClasses.isEmpty()) {
            descriptions.add("storage_class=" + String.join(",", storageClasses));
        }
        return descriptions;
    }
}
