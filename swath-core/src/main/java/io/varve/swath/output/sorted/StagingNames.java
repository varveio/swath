/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.sorted;

import java.util.List;

/** The single source of sort staging/output names and ownership globs. */
public final class StagingNames {

    public static final String PAGE_RUN_SUFFIX = ".pageseg";
    static final String PARQUET_SUFFIX = ".parquet";
    static final String TMP_SUFFIX = ".tmp";

    /** Names a staging entry the cascade owns exclusively; no original input may claim it. */
    static final String CASCADE_PREFIX = "merge-";
    /** Names the router's spilled page references for one oversized overlap component. */
    static final String CLUSTER_REFS_PREFIX = "cluster-";
    static final String CLUSTER_REFS_SUFFIX = ".pagerefs";

    static final String FINAL_TMP_GLOB = "part-*.parquet.tmp";
    public static final String PIPELINE_TMP_GLOB = "pipeline-*.parquet.tmp";
    static final String OWN_FINAL_GLOB = "part-*.parquet";
    static final String ALL_PARQUET_GLOB = "*.parquet";
    static final String CASCADE_PAGE_RUN_GLOB = CASCADE_PREFIX + "*" + PAGE_RUN_SUFFIX;
    static final String CASCADE_PAGE_RUN_TMP_GLOB = CASCADE_PAGE_RUN_GLOB + TMP_SUFFIX;
    public static final String CLUSTER_REFS_TMP_GLOB =
            CLUSTER_REFS_PREFIX + "*" + CLUSTER_REFS_SUFFIX + TMP_SUFFIX;
    /** Retained for resume tests and older attempts that planted Parquet cascade debris. */
    static final String LEGACY_CASCADE_PARQUET_GLOB = CASCADE_PREFIX + "*" + PARQUET_SUFFIX;
    /** Retained only to sweep disposable files left by pre-pipeline finalization attempts. */
    static final String LEGACY_RANGE_TMP_GLOB = "prange-*.parquet.tmp";
    /** Retained only to sweep proof debris left by pre-pipeline finalization attempts. */
    static final String LEGACY_RANGE_PROOF_TMP_GLOB = "prange-proof*.tmp";

    /**
     * Every sorter-owned disposable staging namespace a finalization attempt can populate. Kickoff
     * and pre-publication failure cleanup both sweep this exact set, so neither can drift into
     * leaving a namespace behind for the other to find.
     */
    static final List<String> DISPOSABLE_STAGING_GLOBS = List.of(
            CASCADE_PAGE_RUN_GLOB,
            CASCADE_PAGE_RUN_TMP_GLOB,
            LEGACY_CASCADE_PARQUET_GLOB,
            LEGACY_RANGE_TMP_GLOB,
            LEGACY_RANGE_PROOF_TMP_GLOB,
            CLUSTER_REFS_TMP_GLOB,
            PIPELINE_TMP_GLOB);

    private StagingNames() {
    }

    static String finalPart(int index) {
        return String.format("part-%05d.parquet", index);
    }

    static String finalTmp(int index) {
        return finalPart(index) + TMP_SUFFIX;
    }

    public static String pipelineTmp(int ordinal) {
        return String.format("pipeline-%05d", ordinal) + PARQUET_SUFFIX + TMP_SUFFIX;
    }

    public static String cascadeIntermediate(int sequence) {
        return CASCADE_PREFIX + sequence + PAGE_RUN_SUFFIX;
    }

    /** The name an unfinished cascade intermediate wears until its durable rename commits it. */
    public static String cascadeIntermediateTmp(int sequence) {
        return cascadeIntermediate(sequence) + TMP_SUFFIX;
    }

    /** The name one oversized overlap component's spilled page references wear while routing. */
    public static String clusterRefsTmp(int sequence) {
        return String.format(CLUSTER_REFS_PREFIX + "%05d", sequence)
                + CLUSTER_REFS_SUFFIX + TMP_SUFFIX;
    }

    public static String fixtureSegment(int sequence) {
        return "fixture-" + sequence + PAGE_RUN_SUFFIX;
    }

    public static String listingSegment(String prefix, long sequence) {
        return prefix + "-" + sequence + PAGE_RUN_SUFFIX;
    }
}
