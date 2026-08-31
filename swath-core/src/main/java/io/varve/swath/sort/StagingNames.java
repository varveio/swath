/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** The single source of sort staging/output names and ownership globs. */
final class StagingNames {

    static final String PAGE_RUN_SUFFIX = ".pageseg";
    static final String PARQUET_SUFFIX = ".parquet";
    static final String TMP_SUFFIX = ".tmp";

    static final String FINAL_TMP_GLOB = "part-*.parquet.tmp";
    static final String PIPELINE_TMP_GLOB = "pipeline-*.parquet.tmp";
    static final String OWN_FINAL_GLOB = "part-*.parquet";
    static final String ALL_PARQUET_GLOB = "*.parquet";
    static final String CASCADE_PAGE_RUN_GLOB = "merge-*.pageseg";
    /** Retained for resume tests and older attempts that planted Parquet cascade debris. */
    static final String LEGACY_CASCADE_PARQUET_GLOB = "merge-*.parquet";

    private StagingNames() {
    }

    static String finalPart(int index) {
        return String.format("part-%05d.parquet", index);
    }

    static String finalTmp(int index) {
        return finalPart(index) + TMP_SUFFIX;
    }

    static String pipelineTmp(int ordinal) {
        return String.format("pipeline-%05d", ordinal) + PARQUET_SUFFIX + TMP_SUFFIX;
    }

    static String cascadeIntermediate(String prefix, int sequence) {
        return prefix + sequence + PAGE_RUN_SUFFIX;
    }

    static String fixtureSegment(int sequence) {
        return "fixture-" + sequence + PAGE_RUN_SUFFIX;
    }

    static String listingSegment(String prefix, long sequence) {
        return prefix + "-" + sequence + PAGE_RUN_SUFFIX;
    }
}
