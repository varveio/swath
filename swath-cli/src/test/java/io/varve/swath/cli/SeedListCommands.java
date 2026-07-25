/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.store.PageFetcher;
import java.nio.file.Path;

/**
 * The shared {@link ListCommand} fixture for the seed-fault contract tests ({@code
 * SeedAbortResumableContractTest}, {@code SeedStuckReseedResumeContractTest}): a plain S3
 * localstack-style listing against a fixed bucket/prefix, wired only through {@code
 * fetcherOverride}. {@code SeedRideOutHealsContractTest} / {@code SeedRideOutWatchdogStopContractTest}
 * also add a {@code backoffSleeperOverride} and keep their own variant.
 */
final class SeedListCommands {

    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String REGION = "us-east-1";
    private static final String PREFIX = "data/";

    private SeedListCommands() {
    }

    static ListCommand baseCommand(Path db, PageFetcher fetcher) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.connection.region = REGION;
        cmd.connection.noSignRequest = true;
        cmd.checkpoint.location = db.toString();
        cmd.fetcherOverride = fetcher;
        return cmd;
    }
}
