/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidArgsException;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * {@code --requester-pays} resolution — absent means "do not send the header" (the unset
 * default); the only accepted explicit value is {@code requester}
 * (case-insensitive, mirroring the AWS CLI); anything else is rejected at exit 2, and
 * before any checkpoint is opened (mirrors {@code --request-rate}/{@code --engine-toggle}).
 */
class RequestPayerValidationTest {

    @Test
    void unsetResolvesToDisabled() throws Exception {
        ListCommand cmd = new ListCommand();
        assertThat(cmd.connection.resolveRequestPayer()).isFalse();
    }

    @Test
    void requesterResolvesToEnabled() throws Exception {
        ListCommand cmd = new ListCommand();
        cmd.connection.requestPayer = "requester";
        assertThat(cmd.connection.resolveRequestPayer()).isTrue();
    }

    @Test
    void valueIsCaseInsensitive() throws Exception {
        ListCommand cmd = new ListCommand();
        cmd.connection.requestPayer = "REQUESTER";
        assertThat(cmd.connection.resolveRequestPayer()).isTrue();
    }

    @Test
    void rejectsAnyOtherValue() {
        for (String bad : new String[]{"requestor", "true", "owner", ""}) {
            ListCommand cmd = new ListCommand();
            cmd.connection.requestPayer = bad;
            assertThatThrownBy(cmd.connection::resolveRequestPayer)
                    .as("--requester-pays=%s must be rejected", bad)
                    .isInstanceOf(InvalidArgsException.class)
                    .hasMessageContaining("--requester-pays");
        }
    }

    @Test
    void callRejectsABadRequestPayerValueBeforeRunning() {
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--checkpoint", "none",
                "--requester-pays", "nope");

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--requester-pays");
    }
}
