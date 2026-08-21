/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Flag-level coverage for {@code --latency-scale}: it reaches {@link ServeOptions} from both the
 * {@code serve} subcommand and the top-level invocation that shares the same mixin, and it defaults
 * to {@link ShapeLatency#UNSCALED} so an operator who never passes it gets the profile as written.
 * The scale's arithmetic and its validation live in {@link ShapeLatencyTest}.
 */
class ServeLatencyScaleOptionTest {

    @Test
    void metricsEndpointBracketsIpv6Authorities() {
        assertThat(ReplayServerApp.metricsEndpoint("::1", 9090)).isEqualTo("http://[::1]:9090/metrics");
        assertThat(ReplayServerApp.metricsEndpoint("127.0.0.1", 9090))
                .isEqualTo("http://127.0.0.1:9090/metrics");
    }

    /** Mirrors {@link ReplayServerApp#main}'s own parser configuration. */
    private static CommandLine parser(Object command) {
        return new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true);
    }

    @Test
    void serveParsesTheLatencyScale() {
        ReplayServerApp.ServeCommand cmd = new ReplayServerApp.ServeCommand();
        parser(cmd).parseArgs("--fixture", "f", "--bucket", "b",
                "--inject-latency", "prod-commoncrawl", "--latency-scale", "50");

        assertThat(cmd.serveOptions.latencyScale).isEqualTo(50.0);
    }

    @Test
    void theTopLevelInvocationSharesTheSameFlag() {
        ReplayServerApp app = new ReplayServerApp();
        parser(app).parseArgs("--fixture", "f", "--bucket", "b",
                "--inject-latency", "prod-commoncrawl", "--latency-scale", "12.5");

        assertThat(app.serveOptions.latencyScale).isEqualTo(12.5);
    }

    @Test
    void defaultsToUnscaled() {
        ReplayServerApp.ServeCommand cmd = new ReplayServerApp.ServeCommand();
        parser(cmd).parseArgs("--fixture", "f", "--bucket", "b");

        assertThat(cmd.serveOptions.latencyScale).isEqualTo(ShapeLatency.UNSCALED);
    }
}
