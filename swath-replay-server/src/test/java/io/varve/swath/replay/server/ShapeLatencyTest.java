/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.server.ShapeLatency.Delay;
import io.varve.swath.replay.server.ShapeLatency.Shape;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the per-shape fault-latency injector: request classification, spec parsing (the
 * {@code prod-commoncrawl} profile and custom {@code shape=delay} lists, including the
 * per-CommonPrefix term), fanout-proportional structure-probe delays, and deterministic jitter.
 */
class ShapeLatencyTest {

    private static S3ListRequest req(byte[] prefix, byte[] delimiter, int maxKeys) {
        return new S3ListRequest("b", prefix, delimiter, null, null, maxKeys, false, false);
    }

    /** A result carrying {@code commonPrefixes} CommonPrefix entries and one object row. */
    private static S3ListResult result(S3ListRequest request, int commonPrefixes) {
        List<S3ResultEntry> entries = new ArrayList<>();
        for (int i = 0; i < commonPrefixes; i++) {
            entries.add(new S3ResultEntry.CommonPrefixResult(
                    ("p/" + i + "/").getBytes(StandardCharsets.UTF_8)));
        }
        entries.add(new S3ResultEntry.ObjectResult(new ListedObject(
                "p/obj".getBytes(StandardCharsets.UTF_8), 1, 1, "etag", "STANDARD", null, null, null, null)));
        return new S3ListResult(request, entries, false, null);
    }

    @Test
    void classifiesTheThreeSwathRequestShapes() {
        // A delimiter present is a structure probe, checked before the max-keys probe test so a
        // delimiter'd max-keys=1 still classes structural.
        assertThat(ShapeLatency.classify(req(new byte[]{'p', '/'}, new byte[]{'/'}, 1000)))
                .isEqualTo(Shape.STRUCTURE_PROBE);
        assertThat(ShapeLatency.classify(req(new byte[]{'p', '/'}, new byte[]{'/'}, 1)))
                .isEqualTo(Shape.STRUCTURE_PROBE);
        assertThat(ShapeLatency.classify(req(null, null, 1))).isEqualTo(Shape.PIVOT_PROBE);
        assertThat(ShapeLatency.classify(req(null, null, 1000))).isEqualTo(Shape.WORKER_PAGE);
    }

    @Test
    void appliesTheDelayForEachShape() {
        ShapeLatency latency = new ShapeLatency(Map.of(
                Shape.WORKER_PAGE, Delay.flat(Duration.ofMillis(200)),
                Shape.PIVOT_PROBE, Delay.flat(Duration.ofMillis(120)),
                Shape.STRUCTURE_PROBE, Delay.flat(Duration.ofSeconds(6))), 0.0);
        S3ListRequest page = req(null, null, 1000);
        S3ListRequest pivot = req(null, null, 1);
        S3ListRequest probe = req(new byte[]{'p', '/'}, new byte[]{'/'}, 1000);
        assertThat(latency.apply(page, result(page, 0))).isEqualTo(Duration.ofMillis(200));
        assertThat(latency.apply(pivot, result(pivot, 0))).isEqualTo(Duration.ofMillis(120));
        assertThat(latency.apply(probe, result(probe, 3))).isEqualTo(Duration.ofSeconds(6));
    }

    /**
     * The fanout-aware term — the whole reason the hook sees the response. A structure probe's
     * delay grows with the CommonPrefixes it RETURNED, so an engine that caps {@code max-keys}
     * observes proportionally cheaper probes, exactly as on real S3 (which stops scanning at
     * {@code max-keys}) — a shape the sorted store's own skip-scan already matches locally, but
     * production S3's real cost for it still needs to be synthesized here.
     */
    @Test
    void structureProbeDelayIsProportionalToTheCommonPrefixesReturned() {
        ShapeLatency latency = new ShapeLatency(Map.of(
                Shape.STRUCTURE_PROBE, new Delay(Duration.ofMillis(223), Duration.ofMillis(55))), 0.0);
        S3ListRequest wide = req(new byte[]{'p', '/'}, new byte[]{'/'}, 1000);
        S3ListRequest capped = req(new byte[]{'p', '/'}, new byte[]{'/'}, 32);

        assertThat(latency.apply(wide, result(wide, 121)))
                .isEqualTo(Duration.ofMillis(223 + 55 * 121));   // ≈ the fleet's 6.9 s mean
        assertThat(latency.apply(capped, result(capped, 32)))
                .isEqualTo(Duration.ofMillis(223 + 55 * 32));    // inside a 3 s probe budget
        assertThat(latency.apply(wide, result(wide, 0)))
                .as("zero fanout still pays the base").isEqualTo(Duration.ofMillis(223));
    }

    @Test
    void unlistedShapeGetsNoDelay() {
        ShapeLatency latency = new ShapeLatency(
                Map.of(Shape.STRUCTURE_PROBE, Delay.flat(Duration.ofSeconds(6))), 0.0);
        S3ListRequest page = req(null, null, 1000);
        assertThat(latency.apply(page, result(page, 0))).isEqualTo(Duration.ZERO);
    }

    @Test
    void parsesTheProdCommoncrawlProfile() {
        ShapeLatency latency = ShapeLatency.parse("prod-commoncrawl", 0.0);
        S3ListRequest probe = req(new byte[]{'p', '/'}, new byte[]{'/'}, 1000);
        S3ListRequest page = req(null, null, 1000);
        S3ListRequest pivot = req(null, null, 1);
        assertThat(latency.apply(probe, result(probe, 121)))
                .as("223 base + 55/cp × 121 ≈ the fleet's 6,865 ms structure-probe mean")
                .isEqualTo(Duration.ofMillis(223 + 55 * 121));
        assertThat(latency.apply(page, result(page, 0))).isEqualTo(Duration.ofMillis(223));
        assertThat(latency.apply(pivot, result(pivot, 0))).isEqualTo(Duration.ofMillis(121));
    }

    @Test
    void parsesACustomShapeSpecWithMixedUnitsAndAPerCpTerm() {
        ShapeLatency latency = ShapeLatency.parse(
                "structure_probe=1.5s+40ms/cp,pivot_probe=120ms,worker_page=200", 0.0);
        S3ListRequest probe = req(new byte[]{'p', '/'}, new byte[]{'/'}, 1000);
        S3ListRequest pivot = req(null, null, 1);
        S3ListRequest page = req(null, null, 1000);
        assertThat(latency.apply(probe, result(probe, 10))).isEqualTo(Duration.ofMillis(1500 + 400));
        assertThat(latency.apply(pivot, result(pivot, 0))).isEqualTo(Duration.ofMillis(120));
        assertThat(latency.apply(page, result(page, 0))).isEqualTo(Duration.ofMillis(200));   // bare number = ms
    }

    @Test
    void blankSpecDeclinesSoTheCallerInstallsANoOp() {
        assertThat(ShapeLatency.parse(null, 0.0)).isNull();
        assertThat(ShapeLatency.parse("  ", 0.0)).isNull();
    }

    @Test
    void rejectsBadSpecsAndOutOfRangeJitter() {
        assertThatThrownBy(() -> ShapeLatency.parse("worker_page", 0.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ShapeLatency.parse("bogus_shape=1s", 0.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ShapeLatency.parse("worker_page=fast", 0.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ShapeLatency.parse("worker_page=100ms+5ms/cp", 0.0))
                .as("a /cp term is meaningless without a delimiter'd response")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ShapeLatency.parse("structure_probe=100ms+5ms", 0.0))
                .as("the per-entry term must name its unit (/cp)")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ShapeLatency(Map.of(), 1.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ShapeLatency(Map.of(), Double.NaN))
                .as("NaN would scale every delay to zero — injection silently off")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ShapeLatency.parse("worker_page=-1s", 0.0))
                .as("a negative delay is silently no-sleep at the handler — refuse at parse")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void jitterIsDeterministicPerRequestAndWithinBounds() {
        ShapeLatency latency = new ShapeLatency(
                Map.of(Shape.WORKER_PAGE, Delay.flat(Duration.ofMillis(1000))), 0.2);
        S3ListRequest r = req(new byte[]{'k'}, null, 1000);
        S3ListResult res = result(r, 0);
        Duration first = latency.apply(r, res);
        assertThat(latency.apply(r, res)).as("same request → same jittered delay (reproducible)").isEqualTo(first);
        assertThat(first).isBetween(Duration.ofMillis(800), Duration.ofMillis(1200));
    }

    /**
     * Successive continuation pages of one walk differ ONLY in their continuation token (constant
     * prefix/delimiter/max-keys, null start-after) — the token must therefore feed the jitter key,
     * or every page of the walk draws the identical delay and the "jitter" becomes a constant bias.
     */
    @Test
    void continuationPagesDrawDistinctJitter() {
        ShapeLatency latency = new ShapeLatency(
                Map.of(Shape.WORKER_PAGE, Delay.flat(Duration.ofMillis(1000))), 0.2);
        Duration a = latency.apply(pageWithToken("token-a"), result(pageWithToken("token-a"), 0));
        Duration b = latency.apply(pageWithToken("token-b"), result(pageWithToken("token-b"), 0));
        assertThat(a).isNotEqualTo(b);
    }

    private static S3ListRequest pageWithToken(String token) {
        return new S3ListRequest("b", null, null, null, token, 1000, 1000, false, false);
    }
}
