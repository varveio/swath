/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.WorkStealingScan;
import io.varve.swath.error.ListingException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.store.PageRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

final class MockPageFetcherStrictS3ParamsTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] cp(int codePoint) {
        return new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "strict-s3-params",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    void strictS3ParamsRejectsXmlUnsafeStartAfterAsFatalBadRequest() {
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of(b("a"), b("b")))
                .strictS3Params(true)
                .build();

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(new byte[0], new byte[]{0x01}, 10)))
                .isInstanceOf(ListingException.class)
                .hasMessageContaining("HTTP 400 Bad Request")
                .hasMessageContaining("start_after")
                .hasMessageContaining("U+0001");
    }

    @Test
    void strictS3ParamsMatchesXml10CharLegalityForBoundaryScalars() throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of(b("z")))
                .strictS3Params(true)
                .build();

        assertThat(fetcher.fetchPage(PageRequest.objects(new byte[0], cp(0x1FFFE), 10)).httpStatus())
                .as("supplementary noncharacters are XML 1.0 Char-legal")
                .isEqualTo(200);
        assertThat(fetcher.fetchPage(PageRequest.objects(new byte[0], new byte[]{0x7F}, 10)).httpStatus())
                .as("DEL is XML 1.0 Char-legal even though ByteMidpoint does not synthesize it")
                .isEqualTo(200);

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(new byte[0], cp(0xFFFE), 10)))
                .isInstanceOf(ListingException.class)
                .hasMessageContaining("U+FFFE");
        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(new byte[0], new byte[]{0x01}, 10)))
                .isInstanceOf(ListingException.class)
                .hasMessageContaining("U+0001");
    }

    @Test
    @Timeout(20)
    void strictS3ParamsAcceptsEngineSynthesizedPivotsAcrossWorkStealingScan(@TempDir Path dir)
            throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < 512; i++) {
            keyspace.add(b(String.format("data/%05d", i)));
        }
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keyspace)
                .maxKeysCap(8)
                .slowFirstPage(Duration.ofMillis(40))
                .strictS3Params(true)
                .build();

        List<byte[]> emitted = new ArrayList<>();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS),
                    fetcher, store, 4, 8, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(64, engine, emitted);
        }

        List<String> emittedKeys = emitted.stream()
                .sorted(Arrays::compareUnsigned)
                .map(k -> new String(k, StandardCharsets.UTF_8))
                .toList();
        List<String> expectedKeys = keyspace.stream().map(k -> new String(k, StandardCharsets.UTF_8)).toList();
        assertThat(emittedKeys).hasSize(keyspace.size());
        assertThat(emittedKeys).doesNotHaveDuplicates();
        assertThat(emittedKeys).containsExactlyElementsOf(expectedKeys);
    }
}
