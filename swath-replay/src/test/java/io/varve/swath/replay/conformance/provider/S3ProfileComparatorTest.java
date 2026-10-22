/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Adversarial checks for the S3 ListObjectsV2 field policy and its receipt path. */
class S3ProfileComparatorTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String NATIVE_TOKEN = "token-sha256-" + "b".repeat(64);
    private static final String DATE = "Thu, 01 Oct 2026 00:00:00 GMT";

    private static String contents(String key, String etag, String extra) {
        return "<Contents><Key>" + key + "</Key><LastModified>2026-10-21T10:00:00.000Z</LastModified>"
                + "<ETag>&quot;" + etag + "&quot;</ETag><ChecksumAlgorithm>CRC64NVME</ChecksumAlgorithm>"
                + "<ChecksumType>FULL_OBJECT</ChecksumType><Size>3</Size><StorageClass>STANDARD</StorageClass>"
                + extra + "</Contents>";
    }

    private static String page(String bucket, String keyCount, String truncated, String next, String body) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>" + bucket
                + "</Name><Prefix>p/</Prefix>" + (next == null ? "" : "<NextContinuationToken>" + next
                + "</NextContinuationToken>") + "<KeyCount>" + keyCount + "</KeyCount><MaxKeys>2</MaxKeys>"
                + "<Delimiter>/</Delimiter><IsTruncated>" + truncated + "</IsTruncated>" + body
                + "</ListBucketResult>";
    }

    private static final String BODY = contents("p/a b ", "e1", "") + contents("p/c\u0001x", "e2", "")
            + "<CommonPrefixes><Prefix>p/d/</Prefix></CommonPrefixes>";

    private static void compare(String nativeBody, String replayBody) {
        S3ProfileComparator.assertPage(bytes(nativeBody), bytes(replayBody), "{bucket}", "replay-bucket",
                null, null);
    }

    @Test
    void exactPageWithXmlInvalidKeyMatches() {
        assertThatCode(() -> compare(page("{bucket}", "3", "false", null, BODY),
                page("replay-bucket", "3", "false", null, BODY))).doesNotThrowAnyException();
    }

    @Test
    void refusesEveryAdversarialDifference() {
        String nativeBody = page("{bucket}", "3", "false", null, BODY);
        Map<String, String> cases = Map.ofEntries(
                Map.entry("top-level element sequence", page("replay-bucket", "3", "false", null,
                        contents("p/a b ", "e1", "") + "<CommonPrefixes><Prefix>p/d/</Prefix></CommonPrefixes>")),
                Map.entry("Key", page("replay-bucket", "3", "false", null, contents("p/c\u0001x", "e2", "")
                        + contents("p/a b ", "e1", "") + "<CommonPrefixes><Prefix>p/d/</Prefix></CommonPrefixes>")),
                Map.entry("Contents 0 Key", page("replay-bucket", "3", "false", null, BODY.replace("p/a b ", "p/a b"))),
                Map.entry("Contents 0 ETag", page("replay-bucket", "3", "false", null, BODY.replace("e1", "e9"))),
                Map.entry("KeyCount", page("replay-bucket", "2", "false", null, BODY)),
                Map.entry("Contents 1 Key", page("replay-bucket", "3", "false", null, BODY.replace("\u0001", ""))),
                Map.entry("unknown S3 Contents element", page("replay-bucket", "3", "false", null,
                        BODY.replace("<Size>3</Size>", "<Size>3</Size><Novel>x</Novel>"))),
                Map.entry("profile-omitted", page("replay-bucket", "3", "false", null,
                        BODY.replaceFirst("</Contents>", "<RestoreStatus><IsRestoreInProgress>false"
                                + "</IsRestoreInProgress></RestoreStatus></Contents>"))),
                Map.entry("disagree", page("replay-bucket", "3", "true", null, BODY)));
        cases.forEach((message, replayBody) -> assertThatThrownBy(() -> compare(nativeBody, replayBody))
                .as(message).hasMessageContaining(message));
        assertThatThrownBy(() -> compare(page("{bucket}", "3", "false", null, BODY + "<Novel/>"),
                page("replay-bucket", "3", "false", null, BODY))).hasMessageContaining("unknown S3 listing element");
    }

    @Test
    void ownerMustBePresentOnReplayWhenNativeHasIt() {
        String owner = "<Owner><ID>{owner-id}</ID><DisplayName>{owner-name}</DisplayName></Owner>";
        String nativeBody = page("{bucket}", "1", "false", null, contents("p/a", "e1", owner));
        assertThatCode(() -> compare(nativeBody, page("replay-bucket", "1", "false", null,
                contents("p/a", "e1", "<Owner><ID>synthetic</ID><DisplayName>synthetic</DisplayName></Owner>"))))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> compare(nativeBody, page("replay-bucket", "1", "false", null,
                contents("p/a", "e1", "")))).hasMessageContaining("field sequence");
        assertThatCode(() -> compare(page("{bucket}", "1", "false", null, contents("p/a", "e1",
                "<RestoreStatus><IsRestoreInProgress>false</IsRestoreInProgress></RestoreStatus>")),
                page("replay-bucket", "1", "false", null, contents("p/a", "e1", ""))))
                .doesNotThrowAnyException();
    }

    @Test
    void ownerChildrenAreComparedInOrderWithoutDuplicates() {
        String owner = "<Owner><ID>{owner-id}</ID><DisplayName>{owner-name}</DisplayName></Owner>";
        String nativeBody = page("{bucket}", "1", "false", null, contents("p/a", "e1", owner));
        Map<String, String> cases = Map.of(
                "duplicated ID", "<Owner><ID>s</ID><ID>s</ID><DisplayName>s</DisplayName></Owner>",
                "reordered", "<Owner><DisplayName>s</DisplayName><ID>s</ID></Owner>",
                "missing DisplayName", "<Owner><ID>s</ID></Owner>");
        cases.forEach((label, replayOwner) -> assertThatThrownBy(() -> compare(nativeBody,
                page("replay-bucket", "1", "false", null, contents("p/a", "e1", replayOwner))))
                .as(label).hasMessageContaining("Owner replay children"));
        assertThatThrownBy(() -> compare(page("{bucket}", "1", "false", null, contents("p/a", "e1",
                "<Owner><ID>i</ID><ID>i</ID><DisplayName>n</DisplayName></Owner>")), nativeBody.replace("{bucket}",
                "replay-bucket"))).hasMessageContaining("Owner children");
    }

    @Test
    void walkComparesInventoriesAndRefusesRepeatedCommonPrefixes() {
        String prefix = "<CommonPrefixes><Prefix>p/d/</Prefix></CommonPrefixes>";
        var nativePages = List.of(
                new S3ProfileComparator.CapturedPage(null, bytes(page("{bucket}", "2", "true", NATIVE_TOKEN,
                        contents("p/a", "e1", "") + prefix))),
                new S3ProfileComparator.CapturedPage(NATIVE_TOKEN, bytes(page("{bucket}", "1", "false", null,
                        contents("p/z", "e3", "")).replace("<Prefix>p/</Prefix>", "<Prefix>p/</Prefix><ContinuationToken>"
                        + NATIVE_TOKEN + "</ContinuationToken>"))));
        var replayPages = List.of(new S3ProfileComparator.CapturedPage(null, bytes(page("replay-bucket", "3",
                "false", null, contents("p/a", "e1", "") + contents("p/z", "e3", "") + prefix))));
        assertThatCode(() -> S3ProfileComparator.assertWalk(nativePages, replayPages, "{bucket}", "replay-bucket",
                8, 100)).doesNotThrowAnyException();
        var repeated = List.of(nativePages.getFirst(), new S3ProfileComparator.CapturedPage(NATIVE_TOKEN,
                bytes(page("{bucket}", "1", "false", null, prefix).replace("<Prefix>p/</Prefix>",
                        "<Prefix>p/</Prefix><ContinuationToken>" + NATIVE_TOKEN + "</ContinuationToken>"))));
        assertThatThrownBy(() -> S3ProfileComparator.assertWalk(repeated, replayPages, "{bucket}", "replay-bucket",
                8, 100)).hasMessageContaining("cross-page repeated S3 CommonPrefix");
        assertThatThrownBy(() -> S3ProfileComparator.assertWalk(nativePages.subList(0, 1), replayPages, "{bucket}",
                "replay-bucket", 8, 100)).hasMessageContaining("nonterminal");
    }

    @Test
    void s3RunReceiptMapsTokensAndRequiresSdkTokens(@TempDir Path dir) throws Exception {
        String first = page("{bucket}", "1", "true", NATIVE_TOKEN, contents("p/a", "e1", ""));
        String last = page("{bucket}", "1", "false", null, contents("p/z", "e3", ""))
                .replace("<Prefix>p/</Prefix>", "<Prefix>p/</Prefix><ContinuationToken>" + NATIVE_TOKEN
                        + "</ContinuationToken>");
        Path capture = dir.resolve("capture.json");
        writeRun(capture, List.of(nativeStep("list-type=2&prefix=p%2F", first),
                nativeStep("list-type=2&prefix=p%2F&continuation-token=" + NATIVE_TOKEN, last)));
        String replayFirst = page("replay-bucket", "1", "true", "r1+/=", contents("p/a", "e1", ""));
        String replayLast = page("replay-bucket", "1", "false", null, contents("p/z", "e3", ""))
                .replace("<Prefix>p/</Prefix>", "<Prefix>p/</Prefix><ContinuationToken>r1+/=</ContinuationToken>");
        var replay = List.of(replayStep("list-type=2&prefix=p%2F", replayFirst),
                replayStep("list-type=2&prefix=p%2F&continuation-token=r1%2B%2F%3D", replayLast));
        Path source = cleanRepo(dir);
        ProviderMatchReceipt.s3Run(capture, replay, Map.of("r1%2B%2F%3D", NATIVE_TOKEN), "replay-bucket", dir,
                dir.resolve("dist.jar"), source, "exactTest", dir.resolve("receipt.json"));
        assertThat(JSON.readTree(dir.resolve("receipt.json").toFile()).path("compared_steps").asInt()).isEqualTo(2);
        // Walks follow each endpoint's own tokens; no cross-endpoint token mapping is needed.
        ProviderMatchReceipt.verifyS3Run(capture, replay, Map.of(), "replay-bucket");
        var wrongSdk = List.of(replay.getFirst(), new ProviderMatchReceipt.ReplayStep("walk",
                new ProviderMatchReceipt.Request("GET", "/replay-bucket",
                        "list-type=2&prefix=p%2F&continuation-token=r1%2B%2F%3D",
                        Map.of("user-agent", List.of("aws-sdk-java/2.31.79"))),
                replay.get(1).exchange()));
        assertThatThrownBy(() -> ProviderMatchReceipt.verifyS3Run(capture, wrongSdk,
                Map.of("r1%2B%2F%3D", NATIVE_TOKEN), "replay-bucket")).hasMessageContaining("SDK product/version");
    }

    private static Map<String, Object> nativeStep(String query, String body) {
        return Map.of("phase", "walk", "exchange", Map.of(
                "request", Map.of("method", "GET", "path", "/{bucket}", "query", query,
                        "headers", List.of(Map.of("name", "user-agent", "value", "aws-sdk-java/2.31.78"))),
                "response", Map.of("status", 200, "headers", List.of(
                        Map.of("name", "content-type", "value", "application/xml"),
                        Map.of("name", "date", "value", DATE),
                        Map.of("name", "x-amz-request-id", "value", "request-id-sha256-x"),
                        Map.of("name", "x-amz-bucket-region", "value", "us-east-1")),
                        "body_base64", Base64.getEncoder().encodeToString(bytes(body)))));
    }

    private static ProviderMatchReceipt.ReplayStep replayStep(String query, String body) {
        return new ProviderMatchReceipt.ReplayStep("walk",
                new ProviderMatchReceipt.Request("GET", "/replay-bucket", query,
                        Map.of("user-agent", List.of("aws-sdk-java/2.31.78 md/io#sync"))),
                new ProviderExchangeComparator.Exchange(200, Map.of("content-type", List.of("application/xml"),
                        "x-amz-request-id", List.of("replay")), bytes(body)));
    }

    private static void writeRun(Path file, List<Map<String, Object>> steps) throws Exception {
        Files.writeString(file, JSON.writeValueAsString(Map.of("schema_version", "provider-capture-run-v1",
                "evidence_kind", "sdk_run", "provider", "s3", "probe_id", "s3-09a", "api_version", "2006-03-01",
                "steps", steps)));
    }

    private static Path cleanRepo(Path dir) throws Exception {
        Path source = dir.resolve("EvidenceTest.java");
        Files.writeString(source, "class EvidenceTest { @Test void exactTest() {} }");
        Files.writeString(dir.resolve("dist.jar"), "synthetic distribution");
        for (String[] args : List.of(new String[]{"init"}, new String[]{"config", "user.name", "Test"},
                new String[]{"config", "user.email", "test@example.invalid"}, new String[]{"add", "-A"},
                new String[]{"commit", "-m", "fixture"})) {
            List<String> command = new ArrayList<>(List.of("git", "-C", dir.toString()));
            command.addAll(List.of(args));
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertThat(process.waitFor()).as(output).isZero();
        }
        return source;
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}
