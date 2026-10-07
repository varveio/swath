/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.sorted.SortedParquetRowGroupReader;
import io.varve.swath.output.parquet.sorted.SortedParquetWriter;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortMode;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Local protocol emulator, not a live-cloud or authentication qualification. */
public final class RemoteProbe {
    static final ObjectMapper JSON = new ObjectMapper();

    static void check(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }

    static final class Server implements AutoCloseable {
        final byte[] data;
        final HttpServer server;
        final ExecutorService executor = Executors.newCachedThreadPool();
        final AtomicLong bytes = new AtomicLong();
        final AtomicLong requests = new AtomicLong();
        final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        final List<Map<String, Object>> trace = Collections.synchronizedList(new ArrayList<>());
        volatile String mode = "valid";
        volatile CountDownLatch started = new CountDownLatch(1);
        volatile CountDownLatch release = new CountDownLatch(1);

        Server(byte[] data) throws IOException {
            this.data = data;
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(executor);
            server.createContext(
                    "/",
                    exchange -> {
                        try {
                            handle(exchange);
                        } catch (AssertionError | RuntimeException failure) {
                            failures.add(failure);
                            exchange.close();
                        }
                    });
            server.start();
        }

        String endpoint() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        void reset(String value) {
            mode = value;
            bytes.set(0);
            requests.set(0);
            started = new CountDownLatch(1);
            release = new CountDownLatch(1);
        }

        void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                var headers = exchange.getResponseHeaders();
                headers.set("ETag", "\"original\"");
                headers.set(
                        "x-amz-version-id",
                        mode.equals("changed-version") ? "replacement" : "original-version");
                headers.set("x-goog-generation", mode.equals("changed-identity") ? "2" : "1");
                if (exchange.getRequestMethod().equals("HEAD")) {
                    headers.set("Content-Length", Integer.toString(data.length));
                    exchange.sendResponseHeaders(200, -1);
                    return;
                }
                String range = exchange.getRequestHeaders().getFirst("Range");
                if (range == null) {
                    byte[] metadata =
                            ("{\"size\":\""
                                            + data.length
                                            + "\",\"generation\":\"1\",\"name\":\"file\",\"bucket\":\"bucket\"}")
                                    .getBytes(StandardCharsets.UTF_8);
                    headers.set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, metadata.length);
                    exchange.getResponseBody().write(metadata);
                    return;
                }
                requests.incrementAndGet();
                trace.add(
                        Map.of(
                                "mode",
                                mode,
                                "range",
                                range,
                                "path",
                                exchange.getRequestURI().getPath(),
                                "condition",
                                Objects.toString(
                                        exchange.getRequestHeaders().getFirst("If-Match"),
                                        Objects.toString(
                                                exchange.getRequestURI().getQuery(), ""))));
                boolean s3 = exchange.getRequestURI().getPath().equals("/bucket/file");
                check(
                        s3
                                ? "\"original\""
                                        .equals(exchange.getRequestHeaders().getFirst("If-Match"))
                                : Objects.toString(exchange.getRequestURI().getQuery(), "")
                                        .contains("ifGenerationMatch=1"),
                        "SDK missing condition");
                if (mode.equals("overwrite") || mode.equals("missing")) {
                    headers.set("Content-Type", s3 ? "application/xml" : "application/json");
                    byte[] error =
                            (s3
                                            ? "<Error><Code>PreconditionFailed</Code><Message>changed</Message></Error>"
                                            : "{\"error\":{\"code\":412,\"message\":\"changed\"}}")
                                    .getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(mode.equals("missing") ? 404 : 412, error.length);
                    exchange.getResponseBody().write(error);
                    return;
                }
                String[] bounds = range.substring(6).split("-");
                int start = Integer.parseInt(bounds[0]), end = Integer.parseInt(bounds[1]);
                check(start >= 0 && end < data.length && end >= start, "invalid SDK range");
                int count = end - start + 1;
                headers.set(
                        "Content-Range",
                        "bytes "
                                + (mode.equals("wrong-range") ? start + 1 : start)
                                + "-"
                                + end
                                + "/"
                                + data.length);
                if (mode.equals("changed-identity")) headers.set("ETag", "\"replacement\"");
                if (mode.equals("blocked-headers")) {
                    started.countDown();
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                if (mode.equals("encoded")) headers.set("Content-Encoding", "gzip");
                exchange.sendResponseHeaders(
                        mode.equals("ignored-range") ? 200 : 206,
                        mode.equals("wrong-length") ? count - 1 : count);
                if (mode.equals("blocked")) {
                    exchange.getResponseBody().write(data, start, 1);
                    exchange.getResponseBody().flush();
                    started.countDown();
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return;
                }
                int sent =
                        mode.equals("truncated") || mode.equals("wrong-length") ? count - 1 : count;
                exchange.getResponseBody().write(data, start, sent);
                bytes.addAndGet(sent);
            }
        }

        public void close() {
            release.countDown();
            server.stop(0);
            executor.shutdownNow();
        }
    }

    static ConditionalInput.Source source(String provider, Server server) throws IOException {
        return provider.equals("s3")
                ? new SdkSources.S3(server.endpoint())
                : new SdkSources.Gcs(server.endpoint());
    }

    static List<String> allRows(InputFile input) throws IOException {
        List<String> out = new ArrayList<>();
        try (var reader = ParquetFileReader.open(input)) {
            var schema = reader.getFooter().getFileMetaData().getSchema();
            check(schema.getFieldCount() == 13, "canonical 13 fields");
            var io = new ColumnIOFactory().getColumnIO(schema);
            for (var pages = reader.readNextRowGroup();
                    pages != null;
                    pages = reader.readNextRowGroup()) {
                try (var owned = pages) {
                    var records = io.getRecordReader(owned, new GroupRecordConverter(schema));
                    for (long i = 0; i < pages.getRowCount(); i++)
                        out.add(((Group) records.read()).toString());
                }
            }
        }
        return out;
    }

    static List<String> indexed(InputFile input, Path label) throws IOException {
        List<String> out = new ArrayList<>();
        try (var reader = new SortedParquetRowGroupReader(input, label)) {
            byte[] from = "key/00008000".getBytes(StandardCharsets.UTF_8);
            byte[] to = "key/00008010".getBytes(StandardCharsets.UTF_8);
            try (var cursor = reader.openKeyCursor(0, from, true, to)) {
                cursor.advanceTo(from, true);
                while (cursor.hasCurrent()) {
                    String key = new String(cursor.currentKey(), StandardCharsets.UTF_8);
                    if (key.compareTo("key/00008010") >= 0) break;
                    out.add(key);
                    cursor.advanceTo((key + "\0").getBytes(StandardCharsets.UTF_8), true);
                }
            }
            var rows = reader.objectRange(0, from, true, to, 20, true);
            check(rows.size() == 10, "indexed range row count");
            for (var row : rows) out.add(JSON.writeValueAsString(row));
        }
        return out;
    }

    static List<String> extended(InputFile input, Path label) throws IOException {
        List<String> out = new ArrayList<>();
        try (var reader = new SortedParquetRowGroupReader(input, label)) {
            byte[] from = "key/00008000".getBytes(StandardCharsets.UTF_8);
            byte[] to = "key/00008010".getBytes(StandardCharsets.UTF_8);
            for (var row : reader.objectRange(0, from, true, to, 20, false))
                out.add(JSON.writeValueAsString(row));
            check(
                    reader.objectRange(0, from, false, to, 20, true).size() == 9,
                    "exclusive lower bound");
            check(reader.objectRange(0, to, true, from, 20, true).isEmpty(), "empty range");
            check(
                    reader.objectRange(
                                            0,
                                            "key/00008192".getBytes(StandardCharsets.UTF_8),
                                            true,
                                            "key/00008322".getBytes(StandardCharsets.UTF_8),
                                            129,
                                            true)
                                    .size()
                            == 129,
                    "page crossing limit");
        }
        return out;
    }

    static void overwriteAfterFooter(String provider, Server server, Path label) throws Exception {
        server.reset("valid");
        try (var input = new ConditionalInput(source(provider, server));
                var reader = new SortedParquetRowGroupReader(input, label)) {
            server.reset("overwrite");
            try {
                reader.objectRange(
                        0,
                        "key/00008000".getBytes(StandardCharsets.UTF_8),
                        true,
                        "key/00008010".getBytes(StandardCharsets.UTF_8),
                        10,
                        true);
                throw new AssertionError("footer/page identity mixed");
            } catch (IOException | org.apache.parquet.io.ParquetDecodingException expected) {
                check(
                        server.requests.get() >= 1 && server.requests.get() <= 2,
                        "replacement query request count");
            }
        }
    }

    static String errorChain(Throwable error) {
        StringBuilder out = new StringBuilder();
        for (Throwable cause = error; cause != null; cause = cause.getCause())
            out.append(cause.getClass().getSimpleName())
                    .append(":")
                    .append(cause.getMessage())
                    .append(";");
        return out.toString();
    }

    static void expectedFault(String provider, String mode, IOException error) {
        if (mode.equals("overwrite") || mode.equals("missing")) {
            int status = mode.equals("overwrite") ? 412 : 404;
            if (provider.equals("s3")) {
                check(
                        error.getCause()
                                        instanceof
                                        software.amazon.awssdk.services.s3.model.S3Exception service
                                && service.statusCode() == status,
                        "unexpected S3 service error: " + errorChain(error));
            } else {
                check(
                        error instanceof com.google.api.client.http.HttpResponseException response
                                && response.getStatusCode() == status,
                        "unexpected GCS service error: " + errorChain(error));
            }
            return;
        }
        String expected =
                switch (mode) {
                    case "wrong-range" -> "incorrect range";
                    case "ignored-range" -> "ignored range";
                    case "changed-identity" ->
                            provider.equals("s3") ? "changed identity" : "changed generation";
                    case "truncated" -> "truncated range";
                    case "wrong-length" ->
                            provider.equals("s3") ? "changed identity" : "wrong length";
                    case "encoded" -> "encoded range";
                    case "changed-version" -> "changed S3 version";
                    default -> throw new AssertionError(mode);
                };
        check(expected.equals(error.getMessage()), "unexpected fault reason: " + errorChain(error));
    }

    static List<String> faults(String provider) {
        var values =
                new ArrayList<>(
                        List.of(
                                "overwrite",
                                "missing",
                                "wrong-range",
                                "ignored-range",
                                "changed-identity",
                                "truncated",
                                "wrong-length",
                                "encoded"));
        if (provider.equals("s3")) values.add("changed-version");
        return values;
    }

    static long cancel(String provider, Server server, String phase) throws Exception {
        server.reset("valid");
        var input = new ConditionalInput(source(provider, server));
        var stream = input.newStream();
        server.reset(phase);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> future =
                    worker.submit(
                            () -> {
                                try {
                                    stream.readFully(new byte[100]);
                                    throw new AssertionError("cancelled read succeeded");
                                } catch (IOException expectedError) {
                                    check(
                                            errorChain(expectedError)
                                                    .matches(
                                                            "(?is).*(closed|shutdown|shut"
                                                                + " down|shutting down|abort).*"),
                                            "unexpected cancellation failure: "
                                                    + errorChain(expectedError));
                                }
                            });
            check(server.started.await(5, TimeUnit.SECONDS), "blocked request not started");
            check(!future.isDone(), "read finished before cancellation");
            long start = System.nanoTime();
            input.close();
            future.get(6, TimeUnit.SECONDS);
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            check(millis < 2000, "cancellation too slow: " + phase);
            return millis;
        } finally {
            server.release.countDown();
            worker.shutdownNow();
            stream.close();
            input.close();
            check(worker.awaitTermination(2, TimeUnit.SECONDS), "request worker leaked");
        }
    }

    static void edges(String provider, Server server) throws Exception {
        server.reset("valid");
        try (var input = new ConditionalInput(source(provider, server));
                var stream = input.newStream()) {
            stream.seek(input.getLength());
            check(stream.read() == -1, "EOF byte");
            check(stream.read(new byte[0]) == 0, "EOF zero array");
            check(stream.read(ByteBuffer.allocateDirect(0)) == 0, "EOF zero buffer");
            stream.seek(7);
            ByteBuffer direct = ByteBuffer.allocateDirect(101);
            direct.position(3);
            direct.limit(90);
            stream.readFully(direct);
            check(stream.getPos() == 94 && direct.position() == 90, "ByteBuffer position");
            direct.flip();
            direct.position(3);
            byte[] got = new byte[87];
            direct.get(got);
            check(
                    Arrays.equals(got, Arrays.copyOfRange(server.data, 7, 94)),
                    "direct ByteBuffer bytes");
            stream.seek(2);
            byte[] backward = new byte[3];
            stream.readFully(backward);
            check(Arrays.equals(backward, Arrays.copyOfRange(server.data, 2, 5)), "backward seek");
            for (long invalid : new long[] {-1, Long.MAX_VALUE}) {
                try {
                    stream.seek(invalid);
                    throw new AssertionError("invalid seek accepted");
                } catch (EOFException expected) {
                }
            }
        }
        try (var raw = source(provider, server)) {
            for (long invalid : new long[] {-1, Long.MAX_VALUE}) {
                try {
                    raw.read(invalid, 100);
                    throw new AssertionError("invalid source range accepted");
                } catch (IOException expected) {
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        Files.createDirectories(root);
        Path file = root.resolve("fixture.parquet");
        if (!Files.exists(file)) {
            try (var writer =
                    new SortedParquetWriter(
                            file,
                            SortConfig.fromSystemProperties()
                                    .withFinalPageRows(128)
                                    .withFinalRowGroupBytes(64L << 20),
                            SortMode.OBJECTS,
                            1)) {
                for (int i = 0; i < 20000; i++)
                    writer.write(
                            new ObjectEntry(
                                    KeyBytes.ofUtf8(String.format("key/%08d", i)),
                                    i,
                                    1700000000000000L + i,
                                    i % 3 == 0
                                            ? null
                                            : UUID.nameUUIDFromBytes(
                                                            Integer.toString(i)
                                                                    .getBytes(
                                                                            StandardCharsets.UTF_8))
                                                    .toString(),
                                    "STANDARD",
                                    null,
                                    true,
                                    i % 2 == 0 ? "owner" : null,
                                    "display",
                                    "CRC32",
                                    "FULL_OBJECT"));
            }
        }
        var expected = allRows(new LocalInputFile(file));
        var expectedRange = indexed(new LocalInputFile(file), file);
        List<Map<String, Object>> report = new ArrayList<>();
        try (var server = new Server(Files.readAllBytes(file))) {
            for (String provider : List.of("s3", "gcs")) {
                server.reset("valid");
                try (var input = new ConditionalInput(source(provider, server))) {
                    check(expected.equals(allRows(input)), "all 13 fields mismatch");
                }
                long fullBytes = server.bytes.get(), fullRequests = server.requests.get();
                server.reset("valid");
                try (var input = new ConditionalInput(source(provider, server))) {
                    check(expectedRange.equals(indexed(input, file)), "indexed range mismatch");
                }
                long indexedBytes = server.bytes.get(), indexedRequests = server.requests.get();
                try (var input = new ConditionalInput(source(provider, server))) {
                    check(
                            extended(new LocalInputFile(file), file).equals(extended(input, file)),
                            "owner projection/boundary parity");
                }
                check(indexedBytes < server.data.length, "indexed query should avoid full object");
                List<String> rejected = new ArrayList<>();
                for (String mode : faults(provider)) {
                    server.reset("valid");
                    try (var input = new ConditionalInput(source(provider, server));
                            var stream = input.newStream()) {
                        server.reset(mode);
                        try {
                            stream.readFully(new byte[100]);
                            throw new AssertionError("accepted " + mode);
                        } catch (IOException expectedError) {
                            expectedFault(provider, mode, expectedError);
                            check(
                                    server.failures.isEmpty(),
                                    "fake server assertion failed: " + server.failures);
                            rejected.add(mode);
                        }
                        if (mode.equals("overwrite") || mode.equals("missing"))
                            check(server.requests.get() == 1, "retried permanent error");
                        server.reset("valid");
                        stream.seek(0);
                        byte[] restored = new byte[100];
                        stream.readFully(restored);
                        check(
                                Arrays.equals(restored, Arrays.copyOf(server.data, 100)),
                                "poisoned read after failure");
                    }
                }
                long cancelMillis = cancel(provider, server, "blocked");
                long cancelHeaderMillis = cancel(provider, server, "blocked-headers");
                edges(provider, server);
                overwriteAfterFooter(provider, server, file);

                server.reset("valid");
                try (var fresh = new ConditionalInput(source(provider, server));
                        var freshStream = fresh.newStream()) {
                    freshStream.readFully(new byte[100]);
                }
                var arm = new LinkedHashMap<String, Object>();
                arm.putAll(
                        Map.of(
                                "provider",
                                provider,
                                "rows",
                                expected.size(),
                                "file_bytes",
                                server.data.length,
                                "full_bytes",
                                fullBytes,
                                "full_requests",
                                fullRequests,
                                "indexed_bytes",
                                indexedBytes,
                                "indexed_requests",
                                indexedRequests,
                                "rejected",
                                rejected,
                                "cancel_ms",
                                cancelMillis));
                arm.put("cancel_headers_ms", cancelHeaderMillis);
                report.add(arm);
            }
            JSON.writerWithDefaultPrettyPrinter()
                    .writeValue(root.resolve("requests.json").toFile(), server.trace);
        }
        for (int size : new int[] {0, 6}) {
            try (var tiny = new Server(new byte[size])) {
                for (String provider : List.of("s3", "gcs")) {
                    try (var input = new ConditionalInput(source(provider, tiny))) {
                        try (var ignored = ParquetFileReader.open(input)) {
                            throw new AssertionError("tiny parquet accepted");
                        } catch (IOException | RuntimeException expectedError) {
                        }
                    }
                }
            }
        }
        JSON.writerWithDefaultPrettyPrinter()
                .writeValue(root.resolve("summary.json").toFile(), report);
        System.out.println(JSON.writeValueAsString(report));
    }
}
