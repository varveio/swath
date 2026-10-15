// SPDX-License-Identifier: Apache-2.0
package dev.swath.spike;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.s3.RangeBacking;
import dev.hardwood.s3.S3Credentials;
import dev.hardwood.s3.S3InputFile;
import dev.hardwood.s3.S3Source;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Exercises the unchanged released S3 implementation against an adversarial local endpoint. */
public final class RemoteProbe {
  private static final List<Map<String, Object>> results = new ArrayList<>();
  private static final List<Map<String, Object>> requests = new CopyOnWriteArrayList<>();
  private static Path output;
  private static byte[] fixture;

  public static void main(String[] args) throws Exception {
    output = Path.of(args.length == 0 ? "/workspaces/data/swath/hardwood-spike-20261015/remote" : args[0]);
    Files.createDirectories(output);
    fixture = fixture(output.resolve("fixture.parquet"));
    for (RangeBacking backing : RangeBacking.values()) {
      decode(backing, false, false);
      decode(backing, true, false);
      decode(backing, true, true);
    }
    for (String mode : List.of("normal", "mutation", "version-mutation", "missing-etag", "wrong-range", "malformed-range", "wrong-total", "extra-body", "truncated", "abrupt-truncated", "ignored-range", "encoded", "retry-success", "retry-failure")) raw(mode);
    for (String mode : List.of("open-ignored", "open-malformed", "open-wrong-offset", "open-short", "open-missing-range")) openFault(mode);
    repeatRanges(RangeBacking.NONE);
    repeatRanges(RangeBacking.SPARSE_TEMPFILE);
    large(RangeBacking.NONE);
    large(RangeBacking.SPARSE_TEMPFILE);
    for (boolean body : List.of(false, true)) cancellation(body);
    customBoundary();
    decoderFaults();
    mockMissingRange();
    Files.writeString(output.resolve("summary.json"), json(Map.of("version", "1.1.0.Beta2", "fixtureBytes", fixture.length, "cases", results)) + "\n");
    Files.writeString(output.resolve("requests.json"), json(requests) + "\n");
    System.out.println("Remote evidence: " + output);
  }

  private static byte[] fixture(Path path) throws Exception {
    var schema = FileSchema.builder("remote_probe")
        .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
        .addColumn("payload", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED).build();
    var config = WriterConfig.builder().codec(CompressionCodec.UNCOMPRESSED)
        .pageTargetRows(64).rowGroupTargetRows(1024).build();
    Random random = new Random(42);
    try (var writer = ParquetFileWriter.create(OutputFile.of(path), schema, config)) {
      for (int i = 0; i < 16384; i++) {
        final long id = i;
        byte[] payload = new byte[128];
        random.nextBytes(payload);
        writer.rowWriter().writeRow(row -> row.setLong("id", id).setBinary("payload", payload));
      }
    }
    return Files.readAllBytes(path);
  }

  private static void decode(RangeBacking backing, boolean projection, boolean predicate) throws Exception {
    String name = "decode-" + backing + "-" + projection + "-" + predicate;
    try (Endpoint endpoint = new Endpoint(name, "normal", fixture, fixture.length);
         S3Source source = endpoint.source(backing, 0)) {
      S3InputFile file = source.inputFile("bucket", "object.parquet");
      long rows = 0, sum = 0;
      Random expectedRandom = new Random(42);
      try (var reader = ParquetFileReader.open(file)) {
        var builder = reader.buildRowReader();
        if (projection) builder.projection(ColumnProjection.columns("id"));
        if (predicate) builder.filter(FilterPredicate.and(FilterPredicate.gtEq("id", 7900L), FilterPredicate.lt("id", 7920L)));
        try (var rowReader = builder.build()) {
          while (rowReader.hasNext()) {
            rowReader.next();
            long id = rowReader.getLong("id");
            if (predicate && (id < 7900 || id >= 7920)) throw new AssertionError("predicate returned wrong row");
            if (!predicate && id != rows) throw new AssertionError("full read wrong row");
            if (!projection) {
              byte[] expected = new byte[128]; expectedRandom.nextBytes(expected);
              if (!Arrays.equals(expected, rowReader.getBinary("payload"))) throw new AssertionError("payload bytes at " + id);
            }
            rows++; sum += id;
          }
        }
      }
      if (rows != (predicate ? 20 : 16384)) throw new AssertionError("row count " + rows);
      record(name, "PASS", "rows", rows, "idSum", sum, "serverRequests", endpoint.count.get(), "serverBytes", endpoint.bytes.get(), "libraryRequests", file.networkRequestCount(), "libraryBytes", file.networkBytesFetched());
    }
  }

  private static void raw(String mode) throws Exception {
    byte[] data = new byte[200000];
    for (int i = 0; i < data.length; i++) data[i] = (byte) i;
    try (Endpoint endpoint = new Endpoint(mode, mode, data, data.length);
         S3Source source = endpoint.source(RangeBacking.NONE, mode.startsWith("retry") ? 2 : 0)) {
      S3InputFile file = source.inputFile("bucket", "object");
      file.open();
      String identity = file.identity().orElse("<empty>");
      String outcome;
      boolean expectedBytes = false;
      String exception = "";
      try {
        ByteBuffer read = file.readRange(128, 32);
        expectedBytes = read.remaining() == 32;
        for (int i = 0; i < 32; i++) expectedBytes &= read.get() == data[128 + i];
        outcome = mode.equals("normal") || mode.equals("retry-success") ? "ACCEPTED" : expectedBytes ? "UNVALIDATED" : "UNSAFE_ACCEPTED_CORRUPT";
      } catch (Exception e) { outcome = "REJECTED"; exception = e.toString(); }
      // Bounds, EOF and empty ranges should be local checks.
      int before = endpoint.count.get();
      if (file.readRange(data.length, 0).remaining() != 0) throw new AssertionError("empty");
      int boundsRejected = 0;
      for (long[] range : List.of(new long[]{-1, 1}, new long[]{0, -1}, new long[]{data.length, 1}, new long[]{Long.MAX_VALUE, 1})) {
        try { file.readRange(range[0], (int) range[1]); } catch (IndexOutOfBoundsException e) { boundsRejected++; }
      }
      if (boundsRejected != 4 || endpoint.count.get() != before) throw new AssertionError("bounds");
      record(mode, outcome, "expectedBytes", expectedBytes, "exception", exception, "capturedIdentity", identity, "serverRequests", endpoint.count.get(), "serverBytes", endpoint.bytes.get(), "libraryRequests", file.networkRequestCount(), "libraryBytes", file.networkBytesFetched(), "boundsRejected", boundsRejected);
      file.close();
    }
  }

  private static void openFault(String mode) throws Exception {
    byte[] data = new byte[200000];
    try (Endpoint endpoint = new Endpoint(mode, mode, data, data.length);
         S3Source source = endpoint.source(RangeBacking.NONE, 0)) {
      S3InputFile file = source.inputFile("bucket", "object");
      try {
        file.open();
        record(mode, file.length() == data.length ? "UNVALIDATED" : "UNSAFE_ACCEPTED_WRONG_LENGTH", "length", file.length(), "serverBytes", endpoint.bytes.get());
      } catch (Exception e) { record(mode, "REJECTED", "exception", e.toString()); }
      file.close();
    }
  }

  private static void repeatRanges(RangeBacking backing) throws Exception {
    String name = "repeat-ranges-" + backing;
    byte[] data = new byte[200000];
    for (int i = 0; i < data.length; i++) data[i] = (byte)i;
    try (Endpoint endpoint = new Endpoint(name, "normal", data, data.length);
         S3Source source = endpoint.source(backing, 0)) {
      S3InputFile file = source.inputFile("bucket", "object");
      file.open();
      file.readRange(128, 32);
      file.readRange(128, 32);
      file.readRange(136, 8);
      file.readRange(199990, 10);
      file.readRange(0, 4);
      int expectedRequests = backing == RangeBacking.NONE ? 5 : 3;
      if (endpoint.count.get() != expectedRequests) throw new AssertionError("cache request count");
      file.close();
      record(name, "PASS", "serverRequests", endpoint.count.get(), "serverBytes", endpoint.bytes.get());
    }
  }

  private static void large(RangeBacking backing) throws Exception {
    long size = (1L << 31) + 123456;
    String name = "large-" + backing;
    try (Endpoint endpoint = new Endpoint(name, "normal", null, size);
         S3Source source = endpoint.source(backing, 0)) {
      S3InputFile file = source.inputFile("bucket", "large");
      try {
        file.open();
        long offset = (1L << 31) + 64;
        ByteBuffer range = file.readRange(offset, 32);
        for (int i = 0; i < 32; i++) if (range.get() != (byte)(offset + i)) throw new AssertionError("large offset");
        if (file.length() != size) throw new AssertionError("large length");
        record(name, "ACCEPTED", "length", file.length(), "offset", offset, "serverRequests", endpoint.count.get(), "serverBytes", endpoint.bytes.get());
      } catch (Exception e) { record(name, "REJECTED", "exception", e.toString(), "serverBytes", endpoint.bytes.get()); }
      finally { file.close(); }
    }
  }

  private static void cancellation(boolean body) throws Exception {
    String name = body ? "cancel-during-body" : "cancel-before-headers";
    byte[] data = new byte[200000];
    try (Endpoint endpoint = new Endpoint(name, body ? "stall-body" : "stall-headers", data, data.length);
         HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
         S3Source source = endpoint.source(RangeBacking.NONE, 0, client)) {
      S3InputFile file = source.inputFile("bucket", "object");
      file.open();
      AtomicReference<String> completion = new AtomicReference<>("running");
      Thread worker = Thread.ofPlatform().daemon().start(() -> {
        try { file.readRange(128, 32); completion.set("returned"); }
        catch (Exception e) { completion.set(e.toString()); }
      });
      if (!endpoint.entered.await(3, TimeUnit.SECONDS)) throw new AssertionError("server did not stall");
      long started = System.nanoTime();
      file.close();
      worker.join(body ? 2300 : 300);
      boolean joinedAfterFileClose = !worker.isAlive();
      worker.interrupt();
      worker.join(700);
      boolean joinedAfterInterrupt = !worker.isAlive();
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
      client.shutdownNow();
      worker.join(700);
      boolean joinedAfterClientShutdown = !worker.isAlive();
      endpoint.release.countDown();
      worker.join(1000);
      if (worker.isAlive()) throw new AssertionError("cleanup could not join worker");
      record(name, "LIMITATION", "joinedAfterFileClose", joinedAfterFileClose, "fileCloseJoinBudgetMs", body ? 2300 : 300, "requestTimeoutMs", 2000, "joinedAfterInterrupt700ms", joinedAfterInterrupt, "joinedAfterClientShutdown700ms", joinedAfterClientShutdown, "elapsedBeforeClientShutdownMs", elapsedMs, "completion", completion.get());
    }
  }

  static final class Endpoint implements AutoCloseable {
    final HttpServer server;
    final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    final AtomicInteger count = new AtomicInteger();
    final AtomicLong bytes = new AtomicLong();
    final AtomicBoolean stallOnce = new AtomicBoolean(true);
    final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    final String test, mode;
    final byte[] data;
    final long size;
    Endpoint(String test, String mode, byte[] data, long size) throws IOException {
      this.test = test; this.mode = mode; this.data = data; this.size = size;
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.setExecutor(executor);
      server.createContext("/", this::serve);
      server.start();
    }
    S3Source source(RangeBacking backing, int retries) { return source(backing, retries, null); }
    S3Source source(RangeBacking backing, int retries, HttpClient client) {
      var b = S3Source.builder().endpoint("http://127.0.0.1:" + server.getAddress().getPort()).pathStyle(true)
          .credentials(S3Credentials.of("local-test-access", "local-test-secret"))
          .requestTimeout(Duration.ofSeconds(2)).maxRetries(retries).rangeBacking(backing);
      if (client != null) b.httpClient(client);
      return b.build();
    }
    void serve(HttpExchange exchange) throws IOException {
      int number = count.incrementAndGet();
      String range = exchange.getRequestHeaders().getFirst("Range");
      if (range == null || !range.matches("bytes=(?:-[0-9]+|[0-9]+-[0-9]+)")) {
        requests.add(Map.of("test", test, "number", number, "status", 400, "error", "missing or malformed Range", "range", Objects.toString(range, "<absent>")));
        exchange.sendResponseHeaders(400, -1); exchange.close(); return;
      }
      boolean suffix = range.startsWith("bytes=-");
      long start, end;
      if (suffix) { start = Math.max(0, size - Long.parseLong(range.substring(7))); end = size - 1; }
      else { String[] pieces = range.substring(6).split("-"); start = Long.parseLong(pieces[0]); end = Long.parseLong(pieces[1]); }
      int status = 206;
      if (!suffix && mode.startsWith("retry") && (mode.equals("retry-failure") || number <= 3)) status = 503;
      if ((!suffix && mode.equals("ignored-range")) || (suffix && mode.equals("open-ignored"))) { status = 200; start = 0; end = size - 1; }
      long requestedStart = start;
      String etag = (!suffix && (mode.equals("mutation") || mode.equals("mutation-ignore-condition"))) ? "\"replacement\"" : "\"original\"";
      String version = (!suffix && mode.equals("version-mutation")) ? "v2" : "v1";
      String generation = (!suffix && (mode.equals("mutation") || mode.equals("mutation-ignore-condition") || mode.equals("version-mutation"))) ? "456" : "123";
      if (!mode.equals("missing-etag")) exchange.getResponseHeaders().set("x-goog-generation", generation);
      String ifMatch = exchange.getRequestHeaders().getFirst("If-Match");
      String query = exchange.getRequestURI().getQuery();
      if (!mode.equals("mutation-ignore-condition") && ((ifMatch != null && !ifMatch.equals(etag)) || (query != null && query.startsWith("ifGenerationMatch=") && !query.equals("ifGenerationMatch=" + generation)))) status = 412;
      if (!mode.equals("missing-etag")) exchange.getResponseHeaders().set("ETag", etag);
      exchange.getResponseHeaders().set("x-amz-version-id", version);
      String contentRange = "bytes " + start + "-" + end + "/" + size;
      if ((!suffix && mode.equals("wrong-range")) || (suffix && mode.equals("open-wrong-offset"))) contentRange = "bytes 1-2/" + size;
      if ((!suffix && mode.equals("malformed-range")) || (suffix && mode.equals("open-malformed"))) contentRange = "nonsense/" + size;
      if (!suffix && mode.equals("wrong-total")) contentRange = "bytes " + start + "-" + end + "/" + (size + 1);
      if (!mode.equals("open-missing-range") || !suffix) exchange.getResponseHeaders().set("Content-Range", contentRange);
      if (!suffix && mode.equals("encoded")) exchange.getResponseHeaders().set("Content-Encoding", "gzip");
      int length = Math.toIntExact(end - start + 1);
      if (status == 503 || status == 412) length = 0;
      if ((!suffix && mode.equals("truncated")) || (suffix && mode.equals("open-short"))) length /= 2;
      if (!suffix && mode.equals("extra-body")) length += 3;
      Map<String, Object> receipt = new LinkedHashMap<>();
      receipt.put("test", test); receipt.put("number", number); receipt.put("method", exchange.getRequestMethod()); receipt.put("range", range);
      receipt.put("ifMatch", Objects.toString(exchange.getRequestHeaders().getFirst("If-Match"), "<absent>"));
      receipt.put("uri", exchange.getRequestURI().toString()); receipt.put("authorizationScheme", Objects.toString(exchange.getRequestHeaders().getFirst("Authorization"), "").split(" ")[0]);
      receipt.put("status", status); receipt.put("responseContentRange", contentRange); receipt.put("responseEtag", etag); receipt.put("responseVersion", version); receipt.put("responseGeneration", generation); receipt.put("declaredBodyBytes", length);
      requests.add(receipt);
      try (exchange) {
        boolean stall = !suffix && mode.startsWith("stall-") && stallOnce.compareAndSet(true, false);
        if (stall && mode.equals("stall-headers")) { entered.countDown(); release.await(); }
        exchange.sendResponseHeaders(status, length == 0 ? -1 : length);
        if (stall && mode.equals("stall-body")) { exchange.getResponseBody().write(1); exchange.getResponseBody().flush(); entered.countDown(); release.await(); }
        byte[] body = new byte[length];
        for (int i = 0; i < length; i++) {
          long at = requestedStart + i;
          if (!suffix && mode.equals("wrong-range")) at++;
          body[i] = data == null ? (byte) at : data[(int) Math.min(data.length - 1, at)];
          if (!suffix && (mode.equals("mutation") || mode.equals("mutation-ignore-condition") || mode.equals("version-mutation"))) body[i]++;
        }
        int written = !suffix && mode.equals("abrupt-truncated") ? body.length / 2 : body.length;
        exchange.getResponseBody().write(body, 0, written);
        exchange.getResponseBody().flush();
        bytes.addAndGet(written);
      } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
      catch (IOException e) { receipt.put("serverWriteFailure", e.toString()); }
    }
    public void close() {
      release.countDown(); server.stop(0); executor.shutdownNow();
      try { if (!executor.awaitTermination(2, TimeUnit.SECONDS)) throw new AssertionError("mock handler did not stop"); }
      catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError("mock cleanup interrupted", e); }
    }
  }

  private static void customBoundary() throws Exception {
    for (var condition : PinnedHttpInputFile.Condition.values()) {
      String name = "custom-" + condition + "-predicate";
      try (Endpoint endpoint = new Endpoint(name, "normal", fixture, fixture.length)) {
        var input = new PinnedHttpInputFile(java.net.URI.create("http://127.0.0.1:" + endpoint.server.getAddress().getPort() + "/bucket/object"), condition);
        long count = 0, sum = 0;
        try (var reader = ParquetFileReader.open(input);
             var rows = reader.buildRowReader().projection(ColumnProjection.columns("id"))
                 .filter(FilterPredicate.and(FilterPredicate.gtEq("id", 7900L), FilterPredicate.lt("id", 7920L))).build()) {
          while (rows.hasNext()) { rows.next(); long id = rows.getLong("id"); if (id != 7900 + count) throw new AssertionError("custom row"); count++; sum += id; }
        }
        if (count != 20) throw new AssertionError("custom count");
        record(name, "PASS", "rows", count, "idSum", sum, "serverRequests", endpoint.count.get(), "serverBytes", endpoint.bytes.get(), "qualification", "simulated conditional HTTP; no native provider SDK/auth");
      }
      for (String mode : List.of("normal", "mutation", "mutation-ignore-condition", "version-mutation", "missing-etag", "wrong-range", "malformed-range", "wrong-total", "extra-body", "truncated", "abrupt-truncated", "ignored-range", "encoded", "open-ignored", "open-malformed", "open-wrong-offset", "open-short", "open-missing-range")) {
        name = "custom-" + condition + "-" + mode;
        byte[] data = new byte[200000]; for (int i = 0; i < data.length; i++) data[i] = (byte)i;
        try (Endpoint endpoint = new Endpoint(name, mode, data, data.length);
             var input = new PinnedHttpInputFile(java.net.URI.create("http://127.0.0.1:" + endpoint.server.getAddress().getPort() + "/bucket/object"), condition)) {
          boolean rejected = false; String exception = "";
          try { input.open(); ByteBuffer read = input.readRange(128, 32); for (int i = 0; i < 32; i++) if (read.get() != data[128 + i]) throw new AssertionError("custom bytes"); }
          catch (IOException e) { rejected = true; exception = e.toString(); }
          if (rejected == mode.equals("normal")) throw new AssertionError("custom expected rejection " + name + " rejected=" + rejected);
          if (mode.equals("abrupt-truncated") && input.bodyReadAttempts.get() == 0) throw new AssertionError("actual-short-body path was not reached");
          record(name, rejected ? "PASS_REJECTED" : "PASS", "exception", exception, "serverRequests", endpoint.count.get(), "bodyReadAttemptsAfterHeaderValidation", input.bodyReadAttempts.get());
        }
      }
      for (boolean body : List.of(false, true)) for (boolean closeOwner : List.of(false, true)) {
        name = "custom-" + condition + (body ? "-cancel-body" : "-cancel-headers") + (closeOwner ? "-owner-close" : "-operation-cancel");
        try (HttpClient sharedClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
             Endpoint endpoint = new Endpoint(name, body ? "stall-body" : "stall-headers", new byte[200000], 200000);
             var input = new PinnedHttpInputFile(java.net.URI.create("http://127.0.0.1:" + endpoint.server.getAddress().getPort() + "/bucket/object"), condition, sharedClient)) {
          input.open(); input.bodyReadStarted = new CountDownLatch(1); AtomicReference<String> completion = new AtomicReference<>("running");
          Thread worker = Thread.ofPlatform().daemon().start(() -> { try { input.readRange(128, 32); completion.set("returned"); } catch (Exception e) { completion.set(e.toString()); } });
          if (!endpoint.entered.await(3, TimeUnit.SECONDS)) throw new AssertionError("custom server did not stall");
          if (body && !input.bodyReadStarted.await(3, TimeUnit.SECONDS)) throw new AssertionError("custom client did not begin body read");
          long started = System.nanoTime();
          if (closeOwner) input.close(); else input.cancelPendingReads();
          worker.join(700);
          boolean joined = !worker.isAlive(); endpoint.release.countDown(); worker.join(1000);
          if (!joined || worker.isAlive()) throw new AssertionError("custom cancellation did not join");
          boolean sameInputReusable = false;
          if (!closeOwner) { sameInputReusable = input.readRange(128, 32).remaining() == 32; if (!sameInputReusable) throw new AssertionError("same input unusable"); }
          // No shutdownNow/close on the shared transport has run: a second owner must work.
          try (var second = new PinnedHttpInputFile(java.net.URI.create("http://127.0.0.1:" + endpoint.server.getAddress().getPort() + "/bucket/second"), condition, sharedClient)) { second.open(); if (second.length() != 200000) throw new AssertionError("shared transport unusable"); }
          record(name, "PASS", "joinedWithin700ms", joined, "elapsedIncludingReuseMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started), "completion", completion.get(), "sharedClientReusable", true, "sameInputReusable", sameInputReusable, "clientShutdownUsedForCancellation", false);
        }
      }
    }
  }

  private static void mockMissingRange() throws Exception {
    try (Endpoint endpoint = new Endpoint("mock-missing-range", "normal", new byte[1], 1);
         HttpClient client = HttpClient.newHttpClient()) {
      var request = java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:" + endpoint.server.getAddress().getPort() + "/bucket/object")).timeout(Duration.ofSeconds(2)).build();
      var response = client.send(request, java.net.http.HttpResponse.BodyHandlers.discarding());
      if (response.statusCode() != 400) throw new AssertionError("mock must explicitly reject missing Range");
      record("mock-missing-range", "PASS", "status", response.statusCode());
    }
  }

  private static void decoderFaults() throws Exception {
    for (String mode : List.of("mutation-ignore-condition", "abrupt-truncated", "stall-body")) {
      String name = "custom-decoder-" + mode;
      try (HttpClient sharedClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
           Endpoint endpoint = new Endpoint(name, mode, fixture, fixture.length);
           var input = new PinnedHttpInputFile(java.net.URI.create("http://127.0.0.1:" + endpoint.server.getAddress().getPort() + "/bucket/object"), PinnedHttpInputFile.Condition.S3_ETAG, sharedClient)) {
        AtomicReference<String> completion = new AtomicReference<>("running");
        AtomicLong rowsReturned = new AtomicLong();
        AtomicBoolean exceptionObserved = new AtomicBoolean();
        Thread worker = Thread.ofPlatform().daemon().start(() -> {
          try (var reader = ParquetFileReader.open(input)) {
            input.bodyReadStarted = new CountDownLatch(1);
            try (var rows = reader.buildRowReader().projection(ColumnProjection.columns("id")).build()) {
              while (rows.hasNext()) { rows.next(); rows.getLong("id"); rowsReturned.incrementAndGet(); }
              completion.set("completed");
            }
          } catch (Exception e) { exceptionObserved.set(true); completion.set(e.toString()); }
        });
        if (mode.equals("stall-body")) {
          if (!endpoint.entered.await(3, TimeUnit.SECONDS)) throw new AssertionError("decoder request did not stall");
          if (!input.bodyReadStarted.await(3, TimeUnit.SECONDS)) throw new AssertionError("decoder client did not begin body read");
          input.cancelPendingReads();
        }
        worker.join(2000);
        boolean joined = !worker.isAlive();
        if (!joined) { input.close(); endpoint.release.countDown(); worker.interrupt(); worker.join(2000); }
        endpoint.release.countDown();
        long virtualReaders = input.networkReadThreads.stream().filter(Thread::isVirtual).count();
        long observedReadersAliveAtClose = input.networkReadThreads.stream().filter(Thread::isVirtual).filter(Thread::isAlive).count();
        long joinDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(700);
        for (Thread io : input.networkReadThreads) if (io.isVirtual() && io.isAlive()) {
          long remaining = joinDeadline - System.nanoTime();
          if (remaining > 0) io.join(Duration.ofNanos(remaining));
        }
        long observedReadersAlive = input.networkReadThreads.stream().filter(Thread::isVirtual).filter(Thread::isAlive).count();
        if (!joined || !exceptionObserved.get() || rowsReturned.get() != 0 || virtualReaders == 0 || observedReadersAlive != 0) throw new AssertionError("decoder failure propagation " + name + ": " + completion.get() + " rows=" + rowsReturned + " joined=" + joined + " alive=" + observedReadersAlive);
        record(name, "PASS_REJECTED", "completion", completion.get(), "rowsReturned", rowsReturned.get(), "joinedWithin2000ms", joined, "observedVirtualIoCallers", virtualReaders, "observedVirtualIoCallersAliveAtReaderClose", observedReadersAliveAtClose, "observedVirtualIoCallersStillAliveAfter700ms", observedReadersAlive, "clientShutdownUsedForCancellation", false);
      }
    }
  }

  private static void record(String name, String outcome, Object... values) {
    Map<String, Object> result = new LinkedHashMap<>(); result.put("case", name); result.put("outcome", outcome);
    for (int i = 0; i < values.length; i += 2) result.put(values[i].toString(), values[i + 1]);
    results.add(result); System.out.println(json(result));
  }
  private static String json(Object value) {
    if (value == null) return "null";
    if (value instanceof Number || value instanceof Boolean) return value.toString();
    if (value instanceof Map<?, ?> m) return "{" + String.join(",", m.entrySet().stream().map(e -> json(e.getKey().toString()) + ":" + json(e.getValue())).toList()) + "}";
    if (value instanceof Collection<?> c) return "[" + String.join(",", c.stream().map(RemoteProbe::json).toList()) + "]";
    return "\"" + value.toString().replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\"";
  }
}
