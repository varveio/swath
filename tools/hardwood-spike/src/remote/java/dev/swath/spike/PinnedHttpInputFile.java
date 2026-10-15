// SPDX-License-Identifier: Apache-2.0
package dev.swath.spike;

import dev.hardwood.InputFile;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/**
 * Custom public-InputFile seam demonstration, deliberately separate from hardwood-s3.
 * Local HTTP only: the two modes SIMULATE native S3 If-Match and GCS generation conditions.
 * This has no provider SDK, authentication, cloud discovery, retries, or production qualification.
 */
final class PinnedHttpInputFile implements InputFile {
  enum Condition { S3_ETAG, GCS_GENERATION }
  private static final Pattern CONTENT_RANGE = Pattern.compile("bytes ([0-9]+)-([0-9]+)/([0-9]+)");
  private final URI uri;
  private final Condition condition;
  private final HttpClient client;
  private final boolean ownsClient;
  final java.util.concurrent.atomic.AtomicInteger bodyReadAttempts = new java.util.concurrent.atomic.AtomicInteger();
  final Set<Thread> networkReadThreads = ConcurrentHashMap.newKeySet();
  private final Set<CompletableFuture<?>> futures = ConcurrentHashMap.newKeySet();
  private final Set<InputStream> streams = ConcurrentHashMap.newKeySet();
  private final Object lock = new Object();
  private volatile boolean closed;
  volatile CountDownLatch bodyReadStarted = new CountDownLatch(1);
  private volatile long size = -1;
  private String identity;
  private String version;
  private byte[] tail;

  PinnedHttpInputFile(URI uri, Condition condition) {
    this(uri, condition, HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build(), true);
  }
  PinnedHttpInputFile(URI uri, Condition condition, HttpClient client) { this(uri, condition, client, false); }
  private PinnedHttpInputFile(URI uri, Condition condition, HttpClient client, boolean ownsClient) {
    this.uri = uri; this.condition = condition; this.client = client; this.ownsClient = ownsClient;
  }

  @Override public void open() throws IOException {
    if (size >= 0) return;
    // The public contract requires concurrent reads after open; simultaneous opens are not promised.
    try (Response response = request("bytes=-65536", false)) {
      long[] cr = contentRange(response.response);
      long expectedStart = Math.max(0, cr[2] - 65536);
      if (cr[0] != expectedStart || cr[1] != cr[2] - 1) throw new IOException("Invalid suffix range");
      String captured = response.response.headers().firstValue(condition == Condition.S3_ETAG ? "ETag" : "x-goog-generation")
          .orElseThrow(() -> new IOException("Missing provider identity"));
      byte[] bytes = body(response, Math.toIntExact(cr[1] - cr[0] + 1));
      synchronized (lock) {
        if (closed) throw new IOException("Input closed");
        identity = captured;
        version = response.response.headers().firstValue("x-amz-version-id").orElse(null);
        tail = bytes;
        size = cr[2];
      }
    }
  }

  @Override public ByteBuffer readRange(long offset, int length) throws IOException {
    long currentSize = length();
    if (offset < 0 || length < 0 || offset > currentSize || length > currentSize - offset) throw new IndexOutOfBoundsException();
    if (closed) throw new IOException("Input closed");
    if (length == 0) return ByteBuffer.allocate(0);
    if (offset >= currentSize - tail.length) return direct(Arrays.copyOfRange(tail, (int)(offset - (currentSize - tail.length)), (int)(offset - (currentSize - tail.length)) + length));
    networkReadThreads.add(Thread.currentThread());
    try (Response response = request("bytes=" + offset + "-" + (offset + length - 1), true)) {
      long[] cr = contentRange(response.response);
      if (cr[0] != offset || cr[1] != offset + length - 1 || cr[2] != currentSize) throw new IOException("Range differs from requested object slice");
      String currentIdentity = response.response.headers().firstValue(condition == Condition.S3_ETAG ? "ETag" : "x-goog-generation").orElse(null);
      if (!Objects.equals(identity, currentIdentity)) throw new IOException("Object identity changed");
      if (condition == Condition.S3_ETAG && !Objects.equals(version, response.response.headers().firstValue("x-amz-version-id").orElse(null))) throw new IOException("Object version changed");
      return direct(body(response, length));
    }
  }

  private Response request(String range, boolean pinned) throws IOException {
    URI target = pinned && condition == Condition.GCS_GENERATION ? URI.create(uri + "?ifGenerationMatch=" + identity) : uri;
    var request = HttpRequest.newBuilder(target).timeout(Duration.ofSeconds(2)).header("Range", range).header("Accept-Encoding", "identity");
    if (pinned && condition == Condition.S3_ETAG) request.header("If-Match", identity);
    CompletableFuture<HttpResponse<InputStream>> future;
    synchronized (lock) {
      if (closed) throw new IOException("Input closed");
      future = client.sendAsync(request.build(), HttpResponse.BodyHandlers.ofInputStream());
      futures.add(future);
    }
    try {
      HttpResponse<InputStream> response = future.get();
      InputStream stream = response.body();
      synchronized (lock) {
        if (closed) { stream.close(); throw new IOException("Input closed"); }
        streams.add(stream);
      }
      Response owned = new Response(response);
      try {
        if (response.statusCode() != 206) throw new IOException("Expected 206, got " + response.statusCode());
        if (!response.headers().firstValue("Content-Encoding").orElse("identity").equalsIgnoreCase("identity")) throw new IOException("Encoded range");
        return owned;
      } catch (IOException e) { owned.close(); throw e; }
    } catch (InterruptedException e) {
      future.cancel(true); Thread.currentThread().interrupt(); throw new IOException("Interrupted range request", e);
    } catch (ExecutionException | CancellationException e) { throw new IOException("Range request failed", e); }
    finally { futures.remove(future); }
  }

  private static long[] contentRange(HttpResponse<?> response) throws IOException {
    var match = CONTENT_RANGE.matcher(response.headers().firstValue("Content-Range").orElse(""));
    if (!match.matches()) throw new IOException("Malformed or missing Content-Range");
    try {
      long start = Long.parseLong(match.group(1)), end = Long.parseLong(match.group(2)), total = Long.parseLong(match.group(3));
      if (start > end || end >= total) throw new IOException("Invalid Content-Range bounds");
      return new long[]{start, end, total};
    } catch (NumberFormatException e) { throw new IOException("Content-Range overflow", e); }
  }

  private byte[] body(Response response, int length) throws IOException {
    if (response.response.headers().firstValueAsLong("Content-Length").orElse(-1) != length) throw new IOException("Wrong Content-Length");
    if (size >= 0) bodyReadAttempts.incrementAndGet();
    bodyReadStarted.countDown();
    byte[] bytes = response.response.body().readNBytes(length);
    if (bytes.length != length || response.response.body().read() != -1) throw new IOException("Wrong actual range length");
    return bytes;
  }

  private static ByteBuffer direct(byte[] bytes) { return ByteBuffer.allocateDirect(bytes.length).put(bytes).flip(); }
  @Override public long length() { if (size < 0) throw new IllegalStateException("Unopened"); return size; }
  @Override public String name() { return "custom-" + condition + ":" + uri; }
  @Override public Optional<String> identity() { length(); return Optional.of(identity); }
  @Override public void close() throws IOException {
    synchronized (lock) { closed = true; }
    cancelPendingReads();
    if (ownsClient) client.shutdownNow();
  }
  void cancelPendingReads() throws IOException {
    futures.forEach(f -> f.cancel(true));
    IOException failure = null;
    for (InputStream stream : streams) try { stream.close(); } catch (IOException e) { failure = e; }
    if (failure != null) throw failure;
  }
  private final class Response implements AutoCloseable {
    final HttpResponse<InputStream> response;
    Response(HttpResponse<InputStream> response) { this.response = response; }
    @Override public void close() throws IOException { streams.remove(response.body()); response.body().close(); }
  }
}
