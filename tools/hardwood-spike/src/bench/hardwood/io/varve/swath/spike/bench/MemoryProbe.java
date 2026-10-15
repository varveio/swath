/* SPDX-License-Identifier: Apache-2.0 */
package io.varve.swath.spike.bench;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.Validity;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import java.lang.management.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/** Same-file reader admission experiment; every selected value is checked, none is retained. */
public final class MemoryProbe {
  private static final java.io.PrintStream OUT = System.out;
  private static final String CORPUS = System.getProperty("spike.corpus", "legacy");
  public static void main(String[] args) throws Exception {
    Path path = Path.of(args[0]);
    long expectedRows = Long.parseLong(args[1]);
    int requestedThreads = Integer.parseInt(args[2]);
    String projection = args[3], api = args[4];
    int batchRows = Integer.parseInt(args[5]), warmups = Integer.parseInt(args[6]), passes = Integer.parseInt(args[7]);
    int threads = requestedThreads == 0 ? Runtime.getRuntime().availableProcessors() : requestedThreads;
    emit(Map.of("event", "configuration", "file", path.toAbsolutePath().toString(), "threads", threads,
        "available_processors", Runtime.getRuntime().availableProcessors(), "projection", projection,
        "api", api, "batch_rows", batchRows, "warmups", warmups, "passes", passes,
        "max_outstanding_property", System.getProperty("hardwood.internal.maxOutstanding", "default")));
    for (int pass = 0; pass < warmups + passes; pass++) {
      System.gc();
      long baseline = heap();
      long start = System.nanoTime();
      long rows;
      int groups = -1;
      long maxGroupRows = -1;
      try (Samples samples = new Samples()) {
        if (api.equals("validation-only")) {
          for (rows = 0; rows < expectedRows; rows++) check(Corpus.row(CORPUS, rows), rows, projection);
        } else try (var context = HardwoodContext.create(threads);
                    var reader = ParquetFileReader.open(InputFile.of(path), context)) {
          groups = reader.getFileMetaData().rowGroups().size();
          maxGroupRows = reader.getFileMetaData().rowGroups().stream().mapToLong(g -> g.numRows()).max().orElse(0);
          rows = api.equals("row") ? rowRead(reader, projection) : columnRead(reader, projection, batchRows);
        }
        long operationNanos = System.nanoTime() - start;
        if (rows != expectedRows) throw new AssertionError("row count " + rows + " expected " + expectedRows);
        samples.sample();
        long peak = samples.heapPeak.get(), postGc = samples.postGcPeak.get(), rss = samples.rssPeak.get();
        long mapped = samples.mappedPeak.get(), direct = samples.directPeak.get();
        long gcCount = samples.gcCount() - samples.gcStartCount, gcMillis = samples.gcTime() - samples.gcStartMillis;
        System.gc();
        long afterClose = heap();
        Map<String,Object> result = new LinkedHashMap<>();
        result.put("event", "measurement"); result.put("pass", pass); result.put("phase", pass < warmups ? "warmup" : warmups > 0 ? "warmed" : "cold");
        result.put("api", api); result.put("projection", projection); result.put("batch_rows", batchRows); result.put("threads", threads);
        result.put("rows", rows); result.put("file_bytes", Files.size(path)); result.put("row_groups", groups); result.put("max_group_rows", maxGroupRows);
        result.put("operation_seconds", operationNanos / 1e9); result.put("rows_per_second", rows * 1e9 / operationNanos);
        result.put("heap_baseline_bytes", baseline); result.put("sampled_peak_heap_bytes", peak); result.put("sampled_peak_post_gc_heap_bytes", postGc);
        result.put("heap_after_close_full_gc_bytes", afterClose); result.put("sampled_peak_rss_kib", rss);
        result.put("sampled_peak_mapped_bytes", mapped); result.put("sampled_peak_direct_bytes", direct);
        result.put("gc_count", gcCount); result.put("gc_millis", gcMillis);
        emit(result);
      }
    }
  }

  private static long rowRead(ParquetFileReader reader, String projection) throws Exception {
    var builder = reader.buildRowReader();
    if (projection.equals("key")) builder.projection(ColumnProjection.columns("key"));
    long n = 0;
    try (var rows = builder.build()) {
      while (rows.hasNext()) {
        rows.next(); Object[] actual = new Object[13];
        for (int j = 0; j < (projection.equals("key") ? 1 : 13); j++) if (!rows.isNull(j)) {
          if (j == 1 || j == 2) actual[j] = rows.getLong(j);
          else if (j == 6 || j == 7) actual[j] = rows.getBoolean(j);
          else actual[j] = new String(rows.getBinary(j), StandardCharsets.UTF_8);
        }
        check(actual, n++, projection);
      }
    }
    return n;
  }

  private static long columnRead(ParquetFileReader reader, String projection, int batchRows) throws Exception {
    ColumnProjection selected = projection.equals("key") ? ColumnProjection.columns("key") : ColumnProjection.all();
    var builder = reader.buildColumnReaders(selected);
    if (batchRows > 0) builder.batchSize(batchRows);
    long n = 0;
    try (var columns = builder.build()) {
      int width = projection.equals("key") ? 1 : 13;
      while (columns.nextBatch()) {
        Object[] values = new Object[width]; Validity[] valid = new Validity[width];
        int[][] starts = new int[width][], ends = new int[width][];
        for (int j = 0; j < width; j++) {
          var column = columns.getColumnReader(j); valid[j] = column.getLeafValidity();
          if (j == 1 || j == 2) values[j] = column.getLongs();
          else if (j == 6 || j == 7) values[j] = column.getBooleans();
          else { values[j] = column.getBinaryValues(); starts[j] = column.getBinaryStarts(); ends[j] = column.getBinaryEnds(); }
        }
        for (int i = 0; i < columns.getRecordCount(); i++) {
          Object[] actual = new Object[13];
          for (int j = 0; j < width; j++) if (!valid[j].isNull(i)) {
            if (j == 1 || j == 2) actual[j] = ((long[])values[j])[i];
            else if (j == 6 || j == 7) actual[j] = ((boolean[])values[j])[i];
            else {
              // Match rowRead's transient binary copy and text materialization, avoiding retained row views.
              byte[] copy = Arrays.copyOfRange((byte[])values[j], starts[j][i], ends[j][i]);
              actual[j] = new String(copy, StandardCharsets.UTF_8);
            }
          }
          check(actual, n++, projection);
        }
      }
    }
    return n;
  }

  private static void check(Object[] actual, long n, String projection) {
    Object[] expected = Corpus.row(CORPUS, n);
    if (projection.equals("key")) {
      if (!Objects.equals(actual[0], expected[0])) throw new AssertionError("key mismatch " + n);
    } else if (!Arrays.equals(actual, expected)) throw new AssertionError("row mismatch " + n);
  }
  private static long heap() { return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(); }
  private static void emit(Map<String,?> row) { OUT.println(json(row)); OUT.flush(); }
  private static String json(Object value) {
    if (value == null) return "null";
    if (value instanceof Number || value instanceof Boolean) return value.toString();
    if (value instanceof Map<?,?> m) return "{" + String.join(",", m.entrySet().stream().map(e -> json(e.getKey().toString()) + ":" + json(e.getValue())).toList()) + "}";
    return "\"" + value.toString().replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r") + "\"";
  }

  private static final class Samples implements AutoCloseable {
    final AtomicLong heapPeak = new AtomicLong(), postGcPeak = new AtomicLong(), rssPeak = new AtomicLong(), mappedPeak = new AtomicLong(), directPeak = new AtomicLong();
    final long gcStartCount = gcCount(), gcStartMillis = gcTime();
    final Thread sampler = Thread.ofPlatform().daemon().start(() -> {
      while (!Thread.interrupted()) { sample(); try { Thread.sleep(5); } catch (InterruptedException e) { return; } }
    });
    void sample() {
      heapPeak.accumulateAndGet(heap(), Math::max);
      long collected = 0;
      for (var pool : ManagementFactory.getMemoryPoolMXBeans()) if (pool.getType() == MemoryType.HEAP && pool.getCollectionUsage() != null) collected += pool.getCollectionUsage().getUsed();
      postGcPeak.accumulateAndGet(collected, Math::max);
      for (var pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
        if (pool.getName().equals("mapped")) mappedPeak.accumulateAndGet(pool.getMemoryUsed(), Math::max);
        if (pool.getName().equals("direct")) directPeak.accumulateAndGet(pool.getMemoryUsed(), Math::max);
      }
      try { for (String line : Files.readAllLines(Path.of("/proc/self/status"))) if (line.startsWith("VmRSS:")) rssPeak.accumulateAndGet(Long.parseLong(line.trim().split("\\s+")[1]), Math::max); }
      catch (Exception ignored) { /* Linux-only supplemental RSS; /usr/bin/time supplies process peak. */ }
    }
    long gcCount() { return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(GarbageCollectorMXBean::getCollectionCount).filter(x -> x >= 0).sum(); }
    long gcTime() { return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(GarbageCollectorMXBean::getCollectionTime).filter(x -> x >= 0).sum(); }
    @Override public void close() throws InterruptedException { sampler.interrupt(); sampler.join(); }
  }
}
