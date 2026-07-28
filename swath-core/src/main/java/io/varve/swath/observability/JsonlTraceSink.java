/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.varve.swath.output.ControlCharEscaper;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code --trace <file>} JSONL flight recorder (V1; docs/internals/metrics-internals.md §7). One event
 * object per line, schema {@code "v":1}. Written <b>stream-append</b> (never atomic-rename like
 * {@link JsonRunSummaryWriter}) so a crash mid-run leaves a readable prefix on disk — the trace
 * doubles as a black box, same framing as JFR.
 *
 * <p><b>Flush strategy.</b> Buffered; explicitly flushed only on {@code completed}/{@code failed}
 * events, not every page — {@code page_committed} dominates event volume (~1/page) and flushing there
 * would make tracing dominate I/O cost. A real process kill loses only events buffered since the last
 * node's completion/failure. <b>Consumers must tolerate a torn FINAL line after a hard kill:</b> the
 * underlying {@link BufferedWriter} auto-flushes when its buffer fills, at an arbitrary byte boundary
 * (possibly mid-line) — so the last line on disk may be a partial JSON fragment. Every <i>earlier</i>
 * line is intact and individually parseable; a reader should parse line-by-line and drop a trailing
 * unparseable fragment. (We do not claim per-line write atomicity against buffer-full auto-flush.)
 *
 * <p>Thread-safe: every worker/thief virtual thread can emit concurrently; {@link #writeEvent}
 * serializes the write under a lock (JSONL needs whole-line atomicity across writers).
 */
final class JsonlTraceSink implements TraceSink {

    private static final Logger log = LoggerFactory.getLogger(JsonlTraceSink.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int SCHEMA_VERSION = 1;

    private final BufferedWriter writer;
    private final LongSupplier nanoClock;
    private final Object lock = new Object();
    private boolean closed;

    private JsonlTraceSink(BufferedWriter writer, LongSupplier nanoClock) {
        this.writer = writer;
        this.nanoClock = nanoClock;
    }

    static TraceSink open(Path path, LongSupplier nanoClock) throws IOException {
        Path parent = path.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        return new JsonlTraceSink(writer, nanoClock);
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public void seeded(long nodeId, byte[] lo, byte[] hi) {
        ObjectNode e = newEvent("seeded", -1L, nodeId);
        putKey(e, "lo", lo);
        putKey(e, "hi", hi);
        writeEvent(e, false);
    }

    @Override
    public void claimed(long workerId, long nodeId, byte[] lo, byte[] cursor, byte[] hi) {
        ObjectNode e = newEvent("claimed", workerId, nodeId);
        putKey(e, "lo", lo);
        putKey(e, "cursor", cursor);
        putKey(e, "hi", hi);
        writeEvent(e, false);
    }

    @Override
    public void pageCommitted(long workerId, long nodeId, int keysEmitted, byte[] cursor, boolean completed) {
        ObjectNode e = newEvent("page_committed", workerId, nodeId);
        e.put("keys", keysEmitted);
        putKey(e, "cursor", cursor);
        e.put("completed", completed);
        writeEvent(e, false);
    }

    @Override
    public void stealAttempt(long workerId, String outcome, String reason) {
        ObjectNode e = newEvent("steal_attempt", workerId, -1L);
        e.put("outcome", outcome);
        e.put("reason", reason);
        writeEvent(e, false);
    }

    @Override
    public void ownerSplitDecision(long workerId, long nodeId, String reason, double est,
            long pagesSinceLastSelfSplit, long outstanding, int workerCount, double farAheadFraction,
            double densityRatio, long keysEmitted) {
        ObjectNode e = newEvent("owner_split_decision", workerId, nodeId);
        e.put("reason", reason);
        putNum(e, "est", est);
        e.put("pages_since_last_self_split", pagesSinceLastSelfSplit);
        e.put("outstanding", outstanding);
        e.put("worker_count", workerCount);
        putNum(e, "far_ahead_fraction", farAheadFraction);
        putNum(e, "density_ratio", densityRatio);
        e.put("keys_emitted", keysEmitted);
        writeEvent(e, false);
    }

    @Override
    public void victimScan(long workerId, int seen, int skippedUnsplittable, int skippedPaced, int skippedNoSpan,
            long chosenNodeId, double bestEst, String reason) {
        ObjectNode e = newEvent("victim_scan", workerId, -1L);
        e.put("seen", seen);
        e.put("skipped_unsplittable", skippedUnsplittable);
        e.put("skipped_paced", skippedPaced);
        e.put("skipped_no_span", skippedNoSpan);
        e.put("chosen_node_id", chosenNodeId);
        putNum(e, "best_est", bestEst);
        e.put("reason", reason);
        writeEvent(e, false);
    }

    @Override
    public void split(long workerId, long parentNodeId, long childNodeId, String mechanism, byte[] pivot, byte[] hi) {
        writeSplitLike("split", workerId, parentNodeId, childNodeId, mechanism, pivot, hi);
    }

    @Override
    public void ownerSplit(long workerId, long parentNodeId, long childNodeId, String mechanism, byte[] pivot,
                            byte[] hi) {
        writeSplitLike("owner_split", workerId, parentNodeId, childNodeId, mechanism, pivot, hi);
    }

    private void writeSplitLike(String event, long workerId, long parentNodeId, long childNodeId,
                                 String mechanism, byte[] pivot, byte[] hi) {
        ObjectNode e = newEvent(event, workerId, parentNodeId);
        e.put("child_node_id", childNodeId);
        e.put("mechanism", mechanism);
        putKey(e, "pivot", pivot);
        putKey(e, "hi", hi);
        writeEvent(e, false);
    }

    @Override
    public void completed(long workerId, long nodeId) {
        writeEvent(newEvent("completed", workerId, nodeId), true);
    }

    @Override
    public void failed(long workerId, long nodeId, String reason) {
        ObjectNode e = newEvent("failed", workerId, nodeId);
        e.put("reason", reason);
        writeEvent(e, true);
    }

    /** {@code v, ts_ns, ts_ms, event, worker_id[, node_id]} — the common envelope every event shares. */
    private ObjectNode newEvent(String event, long workerId, long nodeId) {
        ObjectNode e = MAPPER.createObjectNode();
        e.put("v", SCHEMA_VERSION);
        e.put("ts_ns", nanoClock.getAsLong());
        e.put("ts_ms", System.currentTimeMillis());
        e.put("event", event);
        e.put("worker_id", workerId);
        if (nodeId >= 0) {
            e.put("node_id", nodeId);
        }
        return e;
    }

    /** Key fields are escaped exactly like text-sink output — traces carry real keys. */
    private static void putKey(ObjectNode e, String field, byte[] raw) {
        if (raw == null) {
            e.putNull(field);
        } else {
            e.put(field, ControlCharEscaper.escape(new String(raw, StandardCharsets.UTF_8)));
        }
    }

    /**
     * A numeric field, serialized as JSON <b>null</b> when non-finite — JSONL has no {@code NaN}/
     * {@code Infinity} literal, and a trace line must stay strictly parseable. Both non-finite
     * sources are meaningful and disambiguated by the event's {@code reason}: {@code NaN} is a
     * gate input the short-circuiting chain never computed, {@code ±Infinity} a real reading (an
     * open-frontier {@code estRemaining}, or the density ratio with {@code density_ewma=off}). See
     * docs/internals/metrics-internals.md §7.
     */
    private static void putNum(ObjectNode e, String field, double v) {
        if (Double.isFinite(v)) {
            e.put(field, v);
        } else {
            e.putNull(field);
        }
    }

    private void writeEvent(ObjectNode e, boolean flush) {
        synchronized (lock) {
            if (closed) {
                return;
            }
            try {
                writer.write(MAPPER.writeValueAsString(e));
                writer.write('\n');
                if (flush) {
                    writer.flush();
                }
            } catch (IOException ex) {
                log.warn("trace_write_failed message={}", ex.getMessage());
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                log.warn("trace_close_failed message={}", e.getMessage());
            }
        }
    }
}
