/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import io.varve.swath.concurrent.Scope;
import io.varve.swath.error.OutputException;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.PageBatch;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.output.parquet.PartInfo;
import io.varve.swath.output.parquet.PartListener;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * A format-neutral, decoupled dataset writer pool: a bounded number of {@code numWriters} lanes
 * (default 3), each on its own platform thread draining a bounded queue, so
 * sink I/O runs away from the listing workers. <b>Sticky</b> assignment — every
 * page of a node goes to {@code writer = nodeId % numWriters}, so a node's pages
 * occupy a contiguous run of one lane's size-rotated parts (which finalize in
 * order, making the {@code durable_cursor} model sound).
 *
 * <p><b>Publication.</b> A part becomes consumer-visible only after {@link
 * DatasetPartWriter#close()} succeeds; every finalized part is added to the atomic {@code
 * manifest.json}. On {@link #close()} (success) each already-admitted lane batch drains, each
 * lane finalizes its open part, and {@code _SUCCESS} is written last. A concurrent submitter
 * still waiting for a full lane when close begins is rejected rather than admitted into that
 * successful close. On {@link #abort()} (failure) each lane discards its open part. When a
 * durable listener is configured (the Parquet sink), it advances the checkpoint before manifest
 * publication so resume retains only finalized parts. Text datasets deliberately provide no
 * resume contract in this release.
 *
 * <p>Lanes <b>always drain to the poison sentinel</b> (even after a write
 * failure), so a full lane queue can never deadlock {@link #submit} or shutdown.
 *
 * <p><b>Rotation cadence.</b> Beyond the {@code targetBytes} size
 * trigger, a lane also rotates once its open part has been open
 * {@code rotationIntervalNanos} or has buffered {@code rotationMaxRows} rows —
 * whichever fires first — so {@code durable_cursor} advances on a bounded cadence
 * instead of only when a part happens to fill up (the resume RPO gap). Both are
 * checked on the lane's own thread alongside the size check (no new
 * concurrency), reuse {@link #finalizeCurrent} verbatim, and never fire on an
 * empty part (an idle lane never produces empty parts). {@code 0} disables
 * either trigger.
 */
public final class SharedDatasetWriterPool implements DatasetWriterPool {

    /** Defensive process-resource ceiling; format-specific policy may admit fewer lanes. */
    public static final int MAX_WRITERS = 64;

    /**
     * Whole-pool production submission ceiling in batches. This equals the former maximum of 64
     * slots for each of four lanes, but no longer grows when an expert requests more writers.
     */
    public static final int MAX_TOTAL_QUEUE_CAPACITY = 4 * 64;

    /** Preserve the existing queue depth for every count in the measured 1-4 lane envelope. */
    public static final int DEFAULT_QUEUE_CAPACITY_PER_LANE = 64;

    /** Equal per-lane share whose pool-wide product never exceeds the production budget. */
    public static int defaultQueueCapacityPerLane(int writers) {
        if (writers < 1 || writers > MAX_WRITERS) {
            throw new IllegalArgumentException("writers must be 1.." + MAX_WRITERS);
        }
        return Math.max(1, Math.min(DEFAULT_QUEUE_CAPACITY_PER_LANE,
                MAX_TOTAL_QUEUE_CAPACITY / writers));
    }

    /**
     * A bounded snapshot of one writer lane. All durations are elapsed time, not CPU
     * time: lane work can include encoding, filesystem I/O, checkpoint work, and manifest
     * serialization. JFR is the authority for sampled CPU attribution.
     */
    public record LaneStatistics(
            int lane,
            int queueCapacity,
            int queueDepth,
            int queueDepthPeak,
            boolean waitingForWork,
            long rowsWritten,
            long finalizedBytes,
            long batchesWritten,
            long activeElapsedNanos,
            long submitBlockedCount,
            long submitBlockedNanos,
            long headOfLineBlockedCount,
            long headOfLineBlockedNanos,
            long partsFinalized,
            long finalizeCount,
            long finalizeElapsedNanos) {
    }

    private static final PageBatch POISON = new PageBatch(-1, -1, List.of());

    /**
     * Anti-spin floor for the idle-lane poll wait below. The CLI now
     * rejects a too-small {@code --part-rotation-interval} ({@code ListCommand.
     * MIN_PART_ROTATION_INTERVAL}), but this constructor is a public API — a caller could
     * still hand {@link #rotationIntervalNanos} an arbitrarily small positive value (e.g. one
     * nanosecond), which would otherwise make the lane thread {@code poll()}-timeout and
     * re-loop continuously with no work (a CPU wakeup storm). Clamping only the poll WAIT to
     * {@code max(rotationIntervalNanos, ROTATION_POLL_FLOOR_NANOS)} bounds how often an idle
     * lane's thread wakes; it never changes the staleness decision itself — {@link
     * #rotationReason} still compares elapsed time against the true, unclamped {@code
     * rotationIntervalNanos}, so when a part is judged stale/rotated is unaffected.
     */
    private static final long ROTATION_POLL_FLOOR_NANOS = 50_000_000L;   // 50 ms

    private final Path dir;
    private final String sinkName;
    private final DatasetLayout layout;
    private final DatasetFormat format;
    private final String argsHash;
    private final String bucket;
    private final int numWriters;
    private final long targetBytes;
    private final long rotationIntervalNanos;
    private final long rotationMaxRows;
    private final LongSupplier nanoClock;
    private final PartListener partListener;
    // Format-owned observation hooks keep sink-specific metrics out of orchestration.
    private final DatasetWriterObserver observer;

    /** Which of the three rotation triggers fired ({@link #rotationReason}); {@code NONE} = don't rotate. */
    private enum RotationReason {
        NONE, SIZE, ROWS, TIME;

        String tag() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final Lane[] lanes;
    // Encoding and file I/O can block or pin, so the few long-lived lanes use
    // dedicated platform threads instead of consuming listing-worker carriers.
    private final Scope scope;
    private final List<Future<?>> laneFutures = new ArrayList<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicLong partDigestCount = new AtomicLong();
    private final AtomicLong partDigestNanos = new AtomicLong();
    private final AtomicLong manifestWriteCount = new AtomicLong();
    private final AtomicLong manifestWriteNanos = new AtomicLong();

    private final List<PartInfo> committedParts = Collections.synchronizedList(new ArrayList<>());
    private final Object manifestLock = new Object();
    private enum ShutdownMode { OPEN, CLOSE, ABORT }

    /**
     * The read side admits concurrent submitters; the write side closes admission before poison is
     * queued, so no batch can ever land behind a lane's poison sentinel and wait forever.
     */
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private final AtomicReference<ShutdownMode> shutdownMode =
            new AtomicReference<>(ShutdownMode.OPEN);
    private final AtomicBoolean joinInitiated = new AtomicBoolean();
    private final CompletableFuture<Void> joined = new CompletableFuture<>();
    private final AtomicBoolean publicationInitiated = new AtomicBoolean();
    private final CompletableFuture<Void> published = new CompletableFuture<>();

    /** The pool owns scheduling and publication; the adapter owns encoding. */
    public SharedDatasetWriterPool(Path dir, DatasetFormat format, String argsHash,
                             int numWriters, long targetBytes, int queueCapacity,
                             DatasetWriterPoolConfig config) {
        this(dir, format, argsHash, numWriters, targetBytes, queueCapacity, config, System::nanoTime);
    }

    public SharedDatasetWriterPool(Path dir, DatasetFormat format, String argsHash,
                      int numWriters, long targetBytes, int queueCapacity,
                      DatasetWriterPoolConfig config, LongSupplier nanoClock) {
        if (numWriters < 1 || numWriters > MAX_WRITERS) {
            throw new IllegalArgumentException(
                    "numWriters must be 1.." + MAX_WRITERS + " (got " + numWriters + ")");
        }
        if (queueCapacity < 1
                || (long) numWriters * queueCapacity > MAX_TOTAL_QUEUE_CAPACITY) {
            throw new IllegalArgumentException("queueCapacity must be at least 1 and keep aggregate "
                    + "writer queue capacity <= " + MAX_TOTAL_QUEUE_CAPACITY + " batches (writers="
                    + numWriters + ", queueCapacity=" + queueCapacity + ", aggregate="
                    + (long) numWriters * queueCapacity + ")");
        }
        this.dir = dir;
        this.sinkName = config.sinkName();
        this.scope = Scope.ofPlatformThreads(sinkName + "-writer");
        this.layout = DatasetLayout.of(dir);
        this.format = format;
        this.argsHash = argsHash;
        this.bucket = config.bucket();
        this.numWriters = numWriters;
        this.targetBytes = targetBytes;
        this.rotationIntervalNanos = config.rotationIntervalNanos();
        this.rotationMaxRows = config.rotationMaxRows();
        this.nanoClock = nanoClock;
        this.partListener = config.partListener();
        this.observer = config.observer();
        this.committedParts.addAll(config.existingParts());
        this.lanes = new Lane[this.numWriters];
        for (int i = 0; i < this.numWriters; i++) {
            lanes[i] = new Lane(i, queueCapacity, new ArrayBlockingQueue<>(queueCapacity));
        }
        // Resume: continue each lane's part sequence PAST any carried-over part so a
        // resumed run never reuses (and so overwrites) a prior finalized part's filename.
        for (PartInfo p : config.existingParts()) {
            int w = p.writerId();
            if (w >= 0 && w < this.numWriters) {
                lanes[w].seq = Math.max(lanes[w].seq, parseSeq(p.path(), format.partSuffix()) + 1);
            }
        }
        for (Lane lane : lanes) {
            laneFutures.add(scope.fork(() -> runLane(lane)));
        }
    }

    /** Extract the {@code %05d} sequence from a dataset part file name. */
    private static int parseSeq(String fileName, String suffix) {
        String base = fileName.endsWith(suffix)
                ? fileName.substring(0, fileName.length() - suffix.length()) : fileName;
        int dash = base.lastIndexOf('-');
        try {
            return dash < 0 ? -1 : Integer.parseInt(base.substring(dash + 1));
        } catch (NumberFormatException e) {
            return -1;   // unrecognized name ⇒ don't constrain the sequence
        }
    }

    /** Route a batch to its sticky lane (blocking on a full lane queue — backpressure). */
    public void submit(PageBatch batch) throws OutputException, InterruptedException {
        int laneId = (int) Math.floorMod(batch.nodeId(), numWriters);
        lanes[laneId].admit(batch);
        checkFailure();
    }

    /** Whether this blocked sticky lane stranded another writer that had no queued work. */
    private boolean anotherLaneIsWaitingForWork(int selectedLane) {
        for (Lane lane : lanes) {
            if (lane.id != selectedLane && lane.waitingForWork && lane.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /** Bounded per-lane state for periodic and terminal structured summaries. */
    public List<LaneStatistics> laneStatistics() {
        List<LaneStatistics> snapshots = new ArrayList<>(lanes.length);
        for (Lane lane : lanes) {
            snapshots.add(lane.statistics());
        }
        return List.copyOf(snapshots);
    }

    public long committedPartCount() {
        synchronized (committedParts) {
            return committedParts.size();
        }
    }

    public long committedBytes() {
        synchronized (committedParts) {
            return committedParts.stream().mapToLong(PartInfo::bytes).sum();
        }
    }

    long rotationIntervalNanos() { return rotationIntervalNanos; }
    long rotationMaxRows() { return rotationMaxRows; }
    long partDigestCount() { return partDigestCount.get(); }
    long partDigestNanos() { return partDigestNanos.get(); }
    long manifestWriteCount() { return manifestWriteCount.get(); }
    long manifestWriteNanos() { return manifestWriteNanos.get(); }

    private void checkFailure() throws OutputException {
        Throwable t = failure.get();
        if (t != null) {
            throw new OutputException(sinkName + " writer failed", t);
        }
    }

    private Void runLane(Lane lane) throws InterruptedException {
        // Always drain to POISON — never exit early on failure, or a full queue
        // would wedge submit()/shutdown (the lane-failure deadlock).
        //
        // Idle-lane cadence: check-on-write alone never
        // re-evaluates a lane that stopped receiving batches, so a stale non-empty
        // part can sit non-durable indefinitely. When the time trigger is
        // configured, wake on a timeout (instead of blocking forever) so THIS
        // lane's OWN thread — no new thread, no shared mutable state — can
        // re-run the same rotation check with nothing new to write. Disabled
        // (rotationIntervalNanos == 0, incl. --single-file) keeps the exact
        // prior take() behavior.
        try {
            while (true) {
                // Floor the poll WAIT (never the staleness check in rotationReason()) so a tiny
                // positive rotationIntervalNanos can't spin this thread — see ROTATION_POLL_FLOOR_NANOS.
                PageBatch batch = lane.dequeueNow();
                if (batch == null) {
                    // The busy path still pays for one queue operation and no waiting-state
                    // writes. Publish idleness only after an immediate poll finds no work.
                    lane.waitingForWork = true;
                    try {
                        batch = rotationIntervalNanos > 0
                                ? lane.dequeueWithin(Math.max(rotationIntervalNanos, ROTATION_POLL_FLOOR_NANOS),
                                        TimeUnit.NANOSECONDS)
                                : lane.dequeue();
                    } finally {
                        lane.waitingForWork = false;
                    }
                }
                if (batch == null) {
                    // Timed out — no new batch arrived. Re-evaluate the SAME rotation
                    // check; rotationReason() short-circuits on rows==0 so an idle EMPTY
                    // lane still produces nothing, and finalizeCurrent() no-ops when
                    // there is no open part (lane.current == null).
                    if (shutdownMode.get() != ShutdownMode.ABORT
                            && failure.get() == null && lane.current != null) {
                        RotationReason reason = rotationReason(lane);
                        if (reason != RotationReason.NONE) {
                            long startedAt = nanoClock.getAsLong();
                            try {
                                recordRotation(reason);
                                finalizeCurrent(lane);
                            } catch (Throwable t) {
                                recordFailure(t);
                            } finally {
                                recordLaneWork(lane, startedAt);
                            }
                        }
                    }
                    continue;
                }
                if (batch == POISON) {
                    break;
                }
                if (shutdownMode.get() == ShutdownMode.ABORT || failure.get() != null) {
                    continue;   // draining and discarding
                }
                long startedAt = nanoClock.getAsLong();
                try {
                    writeBatch(lane, batch);
                } catch (Throwable t) {
                    recordFailure(t);
                } finally {
                    recordLaneWork(lane, startedAt);
                }
            }
        } catch (InterruptedException e) {
            recordFailure(e);
            throw e;
        } finally {
            finishLane(lane);
        }
        return null;
    }

    /** Finalize or discard exactly once on every lane exit, including queue-wait interruption. */
    private void finishLane(Lane lane) {
        boolean hasOpenPart = lane.current != null;
        long startedAt = nanoClock.getAsLong();
        try {
            if (shutdownMode.get() == ShutdownMode.ABORT || failure.get() != null) {
                discardCurrent(lane);   // partial part is NOT durable → delete it
            } else {
                finalizeCurrent(lane);
            }
        } catch (Throwable t) {
            recordFailure(t);
        } finally {
            if (hasOpenPart) {   // no open part ⇒ both paths no-op; don't record a fabricated zero
                recordLaneWork(lane, startedAt);
            }
        }
    }

    /** First failure wins; later cleanup failures are retained as suppressed diagnostics. */
    private void recordFailure(Throwable next) {
        if (failure.compareAndSet(null, next)) {
            return;
        }
        Throwable primary = failure.get();
        if (primary != next) {
            primary.addSuppressed(next);
        }
    }

    /**
     * Reports one stretch of lane-thread work through the format-owned observer. Called from the
     * three places a lane does work between waits on its queue (batch write, idle-cadence rotation,
     * drain-time finalize/discard), so summing the span accounts for the pool's active elapsed time
     * rather than the dispatch that {@code emit} sees. It is not CPU attribution: file I/O, fsync,
     * checkpoint work, and manifest-lock waits can contribute. Measured on {@link #nanoClock}, the
     * same monotonic clock the rotation triggers read.
     *
     * <p>Swallows anything thrown while recording. This is the only work in the lane loop that is
     * NOT already inside a catch-all, and it is pure observation: letting a metrics/clock failure
     * escape would kill the lane thread before it consumes the poison sentinel, which is exactly the
     * lane-failure deadlock the "always drain to POISON" rule exists to prevent.
     */
    private void recordLaneWork(Lane lane, long startedAtNanos) {
        try {
            long elapsedNanos = Math.max(0L, nanoClock.getAsLong() - startedAtNanos);
            lane.activeElapsedNanos += elapsedNanos;
            observer.recordLaneWork(elapsedNanos);
        } catch (Throwable ignored) {
            // observation only — never take the lane down with it
        }
    }

    private void writeBatch(Lane lane, PageBatch batch) throws Exception {
        if (lane.current == null) {
            lane.openPart();
        }
        for (ListEntry e : batch.entries()) {
            lane.current.write(e);
        }
        // Count only a completely encoded batch. A mid-batch format failure leaves an unknown
        // partial row count, which is deliberately not presented as successfully written work.
        lane.rowsWritten += batch.entryCount();
        lane.batchesWritten++;
        // Track the highest key this node contributed to the open part — the
        // durable_cursor it makes durable when the part finalizes (§4.5). Listing is
        // ascending per node, so the latest batch's last key is the max.
        if (!batch.entries().isEmpty()) {
            lane.partNodeMaxKey.put(batch.nodeId(), batch.entries().getLast().key().rawUnsafe());
        }
        RotationReason reason = rotationReason(lane);
        if (reason != RotationReason.NONE) {
            recordRotation(reason);
            finalizeCurrent(lane);
        }
    }

    /**
     * Which of the three triggers fires — size (unchanged), row count, or
     * time-open — first, or {@code NONE}. Never fires for an empty part (an idle/just-opened
     * lane must not produce empty parts).
     */
    private RotationReason rotationReason(Lane lane) {
        DatasetPartWriter w = lane.current;
        long rows = w.rows();
        if (rows == 0) {
            return RotationReason.NONE;
        }
        if (w.bufferedDataSize() >= targetBytes) {
            return RotationReason.SIZE;
        }
        if (rotationMaxRows > 0 && rows >= rotationMaxRows) {
            return RotationReason.ROWS;
        }
        if (rotationIntervalNanos > 0
                && (nanoClock.getAsLong() - lane.partOpenedAtNanos) >= rotationIntervalNanos) {
            return RotationReason.TIME;
        }
        return RotationReason.NONE;
    }

    /** Engagement counter — did the cadence trigger earn its keep, and how often. */
    private void recordRotation(RotationReason reason) {
        observer.recordRotation(reason.tag());
    }

    private void finalizeCurrent(Lane lane) throws Exception {
        if (lane.current == null) {
            return;
        }
        DatasetPartWriter w = lane.current;
        long rows = w.rows();
        Path path = w.path();
        Object finalizeSample = observer.startFinalize();
        long finalizeStartedAt = nanoClock.getAsLong();
        try {
            w.close();   // format-owned close completes the part before publication
        } catch (IOException e) {
            // Finalization failed (e.g. fsync error). The part is NOT in the manifest
            // and is therefore not durable; delete the half-written file so it cannot
            // be mistaken for a finalized part and so abort()'s later cleanup isn't
            // needed (the `joined` latch would skip it). Then surface the failure.
            lane.current = null;
            lane.partNodeMaxKey.clear();
            try {
                Files.deleteIfExists(path);
            } catch (IOException cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            try {
                observer.recordPart("finalize_failed");
            } catch (Throwable observationFailure) {
                e.addSuppressed(observationFailure);
            }
            throw e;
        } finally {
            lane.finalizeCount++;
            lane.finalizeElapsedNanos += Math.max(0L, nanoClock.getAsLong() - finalizeStartedAt);
        }
        if (finalizeSample != null) {
            observer.recordFinalize(finalizeSample);
        }
        lane.current = null;
        long bytes = Files.size(path);
        // Canonical relative form: data/<filename>, shared verbatim by the consumer
        // manifest files[].key, the checkpoint part_file.path (via the event's fileName), and the
        // resume disk-sweep. MD5 is computed ONCE here, at finalize — never on every
        // manifest rewrite (which would be O(n²)).
        String relPath = DatasetLayout.key(path.getFileName().toString());
        String md5;
        long digestStartedAt = nanoClock.getAsLong();
        try (var in = Files.newInputStream(path)) {
            md5 = DigestUtils.md5Hex(in);
        } finally {
            partDigestCount.incrementAndGet();
            partDigestNanos.addAndGet(Math.max(0L, nanoClock.getAsLong() - digestStartedAt));
        }
        // When the sink wires a durable listener, its checkpoint commit (record part + advance
        // durable_cursor) is the exactly-once boundary BEFORE the manifest. Text wires NONE.
        Map<Long, byte[]> contributions = Map.copyOf(lane.partNodeMaxKey);
        lane.partNodeMaxKey.clear();
        partListener.onFinalized(new PartListener.PartFinalizedEvent(
                lane.id, relPath, rows, bytes, contributions));
        committedParts.add(new PartInfo(relPath, lane.id, rows, bytes, md5));
        observer.recordPart("finalized");
        lane.finalizedBytes += bytes;
        lane.partsFinalized++;
        // A part finalize (footer fsync + manifest commit) is real forward progress that no
        // LISTING page/object counter reflects — a large multi-part non-sort finalize tail
        // (close()'s drainAndJoin() below) can otherwise sit with progressSignal() frozen for
        // longer than the stall window and get falsely aborted (worse: it could force the STUCK
        // exit 75 over what should have been a clean --max-duration exit 0).
        observer.markProgress();
        writeManifest();   // atomic, on each finalize
    }

    private void discardCurrent(Lane lane) throws IOException {
        if (lane.current == null) {
            return;
        }
        Path path = lane.current.path();
        IOException failure = null;
        try {
            lane.current.discard();   // release the handle WITHOUT a durable footer-fsync (I6/RES-4)
        } catch (IOException e) {
            failure = e;
        }
        lane.current = null;
        try {
            observer.recordPart("discarded");
        } catch (Throwable ignored) {
            // observation only — cleanup must still finish
        }
        lane.partNodeMaxKey.clear();
        try {
            Files.deleteIfExists(path);
        } catch (IOException cleanupFailure) {
            if (failure == null) {
                failure = cleanupFailure;
            } else {
                failure.addSuppressed(cleanupFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void writeManifest() throws IOException {
        long startedAt = nanoClock.getAsLong();
        try {
            synchronized (manifestLock) {
                // Snapshot under the publication lock. Otherwise lane A can snapshot an older
                // part set, lane B can publish a newer set, and A can then overwrite it after
                // finally acquiring this lock.
                writeManifestLocked(snapshotParts());
            }
        } finally {
            recordManifestWrite(startedAt);
        }
    }

    /** Includes time queued behind another lane on {@link #manifestLock}. */
    private void writeManifest(List<PartInfo> parts) throws IOException {
        long startedAt = nanoClock.getAsLong();
        try {
            synchronized (manifestLock) {
                writeManifestLocked(parts);
            }
        } finally {
            recordManifestWrite(startedAt);
        }
    }

    private void writeManifestLocked(List<PartInfo> parts) throws IOException {
        Manifest.write(dir, bucket, format.manifestFormat(), format.manifestSchema(),
                parts, false, null);
    }

    private void recordManifestWrite(long startedAt) {
        manifestWriteCount.incrementAndGet();
        manifestWriteNanos.addAndGet(Math.max(0L, nanoClock.getAsLong() - startedAt));
    }

    private List<PartInfo> snapshotParts() {
        synchronized (committedParts) {
            return new ArrayList<>(committedParts);
        }
    }

    /**
     * Graceful completion: finalize every lane's open part, commit the consumer manifest, then write
     * the final-commit artifacts: {@code .swath-state.json} (with a null run id — the
     * pool has none), {@code symlink.txt}, and finally the empty {@code _SUCCESS} marker LAST.
     */
    @Override
    public void close() throws OutputException {
        beginShutdown(ShutdownMode.CLOSE);
        drainAndJoin();
        if (shutdownMode.get() == ShutdownMode.ABORT) {
            throw new OutputException(sinkName + " writer pool was aborted");
        }
        checkFailure();
        publishSuccess();
    }

    /** Failure path: discard open (non-finalized) parts; finalized parts + their manifest remain. */
    public void abort() {
        beginShutdown(ShutdownMode.ABORT);
        try {
            drainAndJoin();
        } catch (OutputException ignored) {
            // already failing — surface the original error, not the shutdown
        }
    }

    /** Close admission atomically; the first close/abort request owns the terminal mode. */
    private void beginShutdown(ShutdownMode requested) {
        boolean changed;
        lifecycleLock.writeLock().lock();
        try {
            changed = shutdownMode.compareAndSet(ShutdownMode.OPEN, requested);
        } finally {
            lifecycleLock.writeLock().unlock();
        }
        if (changed) {
            // A submitter never holds lifecycleLock while waiting for a saturated lane: that
            // would stop this transition from starting. Do not take a lane admission lock until
            // AFTER releasing the lifecycle write lock — submitters take them in the opposite
            // order around their guarded offer. Signal after OPEN has been revoked so a waiter
            // retries its guarded offer, observes the terminal mode, and cannot enqueue behind
            // the poison sentinel that drainAndJoin() will install.
            for (Lane lane : lanes) {
                lane.signalAdmissionWaiters();
            }
        }
    }

    /** Publish final metadata once; idempotent concurrent {@link #close()} callers await it. */
    private void publishSuccess() throws OutputException {
        if (publicationInitiated.compareAndSet(false, true)) {
            try {
                List<PartInfo> snapshot = snapshotParts();
                writeManifest(snapshot);   // ensure a manifest exists
                Manifest.writeState(dir, argsHash, null);
                Manifest.writeSymlink(dir, snapshot);
                Manifest.writeSuccess(dir);   // LAST — the whole-snapshot completion marker
                published.complete(null);
            } catch (Throwable t) {
                published.completeExceptionally(t);
            }
        }
        try {
            published.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OutputException("interrupted publishing " + sinkName + " dataset", e);
        } catch (ExecutionException e) {
            throw new OutputException("failed to write manifest", e.getCause());
        }
    }

    private void drainAndJoin() throws OutputException {
        if (joinInitiated.compareAndSet(false, true)) {
            Throwable terminal = null;
            try {
                for (Lane lane : lanes) {
                    lane.enqueuePoison();
                }
                for (Future<?> f : laneFutures) {
                    f.get();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                terminal = new OutputException(
                        "interrupted closing " + sinkName + " writer pool", e);
            } catch (ExecutionException e) {
                terminal = new OutputException(sinkName + " writer failed", e.getCause());
            } catch (Throwable t) {
                terminal = t;
            } finally {
                if (terminal != null) {
                    // Coordination did not complete gracefully. scope.close() interrupts any lane
                    // still blocked on its queue, so publishable CLOSE semantics are no longer
                    // possible: every open part must take the abort/discard path in runLane's
                    // finally block instead of trying to finalize under interruption.
                    shutdownMode.set(ShutdownMode.ABORT);
                }
                try {
                    scope.close();
                } catch (Throwable closeFailure) {
                    if (terminal == null) {
                        terminal = closeFailure;
                    } else {
                        terminal.addSuppressed(closeFailure);
                    }
                }
                Throwable laneFailure = failure.get();
                if (laneFailure != null && laneFailure != terminal
                        && (terminal == null || terminal.getCause() != laneFailure)) {
                    if (terminal == null) {
                        terminal = laneFailure;
                    } else {
                        terminal.addSuppressed(laneFailure);
                    }
                }
                if (terminal == null) {
                    joined.complete(null);
                } else {
                    joined.completeExceptionally(terminal);
                }
            }
        }
        try {
            joined.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OutputException("interrupted closing " + sinkName + " writer pool", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof OutputException output) {
                throw output;
            }
            throw new OutputException(sinkName + " writer failed", cause);
        }
    }

    private final class Lane {
        final int id;
        final int queueCapacity;
        private final BlockingQueue<PageBatch> queue;
        // Serializes admission to this queue. A full submitter waits on notFullOrShutdown
        // without holding lifecycleLock; dequeue and shutdown both signal it to retry the
        // lifecycle-guarded offer. Keeping the two locks distinct is what lets abort begin even
        // when the lane's writer is wedged.
        final ReentrantLock admissionLock = new ReentrantLock();
        final Condition notFullOrShutdown = admissionLock.newCondition();
        final AtomicInteger queueDepthPeak = new AtomicInteger();
        volatile boolean waitingForWork;
        // The lane thread is the sole writer for service/finalize counters; volatile makes
        // periodic summary reads visible without paying for atomic RMW on every encoded batch.
        volatile long rowsWritten;
        volatile long finalizedBytes;
        volatile long batchesWritten;
        volatile long activeElapsedNanos;
        final Object submitStatisticsLock = new Object();
        long submitBlockedCount;
        long submitBlockedNanos;
        long inFlightSubmitBlockedCount;
        long inFlightSubmitStartedAtSum;
        long headOfLineBlockedCount;
        long headOfLineBlockedNanos;
        long inFlightHeadOfLineCount;
        long inFlightHeadOfLineStartedAtSum;
        volatile long partsFinalized;
        volatile long finalizeCount;
        volatile long finalizeElapsedNanos;
        // Per node id, the highest key written to the open part — flushed to the
        // listener (durable_cursor advance) when the part finalizes; cleared per part.
        final Map<Long, byte[]> partNodeMaxKey = new HashMap<>();
        DatasetPartWriter current;
        int seq;
        // When the open part was created, on the injected clock — the time-trigger baseline.
        long partOpenedAtNanos;

        Lane(int id, int queueCapacity, BlockingQueue<PageBatch> queue) {
            this.id = id;
            this.queueCapacity = queueCapacity;
            this.queue = queue;
        }

        /**
         * Enqueue only while the lifecycle read lock proves admission remains open. On a full
         * queue, wait on the lane-local condition instead of retaining that read lock: shutdown
         * can then revoke OPEN, wake us, and install poison without waiting for the writer.
         */
        void admit(PageBatch batch) throws OutputException, InterruptedException {
            admissionLock.lockInterruptibly();
            boolean blocked = false;
            boolean headOfLine = false;
            long startedAt = 0L;
            try {
                while (true) {
                    lifecycleLock.readLock().lockInterruptibly();
                    try {
                        if (shutdownMode.get() != ShutdownMode.OPEN) {
                            throw new OutputException(sinkName + " writer pool is shutting down");
                        }
                        checkFailure();
                        if (queue.offer(batch)) {
                            return;
                        }
                    } finally {
                        lifecycleLock.readLock().unlock();
                    }
                    if (!blocked) {
                        // Pay for the clock and bounded cross-lane scan only on the saturated path. This is the
                        // missing middle link between consumer-side emit and worker-side channel wait.
                        queueDepthPeak.getAndAccumulate(queueCapacity, Math::max);
                        headOfLine = anotherLaneIsWaitingForWork(id);
                        startedAt = nanoClock.getAsLong();
                        startSubmitBlock(startedAt, headOfLine);
                        blocked = true;
                    }
                    notFullOrShutdown.await();
                }
            } finally {
                if (blocked) {
                    long blockedNanos = Math.max(0L, nanoClock.getAsLong() - startedAt);
                    finishSubmitBlock(startedAt, blockedNanos, headOfLine);
                }
                admissionLock.unlock();
            }
        }

        /** Notify one or more saturated submitters after space becomes available or OPEN closes. */
        void signalAdmissionWaiters() {
            admissionLock.lock();
            try {
                notFullOrShutdown.signalAll();
            } finally {
                admissionLock.unlock();
            }
        }

        /** Remove work and wake a potentially blocked submitter after every successful dequeue. */
        PageBatch dequeueNow() {
            PageBatch batch = queue.poll();
            if (batch != null) {
                signalAdmissionWaiters();
            }
            return batch;
        }

        PageBatch dequeueWithin(long timeout, TimeUnit unit) throws InterruptedException {
            PageBatch batch = queue.poll(timeout, unit);
            if (batch != null) {
                signalAdmissionWaiters();
            }
            return batch;
        }

        PageBatch dequeue() throws InterruptedException {
            PageBatch batch = queue.take();
            signalAdmissionWaiters();
            return batch;
        }

        /** Shutdown has already revoked admission, so poison is the one permitted terminal enqueue. */
        void enqueuePoison() throws InterruptedException {
            queue.put(POISON);
        }

        boolean isEmpty() {
            return queue.isEmpty();
        }

        int queueDepth() {
            return queue.size();
        }

        void startSubmitBlock(long startedAtNanos, boolean headOfLine) {
            synchronized (submitStatisticsLock) {
                submitBlockedCount++;
                inFlightSubmitBlockedCount++;
                inFlightSubmitStartedAtSum += startedAtNanos;
                if (headOfLine) {
                    headOfLineBlockedCount++;
                    inFlightHeadOfLineCount++;
                    inFlightHeadOfLineStartedAtSum += startedAtNanos;
                }
            }
        }

        void finishSubmitBlock(long startedAtNanos, long blockedNanos, boolean headOfLine) {
            synchronized (submitStatisticsLock) {
                inFlightSubmitBlockedCount--;
                inFlightSubmitStartedAtSum -= startedAtNanos;
                submitBlockedNanos += blockedNanos;
                if (headOfLine) {
                    inFlightHeadOfLineCount--;
                    inFlightHeadOfLineStartedAtSum -= startedAtNanos;
                    headOfLineBlockedNanos += blockedNanos;
                }
            }
        }

        LaneStatistics statistics() {
            // Sampling happens on the periodic/terminal reporter, never on the sole dispatcher.
            // A saturated admission records capacity exactly in submit(); otherwise this is a
            // best-effort high-water mark at report cadence.
            int queueDepth = queueDepth();
            queueDepthPeak.getAndAccumulate(queueDepth, Math::max);
            long blockedCount;
            long blockedNanos;
            long headOfLineCount;
            long headOfLineNanos;
            synchronized (submitStatisticsLock) {
                long now = inFlightSubmitBlockedCount == 0 ? 0L : nanoClock.getAsLong();
                blockedCount = submitBlockedCount;
                blockedNanos = submitBlockedNanos + inFlightElapsed(
                        now, inFlightSubmitBlockedCount, inFlightSubmitStartedAtSum);
                headOfLineCount = headOfLineBlockedCount;
                headOfLineNanos = headOfLineBlockedNanos + inFlightElapsed(
                        now, inFlightHeadOfLineCount, inFlightHeadOfLineStartedAtSum);
            }
            return new LaneStatistics(id, queueCapacity, queueDepth, queueDepthPeak.get(),
                    waitingForWork, rowsWritten, finalizedBytes, batchesWritten,
                    activeElapsedNanos, blockedCount, blockedNanos,
                    headOfLineCount, headOfLineNanos,
                    partsFinalized, finalizeCount, finalizeElapsedNanos);
        }

        private long inFlightElapsed(long now, long count, long startedAtSum) {
            return count == 0 ? 0L : Math.max(0L, now * count - startedAtSum);
        }

        void openPart() throws IOException {
            Path dataDir = layout.dataDir();   // parts live under <root>/data/
            Files.createDirectories(dataDir);
            Path path = dataDir.resolve(String.format("part-w%d-%05d%s", id, seq++, format.partSuffix()));
            current = format.openPart(path);
            partOpenedAtNanos = nanoClock.getAsLong();
        }
    }

}
