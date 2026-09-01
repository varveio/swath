/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.stage;

import io.varve.swath.concurrent.Scope;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.PackedPage;
import io.varve.swath.output.sorted.StagingNames;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortLaneMeters;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.spill.PageRunSegmentWriter;
import io.varve.swath.sort.spill.SealTrigger;
import io.varve.swath.sort.spill.SealedBuffer;
import io.varve.swath.sort.spill.SegmentResult;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The single, ordered sort lane. Admitted {@link io.varve.swath.model.PageBatch}
 * pages fill a {@link SortBuffer}; when a gate fires ({@link SortConfig#segmentBytes()} /
 * {@link SortConfig#segmentEntries()}) the buffer is sealed and handed to <b>one</b> off-thread
 * encoder that flushes it as an internally-sorted staging segment. Double-buffered: the drain thread
 * fills buffer B while the encoder writes sealed buffer A.
 *
 * <p><b>Finalize strictly in seal order</b>: the encoder is a <b>single</b>
 * thread draining a FIFO handoff queue, so segments finalize — and {@code durable_cursor} advances,
 * via {@link SegmentSink} — in the exact order buffers were sealed. Parallel encoders are forbidden:
 * an out-of-order finalize would let {@code durable_cursor} over-advance past keys still sitting in an
 * earlier unfinalized buffer and silently lose them on resume.
 *
 * <p><b>Live-buffer invariant: at most {@link SortConfig#buffers()} sealed
 * buffers are ever live at once</b> — the buffer being filled, plus at most {@code buffers() - 1}
 * "off-thread" buffers (queued and/or actively encoding). This is enforced by a counting
 * {@link Semaphore} sized {@code buffers() - 1} that a seal acquires <b>before</b> handing off and
 * releases only after that segment is fully encoded (not merely dequeued) — a plain bounded
 * {@link BlockingQueue} undercounts, because its consumer removes an item from the queue the instant
 * {@code take()} runs, before that item's encode work is done, letting a second buffer queue up
 * alongside the one still being encoded (buffers+1 live, violating the corridor).
 *
 * <p><b>Backpressure, not extra lanes.</b> When the encoder falls behind, {@link #admit} blocks
 * acquiring the next off-thread slot — an accepted, measured trade
 * ({@link SortLaneMeters#backpressureWaited}). "Never blocks" is not promised, and adding sort lanes
 * to keep up is not a permitted fallback. The wait is a bounded-timeout poll (not a blind, unbounded
 * block): each iteration re-checks {@link Scope#rethrowFirstFailure()} so a dead encoder (disk full,
 * fsync error) surfaces the drain thread's failure promptly instead of hanging forever, and re-checks
 * an in-flight {@link #abort()} so a concurrently-aborting lane also unblocks (dropping the buffer).
 *
 * <p>Not thread-safe for {@link #admit}: exactly one drain thread calls {@link #admit} then
 * {@link #close()} (clean) or {@link #abort()} (failure/cancel), mirroring
 * {@code ParquetWriterPool}'s lifecycle. {@link #abort()} itself IS safe to call concurrently with a
 * blocked {@link #admit}/seal (e.g. a cancellation signal on another thread).
 */
public final class SortLane implements AutoCloseable {

    /** Handoff sentinel telling the encoder loop to stop after draining the queue. */
    private static final Object POISON = new Object();

    /** Poll granularity for the bounded-timeout capacity wait: bounds failure/abort detection latency. */
    private static final long POLL_MS = 25;

    private final Comparator<ListEntry> comparator;
    private final DuplicateHook hook;
    private final SortMetrics metrics;
    private final SortLaneMeters laneMeters;
    private final PageRunSegmentWriter segmentWriter;
    private final SegmentSink sink;
    private final Path stagingDir;
    private final String segmentPrefix;

    /** Unbounded — capacity is governed by {@link #offThreadCapacity}, not the queue itself. */
    private final BlockingQueue<Object> handoff = new LinkedBlockingQueue<>();
    private final Scope encoderScope;
    private final Future<?> encoder;
    private final AtomicLong seq = new AtomicLong();
    private final AtomicBoolean aborted = new AtomicBoolean();

    /** Bounds concurrently "off-thread" (queued + actively encoding) sealed buffers to {@code buffers() - 1}. */
    private final Semaphore offThreadSlots;
    private final int offThreadCapacity;
    private final AtomicInteger liveOffThreadBuffers = new AtomicInteger();
    private final AtomicInteger maxLiveOffThreadBuffers = new AtomicInteger();

    /**
     * Total live (admitted-but-not-yet-durable) staging bytes — the fill buffer's current
     * estimate plus every sealed-but-unfinalized buffer's estimate (captured at seal time,
     * {@link SealedBuffer#estimatedBytes()}). Incremented in {@link #admit} as pages accumulate,
     * decremented in {@link #encodeLoop} only once a segment is fully encoded (success or failure) —
     * mirrors the memory-corridor lifecycle {@link #liveOffThreadBuffers} already tracks. Instrumentation
     * only: reported to {@link #laneMeters} for the pipeline's peak/gauge bookkeeping; never gates
     * admission or backpressure itself.
     */
    private final AtomicLong liveStagingBytes = new AtomicLong();

    /**
     * The encoder's failure, recorded by {@link #encodeLoop} BEFORE it releases the failed segment's
     * off-thread permit. {@link Semaphore#release()}/{@link Semaphore#tryAcquire}
     * establish a happens-before edge (documented {@link Semaphore} memory-consistency guarantee), so
     * any thread that successfully acquires a permit freed by a failing encode is guaranteed to
     * observe this field already set — closing the window a plain {@link Scope#rethrowFirstFailure()}
     * recheck alone would leave open (the encoder records into {@code Scope}'s own failure field only
     * AFTER {@code encodeLoop} returns, i.e. strictly after the permit release in its {@code finally}).
     */
    private final AtomicReference<Throwable> encoderFailure = new AtomicReference<>();

    /** Test-only seam (package-private): runs on the encoder thread just before each segment's flush. */
    volatile Runnable beforeEncodeHookForTesting = () -> { };

    private SortBuffer fill;

    public SortLane(SortConfig config, Comparator<ListEntry> comparator, DuplicateHook hook,
                    SortMetrics metrics, SortLaneMeters laneMeters, Path stagingDir,
                    String segmentPrefix, SegmentSink sink) {
        this(config, comparator, hook, metrics, laneMeters, stagingDir, segmentPrefix, sink,
                SortMode.OBJECTS);
    }

    public SortLane(SortConfig config, Comparator<ListEntry> comparator, DuplicateHook hook,
                    SortMetrics metrics, SortLaneMeters laneMeters, Path stagingDir,
                    String segmentPrefix, SegmentSink sink, SortMode orderingMode) {
        this.comparator = comparator;
        this.hook = hook;
        this.metrics = metrics;
        this.laneMeters = laneMeters;
        this.segmentWriter = new PageRunSegmentWriter(
                comparator, hook, metrics, config.segmentCodec(), orderingMode);
        this.sink = sink;
        this.stagingDir = stagingDir;
        this.segmentPrefix = segmentPrefix;
        this.fill = new SortBuffer(config, comparator);
        // buffers=2 (default) ⇒ 1 off-thread slot: one buffer filling, at most one off-thread
        // (queued or encoding), so live sealed buffers never exceed buffers(). SortConfig
        // rejects buffers<2, so this is always >= 1 — never 0, which would
        // deadlock every seal forever (no permit could ever be acquired).
        this.offThreadCapacity = config.buffers() - 1;
        this.offThreadSlots = new Semaphore(offThreadCapacity);
        this.encoderScope = Scope.ofPlatformThreads(segmentPrefix + "-encoder");
        this.encoder = encoderScope.fork(this::encodeLoop);
    }

    /**
     * Admit one raw page under its producing node id (fallback path): packs on THIS drain thread, then
     * seals + hands off when a gate fires. The live {@code --sort} path packs upstream on the fetch
     * worker and admits via {@link #admit(long, PackedPage)} instead; this overload survives for any
     * producer that still hands the lane a raw {@link ListEntry} list.
     */
    public void admit(long nodeId, List<ListEntry> page) throws Exception {
        rethrowEncoderFailureIfAny();         // surface an encoder failure promptly
        encoderScope.rethrowFirstFailure();
        if (page.isEmpty()) {
            return;
        }
        long bytesBefore = fill.estimatedBytes();
        fill.admit(nodeId, page);
        afterAdmit(bytesBefore, page.size());
    }

    /**
     * Admit an ALREADY-PACKED page under its producing node id. The
     * {@link PackedPage} was packed on the fetch worker; here it is only GROUPED into the fill buffer (no
     * pack on this drain thread), then sealed + handed off exactly as the raw path — per-node page order,
     * the seal gates, and what gets checkpointed are unchanged, since a fetch worker still emits its
     * claimed range's pages in order and packing earlier does not reorder them.
     */
    public void admit(long nodeId, PackedPage packed) throws Exception {
        rethrowEncoderFailureIfAny();         // surface an encoder failure promptly
        encoderScope.rethrowFirstFailure();
        if (packed.entryCount() == 0) {
            return;   // never happens (empty pages are skipped before send); defensive, mirrors the raw path
        }
        long bytesBefore = fill.estimatedBytes();
        fill.admit(nodeId, ((PackedPageBlock) packed).block());
        afterAdmit(bytesBefore, packed.entryCount());
    }

    /**
     * Shared admit tail (both overloads): update the live-bytes gauge + the entries meter, then seal
     * if a gate fired. {@code admit()} is single-caller (javadoc), so {@code liveStagingBytes} is a plain
     * read-modify-write (instrumentation only, never gates admission).
     */
    private void afterAdmit(long bytesBefore, long entriesAdded) throws Exception {
        long delta = fill.estimatedBytes() - bytesBefore;
        if (delta > 0) {
            laneMeters.stagingBytesLive(liveStagingBytes.addAndGet(delta));
        }
        laneMeters.entriesAccepted(entriesAdded);
        SealTrigger trigger = fill.triggerOrNone();
        if (trigger == SealTrigger.BYTE_GATE || trigger == SealTrigger.ENTRY_CAP) {
            seal(trigger);
        }
    }

    /**
     * Seal the fill buffer and hand it to the encoder. Blocks (bounded-timeout poll) until an
     * off-thread slot is free; a dead encoder or a concurrent {@link #abort()} unblocks this within
     * one {@link #POLL_MS} interval instead of hanging forever.
     */
    private void seal(SealTrigger trigger) throws InterruptedException, SwathException {
        SealedBuffer sealed = fill.seal(trigger);
        if (sealed.isEmpty()) {
            return;
        }
        // Engagement counter: a non-blocking snapshot of whether the off-thread bound was
        // already exhausted the instant this seal reached it — cheap (no lock), and distinct from
        // (not a duplicate of) swath.sort.backpressure.wait's latency below; records the engagement,
        // not the wait duration. No new backpressure BEHAVIOR — acquireOffThreadSlotOrDrop's gate is
        // unchanged.
        if (offThreadSlots.availablePermits() == 0) {
            metrics.recordStealReason("SORT", "backpressure_engaged");
        }
        long start = System.nanoTime();
        boolean acquired = acquireOffThreadSlotOrDrop();
        laneMeters.backpressureWaited(System.nanoTime() - start);
        if (!acquired) {
            return;   // aborted while waiting: drop the in-flight buffer (re-listed on resume)
        }
        int live = liveOffThreadBuffers.incrementAndGet();
        maxLiveOffThreadBuffers.updateAndGet(prev -> Math.max(prev, live));
        laneMeters.offThreadBuffersLive(live);
        handoff.put(sealed);   // unbounded: never blocks — capacity was already gated above
        laneMeters.handoffQueueDepth(handoff.size());
    }

    /**
     * Bounded-timeout acquire loop: {@code tryAcquire} with a short timeout, then
     * re-check for an encoder failure or an in-flight {@link #abort()} before looping again — so a
     * dead/aborting encoder is detected within {@link #POLL_MS}, never leaving the caller blocked
     * indefinitely on a permit nobody will ever release. Returns {@code false} only when
     * {@link #abort()} fired while waiting (drop the buffer); throws the encoder's failure when the
     * encoder died — including immediately after a successful acquire (the {@link #encoderFailure}
     * check right after {@code tryAcquire} succeeds), since a freed permit may be exactly the one a
     * dying encode just released.
     */
    private boolean acquireOffThreadSlotOrDrop() throws InterruptedException, SwathException {
        while (true) {
            if (aborted.get()) {
                return false;
            }
            if (offThreadSlots.tryAcquire(POLL_MS, TimeUnit.MILLISECONDS)) {
                rethrowEncoderFailureIfAny();   // guaranteed visible: see encoderFailure's javadoc
                return true;
            }
            rethrowEncoderFailureIfAny();
            encoderScope.rethrowFirstFailure();   // defense in depth (e.g. a scope-level failure with
                                                   // no permit ever having been taken)
        }
    }

    private void rethrowEncoderFailureIfAny() throws SwathException, InterruptedException {
        Throwable t = encoderFailure.get();
        if (t == null) {
            return;
        }
        switch (t) {
            case SwathException e -> throw e;
            case InterruptedException e -> throw e;
            case RuntimeException e -> throw e;
            case Error e -> throw e;
            default -> throw new RuntimeException(t);
        }
    }

    /** The encoder loop: FIFO drain of sealed buffers → one sorted staging segment each, in seal order. */
    private void encodeLoop() throws Exception {
        while (true) {
            Object next = handoff.take();
            if (next == POISON) {
                return;
            }
            SealedBuffer sealed = (SealedBuffer) next;
            try {
                beforeEncodeHookForTesting.run();
                int pageRuns = sealed.runCount();
                Path path = stagingDir.resolve(
                        StagingNames.listingSegment(segmentPrefix, seq.getAndIncrement()));
                SegmentResult result = segmentWriter.flush(sealed, path);
                sink.onSegmentFinalized(result);   // commit point: part_file row + durable_cursor advance
                laneMeters.segmentFinalized(result.bytes(), pageRuns);
            } catch (Throwable t) {
                // Record BEFORE releasing the permit (see encoderFailure's javadoc) so a
                // waiter that acquires the freed permit is guaranteed to observe this failure.
                encoderFailure.compareAndSet(null, t);
                throw t;
            } finally {
                // Release only after the buffer's memory is fully reclaimable (encode done, success or
                // failure) — releasing earlier (e.g. on dequeue) is exactly the queue-undercounting
                // failure mode the live-buffer invariant above forbids.
                // liveStagingBytes mirrors this exact lifecycle — the buffer's memory-estimate
                // bytes (captured at seal time, sealed.estimatedBytes()) leave the "live" tally here,
                // not at seal/dequeue, whether the encode succeeded or failed.
                liveOffThreadBuffers.decrementAndGet();
                liveStagingBytes.addAndGet(-sealed.estimatedBytes());
                offThreadSlots.release();
            }
        }
    }

    /**
     * Clean completion: seal the final partial buffer, drain the encoder (all segments finalized +
     * durable), then join. Propagates the first encoder failure. After a successful close every
     * accepted entry is in a durable staging segment.
     */
    @Override
    public void close() throws Exception {
        try {
            if (!fill.isEmpty()) {
                seal(SealTrigger.DRAIN);
            }
            handoff.put(POISON);             // unbounded queue: never blocks, regardless of encoder state
            encoderScope.joinAllOrThrow();   // wait for the encoder to drain + finish
        } finally {
            encoderScope.close();
        }
    }

    /**
     * Failure / cancel: drop the in-flight fill buffer (its rows aren't durable ⇒ re-listed on
     * resume) and stop the encoder now. A segment caught mid-encode is left un-recorded and swept
     * from staging on resume; already-finalized segments survive. Also unblocks any concurrently
     * blocked {@link #admit}/seal — such a call observes {@link #aborted} within one
     * {@link #POLL_MS} interval and drops its buffer instead of waiting on a permit that will never
     * come free once the encoder is torn down.
     */
    public void abort() {
        aborted.set(true);
        encoderScope.close();
    }

    /** Test-only (package-private): the configured off-thread capacity ({@code buffers() - 1}, always {@code >= 1}). */
    int offThreadCapacity() {
        return offThreadCapacity;
    }

    /** Test-only (package-private): the highest observed concurrently-live off-thread buffer count. */
    int maxObservedOffThreadBuffers() {
        return maxLiveOffThreadBuffers.get();
    }
}
