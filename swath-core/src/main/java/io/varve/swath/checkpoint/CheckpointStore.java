/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

import io.varve.swath.error.CheckpointException;
import io.varve.swath.error.InvalidArgsException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * The durable worklist + resume substrate (§3, algorithms.md §4). The
 * worklist <b>is</b> the {@code listing_node} table. All writes funnel
 * through ONE checkpoint-writer thread (SQLite WAL is single-writer), so
 * {@link #commitPage} and {@link #splitNode} are serialized against each other.
 *
 * <p>Pluggable: {@link SqliteCheckpointStore} now, a DynamoDb store
 * later for the multi-host fleet.
 */
public interface CheckpointStore extends AutoCloseable {

    /** {@link #splitNode} sentinel: the CAS guard rejected the split (rowcount 0). */
    long SPLIT_ABORTED = -1L;

    /**
     * Open (create or match-by-{@code args_hash}) a run.
     *
     * @param resume  resume an existing incomplete run (the {@code args_hash} must match)
     * @param restart discard any prior state for this key and start fresh
     * @throws InvalidArgsException on {@code --resume} with no prior run or a mismatched
     *                              {@code args_hash} (exit 2; suggest {@code --restart})
     */
    RunMeta openRun(RunKey key, boolean resume, boolean restart) throws CheckpointException, InvalidArgsException;

    /**
     * The directory-lifecycle-aware open. {@code overwrite} additionally permits discarding an
     * existing <b>completed</b> run for this key ({@code --overwrite}). A plain run (all three flags
     * false) whose key already has a run is refused: an unfinished one steers to {@code swath
     * resume}/{@code --restart}, a completed one to {@code --overwrite} (exit 2 via
     * {@link InvalidArgsException}).
     *
     * <p>Default (for the in-memory/test doubles that never gate on lifecycle) ignores
     * {@code overwrite} and delegates to {@link #openRun(RunKey, boolean, boolean)};
     * {@link SqliteCheckpointStore} overrides it with the real status gate. The idiom matches this
     * interface's other capability defaults ({@link #insertNodes}, {@link #markRunFatalUnlessFinished}).
     */
    default RunMeta openRun(RunKey key, boolean resume, boolean restart, boolean overwrite)
            throws CheckpointException, InvalidArgsException {
        return openRun(key, resume, restart);
    }

    /** Insert a worklist node; returns its id. */
    long insertNode(NodeSpec spec) throws CheckpointException;

    /**
     * Insert all seed nodes for a fresh run in ONE atomic transaction (all-or-nothing).
     * A crash / rollback before the single commit leaves ZERO nodes in the DB — no
     * partial partition, no silent gap (I2). Callers MUST use this (not a loop over
     * {@link #insertNode}) when seeding so the range set either exists complete or not
     * at all. Returns the generated ids in insertion order.
     *
     * <p>The default delegates to {@link #insertNode} in a loop (not atomic — for
     * store implementations without a batch primitive). {@link SqliteCheckpointStore}
     * overrides this with a single-writer-thread task so all inserts share one
     * {@code conn.commit()}.
     */
    default List<Long> insertNodes(List<NodeSpec> specs) throws CheckpointException {
        List<Long> ids = new ArrayList<>(specs.size());
        for (NodeSpec spec : specs) {
            ids.add(insertNode(spec));
        }
        return ids;
    }

    /**
     * Total worklist nodes for this run, in ANY state ({@code SELECT COUNT(*) FROM
     * listing_node WHERE run_id=?}). The CLI resume/re-seed gate uses it to tell a
     * <b>never-seeded</b> run apart from a genuinely <b>completed</b> one: a fresh-run
     * seed that aborted STUCK before {@link #insertNodes} committed leaves EXACTLY zero
     * nodes (I2, all-or-nothing), whereas a completed run has nodes (all {@code
     * COMPLETED}). A zero count on resume ⇒ re-seed; a completed run's nodes are never
     * mistaken for "nothing left to do".
     *
     * <p>The default throws: a store that backs a real resumable run (the SQLite
     * checkpoint) MUST implement it; the in-memory engine-test doubles that never drive
     * the CLI resume gate need not.
     */
    default long countNodes(long runId) throws CheckpointException {
        throw new UnsupportedOperationException(
                "countNodes is not implemented by " + getClass().getName());
    }

    /**
     * Prepare + load the resumable nodes (algorithms.md §4.6): every non-COMPLETED
     * node is reverted {@code IN_PROGRESS → PENDING} <b>preserving {@code cursor}</b>
     * (I5), clearing {@code owner_lease}, bumping {@code generation}. For
     * {@code fileSink} runs (exactly-once), each reopened node's {@code cursor} is
     * additionally reset to its {@code durable_cursor} (§4.5) and any COMPLETED-but-
     * not-output-complete node ({@code durable_cursor < cursor}) is reopened too.
     */
    List<Node> loadResumable(long runId, boolean fileSink) throws CheckpointException;

    /** I1 commit-before-emit (algorithms.md §4.2): advance {@code cursor} + {@code status} in one txn. */
    void commitPage(PageCommit c) throws CheckpointException;

    /**
     * I1 commit-before-emit, async seam (algorithms.md §4.1): enqueue the same per-page
     * commit and return a future that completes when the txn is <b>durably committed</b>,
     * <b>without</b> blocking the caller. The {@code WorkStealingScan} worker advances its
     * in-memory cursor under the worker lock, <b>releases the lock</b>, then awaits this
     * future <b>outside</b> the lock before emitting the page — the durability
     * await must not hold the worker lock, else commits serialize / deadlock. The
     * synchronous {@link #commitPage} is this future {@code .join()}ed.
     */
    CompletableFuture<Void> commitPageAsync(PageCommit c) throws CheckpointException;

    /**
     * I4 split (algorithms.md §4.3): narrow the victim + insert the child in one
     * CAS-guarded transaction. Returns the child id, or {@link #SPLIT_ABORTED} when
     * the guard rejected it (victim advanced past the pivot, the bound moved, or the
     * victim already completed) — the thief then restores {@code hi} and retries.
     */
    long splitNode(SplitSpec s) throws CheckpointException;

    /** I6 (algorithms.md §4.5): record a finalized part + advance {@code durable_cursor}, one txn. */
    void partFinalized(PartFinalize f) throws CheckpointException;

    /** All finalized parts for a run (resume: retain these for completion publication, discard the rest). */
    List<PartRef> finalizedParts(long runId) throws CheckpointException;

    /**
     * Resume format-mismatch guard: the distinct {@code format} tags of this run's
     * FINALIZED {@code part_file} rows, in insertion order. A resuming {@code --sort} run whose
     * staging carries a format other than the current {@code SORT_SEGMENT_FORMAT} ("page-run")
     * — e.g. an older "parquet-segment" row — must refuse cleanly, because the reattach path
     * selects segments by the current format and would otherwise sweep the un-recognized
     * (old-format) finalized segments as "non-finalized" and re-list their data (dup/loss).
     * Default derives from {@link #finalizedParts}; every store gets it for free.
     */
    default Set<String> finalizedPartFormats(long runId) throws CheckpointException {
        return finalizedParts(runId).stream().map(PartRef::format)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * I6 output-complete latch (algorithms.md §4.5): set {@code durable_cursor := cursor}
     * for every COMPLETED node of the run. Call <b>only</b> after a clean
     * {@code pool.close()} has finalized every open part — at that point all of a
     * COMPLETED node's kept rows are durable, so the node is output-complete even
     * when its {@code cursor} advanced past the last kept row (trailing pages whose
     * rows were entirely filtered out, or an all-filtered/empty result). Without this
     * latch such a node ends {@code durable_cursor < cursor} and a later
     * {@code loadResumable(..., true)} would reopen an already-finished node.
     */
    void markOutputComplete(long runId) throws CheckpointException;

    /**
     * The {@code --sort} run's current phase, or {@code null} for a
     * non-sort run. Read on resume to dispatch: {@code MERGING} re-runs the merge, {@code PUBLISHED}
     * means the final output is present. The default throws — a store that a {@code --sort} run
     * drives MUST implement it ({@link SqliteCheckpointStore} does); non-sort test fakes inherit the
     * throw harmlessly (they are never asked).
     */
    default SortPhase sortPhase(long runId) throws CheckpointException {
        throw new UnsupportedOperationException("checkpoint store does not support --sort phases");
    }

    /** Advance the {@code --sort} phase (LISTING → MERGING → PUBLISHED), one txn. */
    default void setSortPhase(long runId, SortPhase phase) throws CheckpointException {
        throw new UnsupportedOperationException("checkpoint store does not support --sort phases");
    }

    /** Mark the run's terminal status. */
    void markRunFinished(long runId, RunStatus status) throws CheckpointException;

    /**
     * Mark a run FAILED with the fatal-in-process-error flag set ({@code run_meta.fatal_error})
     * — but ONLY while it has not finished (compare-and-set on {@code RUNNING}). This is the CLI
     * fatal-error guard's mark call for a generic unrecoverable failure ({@code
     * ListCommand#runEngineGuarded}, the seed-probe catch), and the CAS is what keeps it from
     * overruling a disposition the run already chose for itself: neither an already-durable
     * {@code COMPLETED} row (a failure in a post-completion step, already inside the guarded
     * region) nor a {@code FAILED} row deliberately left flag-unset — a broken-pipe truncation,
     * or the publish failure {@code ListRunner} records before rethrowing into this very guard —
     * may be rewritten, so a run whose output write hit a full disk stays resumable instead of
     * forcing a whole-bucket {@code --restart}.
     *
     * <p>Deliberately a DIFFERENT call than the broken-pipe path's plain {@link #markRunFinished}
     * (FAILED, flag left unset) — a broken-pipe truncation stays normally resumable (INT-12); only
     * a row a fatal mark flagged is refused on {@code --resume}. For the failure that must never be
     * retried at all, see {@link #markRunUnresumable}.
     *
     * <p>The default delegates to an unconditional {@link #markRunFinished} for stores that
     * don't yet support the fatal-flag distinction (test fakes never asked to honor it);
     * {@link SqliteCheckpointStore} overrides it with the real CAS + column.
     */
    default void markRunFatalUnlessFinished(long runId) throws CheckpointException {
        markRunFinished(runId, RunStatus.FAILED);
    }

    /**
     * Mark a run FAILED and fatal-flagged from every status a resume can be admitted from —
     * {@code RUNNING}, and the flag-unset {@code FAILED} a resumed run keeps for its whole second
     * attempt ({@code openRun(..., resume=true)} admits such a row WITHOUT moving it back to
     * {@code RUNNING}) — so no later {@code swath resume} is admitted. This is the mark for a
     * failure that must not be retried at all, a protocol violation ({@code
     * ListCommand#runEngineGuarded}): unlike {@link #markRunFatalUnlessFinished} it deliberately
     * overrides a resumable FAILED, because a resume admitted from that row would walk straight
     * back into the endpoint that violated the protocol. A durable {@code COMPLETED} row is still
     * never downgraded — nothing resumes into a completed run.
     *
     * <p>Defaults and store support as for {@link #markRunFatalUnlessFinished}.
     */
    default void markRunUnresumable(long runId) throws CheckpointException {
        markRunFinished(runId, RunStatus.FAILED);
    }

    @Override
    void close() throws CheckpointException;
}
