/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

import io.varve.swath.error.CheckpointException;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * The SQLite checkpoint store. The worklist <b>is</b> the
 * {@code listing_node} table.
 *
 * <p><b>Single-writer protocol</b> (algorithms.md §4.1): every operation is a task on one dedicated
 * writer thread holding the only JDBC connection, so {@code commitPage} and {@code splitNode} are
 * strictly serialized (so the split can be a standalone, CAS-guarded transaction) and a caller
 * blocks until its transaction is durably committed (I1 commit-before-emit). That engine — the
 * queue, the batching writer loop, and the failure/close latch — lives in {@link
 * CheckpointWriteQueue}; this class is schema + DAO, submitting {@link CheckpointWriteQueue.SqlOp}
 * tasks to it.
 *
 * <p><b>Naming convention — {@code do*}.</b> A private {@code doXxx(Connection, ...)} is the
 * body of the public {@code xxx(...)} that runs <i>on the writer thread</i>, handed the sole
 * JDBC connection. The prefix is the thread-confinement marker, not a stylistic echo of the
 * public name: it says "already inside a writer task, inside the batch's transaction" — so a
 * {@code do*} must never call {@link CheckpointWriteQueue#submit}/{@link CheckpointWriteQueue#enqueue}
 * (self-deadlock), never assume its work is committed (the {@code conn.commit()} is the engine's,
 * after the whole batch), and may freely be composed into one task with its siblings (as
 * {@link #insertNodes} does for the atomic I2 seed).
 */
public final class SqliteCheckpointStore implements CheckpointStore {

    /**
     * How long SQLite blocks on a locked DB before returning {@code SQLITE_BUSY}. Only a
     * second process (a stray {@code sqlite3}, another swath run on the same checkpoint) can
     * contend for the write lock — within one process the single writer thread serializes
     * everything — so this is a safety net for external contention, not a tuning knob.
     */
    private static final int BUSY_TIMEOUT_MILLIS = 5_000;

    private final CheckpointWriteQueue queue;
    // Optional (null-safe) run metrics — resume-engagement counters and the args-hash refusal.
    // Null in every test that opens the store without a run (the overwhelming majority; only
    // ListCommand's production path wires a live RunMetrics). The queue holds its own reference
    // for commit latency / queue depth+wait.
    private final RunMetrics metrics;

    private SqliteCheckpointStore(Connection conn, int queueCapacity, boolean daemonWriter, RunMetrics metrics) {
        this.metrics = metrics;
        this.queue = new CheckpointWriteQueue(conn, queueCapacity, daemonWriter, metrics);
    }

    /** Open (creating if absent) the checkpoint DB at {@code path}, applying PRAGMAs + DDL. */
    public static SqliteCheckpointStore open(Path path) throws CheckpointException {
        return open(path, CheckpointWriteQueue.DEFAULT_QUEUE_CAPACITY, false, null);
    }

    /** Open with run metrics attached — commit latency/queue depth/resume engagement. */
    public static SqliteCheckpointStore open(Path path, RunMetrics metrics) throws CheckpointException {
        return open(path, CheckpointWriteQueue.DEFAULT_QUEUE_CAPACITY, false, metrics);
    }

    static SqliteCheckpointStore openForTesting(Path path, int queueCapacity) throws CheckpointException {
        return open(path, queueCapacity, true, null);
    }

    /**
     * Open an EPHEMERAL, non-durable checkpoint store backed by an in-process SQLite
     * {@code :memory:} database: {@code --checkpoint none} still drives the
     * {@code WorkStealingScan} engine through this exact {@link CheckpointStore}, just with
     * nothing written to disk — the run is complete-or-nothing in one process lifetime, never
     * resumable. Deliberately reuses this store's real, tested single-writer/CAS/commit
     * machinery rather than a hand-rolled in-memory reimplementation of the same invariants
     * (I1/I2/I4/I5/I6) — {@code jdbc:sqlite::memory:} is the "noop" backing, not new engine logic.
     */
    public static SqliteCheckpointStore openEphemeral(RunMetrics metrics) throws CheckpointException {
        return openConnection("jdbc:sqlite::memory:", "<in-memory, --checkpoint none>",
                CheckpointWriteQueue.DEFAULT_QUEUE_CAPACITY, false, metrics);
    }

    private static SqliteCheckpointStore open(Path path, int queueCapacity, boolean daemonWriter, RunMetrics metrics)
            throws CheckpointException {
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be positive");
        }
        return openConnection("jdbc:sqlite:" + path.toAbsolutePath(), path.toString(),
                queueCapacity, daemonWriter, metrics);
    }

    private static SqliteCheckpointStore openConnection(String jdbcUrl, String errorLabel, int queueCapacity,
                                                        boolean daemonWriter, RunMetrics metrics)
            throws CheckpointException {
        Connection c = null;
        try {
            c = DriverManager.getConnection(jdbcUrl);
            c.setAutoCommit(true);
            try (Statement st = c.createStatement()) {
                // The version gate first, and only the lock-wait PRAGMA (connection state, not file
                // state) ahead of it: the setup PRAGMAs below write to the file — journal_mode=WAL
                // durably rewrites its header — so running them first would mutate a file this open
                // is about to refuse.
                st.execute("PRAGMA busy_timeout=" + BUSY_TIMEOUT_MILLIS);
                boolean fresh = CheckpointSchema.checkVersion(st, errorLabel);
                st.execute("PRAGMA journal_mode=WAL");
                st.execute("PRAGMA synchronous=NORMAL");
                st.execute("PRAGMA foreign_keys=ON");
                // From here on the connection is in explicit-transaction mode, which the writer
                // thread inherits to batch its commits. Creation is the first such transaction:
                // DDL, column backfill and version stamp commit together or not at all, so an
                // interrupted create leaves a file the next open still creates cleanly.
                c.setAutoCommit(false);
                try {
                    CheckpointSchema.apply(st, fresh);
                    c.commit();
                } catch (SQLException e) {
                    rollbackQuietly(c);
                    throw e;
                }
            }
            return new SqliteCheckpointStore(c, queueCapacity, daemonWriter, metrics);
        } catch (SQLException e) {
            closeQuietly(c);
            throw new CheckpointException("failed to open checkpoint store at " + errorLabel, e);
        } catch (CheckpointException e) {
            // A refused version leaves no store to own the connection, so drop it here.
            closeQuietly(c);
            throw e;
        }
    }

    /** Discard a half-applied schema transaction, keeping the failure that caused it as the reported one. */
    private static void rollbackQuietly(Connection c) {
        try {
            c.rollback();
        } catch (SQLException ignored) {
            // Closing the connection discards the transaction anyway; the create failure is the one to report.
        }
    }

    private static void closeQuietly(Connection c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (SQLException ignored) {
            // Nothing useful to do: the open has already failed and its error is the one to report.
        }
    }

    /** Minimal run identity for the {@code swath resume} convenience (reconstruct the {@code list} args). */
    public record RunIdentity(String scheme, String endpoint, String bucket, byte[] prefix,
                              String outputFormat, String filterSpec, RunStatus status, long startedAt) {
    }

    /** Read the most-recent run's identity from {@code path} without starting the writer thread. */
    public static Optional<RunIdentity> readLatestRun(Path path) throws CheckpointException {
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + path.toAbsolutePath());
             PreparedStatement ps = c.prepareStatement(
                     "SELECT store_scheme, endpoint, bucket, prefix, output_format, filter_spec, status, started_at "
                             + "FROM run_meta ORDER BY started_at DESC, id DESC LIMIT 1");
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                return Optional.empty();
            }
            return Optional.of(new RunIdentity(
                    rs.getString(1), rs.getString(2), rs.getString(3), rs.getBytes(4),
                    rs.getString(5), rs.getString(6),
                    parseEnum(RunStatus.class, rs.getString(7), "run_meta.status"), rs.getLong(8)));
        } catch (SQLException e) {
            throw new CheckpointException("failed to read run from " + path, e);
        }
    }

    // ---- writer-engine delegation ---------------------------------------------

    /** Submit an op and block until its transaction durably commits. */
    private <T> T submit(CheckpointWriteQueue.SqlOp op) throws CheckpointException {
        return queue.submit(op);
    }

    CompletableFuture<Object> enqueueForTesting(Function<Connection, Object> op) throws CheckpointException {
        return queue.enqueue(op::apply);
    }

    // ---- openRun: DB reads/writes on the writer thread; resume policy on the caller -----

    /** Snapshot of an existing run row (for the resume/restart policy decision). */
    private record ExistingRun(long id, String argsHash, String filterSpec, String outputFormat,
                               String strategy, String mode, boolean noSignRequest, String profile,
                               String region, boolean fetchOwner, boolean rawOutput, String outputPath,
                               boolean sortEnabled, RunStatus status, boolean fatalError,
                               boolean requestPayer, String destinationKind, String outputType,
                               String identitySpec) {
    }

    @Override
    public RunMeta openRun(RunKey key, boolean resume, boolean restart)
            throws CheckpointException, InvalidArgsException {
        return openRun(key, resume, restart, false);
    }

    @Override
    public RunMeta openRun(RunKey key, boolean resume, boolean restart, boolean overwrite)
            throws CheckpointException, InvalidArgsException {
        ExistingRun existing = submit(c -> selectExistingRun(c, key));

        if (resume) {
            if (existing == null) {
                throw new InvalidArgsException("--resume: no checkpointed run for "
                        + key.scheme() + "://" + key.bucket() + "/" + prefixText(key.prefix())
                        + " (nothing to resume; drop --resume to start fresh)");
            }
            if (!existing.argsHash().equals(key.argsHash())) {
                if (metrics != null) {
                    metrics.recordResumeArgsHashRefused();
                }
                throw new InvalidArgsException("--resume refused: listing arguments changed since the "
                        + "checkpointed run (args_hash " + existing.argsHash() + " → " + key.argsHash()
                        + "); use --restart to discard the prior run");
            }
            return new RunMeta(existing.id(), true, existing.argsHash(), existing.strategy(),
                    parseEnum(ListingMode.class, existing.mode(), "run_meta.mode"),
                    existing.filterSpec(), existing.outputFormat(),
                    new SoftRestoreContext(existing.noSignRequest(), existing.profile(), existing.region(),
                            existing.fetchOwner(), existing.rawOutput(), existing.outputPath(),
                            existing.requestPayer(), existing.destinationKind(), existing.outputType()),
                    existing.sortEnabled(), existing.status(), existing.fatalError(), existing.identitySpec());
        }

        // A plain fresh run must never silently discard an existing
        // run for this key. Only an explicit --restart (discard any prior run) or --overwrite (discard
        // a completed run) may. --overwrite is scoped to a COMPLETED run: an unfinished (RUNNING/FAILED)
        // run + --overwrite still refuses and steers to --restart, so --overwrite never silently throws
        // away in-progress work. Refuse otherwise, steering the user to the right escape hatch. Mirrors
        // the --resume refusal branch above: an InvalidArgsException carries the CLI exit-2 mapping.
        boolean overwriteApplies = overwrite && existing != null && existing.status() == RunStatus.COMPLETED;
        if (existing != null && !restart && !overwriteApplies) {
            String location = key.scheme() + "://" + key.bucket() + "/" + prefixText(key.prefix());
            if (existing.status() == RunStatus.COMPLETED) {
                throw new InvalidArgsException("a completed run already exists for " + location
                        + "; pass --overwrite to discard it and re-list, or swath resume for a no-op "
                        + "on the finished dataset");
            }
            throw new InvalidArgsException("an unfinished run already exists for " + location
                    + "; use swath resume to continue it, or --restart to discard it and start fresh");
        }

        // Fresh start (--restart or --overwrite, or a first run): discard any prior state, then insert.
        long id = submit(c -> {
            if (existing != null) {
                deleteRun(c, existing.id());
            }
            return insertRun(c, key);
        });
        // A freshly (re)inserted row always starts RUNNING, fatal_error NULL (insertRun's literals).
        return new RunMeta(id, false, key.argsHash(), key.strategy(), key.mode(),
                key.filterSpec(), key.outputFormat(), key.context(), key.sortEnabled(),
                RunStatus.RUNNING, false, key.identitySpec());
    }

    private static ExistingRun selectExistingRun(Connection c, RunKey key)
            throws SQLException, CheckpointException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT id, args_hash, filter_spec, output_format, strategy, mode, "
                        + "no_sign_request, profile, region, fetch_owner, raw_output, output_path, sort_enabled, "
                        + "status, fatal_error, request_payer, destination_kind, output_type, identity_spec "
                        + "FROM run_meta "
                        + "WHERE store_scheme = ? AND bucket = ? AND prefix = ? AND endpoint IS ? "
                        + "ORDER BY id DESC LIMIT 1")) {
            ps.setString(1, key.scheme());
            ps.setString(2, key.bucket());
            ps.setBytes(3, key.prefix() == null ? new byte[0] : key.prefix());
            ps.setString(4, key.endpoint());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new ExistingRun(rs.getLong(1), rs.getString(2), rs.getString(3),
                        rs.getString(4), rs.getString(5), rs.getString(6),
                        rs.getInt(7) != 0, rs.getString(8), rs.getString(9),
                        rs.getInt(10) != 0, rs.getInt(11) != 0, rs.getString(12), rs.getInt(13) != 0,
                        parseEnum(RunStatus.class, rs.getString(14), "run_meta.status"),
                        // NULL (a pre-migration row backfilled when ALTER TABLE ADD COLUMN added
                        // fatal_error with no DEFAULT, or a row nothing has flagged fatal yet)
                        // reads as 0 via getInt — "not fatal", i.e. normally resumable.
                        rs.getInt(15) != 0,
                        rs.getInt(16) != 0,
                        rs.getString(17), rs.getString(18), rs.getString(19));
            }
        }
    }

    private long insertRun(Connection c, RunKey key) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO run_meta (store_scheme, endpoint, bucket, prefix, args_hash, strategy, "
                        + "filter_spec, output_format, mode, started_at, status, "
                        + "no_sign_request, profile, region, fetch_owner, raw_output, output_path, "
                        + "sort_enabled, sort_phase, request_payer, destination_kind, output_type, identity_spec) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?, 'RUNNING', ?,?,?,?,?,?,?,?,?,?,?,?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, key.scheme());
            ps.setString(2, key.endpoint());
            ps.setString(3, key.bucket());
            ps.setBytes(4, key.prefix() == null ? new byte[0] : key.prefix());
            ps.setString(5, key.argsHash());
            ps.setString(6, key.strategy());
            ps.setString(7, key.filterSpec());
            ps.setString(8, key.outputFormat());
            ps.setString(9, key.mode().name());
            ps.setLong(10, System.currentTimeMillis());
            SoftRestoreContext ctx = key.context();
            ps.setInt(11, ctx.noSignRequest() ? 1 : 0);
            ps.setString(12, ctx.profile());
            ps.setString(13, ctx.region());
            ps.setInt(14, ctx.fetchOwner() ? 1 : 0);
            ps.setInt(15, ctx.rawOutput() ? 1 : 0);
            ps.setString(16, ctx.outputPath());
            ps.setInt(17, key.sortEnabled() ? 1 : 0);
            // A fresh --sort run starts in LISTING; a non-sort run leaves sort_phase NULL.
            ps.setString(18, key.sortEnabled() ? SortPhase.LISTING.name() : null);
            ps.setInt(19, ctx.requestPayer() ? 1 : 0);
            ps.setString(20, ctx.destinationKind());
            ps.setString(21, ctx.outputType());
            ps.setString(22, key.identitySpec());
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                keys.next();
                return keys.getLong(1);
            }
        }
    }

    private void deleteRun(Connection c, long runId) throws SQLException {
        exec(c, "DELETE FROM part_file WHERE run_id = ?", runId);
        exec(c, "DELETE FROM listing_node WHERE run_id = ?", runId);
        exec(c, "DELETE FROM run_meta WHERE id = ?", runId);
    }

    private static void exec(Connection c, String sql, long id) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, id);
            ps.executeUpdate();
        }
    }

    // ---- nodes ----------------------------------------------------------------

    @Override
    public long insertNode(NodeSpec spec) throws CheckpointException {
        return submit(c -> doInsertNode(c, spec));
    }

    /**
     * Atomic batch seed insert (I2): all {@code specs} are inserted in ONE writer-thread
     * task, so the single {@code conn.commit()} at the end of {@link CheckpointWriteQueue}'s
     * batch drain commits them
     * all-or-nothing. A crash / exception before that commit rolls back every insertion,
     * leaving zero nodes — no partial partition, no silent gap.
     */
    @Override
    public List<Long> insertNodes(List<NodeSpec> specs) throws CheckpointException {
        if (specs.isEmpty()) {
            return List.of();
        }
        return submit(c -> {
            List<Long> ids = new ArrayList<>(specs.size());
            for (NodeSpec spec : specs) {
                ids.add(doInsertNode(c, spec));
            }
            return ids;
        });
    }

    private long doInsertNode(Connection c, NodeSpec spec) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO listing_node (run_id, parent_id, kind, range_start, range_end, cursor, "
                        + "inventory_uri, status, generation, updated_at) "
                        + "VALUES (?,?,?,?,?,?,?, 'PENDING', 0, ?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setLong(1, spec.runId());
            if (spec.parentId() == null) {
                ps.setNull(2, Types.INTEGER);
            } else {
                ps.setLong(2, spec.parentId());
            }
            ps.setString(3, spec.kind().name());
            setBytesOrNull(ps, 4, spec.rangeStart());
            setBytesOrNull(ps, 5, spec.rangeEnd());
            setBytesOrNull(ps, 6, spec.cursor());
            ps.setString(7, spec.inventoryUri());
            ps.setLong(8, System.currentTimeMillis());
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                keys.next();
                return keys.getLong(1);
            }
        }
    }

    @Override
    public List<Node> loadResumable(long runId, boolean fileSink) throws CheckpointException {
        return submit(c -> doLoadResumable(c, runId, fileSink));
    }

    @Override
    public long countNodes(long runId) throws CheckpointException {
        return submit(c -> countOne(c, "SELECT COUNT(*) FROM listing_node WHERE run_id=?", runId));
    }

    private List<Node> doLoadResumable(Connection c, long runId, boolean fileSink)
            throws SQLException, CheckpointException {
        // Resume-engagement counters, computed BEFORE the reopen UPDATE below (the
        // file-sink branch resets cursor := durable_cursor as part of that same UPDATE, so any
        // "did this node have a non-durable tail" signal must be read pre-update). Self-gating to
        // zero on a fresh run: a freshly-seeded node is always PENDING with cursor IS durable_cursor
        // (both null, or both the split pivot) — see recordResumeEngagement's javadoc.
        if (metrics != null) {
            recordResumeEngagement(c, runId, fileSink);
        }
        // Revert IN_PROGRESS → PENDING preserving cursor (I5); clear lease, bump generation.
        // A node is "output-complete" iff COMPLETED and (text sink, or durable_cursor IS cursor).
        // Reopen everything that is NOT output-complete.
        String reopen;
        if (fileSink) {
            // File sinks (exactly-once): reset cursor := durable_cursor so the not-yet-durable
            // tail re-lists into new parts (§4.5); a split child whose initial cursor is
            // its range_start has no rows below that boundary to make durable, so old/null
            // child durable cursors fall back to range_start. Reopen COMPLETED-but-not-durable
            // nodes too.
            reopen = "UPDATE listing_node SET status='PENDING', cursor=COALESCE(durable_cursor, range_start), "
                    + "owner_lease=NULL, generation=generation+1, updated_at=? "
                    + "WHERE run_id=? AND NOT (status='COMPLETED' AND durable_cursor IS cursor)";
        } else {
            reopen = "UPDATE listing_node SET status='PENDING', "
                    + "owner_lease=NULL, generation=generation+1, updated_at=? "
                    + "WHERE run_id=? AND status<>'COMPLETED'";
        }
        try (PreparedStatement ps = c.prepareStatement(reopen)) {
            ps.setLong(1, System.currentTimeMillis());
            ps.setLong(2, runId);
            ps.executeUpdate();
        }
        List<Node> nodes = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT id, run_id, parent_id, kind, range_start, range_end, cursor, durable_cursor, "
                        + "key_marker, version_id_marker, inventory_uri, status, generation, unsplittable "
                        + "FROM listing_node WHERE run_id=? AND status<>'COMPLETED' ORDER BY id")) {
            ps.setLong(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    nodes.add(readNode(rs));
                }
            }
        }
        return nodes;
    }

    /**
     * Resume-engagement counters (§5 {@code RESUME.*} category): {@code nodes_reopened}
     * counts nodes that genuinely carry evidence of prior work — {@code status='IN_PROGRESS'}
     * (was claimed and mid-listing when the process stopped), plus, for a file sink, a
     * {@code COMPLETED}-but-non-durable node (its tail gets reopened too). A freshly-seeded node
     * is always inserted as {@code PENDING} ({@link #doInsertNode}/{@link #doSplitNode}) and can
     * never be {@code IN_PROGRESS} or {@code COMPLETED} before its first claim, so this is zero on
     * a fresh run without needing a resumed-run flag threaded through the interface.
     *
     * <p>{@code durable_cursor_lag} is a cheaply-available RPO proxy: a NODE count (not an
     * exact key/page count — that would need per-node durable-vs-total page bookkeeping, hot-path
     * plumbing out of scope here) of reopened nodes whose {@code cursor} was ahead of
     * {@code durable_cursor} pre-update, i.e. genuinely had a non-durable tail to re-list. A
     * freshly-split child's {@code cursor} and {@code durable_cursor} are both set to its pivot at
     * insert ({@link #doSplitNode}), so this is also zero on a fresh/never-crashed node.
     */
    private void recordResumeEngagement(Connection c, long runId, boolean fileSink) throws SQLException {
        String reopenedSql = fileSink
                ? "SELECT COUNT(*) FROM listing_node WHERE run_id=? AND "
                        + "(status='IN_PROGRESS' OR (status='COMPLETED' AND durable_cursor IS NOT cursor))"
                : "SELECT COUNT(*) FROM listing_node WHERE run_id=? AND status='IN_PROGRESS'";
        metrics.recordResumeNodesReopened(countOne(c, reopenedSql, runId));
        if (fileSink) {
            metrics.recordResumeDurableCursorLag(countOne(c,
                    "SELECT COUNT(*) FROM listing_node WHERE run_id=? AND status<>'COMPLETED' "
                            + "AND cursor IS NOT durable_cursor", runId));
        }
    }

    private static long countOne(Connection c, String sql, long runId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static Node readNode(ResultSet rs) throws SQLException, CheckpointException {
        long parentRaw = rs.getLong(3);
        Long parent = rs.wasNull() ? null : parentRaw;
        return new Node(
                rs.getLong(1), rs.getLong(2), parent,
                parseEnum(NodeKind.class, rs.getString(4), "listing_node.kind"),
                rs.getBytes(5), rs.getBytes(6), rs.getBytes(7), rs.getBytes(8),
                rs.getBytes(9), rs.getString(10), rs.getString(11),
                parseEnum(NodeStatus.class, rs.getString(12), "listing_node.status"),
                rs.getLong(13), rs.getInt(14) != 0);
    }

    private static <E extends Enum<E>> E parseEnum(Class<E> type, String value, String column)
            throws CheckpointException {
        try {
            return Enum.valueOf(type, value);
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new CheckpointException("checkpoint row has invalid " + column + ": " + value, e);
        }
    }

    @Override
    public void commitPage(PageCommit p) throws CheckpointException {
        submit(c -> {
            doCommitPage(c, p);
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> commitPageAsync(PageCommit p) throws CheckpointException {
        // Submit the same doCommitPage work to the writer thread; return its future
        // WITHOUT blocking. The future resolves only after the txn durably commits, so the
        // worker can await it outside the worker lock to honor I1 commit-before-emit.
        return queue.enqueue(c -> {
            doCommitPage(c, p);
            return null;
        }).thenApply(ignored -> null);
    }

    private void doCommitPage(Connection c, PageCommit p) throws SQLException {
        // §4.2: non-empty batch → cursor = advanceTo; empty batch leaves cursor unchanged.
        // status = COMPLETED when completed, else IN_PROGRESS. pages/api counters bumped.
        String sql = p.advanceTo() != null
                ? "UPDATE listing_node SET cursor=?, status=?, pages_emitted=pages_emitted+1, "
                + "api_calls=api_calls+1, updated_at=? WHERE id=?"
                : "UPDATE listing_node SET status=?, pages_emitted=pages_emitted+1, "
                + "api_calls=api_calls+1, updated_at=? WHERE id=?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            int i = 1;
            if (p.advanceTo() != null) {
                ps.setBytes(i++, p.advanceTo());
            }
            ps.setString(i++, (p.completed() ? NodeStatus.COMPLETED : NodeStatus.IN_PROGRESS).name());
            ps.setLong(i++, System.currentTimeMillis());
            ps.setLong(i, p.nodeId());
            ps.executeUpdate();
        }
    }

    @Override
    public long splitNode(SplitSpec s) throws CheckpointException {
        return submit(c -> doSplitNode(c, s));
    }

    private long doSplitNode(Connection c, SplitSpec s) throws SQLException {
        // §4.3 CAS guard. `cursor` is NULL-safe (a fresh node, cursor=⊥, is splittable);
        // `range_end IS :oldHi` is null-safe (matches the open frontier oldHi=NULL).
        int narrowed;
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE listing_node SET range_end=?, generation=generation+1, updated_at=? "
                        + "WHERE id=? AND (cursor IS NULL OR cursor < ?) "
                        + "AND range_end IS ? AND status<>'COMPLETED'")) {
            ps.setBytes(1, s.pivot());
            ps.setLong(2, System.currentTimeMillis());
            ps.setLong(3, s.victimId());
            ps.setBytes(4, s.pivot());
            setBytesOrNull(ps, 5, s.oldHi());
            narrowed = ps.executeUpdate();
        }
        if (narrowed == 0) {
            return SPLIT_ABORTED;   // victim advanced past pivot / bound moved / already completed
        }
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO listing_node (run_id, parent_id, kind, range_start, range_end, cursor, "
                        + "durable_cursor, status, generation, updated_at) "
                        + "VALUES (?,?, 'RANGE', ?,?,?,?, 'PENDING', 0, ?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setLong(1, s.runId());
            ps.setLong(2, s.victimId());
            ps.setBytes(3, s.pivot());                 // child range_start = pivot (exclusive lower)
            setBytesOrNull(ps, 4, s.oldHi());          // child range_end = oldHi
            ps.setBytes(5, s.pivot());                 // child cursor starts at pivot (nothing emitted past it)
            ps.setBytes(6, s.pivot());                 // child durable floor is also pivot (no child rows below it)
            ps.setLong(7, System.currentTimeMillis());
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                keys.next();
                return keys.getLong(1);
            }
        }
    }

    // ---- output durability ----------------------------------------------------

    @Override
    public void partFinalized(PartFinalize f) throws CheckpointException {
        submit(c -> {
            doPartFinalized(c, f);
            return null;
        });
    }

    private void doPartFinalized(Connection c, PartFinalize f) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO part_file (run_id, writer_id, path, format, finalized, rows, bytes) "
                        + "VALUES (?,?,?,?,1,?,?)")) {
            ps.setLong(1, f.runId());
            ps.setInt(2, f.writerId());
            ps.setString(3, f.path());
            ps.setString(4, f.format());
            ps.setLong(5, f.rows());
            ps.setLong(6, f.bytes());
            ps.executeUpdate();
        }
        // Advance durable_cursor monotonically for each node whose pages this part held.
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE listing_node SET durable_cursor=? "
                        + "WHERE id=? AND (durable_cursor IS NULL OR durable_cursor < ?)")) {
            for (PartFinalize.DurableAdvance a : f.advances()) {
                ps.setBytes(1, a.maxKey());
                ps.setLong(2, a.nodeId());
                ps.setBytes(3, a.maxKey());
                ps.executeUpdate();
            }
        }
    }

    @Override
    public List<PartRef> finalizedParts(long runId) throws CheckpointException {
        return submit(c -> {
            List<PartRef> parts = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT id, writer_id, path, format, finalized, rows, bytes FROM part_file "
                            + "WHERE run_id=? AND finalized=1 ORDER BY id")) {
                ps.setLong(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        parts.add(new PartRef(rs.getLong(1), rs.getInt(2), rs.getString(3),
                                rs.getString(4), rs.getInt(5) != 0, rs.getLong(6), rs.getLong(7)));
                    }
                }
            }
            return parts;
        });
    }

    @Override
    public void markOutputComplete(long runId) throws CheckpointException {
        submit(c -> {
            // §4.5/I6: latch every COMPLETED node to output-complete after a clean
            // close (all parts finalized). cursor may have advanced past the last kept
            // row (trailing all-filtered pages) — durable_cursor=cursor closes that gap
            // so a later resume does not reopen the finished node. cursor NULL (empty
            // result) → durable_cursor NULL, which is already output-complete.
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE listing_node SET durable_cursor=cursor "
                            + "WHERE run_id=? AND status='COMPLETED'")) {
                ps.setLong(1, runId);
                ps.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public SortPhase sortPhase(long runId) throws CheckpointException {
        String phase = submit(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT sort_phase FROM run_meta WHERE id=?")) {
                ps.setLong(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getString(1) : null;
                }
            }
        });
        return phase == null ? null : parseEnum(SortPhase.class, phase, "run_meta.sort_phase");
    }

    @Override
    public void setSortPhase(long runId, SortPhase phase) throws CheckpointException {
        submit(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE run_meta SET sort_phase=? WHERE id=?")) {
                ps.setString(1, phase.name());
                ps.setLong(2, runId);
                ps.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public void markRunFinished(long runId, RunStatus status) throws CheckpointException {
        submit(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE run_meta SET status=?, finished_at=? WHERE id=?")) {
                ps.setString(1, status.name());
                ps.setLong(2, System.currentTimeMillis());
                ps.setLong(3, runId);
                ps.executeUpdate();
            }
            return null;
        });
    }

    /**
     * CAS on {@code RUNNING}: a run that already recorded a terminal status keeps the disposition
     * it chose. A caller guarding a region that itself committed {@code
     * markRunFinished(COMPLETED)} (then failed in a later post-completion step, e.g. building the
     * final summary) can never downgrade that durable COMPLETED to FAILED; a FAILED row left
     * flag-unset ON PURPOSE — a broken-pipe truncation (INT-12), or the publish failure {@code
     * ListRunner} records before rethrowing into the same guard — is likewise not upgraded to
     * fatal, so it stays resumable.
     */
    @Override
    public void markRunFatalUnlessFinished(long runId) throws CheckpointException {
        markFatal(runId, "status='RUNNING'");
    }

    /**
     * CAS excluding exactly one status: every status {@code openRun(..., resume=true)} admits gets
     * marked, including the flag-unset FAILED a resumed run stays at for its whole second attempt
     * (resume never moves it back to RUNNING) — under a RUNNING-only predicate the mark would
     * silently no-op there and the next resume would be admitted straight back into a violated
     * protocol. Only a durable COMPLETED row is spared.
     */
    @Override
    public void markRunUnresumable(long runId) throws CheckpointException {
        markFatal(runId, "status<>'COMPLETED'");
    }

    /**
     * The two fatal marks, which differ only in which statuses they may overwrite.
     * {@code fatal_error=1} is the distinguisher from the broken-pipe path's plain {@link
     * #markRunFinished} (FAILED, flag left unset/NULL) that {@code --resume}'s refusal check
     * reads: only a row a fatal mark flagged is refused.
     */
    private void markFatal(long runId, String statusGuard) throws CheckpointException {
        submit(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE run_meta SET status=?, fatal_error=1, finished_at=? "
                            + "WHERE id=? AND " + statusGuard)) {
                ps.setString(1, RunStatus.FAILED.name());
                ps.setLong(2, System.currentTimeMillis());
                ps.setLong(3, runId);
                ps.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public void close() throws CheckpointException {
        queue.close();
    }

    private static void setBytesOrNull(PreparedStatement ps, int i, byte[] v) throws SQLException {
        if (v == null) {
            ps.setNull(i, Types.BLOB);
        } else {
            ps.setBytes(i, v);
        }
    }

    private static String prefixText(byte[] prefix) {
        return prefix == null ? "" : new String(prefix, StandardCharsets.UTF_8);
    }
}
