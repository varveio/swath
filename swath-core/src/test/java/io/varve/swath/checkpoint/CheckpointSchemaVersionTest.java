/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

import static io.varve.swath.checkpoint.CheckpointStoreTestSupport.b;
import static io.varve.swath.checkpoint.CheckpointStoreTestSupport.key;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.CheckpointException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@code PRAGMA user_version} stamp is what makes a checkpoint file identifiable as swath's.
 * Every DDL statement is {@code IF NOT EXISTS} or an additive {@code ALTER}, so without the gate a
 * foreign, damaged, or newer-than-this-build database would be half-adopted — tables created
 * alongside whatever was already there, and the run resumed from it — instead of refused. These
 * tests therefore assert both halves: that the open fails, and that the file is left untouched.
 */
final class CheckpointSchemaVersionTest {

    /** Pinned literally: this value is on disk in the field, so a change to it is a breaking one. */
    private static final int STAMPED_VERSION = 1;

    @Test
    void freshCheckpointCarriesTheSchemaStamp(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            store.openRun(key("h1"), false, false);
        }

        assertThat(CheckpointSchema.SCHEMA_VERSION).isEqualTo(STAMPED_VERSION);
        assertThat(userVersion(db))
                .as("a checkpoint swath created stamps its schema version")
                .isEqualTo(STAMPED_VERSION);
    }

    @Test
    void stampedCheckpointStillResumes(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        long runId;
        long nodeId;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            runId = run.id();
            nodeId = store.insertNode(NodeSpec.rootRange(runId));
            store.commitPage(new PageCommit(nodeId, b("k1"), false));
        }

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta resumed = store.openRun(key("h1"), true, false);
            assertThat(resumed.resumed()).isTrue();
            assertThat(resumed.id()).isEqualTo(runId);

            List<Node> resumable = store.loadResumable(runId, false);
            assertThat(resumable).hasSize(1);
            assertThat(resumable.getFirst().id()).isEqualTo(nodeId);
            assertThat(resumable.getFirst().cursor()).isEqualTo(b("k1"));
        }
        assertThat(userVersion(db)).isEqualTo(STAMPED_VERSION);
    }

    @Test
    void unstampedDatabaseIsRefusedWithoutCreatingSchema(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("foreign.sqlite");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             Statement st = c.createStatement()) {
            st.execute(CheckpointStoreTestSupport.PRE_CONTEXT_RUN_META_DDL);
        }

        assertThatThrownBy(() -> SqliteCheckpointStore.open(db))
                .isInstanceOf(CheckpointException.class)
                .hasMessageContaining(db.toString())
                .hasMessageContaining("found 0, expected " + STAMPED_VERSION)
                .hasMessageContaining("not a swath checkpoint")
                .hasMessageContaining("start a fresh run")
                // A database from before versioning is not a newer swath's, so no upgrade advice.
                .hasMessageNotContaining("upgrade");

        assertThat(userVersion(db)).isZero();
        assertThat(tables(db))
                .as("a refused open creates nothing: no half-adopted checkpoint schema")
                .containsExactly("run_meta");
    }

    /**
     * The refusal is worthless if reaching it costs the file a write. {@code journal_mode=WAL} is a
     * persistent header change (bytes 18/19 go 1,1 → 2,2 and stay there), so the version gate has to
     * run before the setup PRAGMAs, not after them — and the only assertion that proves it is the
     * whole file, byte for byte.
     */
    @Test
    void refusedDatabaseIsLeftByteIdentical(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("foreign.sqlite");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             Statement st = c.createStatement()) {
            st.execute(CheckpointStoreTestSupport.PRE_CONTEXT_RUN_META_DDL);
        }
        byte[] before = Files.readAllBytes(db);

        assertThatThrownBy(() -> SqliteCheckpointStore.open(db)).isInstanceOf(CheckpointException.class);

        assertThat(Files.readAllBytes(db))
                .as("a refused open writes nothing to the file it refuses")
                .isEqualTo(before);
        assertThat(journalMode(db))
                .as("the file keeps its own journal mode: swath never switched it to WAL")
                .isEqualTo("delete");
        assertThat(Files.exists(Path.of(db + "-wal"))).isFalse();
        assertThat(Files.exists(Path.of(db + "-shm"))).isFalse();
    }

    /**
     * Creation is one transaction — DDL, column backfill and the version stamp commit together or
     * not at all. Were they separate autocommits, a failure part-way through (or a WAL tail lost to
     * power failure) would leave a durable, unstamped, half-built schema, and every later open —
     * {@code --restart} and {@code resume} included, since both run behind this gate — would refuse
     * it until someone deleted the checkpoint by hand. Two tests below cover the two databases
     * {@link CheckpointSchema#apply} runs against: an already-stamped one it is only backfilling
     * (this one), and a genuinely fresh one it is stamping for the first time (the next one, which
     * is the case the atomicity fix was for).
     *
     * <p>The failure is injected mid-DDL: a stamped database whose {@code listing_node} lacks the
     * columns {@code idx_node_ready} indexes, so the {@code CREATE INDEX} fails after
     * {@code CREATE TABLE run_meta} has already succeeded.
     */
    @Test
    void interruptedBackfillLeavesNoHalfBuiltSchema(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             Statement st = c.createStatement()) {
            st.execute("PRAGMA user_version=" + STAMPED_VERSION);
            st.execute("CREATE TABLE listing_node (id INTEGER PRIMARY KEY)");
        }

        assertThatThrownBy(() -> SqliteCheckpointStore.open(db))
                .isInstanceOf(CheckpointException.class)
                .hasMessageContaining("failed to open checkpoint store at " + db);

        assertThat(tables(db))
                .as("the tables created before the failing statement rolled back with it")
                .containsExactly("listing_node");
        assertThat(userVersion(db)).isEqualTo(STAMPED_VERSION);
    }

    /**
     * The database above is already stamped when {@link CheckpointSchema#checkVersion} sees it, so
     * {@code apply} runs with {@code fresh=false} and never reaches the {@code PRAGMA user_version}
     * write — that test proves DDL rolls back, but says nothing about the stamp. Reaching the {@code
     * fresh=true} branch needs an empty, unstamped database ({@code checkVersion} requires {@code
     * sqlite_master} to have zero rows), which rules out pre-creating a malformed table the way the
     * test above does: any pre-existing object trips the database onto the non-fresh refusal path
     * before {@code apply} ever runs.
     *
     * <p>So the failure is injected a different way, and at the boundary the fix actually closed:
     * {@link CheckpointSchema#checkVersion} and {@link CheckpointSchema#apply} are driven directly —
     * the same calls {@code SqliteCheckpointStore.open} makes, under the same one-transaction
     * discipline it wraps them in — through a {@link Statement} that lets every {@code CREATE TABLE}/
     * {@code CREATE INDEX}/{@code ALTER TABLE} statement run for real and only fails the {@code
     * PRAGMA user_version} write that follows them all. Before the fix that write was a second,
     * separately-committed statement, so a schema this fully built could already be durable by the
     * time it failed; this proves it no longer can be.
     */
    @Test
    void interruptedFreshCreateLeavesNeitherSchemaNorStamp(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             Statement real = c.createStatement()) {
            assertThat(CheckpointSchema.checkVersion(real, db.toString()))
                    .as("an empty, unstamped file is the fresh path apply() stamps")
                    .isTrue();

            c.setAutoCommit(false);
            Statement failingOnStamp = failOnSql(real, "PRAGMA user_version=");
            assertThatThrownBy(() -> CheckpointSchema.apply(failingOnStamp, true))
                    .isInstanceOf(SQLException.class);
            c.rollback();
        }

        assertThat(tables(db))
                .as("every table and index apply() built before the failing stamp write rolled "
                        + "back with it")
                .isEmpty();
        assertThat(userVersion(db))
                .as("a failed fresh create does not leave the stamp behind with no schema under it")
                .isZero();
    }

    /**
     * Wraps a real {@link Statement} so an {@code execute(String)} call whose SQL starts with {@code
     * sqlPrefix} throws instead of running, while every other call passes straight through to {@code
     * delegate}. Stands in for a crash right before one specific statement: everything before it
     * genuinely runs against SQLite, so what the caller's transaction rolls back is real.
     */
    private static Statement failOnSql(Statement delegate, String sqlPrefix) {
        return (Statement) Proxy.newProxyInstance(
                CheckpointSchemaVersionTest.class.getClassLoader(),
                new Class<?>[] {Statement.class},
                new FailOnSqlHandler(delegate, sqlPrefix));
    }

    private static final class FailOnSqlHandler implements InvocationHandler {
        private final Statement delegate;
        private final String sqlPrefix;

        FailOnSqlHandler(Statement delegate, String sqlPrefix) {
            this.delegate = delegate;
            this.sqlPrefix = sqlPrefix;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if ("execute".equals(method.getName()) && args != null && args.length == 1
                    && args[0] instanceof String sql && sql.startsWith(sqlPrefix)) {
                throw new SQLException("simulated crash before: " + sql);
            }
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }
    }

    @Test
    void newerSchemaVersionIsRefusedWithUpgradeAdvice(@TempDir Path dir) throws Exception {
        int newer = STAMPED_VERSION + 1;
        Path db = dir.resolve("ckpt.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            store.openRun(key("h1"), false, false);
        }
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             Statement st = c.createStatement()) {
            st.execute("PRAGMA user_version=" + newer);
        }

        assertThatThrownBy(() -> SqliteCheckpointStore.open(db))
                .isInstanceOf(CheckpointException.class)
                .hasMessageContaining(db.toString())
                .hasMessageContaining("found " + newer + ", expected " + STAMPED_VERSION)
                .hasMessageContaining("written by a newer swath")
                .hasMessageContaining("upgrade swath");

        assertThat(userVersion(db))
                .as("the refused checkpoint is not re-stamped down to this build's version")
                .isEqualTo(newer);
    }

    private static int userVersion(Path db) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA user_version")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    /** The query form of the PRAGMA: it reports the mode without setting it. */
    private static String journalMode(Path db) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA journal_mode")) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static List<String> tables(Path db) throws Exception {
        List<String> names = new ArrayList<>();
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")) {
            while (rs.next()) {
                names.add(rs.getString(1));
            }
        }
        return names;
    }
}
