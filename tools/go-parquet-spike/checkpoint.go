// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// checkpoint is a single-connection adapter to a Java-created Swath checkpoint.
// It creates no schema or run identity. SQL below follows SqliteCheckpointStore
// commitPage, partFinalized and loadResumable at the assessed Java revision.
type checkpoint struct {
	db   *sql.DB
	conn *sql.Conn
}

type checkpointNode struct {
	ID            int64
	Kind          string
	RangeStart    []byte
	RangeEnd      []byte
	Cursor        []byte
	DurableCursor []byte
	Status        string
}

func openCheckpoint(ctx context.Context, path string) (*checkpoint, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("checkpoint is not a regular file")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	location := url.URL{Scheme: "file", Path: filepath.ToSlash(absolute), RawQuery: "mode=rw"}
	db, err := sql.Open("sqlite", location.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	store := &checkpoint{db: db, conn: conn}
	if err := store.configure(ctx); err != nil {
		return nil, errors.Join(err, store.close())
	}
	return store, nil
}

func (store *checkpoint) configure(ctx context.Context) error {
	// Like Java, version refusal precedes every persistent setup PRAGMA.
	if _, err := store.conn.ExecContext(ctx, "PRAGMA busy_timeout=5000"); err != nil {
		return err
	}
	var version int
	if err := store.conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version != 1 {
		return fmt.Errorf("checkpoint schema version mismatch: found %d, expected 1", version)
	}
	for _, pragma := range []string{"PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL", "PRAGMA foreign_keys=ON"} {
		if _, err := store.conn.ExecContext(ctx, pragma); err != nil {
			return err
		}
	}
	return nil
}

func (store *checkpoint) close() error {
	return errors.Join(store.conn.Close(), store.db.Close())
}

func blobKey(key []byte) any {
	if key == nil {
		return nil
	}
	return key
}

func (store *checkpoint) resumeNodes(ctx context.Context, runID int64) ([]checkpointNode, error) {
	tx, err := store.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `UPDATE listing_node SET status='PENDING',
		cursor=COALESCE(durable_cursor, range_start), owner_lease=NULL,
		generation=generation+1, updated_at=?
		WHERE run_id=? AND NOT (status='COMPLETED' AND durable_cursor IS cursor)`, time.Now().UnixMilli(), runID)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id, kind, range_start, range_end, cursor, durable_cursor, status
		FROM listing_node WHERE run_id=? AND status<>'COMPLETED' ORDER BY id`, runID)
	if err != nil {
		return nil, err
	}
	var nodes []checkpointNode
	for rows.Next() {
		var node checkpointNode
		if err := rows.Scan(&node.ID, &node.Kind, &node.RangeStart, &node.RangeEnd, &node.Cursor, &node.DurableCursor, &node.Status); err != nil {
			rows.Close()
			return nil, err
		}
		nodes = append(nodes, node)
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return nodes, nil
}

func (store *checkpoint) commitPage(ctx context.Context, nodeID int64, advance []byte, completed bool) error {
	tx, err := store.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	status := "IN_PROGRESS"
	if completed {
		status = "COMPLETED"
	}
	if advance == nil {
		_, err = tx.ExecContext(ctx, `UPDATE listing_node SET status=?, pages_emitted=pages_emitted+1,
			api_calls=api_calls+1, updated_at=? WHERE id=?`, status, time.Now().UnixMilli(), nodeID)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE listing_node SET cursor=?, status=?, pages_emitted=pages_emitted+1,
			api_calls=api_calls+1, updated_at=? WHERE id=?`, blobKey(advance), status, time.Now().UnixMilli(), nodeID)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

type durableAdvance struct {
	NodeID int64
	Key    []byte
}

// finalizePart commits the real part row and all durable advances atomically.
// afterInsert is a per-call barrier/fault seam inside this real SQLite transaction.
func (store *checkpoint) finalizePart(ctx context.Context, runID int64, writerID int,
	key string, part partResult, advances []durableAdvance, afterInsert func() error) error {
	tx, err := store.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO part_file
		(run_id, writer_id, path, format, format_version, extension_type, finalized, rows, bytes)
		VALUES (?, ?, ?, 'parquet', NULL, NULL, 1, ?, ?)`, runID, writerID, key, part.Rows, part.Bytes)
	if err != nil {
		return err
	}
	if afterInsert != nil {
		if err := afterInsert(); err != nil {
			return err
		}
	}
	for _, advance := range advances {
		_, err = tx.ExecContext(ctx, `UPDATE listing_node SET durable_cursor=?
			WHERE id=? AND (durable_cursor IS NULL OR durable_cursor < ?)`,
			blobKey(advance.Key), advance.NodeID, blobKey(advance.Key))
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
