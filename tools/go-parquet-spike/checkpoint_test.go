// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// copyJavaCheckpoint requires the production Java fixture, not a Go-authored
// lookalike schema. run_resume_probe.py supplies it explicitly at its gate.
func copyJavaCheckpoint(t *testing.T) (*checkpoint, checkpointFixture, string) {
	t.Helper()
	root := os.Getenv("SWATH_CHECKPOINT_FIXTURE")
	if root == "" {
		t.Skip("requires SWATH_CHECKPOINT_FIXTURE from CheckpointInterop create")
	}
	fixture, err := readCheckpointFixture(root)
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, ".swath", "checkpoint.sqlite")
	if info, err := os.Stat(source + "-wal"); err == nil && info.Size() > 0 {
		t.Fatal("Java fixture must be closed and checkpointed before copying")
	}
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "checkpoint.sqlite")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	baseline, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(fixture.BaselinePart)))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(directory, "data"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, filepath.FromSlash(fixture.BaselinePart)), baseline, 0600); err != nil {
		t.Fatal(err)
	}
	store, err := openCheckpoint(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.close(); err != nil {
			t.Error(err)
		}
	})
	return store, fixture, directory
}

func TestCheckpointJavaResumeAndAtomicPartCommit(t *testing.T) {
	store, fixture, directory := copyJavaCheckpoint(t)
	rows, err := loadRows(fixture.InputJSONL)
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := store.resumeNodes(t.Context(), fixture.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || string(nodes[0].Cursor) != rows[127].Key || string(nodes[0].DurableCursor) != rows[127].Key {
		t.Fatalf("Java checkpoint resumed at wrong durable floor: %+v", nodes)
	}
	if err := store.commitPage(t.Context(), fixture.NodeID, []byte(rows[255].Key), false); err != nil {
		t.Fatal(err)
	}
	config := writerConfig{Layout: "direct", KeyEncoding: "dict", IntegerEncoding: "dict", PageStatistics: true,
		PageRows: 32, GroupRows: 64, SortMode: "none"}
	part, err := writePart(filepath.Join(directory, "data", "part-probe.parquet"), rows[128:256], config, diskOps())
	if err != nil {
		t.Fatal(err)
	}
	advance := []durableAdvance{{NodeID: fixture.NodeID, Key: []byte(rows[255].Key)}}
	failure := errors.New("stop after real INSERT before cursor update")
	err = store.finalizePart(t.Context(), fixture.RunID, fixture.WriterID, "data/part-probe.parquet", part, advance,
		func() error { return failure })
	if !errors.Is(err, failure) {
		t.Fatalf("transaction failure = %v", err)
	}
	checkCheckpointState(t, store, fixture, 1, rows[255].Key, rows[127].Key)
	if err := store.finalizePart(t.Context(), fixture.RunID, fixture.WriterID, "data/part-probe.parquet", part, advance, nil); err != nil {
		t.Fatal(err)
	}
	checkCheckpointState(t, store, fixture, 2, rows[255].Key, rows[255].Key)
}

func checkCheckpointState(t *testing.T, store *checkpoint, fixture checkpointFixture, count int, cursor, durable string) {
	t.Helper()
	var gotCount int
	var gotCursor, gotDurable []byte
	if err := store.conn.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM part_file WHERE run_id=? AND finalized=1", fixture.RunID).Scan(&gotCount); err != nil {
		t.Fatal(err)
	}
	if err := store.conn.QueryRowContext(t.Context(), "SELECT cursor, durable_cursor FROM listing_node WHERE id=?", fixture.NodeID).Scan(&gotCursor, &gotDurable); err != nil {
		t.Fatal(err)
	}
	if gotCount != count || string(gotCursor) != cursor || string(gotDurable) != durable {
		t.Fatalf("checkpoint = %d, %q, %q; want %d, %q, %q", gotCount, gotCursor, gotDurable, count, cursor, durable)
	}
}

func TestCheckpointEmptyPagePreservesEmptyBLOB(t *testing.T) {
	store, fixture, _ := copyJavaCheckpoint(t)
	if err := store.commitPage(t.Context(), fixture.NodeID, []byte{}, false); err != nil {
		t.Fatal(err)
	}
	if err := store.commitPage(t.Context(), fixture.NodeID, nil, true); err != nil {
		t.Fatal(err)
	}
	var kind, status string
	var length int
	if err := store.conn.QueryRowContext(t.Context(), "SELECT typeof(cursor), length(cursor), status FROM listing_node WHERE id=?", fixture.NodeID).Scan(&kind, &length, &status); err != nil {
		t.Fatal(err)
	}
	if kind != "blob" || length != 0 || status != "COMPLETED" {
		t.Fatalf("empty-page state = %s, %d, %s", kind, length, status)
	}
}

func TestCheckpointVersionRefusalPreservesFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "foreign.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	// This is deliberately a foreign file, never a handcrafted Swath ledger.
	_, err = db.ExecContext(t.Context(), "PRAGMA user_version=77")
	err = errors.Join(err, db.Close())
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if store, err := openCheckpoint(context.Background(), path); err == nil {
		store.close()
		t.Fatal("future checkpoint version was accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("refused checkpoint changed before its version gate")
	}
}
