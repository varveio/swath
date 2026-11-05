// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortout

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/varveio/swath/tools/swath-local-spike/pqout"
	"github.com/varveio/swath/tools/swath-local-spike/record"
)

func TestStreamerWritesPartsInOrder(t *testing.T) {
	rows := randomRows(30_000, 5)
	slices.SortFunc(rows, func(a, b record.Row) int { return record.Compare(&a, &b) })
	out := t.TempDir()
	if err := Prepare(out, t.TempDir(), true); err != nil {
		t.Fatal(err)
	}
	cfg := pqout.DefaultConfig()
	cfg.GroupBytes, cfg.Encoders = 32<<10, 3
	s := NewStreamer(Options{Out: out, PartSize: 128 << 10, Writer: cfg})
	for i := 0; i < len(rows); i += 1000 {
		s.Emit(slices.Clone(rows[i:min(i+1000, len(rows))]))
	}
	parts, st, err := s.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) < 2 || st.Rows != int64(len(rows)) {
		t.Fatalf("parts=%d rows=%d", len(parts), st.Rows)
	}
	if _, err := Publish(out, "/r", nil, parts, st); err != nil {
		t.Fatal(err)
	}
	got := readDataset(t, out, parts)
	for i := range rows {
		if got[i] != rows[i] {
			t.Fatalf("row %d differs", i)
		}
	}
}

func TestStreamerRejectsDescendingKeys(t *testing.T) {
	out := t.TempDir()
	if err := Prepare(out, t.TempDir(), true); err != nil {
		t.Fatal(err)
	}
	s := NewStreamer(Options{Out: out, PartSize: 1 << 20, Writer: pqout.DefaultConfig()})
	s.Emit([]record.Row{{Key: "b"}, {Key: "a"}})
	if _, _, err := s.Finish(); err == nil {
		t.Fatal("expected ascending-key violation")
	}
	if tmps, _ := filepath.Glob(filepath.Join(out, "data", "*.tmp")); len(tmps) != 0 {
		t.Fatalf("tmp parts left: %v", tmps)
	}
}
