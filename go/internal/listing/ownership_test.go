// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/journal"
)

// TestEveryOutputIsOwned: a filesystem run (no journal) takes the output
// lock like a journaled one, so a second writer is refused before it
// prepares anything; Settle runs while the lock is still held, and not at
// all for a run that never owned the output; a refused foreign directory
// is left untouched (durability review, ownership).
func TestEveryOutputIsOwned(t *testing.T) {
	src := t.TempDir()
	mk(t, src, "a", "b", "c")
	out := filepath.Join(t.TempDir(), "ds")
	held, err := journal.TakeLock(out)
	if err != nil {
		t.Fatal(err)
	}
	settled := 0
	cfg := Config{Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out}, Progress: &Progress{},
		Settle: func(Report, error) { settled++ }}
	if _, err := Run(context.Background(), cfg); !errors.Is(err, journal.ErrLocked) {
		t.Fatalf("a filesystem run over an owned output: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, dataset.DataDir)); err == nil || settled != 0 {
		t.Fatalf("the refused run prepared the output (settled %d)", settled)
	}
	if cfg.Progress.Owned(func() { t.Fatal("a refused run wrote as owner") }) {
		t.Fatal("a refused run reports ownership")
	}
	held.Release()

	cfg.Settle = func(_ Report, err error) {
		settled++
		if err != nil {
			t.Errorf("run: %v", err)
		}
		if l, err := journal.TakeLock(out); !errors.Is(err, journal.ErrLocked) {
			l.Release()
			t.Error("Settle ran after the lock was released")
		}
		if !cfg.Progress.Owned(func() {}) {
			t.Error("Settle ran without ownership")
		}
	}
	if _, err := Run(context.Background(), cfg); err != nil || settled != 1 {
		t.Fatalf("owned run: %v (settled %d)", err, settled)
	}
	if cfg.Progress.Owned(func() {}) {
		t.Fatal("ownership outlived the run")
	}

	foreign := t.TempDir()
	mk(t, foreign, "keep")
	for _, replace := range []bool{false, true} {
		cfg := Config{Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: foreign, Replace: replace}}
		if _, err := Run(context.Background(), cfg); err == nil {
			t.Fatal("a foreign directory was accepted")
		}
		if _, err := os.Stat(filepath.Join(foreign, dataset.Workspace)); err == nil {
			t.Fatalf("replace=%v: a refused foreign directory got a workspace", replace)
		}
	}
	none := filepath.Join(t.TempDir(), "none")
	s3 := &S3Config{Endpoint: "http://127.0.0.1:1", Anonymous: true}
	if _, err := Run(context.Background(), Config{Source: SourceConfig{URI: "s3://bkt", Options: s3}, Output: OutputConfig{Dir: none, Resume: true}}); !errors.Is(err, ErrNothingToResume) {
		t.Fatalf("resume of nothing: %v", err)
	}
	if _, err := os.Stat(none); err == nil {
		t.Fatal("a resume of nothing created the output directory")
	}
}
