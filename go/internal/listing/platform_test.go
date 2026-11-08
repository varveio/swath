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
)

// TestUnenforcedPlatformRefusesJournal (review item 42): where
// directory syncs are not enforced (Windows), a journaled
// run and a resume are refused with ErrUnenforced before the output is
// created or touched; a --no-journal run of a resumable source and a local
// (never journaled) run still work. Before, the journaled run went ahead
// with a warning and no lock.
func TestUnenforcedPlatformRefusesJournal(t *testing.T) {
	defer func(was bool) { dataset.DirSyncEnforced = was }(dataset.DirSyncEnforced)
	dataset.DirSyncEnforced = false
	keys := []string{"a", "b", "c"}
	out := filepath.Join(t.TempDir(), "ds")
	for _, resume := range []bool{false, true} {
		if _, err := runScripted(t, &scripted{keys: keys, chunk: 1}, out, resume); !errors.Is(err, ErrUnenforced) {
			t.Fatalf("resume=%v: err = %v, want ErrUnenforced", resume, err)
		}
		if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("resume=%v: the refused run created the output: %v", resume, err)
		}
	}
	scriptMu.Lock()
	scriptNext = &scripted{keys: keys, chunk: 1}
	scriptMu.Unlock()
	if _, err := Run(context.Background(), Config{Source: SourceConfig{URI: "scripted://x"}, Output: OutputConfig{Dir: out, NoJournal: true}}); err != nil {
		t.Fatalf("--no-journal: %v", err)
	}
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), Config{Source: SourceConfig{URI: src, Options: fsOptions(false)}, Output: OutputConfig{Dir: filepath.Join(t.TempDir(), "local")}}); err != nil {
		t.Fatalf("local run: %v", err)
	}
}
