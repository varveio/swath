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
	for _, destination := range []string{"", "s3://dst/prefix", "gs://dst/prefix", "az://acct/dst/prefix"} {
		for _, resume := range []bool{false, true} {
			// Not runScripted: its config lists unjournaled where that is refused.
			scriptMu.Lock()
			scriptNext = &scripted{keys: keys, chunk: 1}
			scriptMu.Unlock()
			cfg := Config{Source: SourceConfig{URI: "scripted://x"}, Output: OutputConfig{Dir: out, Resume: resume}, Limits: Limits{PartSize: 2 << 10, Workers: 4, PartInterval: -1}}
			if destination != "" {
				loc, err := ParseDestination(destination)
				if err != nil {
					t.Fatal(err)
				}
				cfg.Output.Remote = &RemoteOutput{Location: destination,
					Open: func(context.Context, *dataset.UploadStats) (*dataset.Remote, func() error, error) {
						// Opening a destination must need no I/O before the guard.
						return &dataset.Remote{Location: loc}, nil, nil
					}}
			}
			if _, err := Run(context.Background(), cfg); !errors.Is(err, ErrUnenforced) {
				t.Fatalf("destination=%q resume=%v: err = %v, want ErrUnenforced", destination, resume, err)
			}
			if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("resume=%v: the refused run created the output: %v", resume, err)
			}
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
