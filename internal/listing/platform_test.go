// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/internal/dataset"
)

// TestUnenforcedPlatformRefusesJournal (review item 42): where
// directory syncs are not enforced (Windows), a journaled
// run and a resume are refused with ErrUnenforced before the output is
// created or touched. Before, the journaled run went ahead with a warning
// and no lock.
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
						// Supply the destination identity without I/O; the guard
						// must refuse the run before creating the work directory.
						return &dataset.Remote{Location: loc}, nil, nil
					}}
			}
			if _, err := Run(context.Background(), cfg); !errors.Is(err, ErrUnenforced) {
				t.Fatalf("destination=%q resume=%v: err = %v, want ErrUnenforced", destination, resume, err)
			}
			if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("destination=%q resume=%v: the refused run created the output: %v", destination, resume, err)
			}
		}
	}
}

// Unjournaled runs still publish datasets when directory sync is not
// enforced, both locally and to a bucket.
func TestUnenforcedPlatformAllowsUnjournaledDataset(t *testing.T) {
	defer func(was bool) { dataset.DirSyncEnforced = was }(dataset.DirSyncEnforced)
	dataset.DirSyncEnforced = false
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		source    SourceConfig
		noJournal bool
		rows      int64
	}{
		{"local", SourceConfig{URI: src, Options: fsOptions(false)}, false, 1},
		{"resumable-no-journal", SourceConfig{URI: "scripted://x"}, true, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := func(t *testing.T, remote *RemoteOutput) {
				t.Helper()
				scriptMu.Lock()
				scriptNext = &scripted{keys: []string{"a", "b", "c"}, chunk: 1}
				scriptMu.Unlock()
				out := filepath.Join(t.TempDir(), "ds")
				cfg := Config{Source: tc.source, Output: OutputConfig{Dir: out, NoJournal: tc.noJournal, Remote: remote},
					Limits: Limits{Workers: 1, Encoders: 1, PartSize: 2 << 10, RowBudget: 128 << 10, PartInterval: -1}}
				rep, err := Run(context.Background(), cfg)
				if err != nil {
					t.Fatal(err)
				}
				if rep.Output.RowsCommitted != tc.rows {
					t.Fatalf("committed %d rows, want %d", rep.Output.RowsCommitted, tc.rows)
				}
				if remote == nil && !dataset.Committed(out) {
					t.Fatal("no committed local dataset")
				}
			}
			t.Run("local-output", func(t *testing.T) { run(t, nil) })
			eachProvider(t, func(t *testing.T, p provider) {
				d := newDestination(t, p, nil)
				run(t, d.output("unjournaled"))
				if rows := sumRows(d.manifest(t, "unjournaled")); rows != tc.rows {
					t.Fatalf("manifest has %d rows, want %d", rows, tc.rows)
				}
			})
		})
	}
}
