// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
	dataset.DirSyncEnforced = false
	defer func() { dataset.DirSyncEnforced = true }()
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

// TestWindowsBuild cross-builds the command for Windows, where the
// refusal above is the behaviour (dataset.DirSyncEnforced is false there),
// and compiles the journal's tests, its Windows lock test included.
func TestWindowsBuild(t *testing.T) {
	if testing.Short() {
		t.Skip("cross-build")
	}
	gobin := filepath.Join(runtime.GOROOT(), "bin", "go")
	cmd := exec.Command(gobin, "build", "-o", os.DevNull, "./cmd/swath")
	cmd.Dir = filepath.Join("..", "..")
	cmd.Env = append(os.Environ(), "GOOS=windows", "GOARCH=amd64", "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("windows build: %v\n%s", err, b)
	}
	cmd = exec.Command(gobin, "test", "-c", "-o", os.DevNull, "./internal/journal")
	cmd.Dir = filepath.Join("..", "..")
	cmd.Env = append(os.Environ(), "GOOS=windows", "GOARCH=amd64", "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("windows journal tests: %v\n%s", err, b)
	}
}
