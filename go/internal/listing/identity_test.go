// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/journal"
)

// fileTree is every file and directory under dir with its size and digest.
func fileTree(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		if d.IsDir() {
			out[rel] = "dir"
			return nil
		}
		b, err := os.ReadFile(p)
		out[rel] = fmt.Sprintf("%d %x", len(b), md5.Sum(b))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestReplacedOutputIsNeverTouched (Astra register row 34): the output
// directory is moved away mid-run and another dataset (an interrupted run's
// layout: a temporary parts directory included) is put at its pathname.
// The run's lock is on the moved directory's inode; the pathname now names
// the impostor. The run stops with ErrOutputReplaced before its next
// create, rename or deletion, the impostor is left byte for byte as it was,
// and the moved directory resumes at its new path to the clean run's
// dataset. Before, the run went on writing parts by pathname into the
// impostor and renamed them over its published parts.
func TestReplacedOutputIsNeverTouched(t *testing.T) {
	var keys []string
	for i := range 40000 {
		keys = append(keys, fmt.Sprintf("k%06d", i))
	}
	const chunk = 2000
	base := t.TempDir()
	clean, imp := filepath.Join(base, "clean"), filepath.Join(base, "impostor")
	if _, err := runScripted(t, &scripted{keys: keys, chunk: chunk}, clean, false); err != nil {
		t.Fatal(err)
	}
	if _, err := runScripted(t, &scripted{keys: keys[:5000], chunk: chunk}, imp, false); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(imp, dataset.Workspace, dataset.PartsDir), 0o755); err != nil {
		t.Fatal(err)
	}
	before := fileTree(t, imp)

	out, moved := filepath.Join(base, "out"), filepath.Join(base, "moved")
	scriptMu.Lock()
	scriptNext = &scripted{keys: keys, chunk: chunk, slow: 30 * time.Millisecond}
	scriptMu.Unlock()
	pr := &Progress{}
	done := make(chan error, 1)
	go func() {
		_, err := Run(context.Background(), Config{Source: SourceConfig{URI: "scripted://x"}, Output: OutputConfig{Dir: out},
			Limits: Limits{PartSize: 2 << 10, Workers: 4, Memory: 256 << 10, PartInterval: -1}, Progress: pr})
		done <- err
	}()
	deadline := time.Now().Add(10 * time.Second)
	for pr.Snapshot().Parts < 2 {
		if time.Now().After(deadline) {
			t.Fatal("no part published")
		}
		time.Sleep(time.Millisecond)
	}
	if err := os.Rename(out, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(imp, out); err != nil {
		t.Fatal(err)
	}
	err := <-done
	if !errors.Is(err, journal.ErrOutputReplaced) {
		t.Fatalf("err = %v, want ErrOutputReplaced", err)
	}
	if after := fileTree(t, out); !maps.Equal(before, after) {
		for k, v := range after {
			if before[k] != v {
				t.Errorf("impostor changed: %s %s (was %q)", k, v, before[k])
			}
		}
		for k := range before {
			if _, ok := after[k]; !ok {
				t.Errorf("impostor lost %s", k)
			}
		}
		t.FailNow()
	}
	if _, err := runScripted(t, &scripted{keys: keys, chunk: chunk}, moved, true); err != nil {
		t.Fatalf("resume at the moved path: %v", err)
	}
	sameDataset(t, "moved", clean, moved)
}
