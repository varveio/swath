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
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
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

// TestReplacedOutputIsNeverTouched (review item 34): the output
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
			Limits: Limits{PartSize: 2 << 10, Workers: 4, RowBudget: 128 << 10, PartInterval: -1}, Progress: pr})
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

// copyTree copies dir to dst as it is now; files that vanish while it
// copies (a live run removes spill files) are skipped.
func copyTree(t *testing.T, dir, dst string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		b, err := os.ReadFile(p)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestReplacedOutputWithLiveWriters (durability review):
// the output is replaced mid-run by a copy of itself (so the impostor
// holds the run's own spill directory, spill files and progress file at
// the same names) while spill files are live and a writer like the
// command's rewrites progress.json through Progress.Owned every
// millisecond. Nothing in the copy changes: the progress writer, spill
// appends, trailer removals and the spill cleanup all check the output's
// identity first; the run stops with ErrOutputReplaced and the moved
// directory resumes to the clean run's dataset. Before, the progress
// writer rewrote the impostor's progress.json and the spill cleanup
// removed the impostor's spill directory.
func TestReplacedOutputWithLiveWriters(t *testing.T) {
	var keys []string
	for i := range 40000 {
		keys = append(keys, fmt.Sprintf("k%06d", i))
	}
	const chunk = 2000
	base := t.TempDir()
	clean := filepath.Join(base, "clean")
	if _, err := runScripted(t, &scripted{keys: keys, chunk: chunk}, clean, false); err != nil {
		t.Fatal(err)
	}
	out, moved, imp := filepath.Join(base, "out"), filepath.Join(base, "moved"), filepath.Join(base, "impostor")
	scriptMu.Lock()
	// Every fourth range is slow: the ranges after it finish first and
	// spill while it holds the frontier.
	scriptNext = &scripted{keys: keys, chunk: chunk, slowAt: func(i int) time.Duration {
		if i/chunk%4 == 0 {
			return 300 * time.Millisecond
		}
		return 0
	}}
	scriptMu.Unlock()
	pr := &Progress{}
	done := make(chan error, 1)
	go func() {
		_, err := Run(context.Background(), Config{Source: SourceConfig{URI: "scripted://x"}, Output: OutputConfig{Dir: out},
			Limits: Limits{PartSize: 2 << 10, Workers: 4, RowBudget: 128 << 10, PartInterval: -1}, Progress: pr})
		done <- err
	}()
	stop, stopped := make(chan struct{}), make(chan struct{})
	var writes int
	go func() { // the command's progress writer (supervise), much faster
		defer close(stopped)
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
			writes++
			pr.Owned(func() {
				_ = os.WriteFile(filepath.Join(out, dataset.Workspace, "progress.json"), fmt.Appendf(nil, `{"n":%d}`, writes), 0o644)
			})
		}
	}()
	deadline := time.Now().Add(20 * time.Second)
	for {
		s := pr.Snapshot()
		if s.Parts >= 1 && s.Order != nil && s.Order.SpillLiveBytes > 0 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("the run ended before the replacement: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("no live spill with a published part: %+v %+v", s, s.Order)
		}
		time.Sleep(time.Millisecond)
	}
	copyTree(t, out, imp)
	if ents, _ := os.ReadDir(filepath.Join(imp, dataset.JournalDir, "spill")); len(ents) == 0 {
		t.Fatal("the copy holds no spill directory")
	}
	before := fileTree(t, imp)
	if err := os.Rename(out, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(imp, out); err != nil {
		t.Fatal(err)
	}
	err := <-done
	time.Sleep(20 * time.Millisecond) // the writer goes on after the run
	close(stop)
	<-stopped
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

// TestReplacedAfterCheckGetsNoPart: the output is replaced between the
// router's identity check and its creation of a part file (the fault point
// part-create), the window a loaded host opened in
// TestReplacedOutputIsNeverTouched. The part file is created relative to
// the directory the run locked (os.Root), so it lands in the moved
// directory, never in the impostor; the run stops with ErrOutputReplaced
// at its next check. Before, the file was created by pathname, in the
// impostor's .swath/parts.
func TestReplacedAfterCheckGetsNoPart(t *testing.T) {
	var keys []string
	for i := range 20000 {
		keys = append(keys, fmt.Sprintf("k%06d", i))
	}
	base := t.TempDir()
	imp := filepath.Join(base, "impostor")
	if _, err := runScripted(t, &scripted{keys: keys[:3000], chunk: 1000}, imp, false); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(imp, dataset.Workspace, dataset.PartsDir), 0o755); err != nil {
		t.Fatal(err)
	}
	before := fileTree(t, imp)
	out, moved := filepath.Join(base, "out"), filepath.Join(base, "moved")
	var n atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "part-create" && n.Add(1) == 3 {
			if err := os.Rename(out, moved); err != nil {
				return err
			}
			return os.Rename(imp, out)
		}
		return nil
	})
	defer restore()
	scriptMu.Lock()
	scriptNext = &scripted{keys: keys, chunk: 1000}
	scriptMu.Unlock()
	_, err := Run(context.Background(), Config{Source: SourceConfig{URI: "scripted://x"}, Output: OutputConfig{Dir: out},
		Limits: Limits{PartSize: 2 << 10, Workers: 4, Encoders: 1, RowBudget: 128 << 10, PartInterval: -1}})
	if n.Load() < 3 {
		t.Fatalf("part-create reached %d times", n.Load())
	}
	if !errors.Is(err, journal.ErrOutputReplaced) {
		t.Fatalf("err = %v, want ErrOutputReplaced", err)
	}
	if after := fileTree(t, out); !maps.Equal(before, after) {
		t.Fatalf("the impostor changed:\nbefore %v\nafter  %v", before, after)
	}
}
