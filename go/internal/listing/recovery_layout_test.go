// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/journal"
)

// layoutRun is an interrupted journaled output in base/dataset, locked,
// with records parts (one row each, keys "a", "b", ...) published in data/,
// and base/unrelated, a directory swath does not own, holding sentinel
// files.
type layoutRun struct {
	out, foreign string
	id           journal.Identity
	lock         *journal.Lock
}

func newLayoutRun(t *testing.T, records int) *layoutRun {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("journaled output is refused on Windows")
	}
	base := t.TempDir()
	lr := &layoutRun{out: filepath.Join(base, "dataset"), foreign: filepath.Join(base, "unrelated"),
		id: journal.Identity{Tool: "swath-go", Format: 2, SchemaID: "layout"}}
	if err := os.MkdirAll(lr.foreign, 0o755); err != nil {
		t.Fatal(err)
	}
	lock, err := journal.TakeLock(lr.out)
	if err != nil {
		t.Fatal(err)
	}
	lr.lock = lock
	t.Cleanup(lock.Release)
	if err := os.MkdirAll(filepath.Join(lr.out, dataset.DataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	j, err := journal.Create(lr.out, lr.id, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range records {
		body := []byte("part body " + dataset.PartName(i))
		if err := os.WriteFile(filepath.Join(lr.out, dataset.DataDir, dataset.PartName(i)), body, 0o644); err != nil {
			t.Fatal(err)
		}
		sum := md5.Sum(body)
		key := dataset.Key(string(rune('a' + i)))
		rec := journal.Record{Ordinal: i, RowsTotal: int64(i + 1),
			Part: dataset.PartInfo{Rows: 1, Bytes: int64(len(body)), MinKey: key, MaxKey: key, MD5: hex.EncodeToString(sum[:])}}
		if err := j.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	return lr
}

// sentinel writes a file named name into the unrelated directory.
func (lr *layoutRun) sentinel(t *testing.T, name string) string {
	t.Helper()
	p := filepath.Join(lr.foreign, name)
	if err := os.WriteFile(p, []byte("not dataset data"), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// resume runs the resume's recovery (openLocked) on the output.
func (lr *layoutRun) resume(t *testing.T) error {
	t.Helper()
	if err := lr.lock.Verify(); err != nil {
		t.Fatal(err) // the lock's identity checks pass: only a path below the root changed
	}
	cfg := Config{Output: OutputConfig{Dir: lr.out, Resume: true}}
	jr, err := openLocked(context.Background(), cfg, "", lr.id, lr.lock)
	if jr != nil && jr.j != nil {
		jr.j.Close()
	}
	return err
}

func requireRefusedAndKept(t *testing.T, err error, kept ...string) {
	t.Helper()
	for _, p := range kept {
		if _, serr := os.Stat(p); serr != nil {
			t.Errorf("recovery removed %s outside the output: %v", p, serr)
		}
	}
	if !errors.Is(err, ErrUnsafeLayout) {
		t.Fatalf("recovery error %v, want a refusal with ErrUnsafeLayout", err)
	}
}

// TestRecoveryRefusesSymlinkedData: an interrupted output whose data/ was
// replaced by a link to an unrelated directory (the lock's identity checks
// still pass) is refused before recovery reads or removes anything there
// (review R1, 2026-11-06: recovery returned nil after cleanup deleted the
// unrelated directory's files).
func TestRecoveryRefusesSymlinkedData(t *testing.T) {
	lr := newLayoutRun(t, 0)
	keep := lr.sentinel(t, "keep-me.txt")
	data := filepath.Join(lr.out, dataset.DataDir)
	if err := os.Remove(data); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(lr.foreign, data); err != nil {
		t.Fatal(err)
	}
	err := lr.resume(t)
	requireRefusedAndKept(t, err, keep)
	var le *LayoutError
	if !errors.As(err, &le) || le.Path != dataset.DataDir {
		t.Fatalf("refusal %v does not name %s", err, dataset.DataDir)
	}
}

// TestRecoveryRefusesSymlinkedSuspectDir: the suspect directory replaced
// by a link: recovery would remove the "reproduced" suspects in it, files
// named like parts in an unrelated directory.
func TestRecoveryRefusesSymlinkedSuspectDir(t *testing.T) {
	lr := newLayoutRun(t, 1)
	keep := lr.sentinel(t, dataset.PartName(0))
	if err := os.Symlink(lr.foreign, filepath.Join(lr.out, dataset.Workspace, dataset.SuspectDir)); err != nil {
		t.Fatal(err)
	}
	requireRefusedAndKept(t, lr.resume(t), keep)
}

// TestRecoveryRefusesNonRegularPart: a retained part that is a link (here
// to a file outside the output with the recorded bytes, which would pass
// verification) is refused, not kept in the dataset.
func TestRecoveryRefusesNonRegularPart(t *testing.T) {
	lr := newLayoutRun(t, 1)
	part := filepath.Join(lr.out, dataset.DataDir, dataset.PartName(0))
	body, err := os.ReadFile(part)
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(lr.foreign, "copy.parquet")
	if err := os.WriteFile(outside, body, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(part); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, part); err != nil {
		t.Fatal(err)
	}
	requireRefusedAndKept(t, lr.resume(t), outside)
	if fi, err := os.Lstat(part); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("the refused output was changed: %v %v", fi, err)
	}
}

// TestRecoveryKeepsRegularLayout: the checks pass an output swath made.
func TestRecoveryKeepsRegularLayout(t *testing.T) {
	lr := newLayoutRun(t, 2)
	if err := lr.resume(t); err != nil {
		t.Fatal(err)
	}
}

// TestRecoveryRefusesLinkedJournal: .swath/journal replaced by a link to a
// copy elsewhere, with a valid header and one byte of an incomplete frame
// (a torn tail): the resume refuses before it opens the journal for
// writing, and the copy keeps its byte (tenth durability review: recovery
// read the journal through the link and truncated the external file).
func TestRecoveryRefusesLinkedJournal(t *testing.T) {
	lr := newLayoutRun(t, 0)
	jpath := journal.Path(lr.out)
	b, err := os.ReadFile(jpath)
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, 0x01)
	outside := filepath.Join(lr.foreign, "journal")
	if err := os.WriteFile(outside, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(jpath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, jpath); err != nil {
		t.Fatal(err)
	}
	err = lr.resume(t)
	if got, rerr := os.ReadFile(outside); rerr != nil || !bytes.Equal(got, b) {
		t.Errorf("the journal outside the output changed: %d bytes, want %d (%v)", len(got), len(b), rerr)
	}
	requireRefusedAndKept(t, err, outside)
}

// TestLockRefusesLinkedWorkspace: an output whose .swath is a link to a
// directory elsewhere is refused before the lock file is created there.
func TestLockRefusesLinkedWorkspace(t *testing.T) {
	base := t.TempDir()
	out, foreign := filepath.Join(base, "dataset"), filepath.Join(base, "unrelated")
	for _, d := range []string{filepath.Join(out, dataset.DataDir), foreign} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(foreign, filepath.Join(out, dataset.Workspace)); err != nil {
		t.Fatal(err)
	}
	lock, err := journal.TakeLock(out)
	if err == nil {
		lock.Release()
	}
	if ents, _ := os.ReadDir(foreign); len(ents) != 0 {
		t.Errorf("the lock created %v outside the output", ents)
	}
	if !errors.Is(err, ErrUnsafeLayout) {
		t.Fatalf("TakeLock error %v, want a refusal with ErrUnsafeLayout", err)
	}
}

// TestRestartRefusesLinkedParts: a journal whose header never became
// durable restarts the run, which empties the workspace; a linked parts
// directory is refused first, as recovery would.
func TestRestartRefusesLinkedParts(t *testing.T) {
	lr := newLayoutRun(t, 0)
	if err := os.WriteFile(journal.Path(lr.out), nil, 0o644); err != nil { // the header never durable
		t.Fatal(err)
	}
	keep := lr.sentinel(t, "keep-me.txt")
	parts := filepath.Join(lr.out, dataset.Workspace, dataset.PartsDir)
	os.RemoveAll(parts)
	if err := os.Symlink(lr.foreign, parts); err != nil {
		t.Fatal(err)
	}
	requireRefusedAndKept(t, lr.resume(t), keep)
}

// TestRecoveryRefusesLinkedSuspectUnchanged: a current suspect that is a
// link, in an output where recovery would first publish a claimed part and
// remove an unrecorded file from data/: the refusal comes before either,
// and the output is as it was (tenth durability review: the suspect was
// checked only after those mutations).
func TestRecoveryRefusesLinkedSuspectUnchanged(t *testing.T) {
	lr := newLayoutRun(t, 1)
	final := filepath.Join(lr.out, dataset.DataDir, dataset.PartName(0))
	claim := dataset.ClaimPath(lr.out, 0)
	if err := os.MkdirAll(filepath.Dir(claim), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(final, claim); err != nil { // recorded, not yet renamed into data/
		t.Fatal(err)
	}
	stray := filepath.Join(lr.out, dataset.DataDir, "stray.tmp")
	if err := os.WriteFile(stray, []byte("unrecorded"), 0o644); err != nil {
		t.Fatal(err)
	}
	outside := lr.sentinel(t, "suspect.parquet")
	sus := filepath.Join(lr.out, dataset.Workspace, dataset.SuspectDir, dataset.PartName(1))
	if err := os.MkdirAll(filepath.Dir(sus), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, sus); err != nil {
		t.Fatal(err)
	}
	err := lr.resume(t)
	for _, p := range []string{claim, stray, sus} {
		if _, serr := os.Lstat(p); serr != nil {
			t.Errorf("the refused output changed: %s: %v", p, serr)
		}
	}
	if _, serr := os.Lstat(final); serr == nil {
		t.Errorf("the refused output changed: the claimed part was published")
	}
	requireRefusedAndKept(t, err, outside)
}
