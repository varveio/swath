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
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
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

// TestLockRefusesLinkedJournal: a real workspace with no lock file whose
// journal is a link to a file elsewhere: taking the lock, for a run or for
// the refusal's settlement, is refused for the journal before the lock
// file is created (eleventh durability review: TakeLock typed the
// workspace and the lock leaf only, so a lock was created and taken before
// the resume refused the journal).
func TestLockRefusesLinkedJournal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symbolic links need privileges on Windows")
	}
	base := t.TempDir()
	out, foreign := filepath.Join(base, "dataset"), filepath.Join(base, "unrelated")
	for _, d := range []string{filepath.Join(out, dataset.Workspace), foreign} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	outside := filepath.Join(foreign, "journal")
	if err := os.WriteFile(outside, []byte("not this output's journal"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, journal.Path(out)); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(out, journal.LockName)
	lock, err := journal.TakeLock(out)
	if err == nil {
		lock.Release()
	}
	if _, serr := os.Lstat(lockPath); serr == nil {
		t.Errorf("TakeLock created %s before refusing the journal", lockPath)
	}
	var le *LayoutError
	if !errors.As(err, &le) || le.Path != filepath.Join(journal.Dir, journal.Name) {
		t.Fatalf("TakeLock error %v, want a refusal naming the journal", err)
	}
	why := SettleUnfinished(out, "an unfinished run", func() error {
		t.Error("the settlement wrote its outcome")
		return nil
	})
	if _, serr := os.Lstat(lockPath); serr == nil {
		t.Errorf("the settlement created %s before refusing the journal", lockPath)
	}
	if !strings.Contains(why, ErrUnsafeLayout.Error()) {
		t.Fatalf("settlement unrecorded %q, want the layout refusal", why)
	}
}

// TestReadRefusesLinkedWorkspace: the early journal read (the command's,
// before any lock) of an output whose .swath is a link to a directory
// elsewhere holding a journal refuses with ErrUnsafeLayout naming the
// workspace (eleventh durability review: the journal leaf was opened first
// and the root's escape error surfaced, not the unsafe_layout cause).
func TestReadRefusesLinkedWorkspace(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symbolic links need privileges on Windows")
	}
	base := t.TempDir()
	out, foreign := filepath.Join(base, "dataset"), filepath.Join(base, "unrelated")
	for _, d := range []string{out, foreign} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(foreign, journal.Name), []byte("a journal elsewhere"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(foreign, filepath.Join(out, dataset.Workspace)); err != nil {
		t.Fatal(err)
	}
	_, err := journal.Read(out)
	var le *LayoutError
	if !errors.As(err, &le) || le.Path != dataset.Workspace {
		t.Fatalf("journal.Read error %v, want a refusal with ErrUnsafeLayout naming %s", err, dataset.Workspace)
	}
}

// restartRun is an interrupted output whose journal header never became
// durable (an empty journal) and nothing in data/ or the temporary parts
// directory, which a resume starts over: the preparation removes the
// workspace's entries. link puts a link, the returned path, at rel.
func restartRun(t *testing.T, rel, target string) (lr *layoutRun, link string) {
	t.Helper()
	lr = newLayoutRun(t, 0)
	if err := os.WriteFile(journal.Path(lr.out), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	link = filepath.Join(lr.out, rel)
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(link)
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	return lr, link
}

// requireRestartRefused: the restart was refused before the preparation
// removed anything: the link and the journal are still there.
func requireRestartRefused(t *testing.T, lr *layoutRun, link string, err error) {
	t.Helper()
	if fi, serr := os.Lstat(link); serr != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Errorf("the restart removed or replaced %s: %v", link, serr)
	}
	if _, serr := os.Lstat(journal.Path(lr.out)); serr != nil {
		t.Errorf("the restart removed the journal: %v", serr)
	}
	if !errors.Is(err, ErrUnsafeLayout) {
		t.Fatalf("restart error %v, want a refusal with ErrUnsafeLayout", err)
	}
}

// TestRestartRefusesLinkedParts: a linked parts directory, to an empty
// directory elsewhere (so nothing counts as a part and the run would start
// over), is refused before the preparation removes the workspace's
// entries.
func TestRestartRefusesLinkedParts(t *testing.T) {
	foreign := t.TempDir()
	lr, link := restartRun(t, partsRel, foreign)
	requireRestartRefused(t, lr, link, lr.resume(t))
	if ents, _ := os.ReadDir(foreign); len(ents) != 0 {
		t.Errorf("the restart wrote %v outside the output", ents)
	}
}

// TestRestartRefusesLinkedSuspect: a suspect that is a link to a file
// elsewhere is refused before the restart's preparation removes the
// suspect directory (eleventh durability review: the restart returned
// before the suspect entries were checked).
func TestRestartRefusesLinkedSuspect(t *testing.T) {
	outside := filepath.Join(t.TempDir(), "suspect.parquet")
	if err := os.WriteFile(outside, []byte("not dataset data"), 0o644); err != nil {
		t.Fatal(err)
	}
	lr, link := restartRun(t, filepath.Join(suspectRel, dataset.PartName(0)), outside)
	requireRestartRefused(t, lr, link, lr.resume(t))
	if _, err := os.Stat(outside); err != nil {
		t.Errorf("the restart removed %s outside the output: %v", outside, err)
	}
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

// TestRecoveryPublishesThroughRoot: the publication of a recorded part
// whose rename did not happen syncs data/ through the output root, not by
// pathname (tenth durability review: PublishPartWith's sync opened
// out/data by name). The output is moved away between the rename and the
// sync, and an empty directory put at its path: the sync still reaches the
// moved output's data/, and the resume stops at the next identity check
// (journal.ErrOutputReplaced) instead of failing to open the impostor's
// data/ by name.
func TestRecoveryPublishesThroughRoot(t *testing.T) {
	lr := newLayoutRun(t, 1)
	final := filepath.Join(lr.out, dataset.DataDir, dataset.PartName(0))
	claim := dataset.ClaimPath(lr.out, 0)
	if err := os.MkdirAll(filepath.Dir(claim), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(final, claim); err != nil {
		t.Fatal(err)
	}
	moved := lr.out + ".moved"
	var once sync.Once
	defer faults.Set(func(point string) error {
		if point == "renamed-unsynced" {
			once.Do(func() {
				if err := os.Rename(lr.out, moved); err != nil {
					t.Error(err)
				}
				if err := os.Mkdir(lr.out, 0o755); err != nil {
					t.Error(err)
				}
			})
		}
		return nil
	})()
	err := lr.resume(t)
	if !errors.Is(err, journal.ErrOutputReplaced) || strings.Contains(err.Error(), "sync data directory") {
		t.Fatalf("resume error %v, want the replacement found after a sync through the root", err)
	}
	if _, serr := os.Stat(filepath.Join(moved, dataset.DataDir, dataset.PartName(0))); serr != nil {
		t.Fatalf("the part was not published in the moved output: %v", serr)
	}
	if ents, _ := os.ReadDir(lr.out); len(ents) != 0 {
		t.Fatalf("the directory at the output's path was written: %v", ents)
	}
}
