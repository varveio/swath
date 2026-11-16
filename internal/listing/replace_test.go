// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/faults"
)

// visible is every regular file under dir but its workspace, by path, with
// its digest: what a reader of the dataset there can see.
func visible(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	m := map[string][32]byte{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		switch {
		case err != nil:
			return err
		case d.IsDir() && d.Name() == dataset.Workspace:
			return filepath.SkipDir
		case d.IsDir():
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		m[rel] = sha256.Sum256(b)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// TestReplaceKeepsPreviousUntilCommit: a run that replaces a committed
// dataset (--overwrite) leaves it as it was, every file, while it runs and
// when it stops before its commit (a kill at the third part's journal
// append): the previous dataset still reads whole. Its own run waits in
// the workspace, and a resume continues it there, then installs it: the
// output holds the new dataset alone, data/ exactly its parts, and no
// replacement is left, and its cut file is an uninterrupted run's. Before,
// --overwrite removed the previous dataset before listing anything (C-S1).
func TestReplaceKeepsPreviousUntilCommit(t *testing.T) {
	before := fakeS3(t, 3000)
	after := fakeS3(t, 2000)
	out := filepath.Join(t.TempDir(), "ds")
	if first, err := Run(context.Background(), crashConfig(before, out, false)); err != nil || first.Output.Replaced {
		t.Fatalf("first run: %v, replaced %v", err, first.Output.Replaced)
	}
	old, oldKeys := visible(t, out), checkContract(t, out)
	if len(oldKeys) != 3000 {
		t.Fatalf("%d keys", len(oldKeys))
	}
	var n int
	restore := faults.Set(func(p string) error {
		if p == "journal-append" {
			if n++; n == 3 {
				return faults.ErrCrash
			}
		}
		return nil
	})
	cfg := crashConfig(after, out, false)
	cfg.Output.Replace = true
	_, err := Run(context.Background(), cfg)
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("the killed replacement: %v", err)
	}
	if got := visible(t, out); !maps.Equal(got, old) {
		t.Fatalf("the previous dataset changed: %v, was %v", slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(old)))
	}
	if keys := checkContract(t, out); !slices.Equal(keys, oldKeys) || !dataset.Staged(out) {
		t.Fatalf("%d keys, staged %v", len(keys), dataset.Staged(out))
	}
	resume := crashConfig(after, out, true)
	var w warnings
	resume.Warn = w.warn
	sum, err := Run(context.Background(), resume)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	// Its cut file holds the retained parts' cuts, read where the
	// installation moved them: an uninterrupted run's.
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), crashConfig(after, ref, false)); err != nil {
		t.Fatal(err)
	}
	requireCuts(t, "resumed", out)
	sameCuts(t, "resumed", ref, out)
	if got := w.about(dataset.CutsName); len(got) > 0 {
		t.Fatalf("warnings %q", got)
	}
	if keys := checkContract(t, out); len(keys) != 2000 || sum.Invocation.Out != out || sum.Output.PartsRetained == 0 || !sum.Output.Replaced {
		t.Fatalf("after the resume: %d keys, out %q, %d parts retained", len(keys), sum.Invocation.Out, sum.Output.PartsRetained)
	}
	var rep Report
	readJSON(t, filepath.Join(out, dataset.ReportName), &rep)
	if rep.Invocation.Out != out || !rep.Invocation.Resume || !rep.Output.Replaced {
		t.Fatalf("run.json invocation %+v", rep.Invocation)
	}
	if _, err := os.Lstat(dataset.NextDir(out)); !os.IsNotExist(err) || dataset.Staged(out) {
		t.Fatalf("the replacement's directory is left: %v", err)
	}
	// Nothing staged: a resume finds the dataset complete.
	if _, err := Run(context.Background(), crashConfig(after, out, true)); !errors.Is(err, ErrComplete) {
		t.Fatalf("resume of the installed dataset: %v", err)
	}
}

// TestReplaceInstallationCrash: a kill inside the installation of a
// committed replacement (after the previous dataset's entries are retired,
// after the new data/ is moved in) leaves a state the next invocation
// finishes: InstallReplacement (swath resume) installs it, and a new
// --overwrite run installs it first, then replaces it in turn. Out holds no
// committed dataset in between.
func TestReplaceInstallationCrash(t *testing.T) {
	trees := make([]string, 3)
	for i := range trees {
		trees[i] = t.TempDir()
		for j := range 5 + i {
			mk(t, trees[i], fmt.Sprintf("t%d/f%02d", i, j))
		}
	}
	keys := func(i int) int { return 5 + i + 1 } // the files and their directory
	cfg := func(tree, out string, replace bool) Config {
		return Config{Source: SourceConfig{URI: tree}, Output: OutputConfig{Dir: out, Replace: replace}, Limits: Limits{PartSize: 1}}
	}
	for _, point := range []string{"install-retired", "install-data"} {
		for _, finish := range []string{"install", "overwrite"} {
			t.Run(point+"/"+finish, func(t *testing.T) {
				out := filepath.Join(t.TempDir(), "ds")
				if _, err := Run(context.Background(), cfg(trees[0], out, false)); err != nil {
					t.Fatal(err)
				}
				restore := faults.Set(func(p string) error {
					if p == point {
						return faults.ErrCrash
					}
					return nil
				})
				_, err := Run(context.Background(), cfg(trees[1], out, true))
				restore()
				if !errors.Is(err, faults.ErrCrash) {
					t.Fatalf("the killed installation: %v", err)
				}
				if dataset.Committed(out) || !dataset.InstallPending(out) {
					t.Fatalf("mid-installation: committed %v, pending %v", dataset.Committed(out), dataset.InstallPending(out))
				}
				want := 1
				switch finish {
				case "install":
					err = InstallReplacement(out)
				case "overwrite":
					_, err = Run(context.Background(), cfg(trees[2], out, true))
					want = 2
				}
				if err != nil {
					t.Fatal(err)
				}
				if got := checkContract(t, out); len(got) != keys(want) {
					t.Fatalf("%d keys, want tree %d's %d", len(got), want, keys(want))
				}
				if _, err := os.Lstat(dataset.NextDir(out)); !os.IsNotExist(err) {
					t.Fatalf("the replacement's directory is left: %v", err)
				}
			})
		}
	}
}

// TestReplaceFailureLeavesPrevious: a replacement of a local listing that
// fails before its commit, keeping no journal, leaves the previous dataset
// as it was and nothing of its own.
func TestReplaceFailureLeavesPrevious(t *testing.T) {
	src := t.TempDir()
	mk(t, src, "a/1", "a/2", "b")
	out := filepath.Join(t.TempDir(), "ds")
	if _, err := Run(context.Background(), Config{Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out}, Limits: Limits{PartSize: 1}}); err != nil {
		t.Fatal(err)
	}
	old := visible(t, out)
	boom := errors.New("boom")
	restore := faults.Set(func(p string) error {
		if p == "published" {
			return boom
		}
		return nil
	})
	mk(t, src, "c")
	_, err := Run(context.Background(), Config{Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out, Replace: true}, Limits: Limits{PartSize: 1}})
	restore()
	if !errors.Is(err, boom) {
		t.Fatalf("the failed replacement: %v", err)
	}
	if got := visible(t, out); !maps.Equal(got, old) {
		t.Fatalf("the previous dataset changed: %v", slices.Sorted(maps.Keys(got)))
	}
	if _, err := os.Lstat(dataset.NextDir(out)); !os.IsNotExist(err) {
		t.Fatalf("the failed replacement left its directory: %v", err)
	}
	checkContract(t, out)
}

// TestReplaceRefusedInstallation: an --overwrite whose retirement of the
// old dataset is refused after its manifest and data/ moved (Windows: a
// reader holds the old run.json) moves them back: the old dataset stands,
// the replacement stays pending, and a later installation finishes it.
// When data/ cannot come back either (a scanner holds one of its parts),
// the old manifest stays retired, never beside a data/ that is gone: out
// holds no dataset, the error names both refusals and swath resume, and
// the installation still finishes once the holders let go.
func TestReplaceRefusedInstallation(t *testing.T) {
	trees := []string{t.TempDir(), t.TempDir()}
	mk(t, trees[0], "a/1", "a/2")
	mk(t, trees[1], "a/1", "a/2", "b/1", "b/2")
	cfg := func(tree, out string, replace bool) Config {
		return Config{Source: SourceConfig{URI: tree}, Output: OutputConfig{Dir: out, Replace: replace}, Limits: Limits{PartSize: 1}}
	}
	held := errors.New("held by another program")
	scanned := errors.New("scanned by another program")
	for _, dataBack := range []bool{true, false} {
		t.Run(fmt.Sprintf("data-back=%v", dataBack), func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "ds")
			if _, err := Run(context.Background(), cfg(trees[0], out, false)); err != nil {
				t.Fatal(err)
			}
			old := checkContract(t, out)
			var retires, restores int
			restore := faults.Set(func(p string) error {
				switch {
				case p == "install-retire":
					if retires++; retires == 3 { // manifest.json and data/ moved; run.json refused
						return held
					}
				case p == "install-restore" && !dataBack:
					if restores++; restores == 1 { // data/, the first to go back
						return scanned
					}
				}
				return nil
			})
			_, err := Run(context.Background(), cfg(trees[1], out, true))
			restore()
			if !errors.Is(err, held) || !dataset.InstallPending(out) {
				t.Fatalf("the refused installation: %v (pending %v)", err, dataset.InstallPending(out))
			}
			if dataBack {
				if got := checkContract(t, out); len(got) != len(old) {
					t.Fatalf("after the rollback: %d keys, want the old %d", len(got), len(old))
				}
			} else {
				if dataset.Committed(out) {
					t.Fatal("the old manifest came back without its data/")
				}
				if !errors.Is(err, scanned) || !strings.Contains(err.Error(), "the rollback is incomplete") || !strings.Contains(err.Error(), "swath resume") {
					t.Fatalf("the incomplete rollback's error: %v", err)
				}
			}
			if err := InstallReplacement(out); err != nil {
				t.Fatal(err)
			}
			if got := checkContract(t, out); len(got) != 6 {
				t.Fatalf("after the installation: %d keys, want the replacement's 6", len(got))
			}
			if _, err := os.Lstat(dataset.NextDir(out)); !os.IsNotExist(err) {
				t.Fatalf("the replacement's directory is left: %v", err)
			}
		})
	}
}
