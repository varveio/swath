// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/faults"
)

// Errors returned by Prepare.
var (
	ErrOutIsRoot  = errors.New("output is the listed root or one of its ancestors")
	ErrNotEmpty   = errors.New("output directory is not empty")
	ErrNotDataset = errors.New("output directory does not hold a previous swath dataset")
	// ErrPriorRelease is a resume of a directory that an interrupted 0.3 run left.
	ErrPriorRelease = errors.New("the output directory holds an interrupted run written by swath 0.3, which cannot be resumed; --overwrite replaces it")
	ErrInsideRoot   = errors.New("cannot replace an output inside the listed root")
	ErrRootNotDir   = errors.New("root is not a directory")
)

// Prepare validates out against root (Usable) and creates out/data. out may
// not be root or an ancestor of root (by path and by directory identity,
// along the given and the symlink-resolved root path). A non-empty out is
// refused unless replace is set, and replacing requires a previous dataset
// of this schema outside root; it removes only the dataset's own entries.
// An empty root means the source is not a local tree. The caller holds the
// output lock.
func Prepare(out, root, schemaID string, replace bool) error {
	absOut, err := Usable(out, root, schemaID, replace)
	if err != nil {
		return err
	}
	return prepareOut(absOut, replace)
}

// Usable applies Prepare's refusals without changing anything, so a run
// refuses an output it may not use before it creates its lock there.
// Prepare checks again under the lock. It returns out's absolute path.
func Usable(out, root, schemaID string, replace bool) (string, error) {
	absOut, err := filepath.Abs(out)
	if err != nil {
		return "", err
	}
	// inside reports whether out is inside root, asked only where it
	// decides the refusal.
	inside := func() (bool, error) { return false, nil }
	if root != "" { // a local listing: protect the listed tree
		absRoot, err := filepath.Abs(root)
		if err != nil {
			return "", err
		}
		if absOut == absRoot || isUnder(absRoot, absOut) {
			return "", fmt.Errorf("%s: %w", out, ErrOutIsRoot)
		}
		if fi, err := os.Stat(absOut); err == nil {
			if ancestorOrSelf(absRoot, fi) {
				return "", fmt.Errorf("%s: %w (same directory identity)", out, ErrOutIsRoot)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("stat output: %w", err)
		}
		inside = func() (bool, error) { return insideRoot(absRoot, absOut) }
		if replace {
			in, err := inside()
			if err != nil {
				return "", err
			}
			if in {
				return "", fmt.Errorf("%s: %w", out, ErrInsideRoot)
			}
		}
	}
	ents, err := os.ReadDir(absOut)
	switch {
	case err == nil && nonLock(absOut, ents) > 0:
		if !replace {
			// Inside the root, replacing is refused too: say so now,
			// before --overwrite is tried.
			if in, err := inside(); err == nil && in {
				return "", fmt.Errorf("%s: %w, and %w", out, ErrNotEmpty, ErrInsideRoot)
			}
			return "", fmt.Errorf("%s: %w", out, ErrNotEmpty)
		}
		if !isDataset(absOut, schemaID) {
			return "", fmt.Errorf("%s: %w", out, ErrNotDataset)
		}
	case err != nil && !errors.Is(err, os.ErrNotExist):
		return "", fmt.Errorf("read output: %w", err)
	}
	return absOut, nil
}

// prepareOut removes a previous dataset's own entries (Usable allowed it)
// and creates out/data.
func prepareOut(absOut string, replace bool) error {
	if ents, err := os.ReadDir(absOut); err == nil && replace && nonLock(absOut, ents) > 0 {
		// The commit record goes first, durably: from here the directory
		// holds no dataset, so a crash part way through the removal never
		// leaves a manifest naming parts already gone.
		for _, n := range commitEntries {
			if err := os.RemoveAll(filepath.Join(absOut, n)); err != nil {
				return fmt.Errorf("remove previous dataset: %w", err)
			}
		}
		if err := syncDir(absOut); err != nil {
			return fmt.Errorf("remove previous dataset: %w", err)
		}
		if err := faults.At("overwrite-uncommitted"); err != nil {
			return err
		}
		root, _ := os.ReadDir(absOut)
		for _, name := range ownEntries {
			names := []string{name, name + ".tmp"} // .tmp: earlier builds' temporary files
			for _, e := range root {
				if TempOf(name, e.Name()) {
					names = append(names, e.Name())
				}
			}
			for _, n := range names {
				if err := os.RemoveAll(filepath.Join(absOut, n)); err != nil {
					return fmt.Errorf("remove previous dataset: %w", err)
				}
			}
		}
		// Empty the workspace but keep the lock: its holder is this run.
		ws, _ := os.ReadDir(filepath.Join(absOut, Workspace))
		for _, e := range ws {
			if e.Name() != LockName {
				if err := os.RemoveAll(filepath.Join(absOut, Workspace, e.Name())); err != nil {
					return fmt.Errorf("remove previous run: %w", err)
				}
			}
		}
	}
	for _, d := range []string{DataDir, filepath.Join(Workspace, PartsDir)} {
		if err := os.MkdirAll(filepath.Join(absOut, d), 0o755); err != nil {
			return fmt.Errorf("create output: %w", err)
		}
	}
	// data/ and the parts directory must survive a power loss once a part
	// is recorded in them.
	if err := errors.Join(syncDir(filepath.Join(absOut, Workspace)), syncDir(absOut), syncDir(filepath.Dir(absOut))); err != nil {
		return fmt.Errorf("sync output: %w", err)
	}
	return nil
}

// A run that replaces the committed dataset in out (--overwrite) leaves it
// readable until the replacement is committed: it lists into NextDir(out),
// a dataset directory of its own inside out's workspace, and its commit
// installs that dataset in out (InstallNext); the replaced dataset's
// entries are removed after (RemoveNext).
const (
	nextName     = "next"
	retiringName = "retiring" // in NextDir: the replaced entries, while they are moved
	retiredName  = "retired"  // in NextDir: the replaced entries, all moved
)

// NextDir is the dataset directory of the run that replaces out's dataset.
func NextDir(out string) string { return filepath.Join(out, Workspace, nextName) }

// InstallPending reports whether NextDir(out) holds a committed dataset not
// yet installed in out (its manifest is still there).
func InstallPending(out string) bool {
	fi, err := os.Lstat(filepath.Join(NextDir(out), ManifestName))
	return err == nil && fi.Mode().IsRegular()
}

// Staged reports whether out holds a replacement to continue (an
// interrupted journaled run in NextDir(out)) or to install
// (InstallPending); the leftovers of one installed are neither.
func Staged(out string) bool {
	if InstallPending(out) {
		return true
	}
	if _, err := os.Lstat(filepath.Join(NextDir(out), retiredName)); err == nil {
		return false
	}
	return Resumable(NextDir(out))
}

// NextFound reports whether out holds a replacement's directory: one to
// continue (Staged), to install (InstallPending), or the leftovers of one
// installed, which RemoveNext removes.
func NextFound(out string) bool {
	_, err := os.Lstat(NextDir(out))
	return err == nil
}

// InstallNext installs the dataset committed in NextDir(out) in out, in
// place of out's (nothing when none is pending): first every entry of out's
// dataset moves into NextDir, the manifest first, so that from then on out
// holds no committed dataset (the retired directory, renamed into place
// once all are moved, records that); then the new dataset's data/ moves
// into out, and its manifest last: that rename is the installation. Each
// step is a rename, synced: a crash at any point leaves a state that
// InstallNext, run again, finishes. The part paths are reused: a reader of
// the old manifest during the installation can find its parts missing or
// replaced. The caller holds out's lock.
func InstallNext(out string) error {
	if !InstallPending(out) {
		return nil
	}
	next := NextDir(out)
	retired := filepath.Join(next, retiredName)
	if _, err := os.Lstat(retired); errors.Is(err, os.ErrNotExist) {
		retiring := filepath.Join(next, retiringName)
		if err := os.MkdirAll(retiring, 0o755); err != nil {
			return fmt.Errorf("retire the replaced dataset: %w", err)
		}
		for _, n := range append(slices.Clone(commitEntries), ownEntries...) {
			if _, err := os.Lstat(filepath.Join(out, n)); err == nil {
				if err := os.Rename(filepath.Join(out, n), filepath.Join(retiring, n)); err != nil {
					return fmt.Errorf("retire the replaced dataset: %w", err)
				}
			}
		}
		if err := errors.Join(syncDir(retiring), syncDir(out)); err != nil {
			return fmt.Errorf("retire the replaced dataset: %w", err)
		}
		if err := os.Rename(retiring, retired); err != nil {
			return fmt.Errorf("retire the replaced dataset: %w", err)
		}
		if err := syncDir(next); err != nil {
			return fmt.Errorf("retire the replaced dataset: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("install the replacement: %w", err)
	}
	if err := faults.At("install-retired"); err != nil {
		return err
	}
	for _, n := range ownEntries {
		if n == ManifestName {
			continue
		}
		if _, err := os.Lstat(filepath.Join(next, n)); err == nil {
			if err := os.Rename(filepath.Join(next, n), filepath.Join(out, n)); err != nil {
				return fmt.Errorf("install the replacement: %w", err)
			}
		}
	}
	if err := syncDir(out); err != nil {
		return fmt.Errorf("install the replacement: %w", err)
	}
	if err := faults.At("install-data"); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(next, ManifestName), filepath.Join(out, ManifestName)); err != nil {
		return fmt.Errorf("install the replacement: %w", err)
	}
	if err := syncDir(out); err != nil {
		return fmt.Errorf("install the replacement: %w", err)
	}
	return nil
}

// RemoveNext removes what an installed replacement left in NextDir(out),
// the replaced dataset among it, and the remains of a replacement that
// holds nothing to continue or install; a replacement Staged is kept.
func RemoveNext(out string) error {
	if Staged(out) {
		return nil
	}
	return os.RemoveAll(NextDir(out))
}

// LocateInside reports whether out (which must exist) lies below root, by
// walking out's physical ancestors and comparing directory identity with
// root. rel is out's '/'-separated path below root.
func LocateInside(root, out string) (rel string, inside bool, err error) {
	rootFi, err := os.Stat(root)
	if err != nil {
		return "", false, err
	}
	cur, err := filepath.EvalSymlinks(out)
	if err != nil {
		return "", false, err
	}
	var parts []string
	for {
		fi, err := os.Stat(cur)
		if err != nil {
			return "", false, err
		}
		if os.SameFile(fi, rootFi) {
			if len(parts) == 0 {
				return "", false, nil
			}
			slices.Reverse(parts)
			return strings.Join(parts, "/"), true, nil
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return "", false, nil
		}
		parts = append(parts, filepath.Base(cur))
		cur = parent
	}
}

// Cleanup removes a failed run's unpublished parts. Without a manifest
// nothing was published.
func Cleanup(out string) {
	_ = os.RemoveAll(filepath.Join(out, Workspace, PartsDir))
}

// isUnder reports whether path is strictly below dir (cleaned absolute paths).
func isUnder(path, dir string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// ancestorOrSelf reports whether target is root or an ancestor of root by
// identity, along the given and the symlink-resolved path.
func ancestorOrSelf(root string, target os.FileInfo) bool {
	chains := []string{root}
	if real, err := filepath.EvalSymlinks(root); err == nil && real != root {
		chains = append(chains, real)
	}
	for _, p := range chains {
		for cur := p; ; {
			if fi, err := os.Stat(cur); err == nil && os.SameFile(fi, target) {
				return true
			}
			parent := filepath.Dir(cur)
			if parent == cur {
				break
			}
			cur = parent
		}
	}
	return false
}

// insideRoot reports whether out (existing or not) lies below root, judged
// by the identity of its nearest existing ancestor-or-self.
func insideRoot(root, out string) (bool, error) {
	a := out
	for {
		if _, err := os.Lstat(a); err == nil {
			break
		}
		parent := filepath.Dir(a)
		if parent == a {
			return false, nil
		}
		a = parent
	}
	if a != out {
		rfi, err := os.Stat(root)
		if err != nil {
			return false, err
		}
		if afi, err := os.Stat(a); err == nil && os.SameFile(afi, rfi) {
			return true, nil
		}
	}
	_, inside, err := LocateInside(root, a)
	return inside, err
}

// isDataset reports whether out holds a previous output of this tool that
// a run of schemaID may replace: a current SeekParquet dataset of
// schemaID, the Java tool's (a schemaId manifest, or _SUCCESS beside
// data/), or an interrupted run's data/ and marks. A manifest of any other
// format, the format 2 of builds from 2026-11-05 to 2026-11-08 included,
// is refused whatever else the directory holds.
func isDataset(out, schemaID string) bool {
	if b, err := os.ReadFile(filepath.Join(out, ManifestName)); err == nil {
		var m struct {
			Format        *string `json:"format"`
			FormatVersion int64   `json:"format_version"`
			DraftRevision int64   `json:"draft_revision"`
			SchemaID      string  `json:"schema_id"`
			Legacy        string  `json:"schemaId"`
		}
		if json.Unmarshal(b, &m) == nil {
			switch {
			case m.Legacy != "":
				return true // the Java tool's format 1
			case m.Format == nil:
			case *m.Format != seekparquet.Format || m.FormatVersion != seekparquet.FormatVersion || m.DraftRevision != seekparquet.DraftRevision:
				return false
			case m.SchemaID == schemaID:
				return true
			}
		}
	}
	if InterruptedV03(out) {
		return true
	}
	_, e1 := os.Stat(filepath.Join(out, legacySuccess))
	fi, e2 := os.Stat(filepath.Join(out, DataDir))
	if e1 == nil && e2 == nil && fi.IsDir() {
		return true
	}
	if e2 != nil || !fi.IsDir() {
		return false
	}
	// An interrupted run: data/ and this tool's own marks, no manifest yet:
	// a journaled one's .swath/journal, or an unjournaled one's (a local
	// listing; nothing to resume) output lock or outcome file.
	for _, mark := range []string{filepath.Join(JournalDir, "journal"), filepath.Join(Workspace, LockName), ErrorName} {
		if _, err := os.Stat(filepath.Join(out, mark)); err == nil {
			return true
		}
	}
	return false
}

// InterruptedV03 reports whether out holds an interrupted run of swath 0.3
// that has not committed: its .swath-state.json or .swath/checkpoint.sqlite.
// 0.3 wrote the state file atomically before its first part or _staging/
// segment, so a _staging/ or data/ without one of these is not its (and is
// no evidence of anything). A committed directory is a dataset, not this.
func InterruptedV03(out string) bool {
	if Committed(out) {
		return false
	}
	for _, mark := range []string{".swath-state.json", filepath.Join(Workspace, "checkpoint.sqlite")} {
		if fi, err := os.Lstat(filepath.Join(out, mark)); err == nil && fi.Mode().IsRegular() {
			return true
		}
	}
	return false
}

// Resumable reports whether out holds an interrupted journaled run.
func Resumable(out string) bool {
	_, e2 := os.Stat(filepath.Join(out, JournalDir, "journal"))
	return !Committed(out) && e2 == nil
}

// nonLock counts the entries of out other than a workspace that holds
// only the lock (a fresh run takes the lock before preparing).
func nonLock(out string, ents []os.DirEntry) int {
	n := 0
	for _, e := range ents {
		if e.Name() == Workspace {
			ws, err := os.ReadDir(filepath.Join(out, Workspace))
			if err == nil && len(ws) == 1 && ws[0].Name() == LockName {
				continue
			}
		}
		n++
	}
	return n
}
