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

	"github.com/varveio/swath/go/internal/faults"
)

// Errors returned by Prepare.
var (
	ErrOutIsRoot  = errors.New("output is the listed root or one of its ancestors")
	ErrNotEmpty   = errors.New("output directory is not empty")
	ErrNotDataset = errors.New("output directory does not hold a previous swath-local dataset")
	ErrInsideRoot = errors.New("cannot replace an output inside the listed root")
	ErrRootNotDir = errors.New("root is not a directory")
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
		if replace {
			inside, err := insideRoot(absRoot, absOut)
			if err != nil {
				return "", err
			}
			if inside {
				return "", fmt.Errorf("%s: %w", out, ErrInsideRoot)
			}
		}
	}
	ents, err := os.ReadDir(absOut)
	switch {
	case err == nil && nonLock(absOut, ents) > 0:
		if !replace {
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

func isDataset(out, schemaID string) bool {
	if b, err := os.ReadFile(filepath.Join(out, ManifestName)); err == nil {
		var m struct {
			SchemaID string `json:"schema_id"`
			Legacy   string `json:"schemaId"`
		}
		if json.Unmarshal(b, &m) == nil && (m.SchemaID == schemaID || m.Legacy != "") {
			return true // this tool's dataset, of this or an earlier format
		}
	}
	_, e1 := os.Stat(filepath.Join(out, legacySuccess))
	fi, e2 := os.Stat(filepath.Join(out, DataDir))
	if e1 == nil && e2 == nil && fi.IsDir() {
		return true
	}
	// An interrupted journaled run: data/ and .swath/journal, no manifest yet.
	_, e3 := os.Stat(filepath.Join(out, JournalDir, "journal"))
	return e2 == nil && fi.IsDir() && e3 == nil
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
