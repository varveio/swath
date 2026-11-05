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
)

// Errors returned by Prepare.
var (
	ErrOutIsRoot  = errors.New("output is the listed root or one of its ancestors")
	ErrNotEmpty   = errors.New("output directory is not empty")
	ErrNotDataset = errors.New("output directory does not hold a previous swath-local dataset")
	ErrInsideRoot = errors.New("cannot replace an output inside the listed root")
	ErrRootNotDir = errors.New("root is not a directory")
)

// Prepare validates out against root and creates out/data. out may not be
// root or an ancestor of root (by path and by directory identity, along the
// given and the symlink-resolved root path). A non-empty out is refused
// unless replace is set, and replacing requires a previous dataset of this
// schema outside root; it removes only the dataset's own entries.
func Prepare(out, root, schemaID string, replace bool) error {
	absOut, err := filepath.Abs(out)
	if err != nil {
		return err
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if absOut == absRoot || isUnder(absRoot, absOut) {
		return fmt.Errorf("%s: %w", out, ErrOutIsRoot)
	}
	if fi, err := os.Stat(absOut); err == nil {
		if ancestorOrSelf(absRoot, fi) {
			return fmt.Errorf("%s: %w (same directory identity)", out, ErrOutIsRoot)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat output: %w", err)
	}
	if replace {
		inside, err := insideRoot(absRoot, absOut)
		if err != nil {
			return err
		}
		if inside {
			return fmt.Errorf("%s: %w", out, ErrInsideRoot)
		}
	}
	ents, err := os.ReadDir(absOut)
	switch {
	case err == nil && len(ents) > 0:
		if !replace {
			return fmt.Errorf("%s: %w", out, ErrNotEmpty)
		}
		if !isDataset(absOut, schemaID) {
			return fmt.Errorf("%s: %w", out, ErrNotDataset)
		}
		for _, name := range ownEntries {
			for _, n := range []string{name, name + ".tmp"} {
				if err := os.RemoveAll(filepath.Join(absOut, n)); err != nil {
					return fmt.Errorf("remove previous dataset: %w", err)
				}
			}
		}
	case err != nil && !errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("read output: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(absOut, DataDir), 0o755); err != nil {
		return fmt.Errorf("create output: %w", err)
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

// Cleanup removes a failed run's unpublished parts. Without _SUCCESS nothing
// was published.
func Cleanup(out string) {
	tmps, _ := filepath.Glob(filepath.Join(out, DataDir, "*.tmp"))
	for _, t := range tmps {
		_ = os.Remove(t)
	}
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
			SchemaID string `json:"schemaId"`
		}
		if json.Unmarshal(b, &m) == nil && m.SchemaID == schemaID {
			return true
		}
	}
	_, e1 := os.Stat(filepath.Join(out, SuccessName))
	fi, e2 := os.Stat(filepath.Join(out, DataDir))
	return e1 == nil && e2 == nil && fi.IsDir()
}
