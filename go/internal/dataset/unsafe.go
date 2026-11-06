// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// ErrUnsafeLayout: a path swath acts on inside an output is not what swath
// made there: the workspace (.swath), data/, the parts or suspect
// directory is a symbolic link or not a directory, or the lock file, the
// journal or a part is a symbolic link or not a regular file. Each such
// path is checked without following links before swath creates, renames or
// removes anything through it, and the command refuses (exit 1, class
// fatal, not resumable). The refusal is a *LayoutError.
var ErrUnsafeLayout = errors.New("the output's layout is not one swath made")

// LayoutError is a refusal by ErrUnsafeLayout: Path, relative to the
// output, is Got where swath needs Want.
type LayoutError struct {
	Path, Want, Got string
}

func (e *LayoutError) Error() string {
	return fmt.Sprintf("%s in the output is %s, not %s; refusing to use it: %v", e.Path, e.Got, e.Want, ErrUnsafeLayout)
}

func (e *LayoutError) Unwrap() error { return ErrUnsafeLayout }

// kind names a file mode for a LayoutError.
func kind(m fs.FileMode) string {
	switch {
	case m&fs.ModeSymlink != 0:
		return "a symbolic link"
	case m.IsDir():
		return "a directory"
	case m.IsRegular():
		return "a regular file"
	}
	return "a special file (" + m.Type().String() + ")"
}

// typed checks fi (rel's lstat) is a directory when dir, else a regular
// file.
func typed(rel string, fi fs.FileInfo, dir bool) error {
	switch {
	case dir && !fi.Mode().IsDir():
		return &LayoutError{Path: rel, Want: "a directory", Got: kind(fi.Mode())}
	case !dir && !fi.Mode().IsRegular():
		return &LayoutError{Path: rel, Want: "a regular file", Got: kind(fi.Mode())}
	}
	return nil
}

// NeedIn checks rel (relative to the output root r) without following a
// link: a directory when dir, else a regular file. A missing path is nil,
// nil when optional; otherwise its error is returned.
func NeedIn(r *os.Root, rel string, dir, optional bool) (fs.FileInfo, error) {
	fi, err := r.Lstat(rel)
	switch {
	case optional && errors.Is(err, fs.ErrNotExist):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return fi, typed(rel, fi, dir)
}

// Need is NeedIn by pathname under out, for the checks made before the
// output is opened as a root (the lock).
func Need(out, rel string, dir, optional bool) (fs.FileInfo, error) {
	fi, err := os.Lstat(filepath.Join(out, rel))
	switch {
	case optional && errors.Is(err, fs.ErrNotExist):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return fi, typed(rel, fi, dir)
}

// OpenRegularIn opens rel through r with flag, refusing anything but a
// regular file found by lstat. The opened file is then compared with that
// lstat by identity (os.SameFile): this proves the descriptor is the
// inode found, not that every path component stayed the same in between
// (a link swapped in that resolves to the same inode passes).
func OpenRegularIn(r *os.Root, rel string, flag int) (*os.File, error) {
	fi, err := NeedIn(r, rel, false, false)
	if err != nil {
		return nil, err
	}
	f, err := r.OpenFile(rel, flag, 0)
	if err != nil {
		return nil, err
	}
	if got, err := f.Stat(); err != nil || !os.SameFile(fi, got) {
		f.Close()
		return nil, &LayoutError{Path: rel, Want: "the regular file found there", Got: "another file once opened"}
	}
	return f, nil
}
