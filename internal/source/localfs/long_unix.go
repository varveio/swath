// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package localfs

import (
	"io/fs"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/internal/longpath"
)

// tooLong reports the kernel's refusal of a path by its length.
func tooLong(err error) bool { return longpath.TooLong(err) }

// atParent calls fn with the directory holding a root that is not a
// directory, opened by its path (longpath), and the root's name: a file
// root whose path the kernel refused by its length (expandFile). It
// accounts for the attempt in Stats.LongPaths.
func atParent(path string, st *Stats, fn func(dir int, name string) error) error {
	dirPath, name := filepath.Split(path)
	st.LongPaths.attempt(0, len(path))
	dir, err := longpath.OpenFD(dirPath, longpath.Search|unix.O_DIRECTORY)
	if err == nil {
		err = fn(dir, name)
		unix.Close(dir)
	}
	if err != nil {
		st.LongPaths.fail(openReason(err))
		return &os.PathError{Op: "stat", Path: path, Err: err}
	}
	st.LongPaths.Opened++
	return nil
}

// statLong stats a file root at tier f relative to its directory, keeping
// the tier's columns: only the kind at the names tier.
func statLong(path string, f Fields, r *Entry, st *Stats) error {
	return atParent(path, st, func(dir int, name string) error {
		if err := statAt(dir, name, f, r); err != nil {
			return err
		}
		if f == Names {
			*r = Entry{Kind: r.Kind}
		}
		return nil
	})
}

// readlinkLong reads a file root's link target relative to its directory.
func readlinkLong(path string, st *Stats) (target string, err error) {
	err = atParent(path, st, func(dir int, name string) error {
		target, err = readlinkAt(dir, name)
		return err
	})
	return target, err
}

// statEntry fills r from one fstatat of d relative to its open directory
// (portable engine), never by its path: os.DirEntry.Info is an lstat of
// the full path, which resolves every name of it again for each entry
// (following a directory replaced by a symbolic link since the walk
// opened it), fails past the length limit and, on darwin, leaves out the
// fields os.FileInfo does not expose.
func statEntry(dir *os.File, d fs.DirEntry, r *Entry) error {
	return statAt(int(dir.Fd()), d.Name(), Stat, r)
}

// readlinkIn reads the target of the link name in the open directory dir
// (path names it): relative, as the getdents engine reads it, so a path
// past the length limit reads too.
func readlinkIn(dir *os.File, _, name string) (string, error) { return readlinkAt(int(dir.Fd()), name) }

// readlinkAt reads a symlink target relative to dirfd.
func readlinkAt(dirfd int, name string) (string, error) {
	for size := 256; ; size *= 2 {
		buf := make([]byte, size)
		n, err := unix.Readlinkat(dirfd, name, buf)
		if err != nil {
			return "", err
		}
		if n < size {
			return string(buf[:n]), nil
		}
	}
}
