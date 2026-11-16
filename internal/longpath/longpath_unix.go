// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

// Package longpath opens and stats a path longer than the kernel takes in
// one argument (PATH_MAX: 4,096 bytes on Linux, 1,024 on macOS). A path
// that fits takes the one call it takes without this package; only a path
// the kernel refuses by its length (ENAMETOOLONG) is resolved again, one
// component at a time, each opened relative to the directory before it as
// the kernel resolves a path: symbolic links followed, ".." the parent.
// Windows needs none of this: Go turns a long path into its extended-length
// form there.
package longpath

import (
	"errors"
	"io/fs"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// TooLong reports the kernel's refusal of a path by its length.
func TooLong(err error) bool { return errors.Is(err, unix.ENAMETOOLONG) }

// Components splits a relative slash-separated path into its names,
// leaving out empty ones and ".".
func Components(path string) []string {
	var names []string
	for name := range strings.SplitSeq(path, "/") {
		if name != "" && name != "." {
			names = append(names, name)
		}
	}
	return names
}

// OpenFD opens path with flags (O_CLOEXEC added), as open(2) does, a path
// the kernel refuses by its length one component at a time; every
// directory before the last name is opened with Search, which on Linux
// needs no read permission on it, as a path's own resolution needs none.
// An error is the kernel's, unwrapped.
func OpenFD(path string, flags int) (int, error) {
	fd, err := unix.Open(path, flags|unix.O_CLOEXEC, 0)
	if !TooLong(err) {
		return fd, err
	}
	dir := unix.AT_FDCWD
	if strings.HasPrefix(path, "/") {
		if dir, err = unix.Open("/", Search|unix.O_DIRECTORY|unix.O_CLOEXEC, 0); err != nil {
			return -1, err
		}
	}
	names := Components(path)
	if len(names) == 0 {
		names = []string{"."}
	}
	for i, name := range names {
		mode := Search | unix.O_DIRECTORY
		if i == len(names)-1 {
			mode = flags
		}
		fd, err = unix.Openat(dir, name, mode|unix.O_CLOEXEC, 0)
		if dir != unix.AT_FDCWD {
			unix.Close(dir)
		}
		if err != nil {
			return -1, err
		}
		dir = fd
	}
	return dir, nil
}

// Open is os.Open for a path of any length.
func Open(path string) (*os.File, error) {
	f, err := os.Open(path)
	if !TooLong(err) {
		return f, err
	}
	fd, err := OpenFD(path, unix.O_RDONLY)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(fd), path), nil
}

// Stat is os.Stat for a path of any length.
func Stat(path string) (fs.FileInfo, error) {
	fi, err := os.Stat(path)
	if !TooLong(err) {
		return fi, err
	}
	fd, err := OpenFD(path, statOnly)
	if err != nil {
		return nil, &fs.PathError{Op: "stat", Path: path, Err: err}
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	return f.Stat()
}
