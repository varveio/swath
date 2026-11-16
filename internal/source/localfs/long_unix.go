// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/internal/longpath"
)

// tooLong reports the kernel's refusal of a path by its length.
func tooLong(err error) bool { return longpath.TooLong(err) }

// errOtherDevice: a component openLong opened is on another device than
// the root's (OneFilesystem).
var errOtherDevice = errors.New("on another device than the root")

// openLong opens r's directory for reading after the kernel refused its
// full path by its length: the root by its path, as the full path's
// resolution reaches it (longpath, a symbolic link followed), then each
// name of r's key prefix relative to the directory before it, never
// following a symbolic link (O_NOFOLLOW); each intermediate is closed as
// the next is opened. The root is opened again for every such directory
// rather than held for the walk: the walk's full-path opens resolve the
// root afresh too, so both see the same root, and the provider holds no
// descriptor beyond a listing.
//
// It reports whether the directory was opened, and accounts for the
// attempt (Stats.LongPaths); a failure is counted and named as a failed
// open is (OpenErrors), and with OneFilesystem a component on another
// device stops it as emit stops at a mount point (OtherDevice). err is the
// context's error when the run stopped during the descent.
func (p *Provider) openLong(ctx context.Context, r Range, st *Stats) (fd int, ok bool, err error) {
	names := longpath.Components(r.prefix)
	st.LongPaths.attempt(len(names), len(r.path))
	fd, err = p.descend(ctx, names)
	switch {
	case err == nil:
		st.LongPaths.Opened++
		return fd, true, nil
	case ctx.Err() != nil && errors.Is(err, ctx.Err()):
		return -1, false, err
	}
	st.LongPaths.fail(longReason(err))
	if errors.Is(err, errOtherDevice) {
		st.OtherDevice++
		return -1, false, nil
	}
	st.note(&st.OpenErrors, r.path, err)
	p.unreadableDir(r)
	return -1, false, nil
}

// descend opens the directory names name below the root (openLong).
func (p *Provider) descend(ctx context.Context, names []string) (int, error) {
	mode := unix.O_RDONLY
	if len(names) > 0 {
		mode = longpath.Search
	}
	dir, err := longpath.OpenFD(p.root, mode|unix.O_DIRECTORY)
	if err != nil {
		return -1, fmt.Errorf("the root: %w", err)
	}
	for i, name := range names {
		if err := ctx.Err(); err != nil {
			unix.Close(dir)
			return -1, err
		}
		mode := longpath.Search
		if i == len(names)-1 {
			mode = unix.O_RDONLY
		}
		fd, err := unix.Openat(dir, name, mode|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			err = classifyOpen(dir, name, err)
			unix.Close(dir)
			return -1, fmt.Errorf("level %d below the root: %w", i+1, err)
		}
		unix.Close(dir)
		dir = fd
		if p.opt.OneFilesystem {
			var s unix.Stat_t
			if err := unix.Fstat(dir, &s); err != nil || int64(s.Dev) != p.dev { //nolint:unconvert // Dev's type differs by platform
				unix.Close(dir)
				if err == nil {
					err = errOtherDevice
				}
				return -1, fmt.Errorf("level %d below the root: %w", i+1, err)
			}
		}
	}
	return dir, nil
}

// errSymlink: a component openLong would open is a symbolic link, which it
// never follows.
var errSymlink = errors.New("a symbolic link, not followed")

// classifyOpen tells a symbolic link apart from another non-directory when
// a no-follow directory open of name fails: O_NOFOLLOW refuses a link as
// ELOOP, but O_PATH with O_DIRECTORY as ENOTDIR.
func classifyOpen(dir int, name string, err error) error {
	if !errors.Is(err, unix.ENOTDIR) && !errors.Is(err, unix.ELOOP) {
		return err
	}
	var s unix.Stat_t
	if unix.Fstatat(dir, name, &s, unix.AT_SYMLINK_NOFOLLOW) == nil && s.Mode&unix.S_IFMT == unix.S_IFLNK {
		return errSymlink
	}
	return err
}

// longReason classifies a failed openLong for Stats.LongPaths.Failed.
func longReason(err error) string {
	switch {
	case errors.Is(err, errOtherDevice):
		return "other_device"
	case errors.Is(err, errSymlink):
		return "symlink"
	case errors.Is(err, unix.ENOENT):
		return "not_found"
	case errors.Is(err, unix.EACCES), errors.Is(err, unix.EPERM):
		return "permission"
	case errors.Is(err, unix.ENOTDIR):
		return "not_directory"
	case errors.Is(err, unix.ENAMETOOLONG):
		return "name_too_long"
	}
	return "other"
}

// atParent calls fn with the directory holding a root that is not a
// directory, opened by its path (longpath), and the root's name: a file
// root whose path the kernel refused by its length (expandFile). It
// accounts for the attempt as openLong does.
func atParent(path string, st *Stats, fn func(dir int, name string) error) error {
	dirPath, name := filepath.Split(path)
	st.LongPaths.attempt(0, len(path))
	dir, err := longpath.OpenFD(dirPath, longpath.Search|unix.O_DIRECTORY)
	if err == nil {
		err = fn(dir, name)
		unix.Close(dir)
	}
	if err != nil {
		st.LongPaths.fail(longReason(err))
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
