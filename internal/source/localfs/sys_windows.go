// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package localfs

import (
	"io/fs"
	"syscall"
)

// statLinks: no tier reads a link count, device or inode here.
const statLinks = false

// fillSys: CreationTime is a birth time (Windows has no POSIX ctime through
// FileInfo) and LastAccessTime is atime. uid, gid, inode, dev, nlink and
// blocks stay null.
func fillSys(r *Entry, fi fs.FileInfo) {
	d, ok := fi.Sys().(*syscall.Win32FileAttributeData)
	if !ok {
		return
	}
	r.Btime = d.CreationTime.Nanoseconds() / 1000
	r.Atime = d.LastAccessTime.Nanoseconds() / 1000
	r.Present |= HasBtime | HasAtime
}
