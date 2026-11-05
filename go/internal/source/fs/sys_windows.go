// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package fs

import (
	"io/fs"
	"syscall"

	"github.com/varveio/swath/go/internal/schema"
)

// fillSys: CreationTime is a birth time (Windows has no POSIX ctime through
// FileInfo) and LastAccessTime is atime. uid, gid, inode, dev, nlink and
// blocks stay null.
func fillSys(r *schema.Row, fi fs.FileInfo) {
	d, ok := fi.Sys().(*syscall.Win32FileAttributeData)
	if !ok {
		return
	}
	r.Btime = d.CreationTime.Nanoseconds() / 1000
	r.Atime = d.LastAccessTime.Nanoseconds() / 1000
	r.Present |= schema.HasBtime | schema.HasAtime
}
