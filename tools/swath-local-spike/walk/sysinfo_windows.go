// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package walk

import (
	"io/fs"
	"syscall"

	"github.com/varveio/swath/tools/swath-local-spike/record"
)

// fillSys uses Win32FileAttributeData: CreationTime is a birth time (Windows
// has no POSIX ctime through FileInfo), LastAccessTime is atime. uid, gid,
// inode, dev, nlink and blocks stay null.
func fillSys(r *record.Row, fi fs.FileInfo) {
	d, ok := fi.Sys().(*syscall.Win32FileAttributeData)
	if !ok {
		return
	}
	r.Btime = d.CreationTime.Nanoseconds() / 1000
	r.Atime = d.LastAccessTime.Nanoseconds() / 1000
	r.Present |= record.HasBtime | record.HasAtime
}
