// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package localfs

import (
	"syscall"
	"time"

	"github.com/varveio/swath/go/internal/source"
)

// measureSpace: statfs(2) counts blocks in fragment units (f_frsize);
// f_bsize is the preferred I/O size, which differs on NFS. A kernel that
// leaves f_frsize 0 counts in f_bsize.
func measureSpace(root string, _ bool) *source.FilesystemSpace {
	var st syscall.Statfs_t
	if err := syscall.Statfs(root, &st); err != nil {
		return nil
	}
	unit := int64(st.Frsize) //nolint:unconvert // field types differ by architecture
	if unit == 0 {
		unit = int64(st.Bsize) //nolint:unconvert // field types differ by architecture
	}
	return blockSpace(st.Blocks, st.Bfree, st.Bavail, unit, time.Now())
}
