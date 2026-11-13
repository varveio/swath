// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build darwin

package localfs

import (
	"syscall"
	"time"

	"github.com/varveio/swath/internal/source"
)

// measureSpace: statfs(2) counts blocks of f_bsize, the fundamental block
// size (f_iosize is the preferred I/O size).
func measureSpace(root string, _ bool) *source.FilesystemSpace {
	var st syscall.Statfs_t
	if err := syscall.Statfs(root, &st); err != nil {
		return nil
	}
	return blockSpace(st.Blocks, st.Bfree, st.Bavail, int64(st.Bsize), time.Now())
}
