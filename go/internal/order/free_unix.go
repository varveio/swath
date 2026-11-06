// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package order

import "syscall"

// freeBytes is the space available to an unprivileged writer on dir's
// filesystem; on error it reports unlimited (the cap and ENOSPC still hold).
func freeBytes(dir string) int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 1<<63 - 1
	}
	return int64(uint64(st.Bavail) * uint64(st.Bsize)) //nolint:unconvert // field types differ by platform
}
