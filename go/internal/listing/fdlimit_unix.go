// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package listing

import "syscall"

// fdLimit is the process's soft descriptor limit (0: unknown).
func fdLimit() int64 {
	var r syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &r); err != nil {
		return 0
	}
	return int64(r.Cur) //nolint:unconvert // field types differ by platform
}
