// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package sortedparquet

import "syscall"

// DefaultMaxOpenFiles is half the process's soft descriptor limit, leaving
// the other half to the server's connections and everything else, at
// least 64 and at most 4,096 (more open parts buy nothing: a read touches
// one part at a time).
func DefaultMaxOpenFiles() int {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return 64
	}
	return int(min(max(uint64(limit.Cur)/2, 64), 4096))
}
