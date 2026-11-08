// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build darwin

package listing

import (
	"syscall"
	"time"
)

// rusage is the process's CPU times and peak resident set (getrusage; on
// macOS ru_maxrss is in bytes).
func rusage() (user, sys time.Duration, peak int64, ok bool) {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return 0, 0, 0, false
	}
	return time.Duration(ru.Utime.Nano()), time.Duration(ru.Stime.Nano()), int64(ru.Maxrss), true
}

// residentBytes is the resident set now; not measured on this platform
// (the history leaves it out).
func residentBytes() (int64, bool) { return 0, false }
