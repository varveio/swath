// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package sortab

import (
	"os"
	"syscall"
)

// usage fills the child's CPU times and peak RSS from its rusage.
func usage(st *os.ProcessState, at *Attempt) {
	if ru, ok := st.SysUsage().(*syscall.Rusage); ok {
		at.UserMs = ru.Utime.Sec*1000 + int64(ru.Utime.Usec)/1000
		at.SysMs = ru.Stime.Sec*1000 + int64(ru.Stime.Usec)/1000
		at.RuMaxRSS = ru.Maxrss
	}
}
