// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package journal

import (
	"os"
	"syscall"
)

// LockEnforced reports whether the output lock holds here.
var LockEnforced = true // a variable for tests

// lockFile takes an exclusive advisory lock; the kernel releases it when
// the process dies, so a killed run never leaves a stale lock.
func lockFile(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) }

// unlockFile: closing the descriptor releases the lock at once.
func unlockFile(*os.File) {}
