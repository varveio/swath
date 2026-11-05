// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package journal

import (
	"os"
	"syscall"
)

// lockFile takes an exclusive advisory lock; the kernel releases it when
// the process dies, so a killed run never leaves a stale lock.
// Enforced reports whether the output lock and directory syncs hold here.
const Enforced = true

func lockFile(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) }
