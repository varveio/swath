// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package journal

import (
	"os"

	"golang.org/x/sys/windows"
)

// LockEnforced reports whether the output lock holds here.
var LockEnforced = true // a variable for tests

// lockFile takes an exclusive lock on the lock file's first byte, failing
// at once if another handle holds it; Windows releases it when the handle
// closes, so a killed run never leaves a stale lock.
func lockFile(f *os.File) error {
	return windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, new(windows.Overlapped))
}

// unlockFile releases the lock before the handle closes: Windows releases
// a closed handle's locks only when its resources allow, so an explicit
// unlock frees the output for the next run at once.
func unlockFile(f *os.File) {
	windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, new(windows.Overlapped))
}
