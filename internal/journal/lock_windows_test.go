// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package journal

import (
	"errors"
	"testing"
)

// TestWindowsLock: LockFileEx excludes a second owner, and Release frees
// the output at once for the next run. Compiled by TestWindowsBuild; runs
// only on Windows.
func TestWindowsLock(t *testing.T) {
	out := t.TempDir()
	lock, err := TakeLock(out)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := TakeLock(out); !errors.Is(err, ErrLocked) {
		t.Fatalf("second owner: %v", err)
	}
	lock.Release()
	again, err := TakeLock(out)
	if err != nil {
		t.Fatalf("after release: %v", err)
	}
	again.Release()
}
