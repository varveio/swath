// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package dataset

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/windows"
)

// replaceRetry bounds the retries of renameFile; a variable for tests.
var replaceRetry = 2 * time.Second

// renameFile renames old to new, replacing new: MoveFileEx with
// MOVEFILE_REPLACE_EXISTING, as os.Rename, and MOVEFILE_WRITE_THROUGH, so
// the call returns once the move is on disk; the directory sync that
// follows every rename the durability contract relies on flushes the
// entries too. Windows documents no atomicity beyond the replacement, so a
// reader may see no file for an instant. Windows refuses the replacement
// while another handle holds the target open without delete sharing (a
// reader of the report, a concurrent replacement of the same path that has
// not finished): the rename fails with ERROR_ACCESS_DENIED or
// ERROR_SHARING_VIOLATION and nothing has changed. Such a failure is
// retried with backoff until replaceRetry has passed (checked before every
// attempt), then returned.
func renameFile(old, new string) error {
	from, err := movePath(old)
	if err != nil {
		return &os.LinkError{Op: "rename", Old: old, New: new, Err: err}
	}
	to, err := movePath(new)
	if err != nil {
		return &os.LinkError{Op: "rename", Old: old, New: new, Err: err}
	}
	deadline := time.Now().Add(replaceRetry)
	wait := time.Millisecond
	for {
		err := windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
		if err == nil {
			return nil
		}
		if !(errors.Is(err, windows.ERROR_ACCESS_DENIED) || errors.Is(err, windows.ERROR_SHARING_VIOLATION)) || !time.Now().Add(wait).Before(deadline) {
			return &os.LinkError{Op: "rename", Old: old, New: new, Err: err}
		}
		time.Sleep(wait)
		wait = min(2*wait, 100*time.Millisecond)
	}
}

// movePath is p as MoveFileExW takes it: as given while its absolute
// form fits in MAX_PATH, else absolute under the \\?\ prefix, as package
// os does for its own calls.
func movePath(p string) (*uint16, error) {
	if a, err := filepath.Abs(p); err == nil && len(a) >= windows.MAX_PATH-12 && !strings.HasPrefix(a, `\\?\`) {
		if strings.HasPrefix(a, `\\`) {
			p = `\\?\UNC\` + a[2:]
		} else {
			p = `\\?\` + a
		}
	}
	return windows.UTF16PtrFromString(p)
}
