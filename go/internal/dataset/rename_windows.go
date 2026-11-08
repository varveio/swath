// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package dataset

import (
	"errors"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

// replaceRetry bounds the retries of replaceFile; a variable for tests.
var replaceRetry = 2 * time.Second

// replaceFile renames tmp over path. Go's os.Rename on Windows replaces an
// existing file (MoveFileEx with MOVEFILE_REPLACE_EXISTING); Go documents
// no atomicity there beyond that, so a reader may see no file for an
// instant. Windows refuses the replacement while another handle holds the
// target open without delete sharing (a reader of the report, a concurrent
// replacement of the same path that has not finished): the rename fails
// with ERROR_ACCESS_DENIED or ERROR_SHARING_VIOLATION and nothing has
// changed. Such a failure is retried with backoff until replaceRetry has
// passed (checked before every attempt), then returned.
func replaceFile(tmp, path string) error {
	deadline := time.Now().Add(replaceRetry)
	wait := time.Millisecond
	for {
		err := os.Rename(tmp, path)
		if err == nil || !(errors.Is(err, windows.ERROR_ACCESS_DENIED) || errors.Is(err, windows.ERROR_SHARING_VIOLATION)) {
			return err
		}
		if !time.Now().Add(wait).Before(deadline) {
			return err
		}
		time.Sleep(wait)
		wait = min(2*wait, 100*time.Millisecond)
	}
}
