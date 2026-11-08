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

// replaceFile renames tmp over path (an atomic replacement). Windows
// refuses to replace a file while another process or writer holds it
// open without delete sharing (a reader of the report, a concurrent
// replacement of the same path that has not finished): the rename fails
// with ERROR_ACCESS_DENIED or ERROR_SHARING_VIOLATION and nothing has
// changed. It is retried with backoff for up to two seconds, then the
// error is returned.
func replaceFile(tmp, path string) error {
	deadline := time.Now().Add(2 * time.Second)
	for wait := time.Millisecond; ; wait = min(2*wait, 100*time.Millisecond) {
		err := os.Rename(tmp, path)
		if err == nil || !(errors.Is(err, windows.ERROR_ACCESS_DENIED) || errors.Is(err, windows.ERROR_SHARING_VIOLATION)) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(wait)
	}
}
