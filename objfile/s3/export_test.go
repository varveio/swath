// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import "github.com/varveio/swath/objfile"

// SetMaxParts lowers the part limit for a test; it returns the restore.
func SetMaxParts(n int) func() {
	old := maxParts
	maxParts = n
	return func() { maxParts = old }
}

// Cancelled says an upload of NewWriter's has been cancelled (Abort's
// first step).
func Cancelled(u objfile.Upload) bool { return u.(*upload).ctx.Err() != nil }
