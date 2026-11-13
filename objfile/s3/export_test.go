// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

// SetMaxParts lowers the part limit for a test; it returns the restore.
func SetMaxParts(n int) func() {
	old := maxParts
	maxParts = n
	return func() { maxParts = old }
}
