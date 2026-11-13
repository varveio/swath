// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

// SetMaxBlocks lowers the block limit for a test; it returns the restore.
func SetMaxBlocks(n int) func() {
	old := maxBlocks
	maxBlocks = n
	return func() { maxBlocks = old }
}
