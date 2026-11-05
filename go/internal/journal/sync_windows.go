// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package journal

// syncDir is a no-op on Windows, where directories cannot be fsynced.
func syncDir(string) error { return nil }
